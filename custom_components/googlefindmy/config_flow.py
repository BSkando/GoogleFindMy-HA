# custom_components/googlefindmy/config_flow.py
"""Config flow for the Google Find My Device custom integration.

This module implements the complete configuration and options flows for the
integration, following Home Assistant best practices:

Key design decisions (Best Practice):
- Test-before-configure: We validate credentials *before* creating a config entry.
  If validation fails, no entry is created, the form is shown again with an error.
- Early unique_id: We set the config entry unique ID (normalized Google email)
  as soon as it is known, to avoid duplicate flows and duplicate entries.
- No persistence during the flow: We never write tokens/secrets to disk during
  the flow. All flow-time validation uses ephemeral clients only.
- No irreversible cleanup inside a flow step, on *any* path. `async_create_entry`
  only *builds* a FlowResult; Home Assistant stores the entry afterwards in
  `ConfigEntriesFlowManager.async_finish_flow`. And `async_update_entry` only
  mutates the in-memory entry and schedules Home Assistant's *debounced* store
  save (`ConfigEntries._async_save_and_notify` -> `_async_schedule_save` ->
  `Store.async_delay_save(SAVE_DELAY)`); it does not commit before returning.
  Deleting an imported secrets file is therefore always staged in memory (`hass.data[DOMAIN]["pending_container_cleanup"]`, one
  ticket per flow) and handed to a durability gate by `async_setup_entry`, which
  executes the jobs only once the *relevant* state has provably reached Home
  Assistant's storage. The create case waits for the entry to appear at all; the
  paths that update an *existing* entry (discovery-update, reconfigure, reauth
  and options) additionally pin a `modified_at` watermark, because for them the
  entry id alone was already in storage before the update and would prove
  nothing. Every update path schedules a reload of that entry, so the gate is
  re-armed by the very `async_setup_entry` run that reload triggers.
- Duplicate protection: If a config entry for the same Google account already
  exists, we abort the flow using `_abort_if_unique_id_configured()`.
- Guard handling: If the API raises a "multiple config entries" guard (e.g.,
  "Multiple config entries active" / "... pass entry.runtime_data"), we accept
  the candidate and *defer* validation to setup, where an entry-scoped cache
  exists. We do *not* skip online validation in general.
- Defensive API calls: We support multiple call signatures for the basic
  device-list probe and map likely exceptions to HA-standard error keys
  (`invalid_auth`, `cannot_connect`, `unknown`) without leaking sensitive data.

Security & privacy:
- No secrets in logs or exceptions; messages are redacted and bounded.
- No secrets are written to disk or to Home Assistant storage by the flow. The
  staged cleanup jobs live in `hass.data` only (in-memory): they name a file to
  delete, never a credential.
- Email addresses are normalized (lowercased) before being used as unique IDs.

Docstring & comments:
- All docstrings and inline comments are written in English.
"""

# custom_components/googlefindmy/config_flow.py

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import re
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Container, Mapping, MutableMapping
from collections.abc import Iterable as CollIterable
from collections.abc import Mapping as CollMapping
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Final,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
)
from uuid import uuid4

import voluptuous as vol
from homeassistant import config_entries, data_entry_flow
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.storage import Store

try:
    from homeassistant.config_entries import (
        ConfigEntry,
        OperationNotAllowed,
    )
except ImportError:  # pragma: no cover - pre-2025.5 builds are below our floor
    # Unreachable on every supported core: hacs.json declares 2025.9.1, and
    # ``OperationNotAllowed`` has been exported since 2025.5. Kept as a landing
    # pad rather than removed, and marked instead of tested because reaching it
    # would mean importing a core we do not support. Note the asymmetry: only
    # this helper is optional. ``ConfigEntry`` is a hard requirement here, so its
    # disappearance is meant to fail loudly at import time rather than degrade
    # quietly. ``ConfigEntryState`` is no longer imported here at all: the state
    # comparisons moved to ``entry_reload_gate``, which binds the submodule form
    # itself, and a second binding in this module would be a second module world
    # for the same enum.
    from homeassistant.config_entries import ConfigEntry

    OperationNotAllowed = type("OperationNotAllowed", (HomeAssistantError,), {})

from .const import (
    CONF_GOOGLE_EMAIL,
    CONF_OAUTH_TOKEN,
    CONFIG_ENTRY_VERSION,
    CONTRIBUTOR_MODE_HIGH_TRAFFIC,
    CONTRIBUTOR_MODE_IN_ALL_AREAS,
    DATA_AAS_TOKEN,
    DATA_AUTH_METHOD,
    DATA_SECRET_BUNDLE,
    DATA_SUBENTRY_KEY,
    DEFAULT_ACCURACY_GATE_ENABLED,
    DEFAULT_CONTRIBUTOR_MODE,
    DEFAULT_DELETE_CACHES_ON_REMOVE,
    DEFAULT_DEVICE_POLL_DELAY,
    DEFAULT_ENABLE_STATS_ENTITIES,
    # Defaults
    DEFAULT_LOCATION_POLL_INTERVAL,
    DEFAULT_MAP_VIEW_ENABLED,
    DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
    DEFAULT_OPTIONS,
    DEFAULT_ROUNDTRIP_CONFIRM,
    DEFAULT_SEMANTIC_DETECTION_RADIUS,
    DEFAULT_SHOW_LOCATION_AGE,
    DEFAULT_SPEED_GATE_ENABLED,
    DEFAULT_STALE_THRESHOLD,
    # Core domain & credential keys
    DOMAIN,
    LITERAL_CORE_KEY_OWNER,
    NON_DEVICE_SUBENTRY_KEYS,
    NON_DEVICE_SUBENTRY_TYPES,
    OPT_ACCURACY_GATE_ENABLED,
    OPT_CONTRIBUTOR_MODE,
    OPT_DELETE_CACHES_ON_REMOVE,
    OPT_DEVICE_POLL_DELAY,
    OPT_ENABLE_STATS_ENTITIES,
    OPT_IGNORED_DEVICES,
    # Options (non-secret runtime settings)
    OPT_LOCATION_POLL_INTERVAL,
    OPT_MAP_VIEW_ENABLED,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,
    OPT_OPTIONS_SCHEMA_VERSION,
    OPT_ROUNDTRIP_CONFIRM,
    OPT_SEMANTIC_LOCATIONS,
    OPT_SHOW_LOCATION_AGE,
    OPT_SPEED_GATE_ENABLED,
    OPT_STALE_THRESHOLD,
    OPTION_KEYS,
    OPTIONAL_CREDENTIAL_KEYS,
    SECRETS_EXTRA_WATCH_PATHS,
    SERVICE_FEATURE_PLATFORMS,
    SERVICE_SUBENTRY_KEY,
    SERVICE_SUBENTRY_TRANSLATION_KEY,
    SUBENTRY_TYPE_HUB,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_FEATURE_PLATFORMS,
    TRACKER_SUBENTRY_KEY,
    TRACKER_SUBENTRY_TRANSLATION_KEY,
    coerce_ignored_mapping,
    service_device_identifier,
)
from .email_utils import normalize_email, normalize_email_or_default, unique_account_id
from .entry_reload_gate import (
    entry_reload_is_hopeless as _entry_reload_is_hopeless,
)
from .entry_reload_gate import (
    falsy_reload_left_the_latch_behind as _falsy_reload_left_the_latch_behind,
)
from .integration_modules import (
    import_integration_api_module,
    import_integration_package,
)
from .shared_helpers import normalize_secrets_bundle

_ResolveEntryEmailCallable = Callable[[ConfigEntry], tuple[str | None, str | None]]
_CoalesceCallable = Callable[
    [HomeAssistant, ConfigEntry],
    Awaitable[ConfigEntry | None],
]

_RESOLVE_ENTRY_EMAIL: _ResolveEntryEmailCallable | None = None
_COALESCE_ENTRIES: _CoalesceCallable | None = None

if TYPE_CHECKING:
    from .api import GoogleFindMyAPI
    from .discovery import _SecretsScanResult


class _SubentryManagerProto(Protocol):
    """Protocol for the subentry manager to support strict typing."""

    managed_subentries: Mapping[str, Any]

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None: ...


_LOGGER = logging.getLogger(__name__)


try:
    SOURCE_DISCOVERY = config_entries.SOURCE_DISCOVERY
except AttributeError as err:  # pragma: no cover - configuration critical
    _LOGGER.exception(
        "Critical import failure: SOURCE_DISCOVERY not available: %s",
        err,
    )
    raise

SOURCE_RECONFIGURE = getattr(config_entries, "SOURCE_RECONFIGURE", "reconfigure")

DiscoveryKey: type[Any]
try:  # pragma: no cover - runtime optional dependency
    DiscoveryKey = cast(type[Any], getattr(config_entries, "DiscoveryKey"))
except AttributeError:
    try:  # pragma: no cover - runtime optional dependency
        from homeassistant.helpers.discovery_flow import DiscoveryKey as _DiscoveryKey
    except Exception:  # noqa: BLE001

        @dataclass(slots=True)
        class _FallbackDiscoveryKey:
            """Fallback DiscoveryKey representation for legacy cores."""

            domain: str
            key: str | tuple[str, ...]
            version: int = 1

        DiscoveryKey = cast(type[Any], _FallbackDiscoveryKey)
    else:  # pragma: no cover - simple aliasing
        DiscoveryKey = cast(type[Any], _DiscoveryKey)


class _DiscoveryFlowHelper(Protocol):
    """Callable contract for discovery flow helpers."""

    def __call__(
        self,
        hass: HomeAssistant,
        domain: str,
        context: Mapping[str, Any] | None,
        data: Mapping[str, Any],
        *,
        discovery_key: Any | None = ...,
    ) -> Awaitable[FlowResult | None] | FlowResult | None:
        """Invoke the discovery flow helper."""


_MaybeFlowResult: TypeAlias = FlowResult | None
_AwaitableFlowResult: TypeAlias = Awaitable[_MaybeFlowResult] | _MaybeFlowResult


async def _resolve_flow_result(result: _AwaitableFlowResult) -> _MaybeFlowResult:
    """Await helper results when necessary."""

    if inspect.isawaitable(result):
        awaited_result: _MaybeFlowResult = await result
        return awaited_result
    return result


class _DiscoveryFallbackUnavailable(RuntimeError):
    """Raised when the discovery flow helper cannot provide a fallback."""


_discovery_flow_helper = cast(
    _DiscoveryFlowHelper | None,
    getattr(
        config_entries,
        "async_create_discovery_flow",
        None,
    ),
)

_fallback_discovery_flow_helper: _DiscoveryFlowHelper | None

# NOT a legacy branch, despite its "fallback" name: Home Assistant does not
# export ``async_create_discovery_flow`` from ``homeassistant.config_entries``
# (verified against 2026.2.3), so ``_discovery_flow_helper`` is ``None`` and the
# body below is the path every real discovery takes. It used to carry a
# ``# pragma: no cover - legacy fallback``, which excluded the production path
# from coverage and let a wrong abort synthesis inside it go unnoticed; the
# pragma is deliberately gone. The ``else`` branch is the one that is currently
# unreachable, and it is kept because the export may return.
if _discovery_flow_helper is None:

    async def _async_create_discovery_flow(
        hass: HomeAssistant,
        domain: str,
        context: Mapping[str, Any] | None,
        data: Mapping[str, Any],
        *,
        discovery_key: Any | None = None,
    ) -> FlowResult:
        """Fallback helper mirroring modern discovery flow creation."""

        create_flow_helper: Callable[..., Awaitable[FlowResult] | FlowResult] | None = (
            None
        )

        module = sys.modules.get(__name__)
        if module is not None:
            candidate = getattr(module, "async_create_discovery_flow", None)
            if callable(candidate):
                create_flow_helper = candidate

        if create_flow_helper is not None:
            try:
                result = await create_flow_helper(
                    hass,
                    domain,
                    context=context,
                    data=data,
                    discovery_key=discovery_key,
                    _skip_fallback=True,
                )
            except _DiscoveryFallbackUnavailable:
                create_flow_helper = None
            except Exception:  # noqa: BLE001
                _LOGGER.error(
                    "Discovery flow helper invocation failed (domain=%s, context=%s)",
                    domain,
                    context,
                    exc_info=True,
                )
                return cast(
                    FlowResult,
                    {
                        "type": data_entry_flow.FlowResultType.ABORT,
                        "reason": "unknown",
                    },
                )
            else:
                if result is None:
                    _LOGGER.error(
                        "Discovery flow helper returned None (domain=%s, context=%s)",
                        domain,
                        context,
                    )
                    return cast(
                        FlowResult,
                        {
                            "type": data_entry_flow.FlowResultType.ABORT,
                            "reason": "unknown",
                        },
                    )
                return cast(FlowResult, result)

        # The modern production path used to defer to
        # ``homeassistant.helpers.discovery_flow.async_create_flow``. That
        # helper is *fire-and-forget*: it is declared ``-> None`` and dispatches
        # the flow through ``async_create_background_task``, so its ``None``
        # means "a flow was scheduled", never "the import succeeded". A flow
        # that later aborted transiently (``cannot_connect``/``invalid_auth``/
        # ``unknown``) stayed invisible here, so ``discovery`` classified the
        # creation as ACCEPTED and ``SecretsJSONWatcher`` settled the bundle
        # signature for good — the user then had to rewrite ``secrets.json`` or
        # restart Home Assistant to recover.
        #
        # We therefore reproduce the helper's *observable* core ourselves and
        # ``await`` ``flow.async_init`` directly, exactly as
        # ``helpers.discovery_flow._async_init_flow`` does, so the real
        # ``FlowResult`` reaches ``discovery._classify_discovery_flow_result``:
        # a transient abort becomes ``RETRY`` and re-arms the producer instead
        # of vanishing. ``already_in_progress`` is now synthesized ONLY for the
        # two conditions under which HA's own helper returns ``None`` (a
        # matching in-progress flow already owns the payload, or the core is
        # stopping), not for every creation.
        flow_manager = cast(
            "ConfigEntriesFlowManager",
            getattr(hass.config_entries, "flow", None),
        )
        init = getattr(flow_manager, "async_init", None)
        if not callable(init):
            _LOGGER.error(
                "Discovery flow manager exposes no async_init (domain=%s, context=%s)",
                domain,
                context,
            )
            return cast(
                FlowResult,
                {
                    "type": data_entry_flow.FlowResultType.ABORT,
                    "reason": "unknown",
                },
            )

        # discovery_key parity: HA's helper merges it into the context before
        # creating the flow (``context | {"discovery_key": discovery_key}``), so
        # Home Assistant's rediscovery/ignore bookkeeping keys off the same
        # value and the matcher below sees the same context HA would.
        merged_context: dict[str, Any] = dict(context or {})
        if discovery_key:
            merged_context["discovery_key"] = discovery_key

        # Dedup / shutdown parity with ``helpers.discovery_flow._async_init_flow``:
        # it returns ``None`` (no flow) when a matching discovery flow already
        # owns this payload or when the core is stopping. Both are legitimate,
        # non-retryable end states, so — and ONLY for them — synthesize the
        # terminal ``already_in_progress`` reason. The matcher is a synchronous
        # ``@callback`` on the flow manager; guard it for stripped test cores.
        matcher = getattr(flow_manager, "async_has_matching_discovery_flow", None)
        matching = False
        if callable(matcher):
            try:
                matching = bool(matcher(domain, merged_context, data))
            except Exception:  # noqa: BLE001 - a matcher failure must not abort
                _LOGGER.debug(
                    "async_has_matching_discovery_flow raised (domain=%s); "
                    "assuming no match",
                    domain,
                    exc_info=True,
                )
        if matching or bool(getattr(hass, "is_stopping", False)):
            _LOGGER.debug(
                "Discovery flow already owned or core stopping "
                "(domain=%s, context=%s, matching=%s) — already in progress",
                domain,
                context,
                matching,
            )
            return cast(
                FlowResult,
                {
                    "type": data_entry_flow.FlowResultType.ABORT,
                    "reason": "already_in_progress",
                },
            )

        try:
            init_result = await init(
                domain,
                context=merged_context,
                data=data,
            )
        except Exception:
            _LOGGER.error(
                "Discovery flow init failed (domain=%s, context=%s)",
                domain,
                context,
                exc_info=True,
            )
            return cast(
                FlowResult,
                {
                    "type": data_entry_flow.FlowResultType.ABORT,
                    "reason": "unknown",
                },
            )
        if init_result is None:
            # ``flow.async_init`` returns a FlowResult; a ``None`` here is an
            # anomaly, not "already in progress". Treat it as transient so the
            # watcher retries rather than settling on an unverified import.
            _LOGGER.error(
                "Discovery flow init returned None (domain=%s, context=%s)",
                domain,
                context,
            )
            return cast(
                FlowResult,
                {
                    "type": data_entry_flow.FlowResultType.ABORT,
                    "reason": "unknown",
                },
            )
        return cast(FlowResult, init_result)

    _fallback_discovery_flow_helper = cast(
        _DiscoveryFlowHelper,
        _async_create_discovery_flow,
    )
else:
    _fallback_discovery_flow_helper = None


async def async_create_discovery_flow(
    hass: HomeAssistant,
    domain: str,
    context: Mapping[str, Any] | None,
    data: Mapping[str, Any],
    *,
    discovery_key: Any | None = None,
    _skip_fallback: bool = False,
) -> FlowResult:
    """Proxy helper that tolerates runtime helper resolution failures."""

    helper_candidate = getattr(config_entries, "async_create_discovery_flow", None)
    if callable(helper_candidate):
        helper = cast(_DiscoveryFlowHelper, helper_candidate)
        try:
            result = helper(
                hass,
                domain,
                context,
                data,
                discovery_key=discovery_key,
            )
        except (AttributeError, NotImplementedError):
            exc_type, _, _ = sys.exc_info()
            exc_label = exc_type.__name__ if exc_type else "RuntimeError"
            _LOGGER.debug(
                "Discovery flow helper raised %s (domain=%s, context=%s); falling back",
                exc_label,
                domain,
                context,
                exc_info=True,
            )
        except Exception:  # noqa: BLE001 - surface unexpected failures to callers
            _LOGGER.error(
                "Discovery flow helper raised unexpectedly (domain=%s, context=%s)",
                domain,
                context,
                exc_info=True,
            )
            raise
        else:
            try:
                resolved = await _resolve_flow_result(result)
            except (AttributeError, NotImplementedError):
                exc_type, _, _ = sys.exc_info()
                exc_label = exc_type.__name__ if exc_type else "RuntimeError"
                _LOGGER.debug(
                    "Discovery flow helper raised %s while awaiting result (domain=%s, context=%s); falling back",
                    exc_label,
                    domain,
                    context,
                    exc_info=True,
                )
            except Exception:  # noqa: BLE001 - surface unexpected failures to callers
                _LOGGER.error(
                    "Discovery flow helper raised unexpectedly during await (domain=%s, context=%s)",
                    domain,
                    context,
                    exc_info=True,
                )
                raise
            else:
                if resolved is not None:
                    return cast(FlowResult, resolved)
                # Dormant sibling of the observable fix: this branch only runs if
                # Home Assistant re-exports
                # ``config_entries.async_create_discovery_flow`` (it does not on
                # current cores — the test suite asserts this). That helper is
                # Home Assistant's own; we cannot observe its background flow, so
                # a None here still maps to the terminal ``already_in_progress``.
                # If HA ever ships a fire-and-forget export, this default must be
                # revisited (tracked as a follow-up, not this PR's defect).
                _LOGGER.debug(
                    "Exported discovery flow helper returned None (domain=%s, context=%s) — treating as already in progress",
                    domain,
                    context,
                )
                return cast(
                    FlowResult,
                    {
                        "type": data_entry_flow.FlowResultType.ABORT,
                        "reason": "already_in_progress",
                    },
                )

    fallback_helper = _fallback_discovery_flow_helper
    if fallback_helper is None:
        module = sys.modules.get(__name__)
        if module is not None:
            fallback_helper = cast(
                _DiscoveryFlowHelper | None,
                getattr(module, "_async_create_discovery_flow", None),
            )

    if fallback_helper is not None:
        if _skip_fallback:
            raise _DiscoveryFallbackUnavailable
        fallback_result = await _resolve_flow_result(
            fallback_helper(
                hass,
                domain,
                context,
                data,
                discovery_key=discovery_key,
            )
        )
        # The observable fallback ``_async_create_discovery_flow`` always yields
        # a real FlowResult or a classified abort — it no longer returns None —
        # so the ``None`` -> ``already_in_progress`` synthesis that used to live
        # here is gone (the Codex-flagged pattern is removed from this site too).
        # A None would now be a future anomaly; fail safe to ``unknown`` (the
        # watcher then retries) rather than settling on an unverified import.
        if (
            fallback_result is None
        ):  # pragma: no cover - unreachable after the observable fix
            return cast(
                FlowResult,
                {
                    "type": data_entry_flow.FlowResultType.ABORT,
                    "reason": "unknown",
                },
            )
        return cast(FlowResult, fallback_result)

    _LOGGER.debug(
        "Discovery flow helper unavailable; aborting flow creation (domain=%s, context=%s)",
        domain,
        context,
    )
    return cast(
        FlowResult,
        {
            "type": data_entry_flow.FlowResultType.ABORT,
            "reason": "unknown",
        },
    )


_FALLBACK_CONFIG_SUBENTRY_FLOW: type[Any] | None = None

try:  # pragma: no cover - compatibility shim for stripped environments
    from homeassistant.config_entries import (
        ConfigSubentry,
        ConfigSubentryFlow,
        SubentryFlowResult,
    )
    from homeassistant.helpers.typing import UNDEFINED, UndefinedType
except ImportError:
    try:  # pragma: no cover - best-effort partial import
        from homeassistant.config_entries import ConfigSubentry as _ConfigSubentry
    except ImportError:
        ConfigSubentry = None
    else:
        ConfigSubentry = _ConfigSubentry

    try:
        from homeassistant.helpers.typing import UNDEFINED, UndefinedType
    except ImportError:

        class _UndefinedType:
            """Fallback sentinel for legacy Home Assistant builds."""

            def __repr__(self) -> str:
                return "UNDEFINED"

        UNDEFINED = _UndefinedType()
        UndefinedType = type(UNDEFINED)

    SubentryFlowResult = FlowResult

    class _FallbackConfigSubentryFlow:
        """Fallback stub for Home Assistant's ConfigSubentryFlow."""

        def __init__(self, config_entry: ConfigEntry) -> None:
            self.config_entry = config_entry
            self.subentry: ConfigSubentry | None = None

        async def async_step_user(
            self, user_input: dict[str, Any] | None = None
        ) -> FlowResult:
            raise NotImplementedError

        async def async_step_reconfigure(
            self, user_input: dict[str, Any] | None = None
        ) -> FlowResult:
            raise NotImplementedError

        def async_create_entry(self, *, title: str, data: dict[str, Any]) -> FlowResult:
            return {
                "type": "create_entry",
                "title": title,
                "data": data,
            }

        def async_update_and_abort(
            self,
            entry: ConfigEntry,
            subentry: ConfigSubentry,
            *,
            unique_id: str | None | UndefinedType = UNDEFINED,
            title: str | UndefinedType = UNDEFINED,
            data: Mapping[str, Any] | UndefinedType = UNDEFINED,
            data_updates: Mapping[str, Any] | UndefinedType = UNDEFINED,
        ) -> SubentryFlowResult:
            merged_data: dict[str, Any] = {}

            if data is not UNDEFINED and data is not None:
                merged_data.update(data)

            if data_updates is not UNDEFINED and data_updates is not None:
                merged_data.update(data_updates)

            if merged_data:
                setattr(subentry, "data", merged_data)

            if title is not UNDEFINED:
                setattr(subentry, "title", title)

            if unique_id is not UNDEFINED:
                setattr(subentry, "unique_id", unique_id)

            self.config_entry = entry
            self.subentry = subentry

            return {
                "type": data_entry_flow.FlowResultType.ABORT,
                "reason": "reconfigure_successful",
                "data": merged_data or None,
                "title": None if title is UNDEFINED else title,
                "unique_id": None if unique_id is UNDEFINED else unique_id,
            }

    ConfigSubentryFlow = _FallbackConfigSubentryFlow
    _FALLBACK_CONFIG_SUBENTRY_FLOW = _FallbackConfigSubentryFlow
else:
    _FALLBACK_CONFIG_SUBENTRY_FLOW = None

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntriesFlowManager

if TYPE_CHECKING:
    HomeAssistantErrorBase = Exception
else:
    HomeAssistantErrorBase = HomeAssistantError


class DependencyNotReady(HomeAssistantErrorBase):
    """Raised when integration dependencies are unavailable."""


def _register_dependency_error(
    errors: dict[str, str],
    err: Exception,
    *,
    field: str = "base",
) -> None:
    """Record an import-related dependency error for the current form."""

    if field not in errors:
        _LOGGER.error("Failed to import Google Find My dependencies: %s", err)
        errors[field] = "import_failed"


@lru_cache(maxsize=1)
def _import_api() -> type[GoogleFindMyAPI]:
    """Import the API lazily so config flows load without optional deps."""

    try:
        module = import_integration_api_module()
    except ImportError as err:  # pragma: no cover - exercised via tests
        raise DependencyNotReady(
            "Google Find My Device dependencies are not installed."
        ) from err

    api_cls = getattr(module, "GoogleFindMyAPI", None)
    if api_cls is None:
        raise DependencyNotReady("GoogleFindMyAPI is unavailable in googlefindmy.api.")

    return cast(type["GoogleFindMyAPI"], api_cls)


async def _async_import_api(hass: HomeAssistant) -> type[GoogleFindMyAPI]:
    """Import the API in an executor to avoid blocking the event loop."""

    executor = getattr(hass, "async_add_executor_job", None)
    if not callable(executor):
        return _import_api()
    return cast(type["GoogleFindMyAPI"], await executor(_import_api))


# Optional network exception typing (robust mapping without hard dependency)
aiohttp: ModuleType | None
try:  # pragma: no cover - environment dependent
    import aiohttp as _aiohttp_mod

    aiohttp = _aiohttp_mod
except Exception:  # noqa: BLE001
    aiohttp = None

# Selector is not guaranteed in older cores; import defensively.
selector: Callable[[Mapping[str, Any]], Any] | None
try:  # pragma: no cover - environment dependent
    from homeassistant.helpers.selector import selector as _selector
except Exception:  # noqa: BLE001
    selector = None
else:
    selector = cast(Callable[[Mapping[str, Any]], Any], _selector)

# Standard discovery update info source exposed for helper-triggered updates.
DISCOVERY_UPDATE_SOURCE = "discovery_update_info"
LEGACY_DISCOVERY_UPDATE_SOURCE = "discovery_update"

OPT_GOOGLE_HOME_FILTER_ENABLED: str | None
OPT_GOOGLE_HOME_FILTER_KEYWORDS: str | None
DEFAULT_GOOGLE_HOME_FILTER_ENABLED: bool | None
DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS: str | None
try:
    from .const import (
        DEFAULT_GOOGLE_HOME_FILTER_ENABLED,
        DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS,
        OPT_GOOGLE_HOME_FILTER_ENABLED,
        OPT_GOOGLE_HOME_FILTER_KEYWORDS,
    )
except Exception:  # noqa: BLE001
    OPT_GOOGLE_HOME_FILTER_ENABLED = None
    OPT_GOOGLE_HOME_FILTER_KEYWORDS = None
    DEFAULT_GOOGLE_HOME_FILTER_ENABLED = None
    DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS = None

# Optional UI helper for visibility menu
ignored_choices_for_ui: (
    Callable[[Mapping[str, Mapping[str, object]]], dict[str, str]] | None
)
try:
    from .const import ignored_choices_for_ui  # helper that formats UI choices
except Exception:  # noqa: BLE001
    ignored_choices_for_ui = None
# -----------------------------------------------------------------------------------

_CallbackT = TypeVar("_CallbackT", bound=Callable[..., Any])


def _typed_callback(func: _CallbackT) -> _CallbackT:
    """Return a callback decorator that preserves type information."""

    return cast(_CallbackT, callback(func))


def _is_discovery_update_info(
    context: Mapping[str, Any] | None,
) -> bool:
    """Return True if the flow context indicates a discovery-update-info source."""

    if not isinstance(context, CollMapping):
        return False

    source = context.get("source")
    return source in {DISCOVERY_UPDATE_SOURCE, LEGACY_DISCOVERY_UPDATE_SOURCE}


async def _async_delete_watched_secrets(
    hass: HomeAssistant | None,
    *,
    imported_stable_key: str | None = None,
    imported_digest: str | None = None,
) -> None:
    """Delete the watched secrets.json copies the import actually consumed.

    Mirrors the legacy ``Auth/secrets.json`` cleanup (``Auth/token_cache.py``):
    once the bundle has been persisted into the config entry the on-disk copies
    are transient secrets and must not linger.

    **When this runs.** Only from the durability gate that ``async_setup_entry``
    arms via :func:`async_schedule_pending_container_cleanup`, i.e. only after
    Home Assistant's storage has been *observed* to hold the state that
    authorises the deletion. A flow step may only ever *stage* the job. That
    holds for the update paths as much as for the create path: a ``CREATE_ENTRY``
    FlowResult is not yet an entry, and ``async_update_entry`` only schedules a
    debounced save, so neither has committed anything when the step returns.
    Never call this from a config-flow step.

    Two things make the deletion unsafe to do blindly, so the hook is both
    *account-aware* and *content-aware*:

    * :data:`SECRETS_EXTRA_WATCH_PATHS` may point at bundles of **different**
      Google accounts, and the account key of an identified bundle collapses to
      ``email:<addr>`` whenever an email is present.
    * A login container may write **fresher** credentials of the *same*
      account while the discovery flow is still waiting for the user's
      confirmation. Matching on the account key alone would then delete that
      newer bundle even though the entry only holds the older payload.

    Decision per watched file (``imported_digest`` is the content digest of the
    imported payload, :func:`discovery.secrets_bundle_digest`):

    - Account not determinable (missing, unreadable, unparseable, no email):
      kept, debug log. Nothing unidentified is ever destroyed.
    - Different account: kept, redacted warning (no plaintext account).
    - Same account **and** the file's own digest equals ``imported_digest``:
      removed. This is exactly the content that was consumed, so it is the only
      unconditionally safe delete; it covers the ``Auth/`` +
      ``docker-login/data/`` redundancy case where both copies are identical.
    - Same account but a **different** digest: removed only if the file is
      provably stale, i.e. its mtime is strictly older than the mtime of *every*
      on-disk copy of the imported content. Anything younger or equal (including
      the mtime tie) is kept with an info log; the watcher imports it on its next
      scan, so the system converges.
    - Fail-safe: with ``imported_stable_key`` or ``imported_digest`` ``None``
      nothing is deleted (debug log). A payload without a secrets bundle has no
      content identity and, more importantly, did not come from one of these
      files, so deleting them would destroy unrelated data.

    Why the freshness test is a per-file mtime comparison and not "is the import
    still the scan winner": :func:`discovery.scan_secrets_bundles` orders by
    ``(mtime, digest)`` and therefore resolves *identical* mtimes by the larger
    SHA-256 -- a coin flip with respect to age, and coarse mtimes are exactly
    what the tiebreak exists for (network mounts/QNAP). Trusting that winner
    would let a lexicographically larger *old* digest mark the import as
    "current" and take a fresher, never-imported bundle down with it. Comparing
    each candidate against the imported bundle's own timestamp needs no
    additional state (:class:`discovery._SecretsScanResult` carries ``mtime``)
    and fails towards keeping: a lost credential costs the user the full Google
    login including 2FA, an orphaned secret file does not.

    Race window: the scan snapshot and the delete are separated by executor
    hops, so each removal re-reads the file inside the *same* executor job and
    only unlinks while the digest still matches what the decision was based on
    (:func:`discovery.read_secrets_bundle`). That closes the window down to the
    gap between that re-read and ``os.remove``, which POSIX offers no atomic
    primitive for; a container writing into exactly that gap can still lose the
    copy it just wrote, and the watcher re-imports whatever survives on its next
    scan.

    The removal is idempotent (a missing file is a no-op) and never raises: a
    non-writable external path only logs a warning so the flow completes. Because
    ``SecretsJSONWatcher._scan`` forgets the settled signatures when a file
    disappears, the watcher does not re-trigger on its own delete.
    """

    domain_data = getattr(hass, "data", None)
    if hass is None or not isinstance(domain_data, Mapping):
        return
    manager = domain_data.get(DOMAIN, {})
    manager = manager.get("discovery_manager") if isinstance(manager, Mapping) else None
    watch_paths = getattr(manager, "watch_paths", None)
    if not watch_paths:
        return

    if imported_stable_key is None or imported_digest is None:
        _LOGGER.debug(
            "Skipping watched-secrets cleanup: imported bundle identity unknown "
            "(account=%s, content=%s); keeping all watched bundles",
            "known" if imported_stable_key is not None else "unknown",
            "known" if imported_digest is not None else "unknown",
        )
        return

    # Imported inside the function to avoid a circular import at module load
    # (discovery imports config_flow).
    from . import discovery as discovery_module

    paths = [Path(str(path)) for path in watch_paths]

    def _scan() -> list[tuple[Path, Any]]:
        return discovery_module.scan_secrets_bundles(paths)

    def _remove_if_digest_matches(path_str: str, expected_digest: str) -> None:
        """Re-read the file and unlink it only if it still holds the scanned content.

        Check and unlink share one executor job, so the flow cannot hand the
        event loop back between "this is the bundle I decided about" and the
        ``os.remove``. A file that changed, became unreadable or lost its account
        identity in the meantime is kept.
        """

        current = discovery_module.read_secrets_bundle(Path(path_str))
        if current is None:
            _LOGGER.debug(
                "Keeping watched secrets file: it is gone or no longer "
                "identifiable since the scan: %s",
                path_str,
            )
            return
        if current.digest != expected_digest:
            _LOGGER.info(
                "Keeping watched secrets bundle: its content changed after the "
                "scan and before the delete: %s",
                path_str,
            )
            return
        try:
            os.remove(path_str)
        except FileNotFoundError:
            return
        except OSError:
            _LOGGER.warning(
                "Failed to remove watched bundle file after import: %s",
                path_str,
            )

    scanned = await hass.async_add_executor_job(_scan)
    results = {path: result for path, result in scanned}

    # Oldest on-disk copy of the imported content: only a same-account bundle
    # older than *that* can be ruled out as a fresher credential. ``None`` (the
    # imported content is no longer on disk) disables the stale-copy branch
    # entirely, so only exact content matches are ever removed.
    imported_mtimes = [
        result.mtime
        for _path, result in scanned
        if result.stable_key == imported_stable_key and result.digest == imported_digest
    ]
    oldest_imported_mtime = min(imported_mtimes) if imported_mtimes else None

    for path in paths:
        path_str = str(path)
        result = results.get(path)
        if result is None:
            # Missing file: nothing to do. Unreadable/unparseable: keep it,
            # since the account cannot be positively determined.
            _LOGGER.debug(
                "Keeping watched bundle file: account could not be determined: %s",
                path_str,
            )
            continue
        if result.stable_key != imported_stable_key:
            _LOGGER.warning(
                "Keeping watched bundle of a different account (%s); only "
                "the imported account's copies were removed: %s",
                discovery_module._redact_account_for_log(
                    result.email, result.stable_key
                ),
                path_str,
            )
            continue
        if result.digest == imported_digest or (
            oldest_imported_mtime is not None and result.mtime < oldest_imported_mtime
        ):
            await hass.async_add_executor_job(
                _remove_if_digest_matches, path_str, result.digest
            )
            continue
        _LOGGER.info(
            "Keeping watched secrets bundle: it is not older than the imported "
            "one for the same account; the watcher imports it on its next scan: %s",
            path_str,
        )


def _stable_key_for_discovery_payload(
    payload: CloudDiscoveryData | None,
) -> str | None:
    """Derive the account stable-key of a confirmed discovery payload.

    Uses the same identity function as the watcher
    (``discovery._cloud_discovery_stable_key``) over the payload's email, its
    first OAuth candidate token and its secrets bundle, so the value matches the
    watched bundles' ``stable_key`` used for the account-aware delete. Returns
    ``None`` if the payload is absent (the caller then keeps all bundles).
    """

    if payload is None:
        return None

    from . import discovery as discovery_module

    token = payload.candidates[0][1] if payload.candidates else None
    return discovery_module._cloud_discovery_stable_key(
        payload.email, token, payload.secrets_bundle
    )


def _digest_for_discovery_payload(
    payload: CloudDiscoveryData | None,
) -> str | None:
    """Derive the content digest of a confirmed discovery payload.

    Sister helper of :func:`_stable_key_for_discovery_payload`: while the stable
    key answers "which account", this answers "which exact bundle". It reuses
    :func:`discovery.secrets_bundle_digest`, the same function that stamps every
    watched file, so the values are directly comparable and no second digest
    definition exists.

    Returns ``None`` when the payload is absent or carries no secrets bundle. In
    that case nothing was imported from a watched file, so the delete hook must
    keep every bundle (see its docstring).
    """

    if payload is None or payload.secrets_bundle is None:
        return None

    from . import discovery as discovery_module

    return discovery_module.secrets_bundle_digest(payload.secrets_bundle)


def _mask_email_for_logs(email: str | None) -> str:
    """Return a privacy-friendly representation of an email for logs."""

    if not email or "@" not in email:
        return "<unknown>"

    local, domain = email.split("@", 1)
    if not local:
        return f"*@{domain}"

    masked_local = (local[0] + "***") if len(local) > 1 else "*"
    return f"{masked_local}@{domain}"


class _ConfigFlowMixin:
    hass: HomeAssistant
    context: dict[str, Any]
    unique_id: str | None

    async def async_set_unique_id(
        self, unique_id: str | None, *, raise_on_progress: bool = False
    ) -> None: ...

    def async_show_form(
        self,
        *,
        step_id: str,
        data_schema: vol.Schema | None = None,
        errors: Mapping[str, str] | None = None,
        description_placeholders: Mapping[str, Any] | None = None,
    ) -> FlowResult: ...

    def async_show_menu(
        self,
        *,
        step_id: str,
        menu_options: list[str],
        description_placeholders: Mapping[str, Any] | None = None,
    ) -> FlowResult: ...

    def async_create_entry(
        self,
        *,
        title: str,
        data: Mapping[str, Any],
        **kwargs: Any,
    ) -> FlowResult: ...

    def async_abort(
        self,
        *,
        reason: str,
        description_placeholders: Mapping[str, Any] | None = None,
    ) -> FlowResult: ...

    def _abort_if_unique_id_configured(
        self,
        *,
        updates: Mapping[str, Any] | None = None,
        reload_on_update: bool = True,
    ) -> None: ...

    def _set_confirm_only(self) -> None: ...

    def add_suggested_values_to_schema(
        self, schema: vol.Schema, suggested_values: Mapping[str, Any]
    ) -> vol.Schema:
        return schema

    def _get_entry_cache(self, entry: ConfigEntry) -> Any | None: ...

    async def _async_clear_cached_aas_token(self, entry: ConfigEntry) -> None: ...


class _ConfigSubentryFlowMixin:
    config_entry: ConfigEntry
    subentry: ConfigSubentry | None

    def async_create_entry(self, *, title: str, data: dict[str, Any]) -> FlowResult: ...

    def async_update_and_abort(self, *args: Any, **kwargs: Any) -> FlowResult: ...


class _OptionsFlowMixin:
    hass: HomeAssistant
    config_entry: ConfigEntry

    def async_show_form(
        self,
        *,
        step_id: str,
        data_schema: vol.Schema | None = None,
        errors: Mapping[str, str] | None = None,
        description_placeholders: Mapping[str, Any] | None = None,
    ) -> FlowResult: ...

    def async_show_menu(
        self,
        *,
        step_id: str,
        menu_options: list[str],
    ) -> FlowResult: ...

    def async_create_entry(
        self,
        *,
        title: str,
        data: Mapping[str, Any],
        **kwargs: Any,
    ) -> FlowResult: ...

    def async_abort(
        self,
        *,
        reason: str,
        description_placeholders: Mapping[str, Any] | None = None,
    ) -> FlowResult: ...

    def async_update_and_abort(self, *args: Any, **kwargs: Any) -> FlowResult: ...

    def add_suggested_values_to_schema(
        self, schema: vol.Schema, suggested_values: Mapping[str, Any]
    ) -> vol.Schema:
        return schema

    def _get_entry_cache(self, entry: ConfigEntry) -> Any | None: ...

    async def _async_clear_cached_aas_token(self, entry: ConfigEntry) -> None: ...


# Deliberately the plain base, never ``OptionsFlowWithReload``. This
# integration registers a config entry update listener in ``async_setup_entry``
# (the live watch-path refresh, which also carries the credential reload), and
# Home Assistant forbids that combination: ``OptionsFlowManager`` raises
# ``ValueError("Config entry update listeners should not be used with
# OptionsFlowWithReload")`` *before* it writes the options, so with the reloading
# base every options submission on a loaded entry would fail and persist
# nothing. Upstream deprecated the pairing in 2026.6 and turns it into an error
# in 2026.12, but for this one shape it already raises today (verified in cores
# 2026.1.3 and 2026.2.3). Beyond that the automatic reload is a reload owner that
# cannot take ``claim_pending_entry_reload``, so it would tear the entry down
# next to a claimed reload -- exactly what the single-owner contract in
# ``agents/runtime_patterns/AGENTS.md`` exists to prevent. Steps that need a
# reload therefore ask for one explicitly, through ``_schedule_claimed_reload``.
OptionsFlowBase = cast(type[config_entries.OptionsFlow], config_entries.OptionsFlow)


@dataclass(slots=True)
class _SubentryOption:
    """Lightweight representation of a selectable subentry."""

    key: str
    label: str
    subentry: ConfigSubentry | None
    visible_device_ids: tuple[str, ...]
    #: The ``group_key`` the subentry actually stores, or ``None`` for a
    #: synthesised option and for a subentry that stores none.
    #:
    #: Separate from ``key`` because the two answer different questions and
    #: only one of them survives a collision: ``_gather_subentry_options``
    #: rewrites ``key`` to the ``subentry_id`` of every option as soon as one
    #: key is duplicated, which is what makes the keys injective and is
    #: deliberate. ``key`` is therefore an *identity* and carries no meaning
    #: after such a rewrite, while ``_accepts_device_assignment`` needs the
    #: stored key's *meaning* (is it the reserved service key?). Reading
    #: ``key`` for that question is what let a legacy ``tracker`` storing
    #: ``SERVICE_SUBENTRY_KEY`` become an assignable target the moment any
    #: duplicate existed elsewhere in the entry.
    stored_key: str | None = None

    @property
    def subentry_id(self) -> str | None:
        """Return the backing Home Assistant subentry identifier when available."""

        if self.subentry is None:
            return None
        return getattr(self.subentry, "subentry_id", None)


# Feature groups that never hold device assignments. The invariant is older
# than this checkpoint: ``ServiceSubentryFlowHandler._visible_device_ids`` and
# ``HubSubentryFlowHandler._visible_device_ids`` both return ``()`` unconditionally,
# ``_async_sync_feature_subentries`` builds its ``service_payload`` without the
# key while ``tracker_payload`` keeps the stored ids, and
# ``tests/test_config_flow_subentry_sync.py`` asserts that absence. On the
# reading side ``coordinator/subentry.py::_refresh_subentry_index`` forces
# ``visible_device_ids`` back to ``()`` for the service key on every refresh.
#
# How far that carries. An earlier version of this note said the reading side
# tested the **key** only, so that a subentry typed ``service`` storing a
# diverging key kept its ids. That is no longer true: the reading side folds
# every type in ``NON_DEVICE_SUBENTRY_TYPES`` onto the service key before the
# branch runs, so both axes are now enforced on both sides. The type axis is
# still the one that carries the argument, because the manager canonicalises
# primarily by type and keeps the stored key as an alias: a group offered
# under an alias is one the manager may move out from under the assignment.
# Module-private name kept for the many references below; it is bound to the
# shared object in ``const.py`` rather than to a second frozenset, so the
# writing side in ``__init__.py`` and this predicate cannot drift apart.
_NON_DEVICE_SUBENTRY_KEYS = NON_DEVICE_SUBENTRY_KEYS
# The type axis is shared with the reading side rather than restated here:
# ``coordinator/subentry.py`` folds exactly these types onto the service key
# and exempts their mis-keyed ids from its in-memory assignment bookkeeping,
# so a type added on one side cannot be forgotten on the other. The exemption
# reaches neither storage nor that group's own allow-list: the stored subentry
# keeps those ids, which is what makes the re-homing reversible, and the group
# keeps filtering by them, because an absent list would let it see every device
# rather than none.
_NON_DEVICE_SUBENTRY_TYPES = NON_DEVICE_SUBENTRY_TYPES


def _unclaimed_fallback_key(taken: Container[str]) -> str:
    """Return a tracker-group key no existing option already holds.

    The synthesised fallback of ``_device_target_choice_map`` used to take
    ``TRACKER_SUBENTRY_KEY`` unconditionally. That borrows an identity: an
    entry whose only subentry is typed ``service`` while storing the legacy
    ``group_key`` ``core_tracking`` is filtered out of the target list, and the
    fallback then offered *its* key. ``_async_assign_devices_to_subentry``
    resolves the key against the **unfiltered** set, found that real subentry,
    refused the write, and ``async_step_repairs_move`` reported
    ``subentry_move_success`` for a move that never happened.

    The search is total rather than a single alternative: a second subentry may
    store the very substitute the first one is displaced to, so a fixed number
    of attempts is not enough. ``taken`` is finite, so the loop terminates.

    ``taken`` must carry **both** axes, and the caller is what makes that true.
    Passing only ``option.key`` was correct until the collision rewrite in
    ``_gather_subentry_options`` turned every key into a ``subentry_id``: the
    set then holds no stored ``group_key`` at all and this helper hands back
    ``core_tracking`` while a real subentry stores it, which is the borrow the
    whole function exists to prevent. Pinned by
    ``::test_the_synthesised_fallback_does_not_borrow_a_rewritten_key``.
    """

    if TRACKER_SUBENTRY_KEY not in taken:
        return TRACKER_SUBENTRY_KEY
    suffix = 2
    while f"{TRACKER_SUBENTRY_KEY}_{suffix}" in taken:
        suffix += 1
    return f"{TRACKER_SUBENTRY_KEY}_{suffix}"


def _accepts_device_assignment(option: _SubentryOption) -> bool:
    """Return whether ``option`` may hold ``visible_device_ids``.

    Both axes are read on purpose, and the type axis is not redundant.
    ``ConfigEntrySubEntryManager._refresh_from_entry`` derives the canonical key
    primarily from ``subentry_type`` and keeps a diverging stored ``group_key``
    as an alias, which ``agents/config_flow/AGENTS.md`` requires under
    ``Subentry alias handling``. A predicate comparing only the key would let
    through a legacy subentry that stores an e-mail-like ``group_key`` while
    being typed ``service``.

    A synthesised option without a backing subentry (the ``core_tracking``
    fallback of ``_gather_subentry_options``) carries no type and is accepted;
    it is the tracker group, which is precisely the one that holds devices.

    The key axis reads ``stored_key``, not ``key``. ``key`` is an identity that
    ``_gather_subentry_options`` rewrites to the ``subentry_id`` whenever any
    two options collide, so asking it a question about *meaning* answered
    correctly only on entries that happened to have no duplicate. ``stored_key``
    is ``None`` exactly where there is no stored key to judge -- a synthesised
    option, or a subentry that stores none -- and there ``key`` is the
    ``subentry_id`` or the tracker key, neither of which is reserved, so the
    fallback preserves the previous answer instead of inventing one.
    """

    if (option.stored_key or option.key) in _NON_DEVICE_SUBENTRY_KEYS:
        return False
    return _subentry_type_accepts_devices(option.subentry)


def _subentry_type_accepts_devices(subentry: Any) -> bool:
    """Return whether ``subentry``'s *type* permits ``visible_device_ids``.

    Extracted from :func:`_accepts_device_assignment` rather than restated,
    because ``_async_sync_feature_subentries`` needs the same axis on the
    writing side and ``const.NON_DEVICE_SUBENTRY_TYPES`` exists precisely so
    the offering, indexing and writing sites cannot drift apart. Keeping one
    comparison against the set is the point; a second one would reintroduce the
    drift the shared set was created to prevent.

    ``None`` (an option without a backing subentry, or a double that does not
    model the attribute) is accepted, which is the caller's existing
    convention: only a *known* non-device type disqualifies.
    """

    subentry_type = getattr(subentry, "subentry_type", None)
    return subentry_type not in _NON_DEVICE_SUBENTRY_TYPES


def _canonical_core_key_of(subentry: Any) -> str | None:
    """Return the core group key ``subentry``'s *type* claims, or ``None``.

    This is the writing side of the fold the reading side already performs:
    ``coordinator/subentry.py`` indexes every type in
    ``NON_DEVICE_SUBENTRY_TYPES`` under ``SERVICE_SUBENTRY_KEY`` whatever the
    subentry stores, and ``HubSubentryFlowHandler._group_key`` is that same key,
    so a ``hub`` *is* the service group under a second entry point.

    The two directions are **not** symmetric, and reading them as if they were
    is the mistake this docstring exists to prevent. A non-device type folds
    unconditionally, because ``SERVICE_SUBENTRY_KEY`` is the only key such a
    type may ever answer for. A ``tracker`` type folds *only* when it stores the
    service key, because several tracker groups with distinct keys are a
    supported shape: ``coordinator/subentry.py`` says so in as many words and
    leaves tracker subentries on their stored key. Folding every tracker onto
    ``TRACKER_SUBENTRY_KEY`` would hand a legacy per-account group (``group_key
    = "owner@example.com"``) to the core tracker sync, which then overwrites its
    title and identity: a data defect introduced by the very axis meant to
    prevent one. Only the service-keyed tracker is a genuine mis-key, since
    ``_accepts_device_assignment`` and the reading side both reserve that key
    for the service group.

    ``None`` means the type does not decide and the stored ``group_key`` keeps
    deciding, which covers an untyped legacy subentry, a tracker on its own key
    and any type outside the pair. Callers must treat that as "no objection",
    not as "no match".
    """

    subentry_type = getattr(subentry, "subentry_type", None)
    if subentry_type is None:
        return None
    if not _subentry_type_accepts_devices(subentry):
        return SERVICE_SUBENTRY_KEY
    if subentry_type == SUBENTRY_TYPE_TRACKER:
        data = getattr(subentry, "data", {}) or {}
        if data.get("group_key") == SERVICE_SUBENTRY_KEY:
            return TRACKER_SUBENTRY_KEY
        return None
    return None


def _may_answer_for(candidate: Any, key: str) -> bool:
    """Return whether ``candidate`` may be resolved as the ``key`` group.

    Module level rather than nested, because both *live* resolvers in this file
    need the identical judgement and a second spelling of ``canonical is None
    or canonical == key`` is precisely the drift ``_canonical_core_key_of`` was
    extracted to prevent. It lived inside ``_async_sync_feature_subentries``
    while that held the only two call sites;
    ``_BaseSubentryFlow._resolve_existing`` is the third, and it closes over
    nothing, so lifting it changes no behaviour at the original call sites.

    "Live" is meant literally and is not a hedge: a third key-only lookup of
    the same shape exists at ``ConfigFlow._lookup_subentry`` -- on the outer
    flow, not on ``_BaseSubentryFlow``, which only begins below it -- and it
    has **no caller anywhere in the tree**, so it is dead rather than exempt.
    Guarding it would add a line no test executes; removing it is a production
    change and belongs to its own step, tracked as ``U-29`` in
    ``PLAN_GFMY_SUBENTRY_FLOW_CREATE_PATH`` and carried in the remainder
    register of ``agents/config_flow/AGENTS.md``.

    The permissive default is deliberate and is the reason this reads as *may*
    rather than *is*: ``_canonical_core_key_of`` returns ``None`` wherever the
    type declines to decide -- an untyped legacy subentry, a tracker on its own
    key, any type outside the pair -- and there the stored ``group_key`` keeps
    deciding alone. Only a type that names a *different* core key objects.
    """

    canonical = _canonical_core_key_of(candidate)
    return canonical is None or canonical == key


_LITERAL_CORE_KEY_OWNER = LITERAL_CORE_KEY_OWNER
"""Module-local alias of the shared literal-owner table (``const.py``).

The definition moved to ``const.py`` when the runtime index needed the same
ranking: a slot won here and lost there is precisely the drift a single
definition prevents. Only the name is kept local, so the call sites below read
unchanged.
"""


_FIELD_SUBENTRY = "subentry"
_FIELD_REPAIR_TARGET = "target_subentry"
_FIELD_REPAIR_DELETE = "delete_subentry"
_FIELD_REPAIR_FALLBACK = "fallback_subentry"
_FIELD_VISIBILITY_HUB = "hub"
# Field identifiers used in options/visibility flows
_FIELD_REPAIR_DEVICES = "device_ids"
# Sole field of the ``found_local_bundle`` preflight step: "import the bundle
# that is already on disk?". Unset means "no", which returns to the auth-method
# form.
_FIELD_USE_FOUND_BUNDLE = "use_found_bundle"

# Discovery for an account that is already configured: "replace the stored
# credentials with the ones just discovered?". Unset means "no", which leaves the
# entry untouched. Defaults to yes, because a discovery for a configured account
# follows a login the user just performed on purpose.
_FIELD_OVERWRITE_CREDENTIALS = "overwrite_credentials"

_SUBENTRIES_DOCS_URL = (
    "https://github.com/BSkando/GoogleFindMy-HA/blob/main/README.md"
    "#subentries-and-feature-groups"
)
_SUBENTRY_PLACEHOLDERS: dict[str, str] = {
    "subentries_docs_url": _SUBENTRIES_DOCS_URL,
}

# ---------------------------
# Validators (format/plausibility)
# ---------------------------
_EMAIL_RE = re.compile(
    r"^(?=.{3,254}$)[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@([A-Za-z0-9-]+\.)+[A-Za-z]{2,}$"
)
_TOKEN_RE = re.compile(r"^\S{16,}$")


def _email_valid(value: str) -> bool:
    """Return True if value looks like a real email address."""
    return bool(_EMAIL_RE.match(value or ""))


def _token_plausible(value: str) -> bool:
    """Return True if value looks like a token (no spaces, at least 16 chars)."""
    return bool(_TOKEN_RE.match(value or ""))


def _looks_like_jwt(value: str) -> bool:
    """Lightweight detection for JWT-like blobs (Base64URL x3; often starts with 'eyJ')."""
    return value.count(".") >= 2 and value[:3] == "eyJ"


_TRACKER_FEATURE_PLATFORMS: tuple[str, ...] = TRACKER_FEATURE_PLATFORMS

_SERVICE_FEATURE_PLATFORMS: tuple[str, ...] = SERVICE_FEATURE_PLATFORMS


def _normalize_feature_list(features: CollIterable[str]) -> list[str]:
    """Return a sorted list of unique, lower-cased feature identifiers."""

    normalized: list[str] = []
    for feature in features:
        if not isinstance(feature, str):
            continue
        candidate = feature.strip().lower()
        if candidate:
            normalized.append(candidate)
    ordered = list(dict.fromkeys(normalized))
    return sorted(ordered)


def _normalize_visible_ids(visible_ids: CollIterable[str]) -> list[str]:
    """Return a sorted list of unique device identifiers suitable for storage."""

    candidates: list[str] = []
    for device_id in visible_ids:
        if not isinstance(device_id, str):
            continue
        candidate = device_id.strip()
        if candidate:
            candidates.append(candidate)
    return sorted(dict.fromkeys(candidates))


def _derive_feature_settings(
    *, options_payload: Mapping[str, Any], defaults: Mapping[str, Any]
) -> tuple[bool, dict[str, Any]]:
    """Return the Google Home filter flag and feature toggles for a subentry."""

    default_filter_enabled = False
    if OPT_GOOGLE_HOME_FILTER_ENABLED is not None:
        if OPT_GOOGLE_HOME_FILTER_ENABLED in options_payload:
            default_filter_enabled = bool(
                options_payload[OPT_GOOGLE_HOME_FILTER_ENABLED]
            )
        elif defaults.get(OPT_GOOGLE_HOME_FILTER_ENABLED) is not None:
            default_filter_enabled = bool(defaults[OPT_GOOGLE_HOME_FILTER_ENABLED])
        elif DEFAULT_GOOGLE_HOME_FILTER_ENABLED is not None:
            default_filter_enabled = bool(DEFAULT_GOOGLE_HOME_FILTER_ENABLED)

    has_filter = default_filter_enabled
    if (
        OPT_GOOGLE_HOME_FILTER_ENABLED is not None
        and OPT_GOOGLE_HOME_FILTER_ENABLED in options_payload
    ):
        has_filter = bool(options_payload[OPT_GOOGLE_HOME_FILTER_ENABLED])

    feature_flags: dict[str, Any] = {}
    if OPT_ENABLE_STATS_ENTITIES is not None:
        if OPT_ENABLE_STATS_ENTITIES in options_payload:
            feature_flags[OPT_ENABLE_STATS_ENTITIES] = bool(
                options_payload[OPT_ENABLE_STATS_ENTITIES]
            )
        elif defaults.get(OPT_ENABLE_STATS_ENTITIES) is not None:
            feature_flags[OPT_ENABLE_STATS_ENTITIES] = bool(
                defaults[OPT_ENABLE_STATS_ENTITIES]
            )

    if OPT_MAP_VIEW_TOKEN_EXPIRATION in options_payload:
        feature_flags[OPT_MAP_VIEW_TOKEN_EXPIRATION] = bool(
            options_payload[OPT_MAP_VIEW_TOKEN_EXPIRATION]
        )

    if OPT_MAP_VIEW_ENABLED in options_payload:
        feature_flags[OPT_MAP_VIEW_ENABLED] = bool(
            options_payload[OPT_MAP_VIEW_ENABLED]
        )

    if OPT_GOOGLE_HOME_FILTER_ENABLED is not None:
        feature_flags[OPT_GOOGLE_HOME_FILTER_ENABLED] = has_filter

    contributor_mode = options_payload.get(OPT_CONTRIBUTOR_MODE)
    if contributor_mode is not None:
        feature_flags[OPT_CONTRIBUTOR_MODE] = contributor_mode

    return has_filter, feature_flags


def _build_subentry_payload(
    *,
    group_key: str,
    features: CollIterable[str],
    entry_title: str,
    has_google_home_filter: bool,
    feature_flags: Mapping[str, Any],
    visible_device_ids: CollIterable[str] | None = None,
) -> dict[str, Any]:
    """Construct the payload stored on a config subentry."""

    payload: dict[str, Any] = {
        "group_key": group_key,
        "features": _normalize_feature_list(features),
        "fcm_push_enabled": False,
        "has_google_home_filter": has_google_home_filter,
        "feature_flags": dict(feature_flags),
        "entry_title": entry_title,
    }
    if visible_device_ids:
        normalized_ids = _normalize_visible_ids(visible_device_ids)
        if normalized_ids:
            payload["visible_device_ids"] = normalized_ids
    return payload


_DEFAULT_SUBENTRY_TITLES: dict[str, str] = {
    TRACKER_SUBENTRY_KEY: "Google Find My devices",
    SERVICE_SUBENTRY_KEY: "Google Find Hub Service",
}


def _disqualifies_for_persistence(value: str) -> str | None:
    """Return a reason string if token must NOT be persisted.

    IMPORTANT CHANGE:
    - AAS (aas_et/...) master tokens ARE allowed to be stored (they are needed
      to mint service tokens in the background).
    - JWT-like installation/ID tokens are rejected (not stable/refreshable).
    """
    if _looks_like_jwt(value):
        return "token looks like a JWT (installation/ID token), not a stable API token"
    return None


def _is_multi_entry_guard_error(err: Exception) -> bool:
    """Return True if the exception message indicates an entry-scope guard."""
    msg = f"{err}"
    return ("Multiple config entries active" in msg) or ("entry.runtime_data" in msg)


# ---------------------------
# Error mapping for API exceptions
# ---------------------------
def _map_api_exc_to_error_key(err: Exception) -> str:
    """Map library/network errors to HA error keys without leaking details."""
    if isinstance(err, DependencyNotReady):
        return "dependency_not_ready"

    name = err.__class__.__name__.lower()

    if any(k in name for k in ("auth", "unauthor", "forbidden", "credential")):
        return "invalid_auth"

    status_obj = getattr(err, "status", None)
    if status_obj is None:
        status_obj = getattr(err, "status_code", None)
    status_int: int | None = None
    if isinstance(status_obj, bool):
        status_int = int(status_obj)
    elif isinstance(status_obj, (int, float)):
        status_int = int(status_obj)
    elif isinstance(status_obj, str) and status_obj.isdigit():
        status_int = int(status_obj)
    if status_int in (401, 403):
        return "invalid_auth"

    if aiohttp is not None and isinstance(
        err, (aiohttp.ClientError, aiohttp.ServerTimeoutError)
    ):
        return "cannot_connect"
    if any(k in name for k in ("timeout", "dns", "socket", "connection", "connect")):
        return "cannot_connect"

    if _is_multi_entry_guard_error(err):
        return "unknown"

    return "unknown"


# ---------------------------
# Auth method choice UI
# ---------------------------
_AUTH_METHOD_SECRETS = "secrets_json"
_AUTH_METHOD_INDIVIDUAL = "individual_tokens"


def _extra_watch_paths_to_text(raw: object) -> str:
    """Render the stored ``SECRETS_EXTRA_WATCH_PATHS`` option as an editable text block.

    The option is stored as a list of path strings (or, tolerantly, a single
    string). The options form edits it as a newline-separated text block, so this
    joins list entries with newlines and passes a plain string through.
    """

    if isinstance(raw, str):
        return raw
    if isinstance(raw, (list, tuple)):
        return "\n".join(str(item) for item in raw if str(item).strip())
    return ""


def _parse_extra_watch_paths_text(raw: object) -> list[str]:
    """Parse the extra-watch-paths text block into a clean list of path strings.

    Splits on newlines, trims whitespace, drops blanks and de-duplicates while
    preserving order. A non-string/empty input yields an empty list (the override
    is then removed and the zero-config defaults apply).
    """

    if isinstance(raw, (list, tuple)):
        candidates = [str(item) for item in raw]
    elif isinstance(raw, str):
        candidates = raw.splitlines()
    else:
        return []

    seen: set[str] = set()
    result: list[str] = []
    for candidate in candidates:
        cleaned = candidate.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required("auth_method"): vol.In(
            {
                _AUTH_METHOD_SECRETS: "GoogleFindMyTools secrets.json",
                # _AUTH_METHOD_INDIVIDUAL: "Manual token + email",  # Disabled: broken manual token path is intentionally hidden.
            }
        )
    }
)

STEP_SECRETS_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(
            "secrets_json",
            description="Paste the complete contents of your secrets.json file",
        ): str
    }
)

STEP_INDIVIDUAL_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_OAUTH_TOKEN, description="OAuth/AAS token"): str,
        vol.Required(CONF_GOOGLE_EMAIL, description="Google email address"): str,
    }
)


# ---------------------------
# Extractors (email + token candidates with preference order)
# ---------------------------
def _extract_email_from_secrets(data: dict[str, Any]) -> str | None:
    """Best-effort extractor for the Google account email from secrets.json."""
    candidates = [
        "googleHomeUsername",
        CONF_GOOGLE_EMAIL,
        "google_email",
        "email",
        "username",
        "user",
    ]
    for key in candidates:
        val = data.get(key)
        if isinstance(val, str) and "@" in val:
            return val
    # Nested fallback shapes
    try:
        val = data["account"]["email"]
        if isinstance(val, str) and "@" in val:
            return val
    except Exception:
        pass
    return None


def _extract_oauth_candidates_from_secrets(
    data: dict[str, Any],
) -> list[tuple[str, str]]:
    """Return plausible tokens in preferred order from a secrets bundle.

    Priority:
      1) 'aas_token' (Account Authentication Service master token)
      2) Flat OAuth-ish keys ('oauth_token', 'access_token', etc.)
      3) 'fcm_credentials.installation.token' (installation JWT)  [discouraged]
      4) 'fcm_credentials.fcm.registration.token' (registration token)  [discouraged]
    Duplicate values are de-duplicated while preserving source labels.
    """
    cands: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _add(label: str, value: Any) -> None:
        if isinstance(value, str) and _token_plausible(value) and value not in seen:
            cands.append((label, value))
            seen.add(value)

    _add("aas_token", data.get("aas_token"))

    for key in (
        CONF_OAUTH_TOKEN,
        "oauth_token",
        "oauthToken",
        "OAuthToken",
        "access_token",
        "token",
        "adm_token",
        "admToken",
        "Auth",
    ):
        _add(key, data.get(key))

    try:
        _add("fcm_installation", data["fcm_credentials"]["installation"]["token"])
    except Exception:
        pass
    try:
        _add(
            "fcm_registration", data["fcm_credentials"]["fcm"]["registration"]["token"]
        )
    except Exception:
        pass

    return cands


def _persist_secrets_bundle(
    parsed: dict[str, Any],
    token: str,
    *,
    email: str | None = None,
) -> dict[str, Any]:
    """Build the shared secrets-bundle sub-dict for every persist surface.

    This is the single source of truth for the credential sub-dict written by
    the paste and preflight-import paths across initial setup, reauth and
    options. Extracting it prevents a further copy-paste divergence between
    those surfaces. The FCM registration stays inside the bundle: it is not
    copied to the top level of the entry, because nothing reads such a copy and
    it would only go stale (``Auth/AGENTS.md``).

    The helper only builds the bundle-specific keys; it deliberately does NOT
    own the surface-specific concerns that legitimately differ between call
    sites:

    - the ``{**entry.data}`` merge (reauth/options),
    - the ``_async_clear_cached_aas_token`` side effect (reauth/options),
    - the ``pop(DATA_AAS_TOKEN)`` else-branch (reauth/options have it, the
      initial setup does not) -- this asymmetry stays at the call site.

    ``DATA_AAS_TOKEN`` is only set here when ``token`` is an ``aas_et/`` token;
    call sites that need the removal of a stale AAS token keep their own
    else-branch.

    Args:
        parsed: An already normalized secrets bundle (``normalize_secrets_bundle``).
        token: The validated OAuth/AAS token to persist.
        email: When provided, ``CONF_GOOGLE_EMAIL`` is included (initial-setup
            surface, which builds a fresh dict rather than merging ``entry.data``).

    Returns:
        A new sub-dict with ``CONF_OAUTH_TOKEN``, ``DATA_SECRET_BUNDLE``,
        optionally ``CONF_GOOGLE_EMAIL`` and optionally ``DATA_AAS_TOKEN``.
    """

    bundle: dict[str, Any] = {
        CONF_OAUTH_TOKEN: token,
        DATA_SECRET_BUNDLE: parsed,
    }
    if email is not None:
        bundle[CONF_GOOGLE_EMAIL] = email
    if isinstance(token, str) and token.startswith("aas_et/"):
        bundle[DATA_AAS_TOKEN] = token
    return bundle


def _count_supplied_credential_methods(
    user_input: Mapping[str, Any], field_names: tuple[str, ...]
) -> int:
    """Count how many of ``field_names`` carry a non-blank value.

    Credential forms accept several mutually exclusive methods. A submission
    carrying more than one has to be rejected rather than resolved by
    precedence, because picking a winner would silently discard the other input.
    Kept as one helper so the rule is not re-derived at each call site.
    """

    return sum(1 for name in field_names if str(user_input.get(name) or "").strip())


def _format_bundle_age(mtime: float) -> str:
    """Render the age of a found bundle as a prose-free ``2d 3h 4m`` string.

    Display only. The preflight deliberately has no age *limit* (an old bundle
    is not a wrong bundle), so this value never enters a decision; it exists so
    the user can tell a leftover from a login they just performed.

    Prose-free on purpose: the value is substituted into a translated sentence and is not itself
    translatable, so it must not carry English words. A clock that jumped
    backwards (or a file stamped in the future) clamps to ``0m`` rather than
    rendering a negative age.
    """

    seconds = max(0, int(time.time() - mtime))
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


PENDING_CONTAINER_CLEANUP_KEY = "pending_container_cleanup"
"""``hass.data[DOMAIN]`` key holding the staged, in-memory cleanup tickets."""

PERSIST_PROOF_TIMEOUT = 60.0
"""Seconds the durability gate waits for the entry to appear in HA storage.

The config-entry store saves *debounced* (``ConfigEntries._async_schedule_save``
hands ``SAVE_DELAY`` seconds to ``Store.async_delay_save``), so the wait is
normally over after one poll. The budget only bounds the pathological case; it
is not a deadline the cleanup is allowed to run without proof after.
"""

PERSIST_PROOF_POLL_INTERVAL = 0.5
"""Seconds between two durability probes.

Deliberately not much smaller than ``SAVE_DELAY``: every probe parses the whole
config-entry store, which is a six-figure byte count on a well-populated
installation. On the path this gate exists for, the initial create, the first
probe is necessarily a miss (the save is not even scheduled while
``async_setup_entry`` runs), so a shorter interval buys nothing there but extra
parses of a file that cannot have changed yet. The update paths behave the same
way for a different reason: the entry id is stored long since, but the
``modified_at`` watermark still has to be flushed, so the first probe is
normally a miss there too and the interval does apply.
"""


@dataclass(frozen=True, slots=True)
class PendingContainerCleanup:
    """One irreversible post-persist cleanup, staged in memory by the flow.

    The flow cannot run these actions itself: ``ConfigFlow.async_create_entry``
    only *builds* a ``FlowResult``: Home Assistant creates and stores the entry
    afterwards in ``ConfigEntriesFlowManager.async_finish_flow`` (see
    ``homeassistant/config_entries.py``, ``await self.config_entries.async_add(
    entry)``), i.e. after the step has already returned. Deleting the on-disk
    credentials from inside the step would therefore destroy the only remaining
    copy while the entry may still fail to materialise.

    So the flow stages the job here and ``async_setup_entry`` hands it to the
    durability gate (:func:`async_schedule_pending_container_cleanup`). The
    staging area is deliberately **in-memory only** (``hass.data``) and is never
    written to Home Assistant storage. Losing it on a crash is the *desired*
    failure direction: the job is gone, the credential file is still there, and
    the secrets watcher re-imports it on its next scan.
    """

    imported_stable_key: str | None = None
    imported_digest: str | None = None


@dataclass(slots=True)
class _StagedCleanupTicket:
    """All cleanup jobs staged by exactly ONE flow, plus what must be proven.

    The staging area is a FIFO list of tickets keyed by ``flow_id``, not a
    mapping keyed by account unique id. That distinction is the whole point:
    two overlapping flows for the same account produce two tickets, and every
    ``async_setup_entry`` run claims **at most one** of them. Bucketing by
    unique id merged both flows' jobs into one list, so the first entry that
    reached setup executed the second flow's irreversible cleanup as well -- for
    an entry that might never materialise.

    Correlation, in order of strength:

    * ``entry_id`` -- set by the paths that update an entry that *already
      exists* (discovery-update, reconfigure, reauth, options). Such a ticket
      belongs to exactly one entry and is claimable by no other, so no
      ordering heuristic is involved at all.
    * ``unique_id`` -- the flow's unique id, which Home Assistant copies
      verbatim onto a newly created entry
      (``ConfigEntry(unique_id=flow.unique_id)``). Used by the create path,
      where the entry id cannot be known while the flow is still running. It
      stays ``None`` for a flow that had not resolved its account yet; such a
      ticket is claimable by any entry of this integration, but still only by
      one.

    FIFO order alone does **not** keep two overlapping create flows apart: Home
    Assistant may abort the loser at any point, and its ticket would otherwise
    be inherited by the winner. What keeps them apart is that a removed flow
    drops its own ticket (``ConfigFlow.async_remove`` ->
    :func:`_async_discard_cleanup_ticket_for_flow`), so only tickets of flows
    that are still alive or that already produced an entry survive to be
    claimed.

    ``entry_promised`` is what makes "already produced an entry" observable on
    the create path, where ``entry_id`` cannot be. It is set once the flow has
    returned a ``CREATE_ENTRY`` FlowResult (see
    :func:`_async_mark_cleanup_ticket_entry_promised`), and it is the *only*
    thing that distinguishes the two endings Home Assistant reports through the
    very same hook: an aborted flow (nothing will ever exist) and a created
    entry whose first ``async_setup_entry`` raised ``ConfigEntryNotReady``
    (the entry exists and Home Assistant will retry its setup). Without it the
    removal hook drops the ticket of an entry that is merely *retrying*, and the
    later successful attempt finds nothing to claim.

    ``min_modified_at`` is the durability watermark. ``None`` means "the entry
    merely has to exist in storage" (create path). A value means "the stored
    record must additionally carry a ``modified_at`` at least this recent",
    which is what makes the proof non-vacuous for an entry that was already
    stored before the update.
    """

    flow_id: str
    unique_id: str | None
    entry_id: str | None = None
    entry_promised: bool = False
    min_modified_at: datetime | None = None
    jobs: list[PendingContainerCleanup] = field(default_factory=list)


@_typed_callback
def _async_stage_container_cleanup(
    hass: HomeAssistant | None,
    *,
    flow_id: str,
    unique_id: str | None,
    job: PendingContainerCleanup,
    entry_id: str | None = None,
    min_modified_at: datetime | None = None,
) -> None:
    """Stage ``job`` on the ticket of the flow identified by ``flow_id``.

    Appends to the calling flow's own ticket, creating it on first use, so a
    concurrent flow for the same account can never end up sharing the list.

    ``entry_id``/``min_modified_at`` are the correlation and the durability
    watermark of an *existing-entry update* (see :class:`_StagedCleanupTicket`).
    When a ticket already exists, both are merged towards the *stricter* value:
    a correlation is only ever added, never dropped, and the watermark only ever
    moves forward. Merging that way keeps the fail-safe direction, because a
    stricter proof can only ever delay or cancel a cleanup, never authorise one
    that the individual jobs would not have authorised on their own.
    """

    if hass is None:
        return
    bucket = cast(MutableMapping[str, Any], hass.data.setdefault(DOMAIN, {}))
    tickets = cast(
        list[_StagedCleanupTicket],
        bucket.setdefault(PENDING_CONTAINER_CLEANUP_KEY, []),
    )
    for ticket in tickets:
        if isinstance(ticket, _StagedCleanupTicket) and ticket.flow_id == flow_id:
            if ticket.unique_id is None and unique_id:
                # The flow resolved its account between two staged jobs.
                ticket.unique_id = unique_id
            if entry_id and ticket.entry_id is None:
                ticket.entry_id = entry_id
            if min_modified_at is not None and (
                ticket.min_modified_at is None
                or min_modified_at > ticket.min_modified_at
            ):
                ticket.min_modified_at = min_modified_at
            ticket.jobs.append(job)
            return
    tickets.append(
        _StagedCleanupTicket(
            flow_id=flow_id,
            unique_id=unique_id or None,
            entry_id=entry_id or None,
            min_modified_at=min_modified_at,
            jobs=[job],
        )
    )


def _entry_modified_at(entry: Any) -> datetime | None:
    """Return ``entry.modified_at`` if it is a usable durability watermark.

    ``ConfigEntries._async_update_entry`` stamps a fresh ``modified_at``
    immediately before it schedules the debounced store save, so reading it
    straight off the entry the flow just updated yields the value the storage
    has to catch up to. An update that changes nothing returns early and keeps
    the previous stamp (``if not changed: return False`` sits before the
    stamping); that older value is still a valid watermark, because the stored
    record already carries exactly that state. ``None`` means "this entry
    cannot tell me what to wait for", which callers must treat as "do not stage
    an irreversible job at all".
    """

    value = getattr(entry, "modified_at", None)
    return value if isinstance(value, datetime) else None


@_typed_callback
def _async_stage_container_cleanup_for(
    hass: HomeAssistant | None,
    *,
    flow_id: str,
    unique_id: str | None,
    job: PendingContainerCleanup,
    entry: Any | None = None,
) -> bool:
    """Stage ``job`` for the create path (``entry`` is ``None``) or an update.

    The one place that turns "which entry is this cleanup about" into a ticket
    correlation plus a durability watermark, so no call site has to get that
    pairing right on its own.

    Returns whether the job was staged. It is dropped, with a debug log and
    without ever being executed, when an update-path entry cannot supply a
    ``modified_at``: without it the gate would degrade to "the entry id exists",
    which was already true before the update and would therefore authorise the
    irreversible cleanup on no evidence at all. Dropping is the fail-safe
    outcome -- the credential file stays on disk and the watcher re-imports it.
    """

    if hass is None:
        # ``_async_stage_container_cleanup`` drops the job in this case, so
        # reporting "staged" here would be a false positive.
        return False

    if entry is None:
        _async_stage_container_cleanup(
            hass, flow_id=flow_id, unique_id=unique_id, job=job
        )
        return True

    entry_id = getattr(entry, "entry_id", None)
    modified_at = _entry_modified_at(entry)
    if not isinstance(entry_id, str) or not entry_id or modified_at is None:
        _LOGGER.debug(
            "Not staging the container-login cleanup for an updated entry "
            "without a usable durability watermark (entry_id=%s, modified_at=%s); "
            "credential files are kept on disk",
            entry_id,
            type(getattr(entry, "modified_at", None)).__name__,
        )
        return False

    _async_stage_container_cleanup(
        hass,
        flow_id=flow_id,
        unique_id=unique_id,
        job=job,
        entry_id=entry_id,
        min_modified_at=modified_at,
    )
    return True


@_typed_callback
def _async_staged_cleanup_tickets(
    hass: HomeAssistant | None,
) -> tuple[MutableMapping[str, Any], list[Any]] | None:
    """Return the staging bucket and its ticket list, or ``None`` if absent.

    One place that knows the shape of the staging area, so the claim, the
    discard-on-flow-removal and the housekeeping below cannot drift apart.

    ``hass.data`` is read through ``getattr`` and type-checked, not assumed.
    The readers do not share one call context: the claim runs from
    ``async_setup_entry``, the discards and the entry-promise marking run from
    flow-lifecycle ``@callback`` hooks, and the ``hass`` object reaching them
    ranges from a real core to a flow test double or a stripped core without a
    ``data`` mapping at all. "No readable staging area" resolves to "nothing
    staged", which is the fail-safe answer for every one of them: nothing is
    claimed, nothing is dropped and nothing is marked, so the credential files
    stay on disk.
    """

    if hass is None:
        return None
    data = getattr(hass, "data", None)
    if not isinstance(data, Mapping):
        return None
    bucket = data.get(DOMAIN)
    if not isinstance(bucket, MutableMapping):
        return None
    tickets = bucket.get(PENDING_CONTAINER_CLEANUP_KEY)
    if not isinstance(tickets, list):
        return None
    return bucket, tickets


@_typed_callback
def _async_claim_container_cleanup_ticket(
    hass: HomeAssistant, *, unique_id: str | None, entry_id: str | None = None
) -> _StagedCleanupTicket | None:
    """Claim at most ONE staged ticket for this entry.

    Claiming, never peeking: ``async_setup_entry`` runs again on every reload,
    and an irreversible cleanup must fire exactly once per staged job.

    Selection order, strongest correlation first:

    1. the oldest ticket carrying exactly this ``entry_id``. Those are the
       existing-entry update tickets; they name their entry, so no heuristic is
       involved and no other entry can ever take them.
    2. the oldest *uncorrelated* ticket whose ``unique_id`` matches the entry.
    3. the oldest uncorrelated ticket without an account.

    Rules 2 and 3 skip every ticket that carries an ``entry_id``: a ticket that
    already names its entry must never be inherited by a different one, not even
    when the accounts match.

    Rules 2 and 3 are the create path, where the entry id cannot be known while
    the flow is still running. They are only sound because a flow that Home
    Assistant removes without producing an entry takes its own ticket with it
    (:func:`_async_discard_cleanup_ticket_for_flow`). Without that, an aborted
    same-account flow would leave a ticket behind for the winning entry to
    claim -- which is what the FIFO order alone was wrongly assumed to prevent.
    """

    found = _async_staged_cleanup_tickets(hass)
    if found is None:
        return None
    bucket, tickets = found

    def _first(predicate: Callable[[_StagedCleanupTicket], bool]) -> int | None:
        return next(
            (
                position
                for position, ticket in enumerate(tickets)
                if isinstance(ticket, _StagedCleanupTicket) and predicate(ticket)
            ),
            None,
        )

    index: int | None = None
    if entry_id:
        index = _first(lambda ticket: ticket.entry_id == entry_id)
    if index is None and unique_id:
        index = _first(
            lambda ticket: ticket.entry_id is None and ticket.unique_id == unique_id
        )
    if index is None:
        index = _first(
            lambda ticket: ticket.entry_id is None and ticket.unique_id is None
        )

    claimed: _StagedCleanupTicket | None = None
    if index is not None:
        claimed = cast(_StagedCleanupTicket, tickets.pop(index))
    if not tickets:
        bucket.pop(PENDING_CONTAINER_CLEANUP_KEY, None)
    return claimed


@_typed_callback
def _async_claim_container_cleanup(
    hass: HomeAssistant, *, unique_id: str | None, entry_id: str | None = None
) -> list[PendingContainerCleanup]:
    """Claim one ticket (see above) and return its jobs, dropping the rest."""

    claimed = _async_claim_container_cleanup_ticket(
        hass, unique_id=unique_id, entry_id=entry_id
    )
    if claimed is None:
        return []
    return [item for item in claimed.jobs if isinstance(item, PendingContainerCleanup)]


@_typed_callback
def _async_drop_cleanup_tickets(
    hass: HomeAssistant | None,
    predicate: Callable[[_StagedCleanupTicket], bool],
) -> int:
    """Drop every staged ticket matching ``predicate`` *without* executing it.

    The single place that mutates the staging list for a *predicate-addressed*
    discard. The two such addressings -- by flow (an ended flow takes its own
    uncorrelated ticket with it) and by entry (a removed entry takes all of its
    tickets) -- differ only in their predicate, so they share this body and
    cannot drift apart in their bookkeeping. The third discard,
    :func:`async_discard_pending_container_cleanup`, deliberately goes through
    the claim helper instead, because it must drop exactly the one ticket the
    failed setup would have claimed.

    Removal is decided by the ticket count, not by the job count: a matching
    ticket that happens to carry no jobs must still be dropped.

    Args:
        hass: The Home Assistant instance holding the staging bucket.
        predicate: Selects the tickets to drop. Non-ticket objects in the list
            are never passed to it and are always kept.

    Returns:
        The number of discarded jobs, for logging and tests.
    """

    found = _async_staged_cleanup_tickets(hass)
    if found is None:
        return 0
    bucket, tickets = found

    discarded = 0
    kept: list[Any] = []
    for ticket in tickets:
        if isinstance(ticket, _StagedCleanupTicket) and predicate(ticket):
            discarded += len(ticket.jobs)
            continue
        kept.append(ticket)
    if len(kept) != len(tickets):
        tickets[:] = kept
    if not tickets:
        bucket.pop(PENDING_CONTAINER_CLEANUP_KEY, None)
    return discarded


@_typed_callback
def async_discard_pending_container_cleanup_for_entry(
    hass: HomeAssistant | None, *, entry_id: str | None
) -> int:
    """Drop **every** staged ticket that names ``entry_id``, on entry removal.

    Addressed by entry id *only*, deliberately without the account and
    account-less fallbacks of :func:`_async_claim_container_cleanup_ticket`.
    Those fallbacks exist for the create path, where a flow cannot yet know its
    entry id; applying them here would let a removed entry discard the ticket of
    a *different*, still running same-account flow, leaving that flow's watched
    bundle undeleted.

    This addressing alone is therefore **not sufficient** on the removal path,
    and callers must not treat it as such. An entry can outlive an uncorrelated
    ticket that is about it: a create-path ticket marked ``entry_promised``
    keeps ``entry_id is None`` on purpose (the flow could not know the id yet)
    and is deliberately kept by
    :func:`_async_discard_cleanup_ticket_for_flow`, so a setup that never
    succeeded leaves it staged and invisible to this function. Left behind, it
    is claimable by the *next* entry for the same account through rule 2 of
    :func:`_async_claim_container_cleanup_ticket`. ``async_remove_entry``
    therefore pairs this drain with a claim-based
    :func:`async_discard_pending_container_cleanup` call, exactly as the
    duplicate-account abort in ``async_setup_entry`` does. What this function
    guarantees on its own is only the narrower half: every ticket that *names*
    the entry is gone, and no ticket of a foreign flow was touched.

    Every matching ticket is dropped in one pass. There is no upper bound: the
    staging list is finite, and a cutoff would strand exactly the tickets this
    call exists to clear, leaving stale delete jobs in ``hass.data`` for the rest
    of the process lifetime.

    Args:
        hass: The Home Assistant instance holding the staging bucket.
        entry_id: The id of the entry being removed. A falsy value drops
            nothing. No production caller can pass one today (``async_remove_entry``
            dereferences ``entry.entry_id`` earlier), so this is defence in depth
            rather than a reachable path: an unguarded ``ticket.entry_id == None``
            would match precisely the uncorrelated tickets of other flows.

    Returns:
        The number of discarded jobs, for logging and tests.
    """

    if not entry_id:
        return 0
    return _async_drop_cleanup_tickets(hass, lambda ticket: ticket.entry_id == entry_id)


@_typed_callback
def _async_discard_cleanup_ticket_for_flow(
    hass: HomeAssistant | None, flow_id: str
) -> int:
    """Drop the *uncorrelated* ticket of a flow Home Assistant just removed.

    Home Assistant removes a flow from its progress index when the flow ends,
    for **both** endings: ``FlowManager._async_handle_step`` calls
    ``_async_remove_flow_progress`` (and through it the overridable
    ``FlowHandler.async_remove`` hook) only *after* ``async_finish_flow`` has
    run, and ``FlowManager.async_abort`` calls it directly. So on the successful
    create path the entry already exists and ``async_setup_entry`` has already
    claimed this flow's ticket, which makes this a no-op; on the abort path the
    ticket is still staged and is exactly the one that would otherwise be
    mis-claimed by a competing same-account entry.

    Tickets that carry an ``entry_id`` are deliberately **kept**. Those come
    from the update paths, which finish by aborting the flow on purpose
    (``_async_update_entry_and_abort`` and friends): their entry exists, the
    update has been applied and the update listener reloads on it, so the flow
    going away says nothing about the pending cleanup. Discarding them here would
    silently disable the cleanup on every update path.

    Tickets marked ``entry_promised`` are kept for the same reason, one step
    earlier in the lifecycle. The docstring above says "on the successful create
    path the entry already exists and ``async_setup_entry`` has already claimed
    this flow's ticket" -- that holds only when the *first* setup attempt
    succeeded. ``ConfigEntries.async_add`` awaits ``async_setup``, and a setup
    that raises ``ConfigEntryNotReady`` is caught, scheduled for a retry and
    returns normally, so the entry exists, the ticket is still staged and Home
    Assistant removes the flow all the same. Flow removal is therefore an
    ambiguous signal: it fires for "no entry, ever" *and* for "entry created,
    setup retrying". Only the second one must survive, and
    ``entry_promised`` is what tells them apart. Dropping it here would
    contradict :func:`async_discard_pending_container_cleanup`, which already
    states that a retryable abort must leave the job staged until the retry
    succeeds.

    Returns the number of discarded jobs, for logging and tests.
    """

    return _async_drop_cleanup_tickets(
        hass,
        lambda ticket: (
            ticket.flow_id == flow_id
            and ticket.entry_id is None
            and not ticket.entry_promised
        ),
    )


@_typed_callback
def _async_mark_cleanup_ticket_entry_promised(
    hass: HomeAssistant | None, flow_id: str
) -> int:
    """Mark the ticket(s) of ``flow_id`` as belonging to a promised entry.

    Called by the create path immediately after ``async_create_entry`` produced
    its FlowResult, i.e. at the last moment the flow is still executing and the
    first moment an entry is certain to be created. The mark is what keeps the
    ticket alive across the flow removal that Home Assistant performs right
    after ``async_finish_flow`` (see
    :func:`_async_discard_cleanup_ticket_for_flow`).

    Ordering: every call must come *after* the staging it is meant to protect,
    because a ticket is only created on first use, and it is idempotent, so a
    later staging on the same flow can be followed by another call.

    No claim of "the last thing this flow does" is made here, and none would
    hold: ``async_step_discovery_confirm`` inspects the ``CREATE_ENTRY`` FlowResult of
    ``async_step_device_selection`` and stages a further job *afterwards*,
    inside the very same flow. Each site that can produce or follow a
    ``CREATE_ENTRY`` therefore marks for itself; what makes that safe is the
    idempotence above, not an ordering guarantee.

    Deliberately does **not** set ``entry_id``: the flow cannot know it (Home
    Assistant constructs the ``ConfigEntry`` afterwards, in
    ``ConfigEntriesFlowManager.async_finish_flow``), and inventing one would
    break the "a ticket that names its entry is claimable by no other"
    invariant. The account correlation (``unique_id``) that the create path
    already carries stays the selection key.

    Returns the number of marked tickets, for logging and tests.
    """

    found = _async_staged_cleanup_tickets(hass)
    if found is None:
        return 0
    _bucket, tickets = found

    marked = 0
    for ticket in tickets:
        if isinstance(ticket, _StagedCleanupTicket) and ticket.flow_id == flow_id:
            ticket.entry_promised = True
            marked += 1
    return marked


def _parse_stored_modified_at(record: CollMapping[str, Any]) -> datetime | None:
    """Return the ``modified_at`` of a *stored* config-entry record.

    Home Assistant serialises config entries through ``ConfigEntry.as_dict``,
    which writes ``modified_at`` as an ISO 8601 string (see
    ``ConfigEntry.as_storage_fragment``). Anything else -- a missing field, a
    non-string, an unparsable value -- yields ``None``, which the caller reads
    as "no proof", never as "proof".
    """

    raw = record.get("modified_at")
    if not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


async def _async_config_entry_is_persisted(
    hass: HomeAssistant, entry_id: str, *, min_modified_at: datetime | None = None
) -> bool:
    """Return whether ``entry_id`` is present in Home Assistant's entry storage.

    With ``min_modified_at`` the bar is raised from "the entry exists" to "the
    stored record is at least this recent". That is what makes the proof mean
    anything on an *update* path: the entry id was in storage long before the
    update, so mere presence would authorise the irreversible cleanup while the
    debounced save of the new credentials is still pending, and a crash in that
    window would restore the old credentials with the imported bundle already
    deleted. ``ConfigEntries._async_update_entry`` bumps ``modified_at`` right
    before it schedules that save, so the flow can capture the value it has to
    wait for straight off the entry it just updated.

    The deliberate limit of that proof: it establishes "a state at least this
    recent reached storage", not "*our* state reached storage". A second writer
    updating the same entry inside the debounce window lifts the stored
    ``modified_at`` past the watermark on its own. Both writes share that one
    debounced save, so in practice they land together, and the competing writer
    leaves valid credentials behind either way; an exact proof would have to
    carry a digest of the expected payload, which is not worth the coupling
    here. The residue is named rather than hidden.

    This is an *observation*, not a timing assumption: it reads the very file
    Home Assistant writes its config entries to, through the public
    ``homeassistant.helpers.storage.Store`` helper and the public storage
    constants of ``homeassistant.config_entries``. The store is opened
    ``read_only``, so no ``Store`` write path can fire from here, not even the
    migration one. Reading is not perfectly inert, though: a store whose JSON
    does not parse is renamed to ``*.corrupt.*`` by ``Store._async_load_data``
    regardless of ``read_only``. That is Core behaviour on an already unusable
    file, not a decision of this probe, but it is the one side effect a reader
    should know about.

    Using the same ``Store`` API as Home Assistant itself (rather than reading
    the JSON file directly) is also what keeps the probe testable: the standard
    Home Assistant test harness intercepts ``Store``, not the filesystem, so a
    direct file read would silently bypass it. The contract itself is pinned in
    ``tests/test_container_cleanup_persist_probe.py`` against a store that Home
    Assistant wrote.
    """

    store: Store[Mapping[str, Any]] = Store(
        hass,
        config_entries.STORAGE_VERSION,
        config_entries.STORAGE_KEY,
        minor_version=config_entries.STORAGE_VERSION_MINOR,
        read_only=True,
    )
    data = await store.async_load()
    if not isinstance(data, CollMapping):
        return False
    entries = data.get("entries")
    if not isinstance(entries, list):
        return False
    record = next(
        (
            item
            for item in entries
            if isinstance(item, CollMapping) and item.get("entry_id") == entry_id
        ),
        None,
    )
    if record is None:
        return False
    if min_modified_at is None:
        return True
    stored_modified_at = _parse_stored_modified_at(record)
    if stored_modified_at is None:
        return False
    try:
        return stored_modified_at >= min_modified_at
    except TypeError:
        # One side naive, the other aware. Home Assistant stamps ``modified_at``
        # with ``dt_util.utcnow()`` (aware), so this cannot happen against a
        # real core; if it ever does, "cannot compare" must read as "no proof".
        return False


async def _async_wait_for_persisted_entry(
    hass: HomeAssistant,
    entry_id: str,
    *,
    min_modified_at: datetime | None = None,
    timeout: float | None = None,
    interval: float | None = None,
) -> bool:
    """Poll until ``entry_id`` is durably stored, or give up.

    Returns ``False`` on timeout instead of raising: a missing proof must lead
    to *keeping* the credentials, never to failing anything.

    ``timeout`` and ``interval`` default to ``None`` and are resolved from the
    module constants *inside* the body on purpose. As signature defaults they
    would be bound once at definition time, which silently disconnects them from
    the constants: a test that patches ``PERSIST_PROOF_TIMEOUT`` would keep
    waiting the full production budget, and the timeout warning below would
    report a number the loop never used.
    """

    budget = PERSIST_PROOF_TIMEOUT if timeout is None else timeout
    delay = PERSIST_PROOF_POLL_INTERVAL if interval is None else interval
    deadline = time.monotonic() + max(budget, 0.0)
    while True:
        if await _async_config_entry_is_persisted(
            hass, entry_id, min_modified_at=min_modified_at
        ):
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(delay)


async def _async_run_container_cleanup_when_persisted(
    hass: HomeAssistant,
    entry_id: str,
    jobs: list[PendingContainerCleanup],
    *,
    min_modified_at: datetime | None = None,
) -> None:
    """Run ``jobs`` once the entry is provably in Home Assistant's storage.

    ``min_modified_at`` carries the ticket's durability watermark: ``None`` for
    the create path ("the entry must exist"), a timestamp for the update paths
    ("the stored record must be at least this recent").

    Every path that is not a positive durability proof drops the jobs. That is
    the invariant this whole subsystem exists for: it must always fail towards
    "the credentials survive and the cleanup is lost", never towards "the entry
    is gone and the credentials are gone too".
    """

    try:
        persisted = await _async_wait_for_persisted_entry(
            hass, entry_id, min_modified_at=min_modified_at
        )
    except asyncio.CancelledError:
        # Home Assistant cancels an entry's background tasks on shutdown and on
        # unload. Exactly the case Codex flagged: dropping the jobs here is what
        # keeps the credentials on disk when the runtime goes away mid-setup.
        _LOGGER.debug(
            "[%s] Deferred container-login cleanup cancelled before the entry was "
            "durably stored; credential files are kept on disk",
            entry_id,
        )
        raise
    except Exception as err:  # noqa: BLE001 - cleanup must never fail setup
        _LOGGER.warning(
            "[%s] Could not verify that the config entry was stored (%s); the "
            "deferred container-login cleanup is skipped and the credential "
            "files are kept on disk",
            entry_id,
            err,
        )
        return

    if not persisted:
        _LOGGER.warning(
            "[%s] The config entry did not reach Home Assistant storage within "
            "%.0fs (%s); the deferred container-login cleanup is skipped and the "
            "credential files are kept on disk",
            entry_id,
            PERSIST_PROOF_TIMEOUT,
            "no stored record"
            if min_modified_at is None
            else "the stored record stayed older than the update",
        )
        return

    await _async_execute_container_cleanup(hass, jobs)


@_typed_callback
def async_schedule_pending_container_cleanup(
    hass: HomeAssistant, entry: ConfigEntry
) -> asyncio.Task[None] | None:
    """Claim this entry's staged cleanup and arm it behind the durability gate.

    Called from ``async_setup_entry`` after the setup core succeeded -- on the
    initial setup for the create path, and on the reload that every update path
    schedules for itself. Two things happen here, and the split matters:

    * The ticket is claimed **synchronously**, so a reload that runs
      ``async_setup_entry`` again cannot claim it a second time (an irreversible
      job must fire at most once).
    * The jobs are executed from an entry-scoped **background task**, never
      inline. Inline is not merely risky, it is impossible to do correctly:
      ``ConfigEntries.async_add`` awaits ``async_setup_entry`` and only calls
      ``_async_schedule_save`` afterwards, so while this function runs the entry
      has not even been *scheduled* for saving yet. Waiting for the proof inline
      would deadlock against the very write it waits for. Home Assistant
      cancels such background tasks on shutdown and on unload, which is what
      turns "HA stopped mid-setup" into a dropped cleanup instead of a destroyed
      credential.

    The update paths do not deadlock either way -- their save was scheduled
    before the reload -- but they run through the same background task on
    purpose: one gate, one cancellation semantics, one place where the proof is
    defined.

    Returns the task, or ``None`` when nothing was staged for this entry.
    """

    entry_id = getattr(entry, "entry_id", "") or ""
    ticket = _async_claim_container_cleanup_ticket(
        hass,
        unique_id=getattr(entry, "unique_id", None),
        entry_id=entry_id or None,
    )
    if ticket is None:
        return None
    jobs = [item for item in ticket.jobs if isinstance(item, PendingContainerCleanup)]
    if not jobs:
        return None
    return cast(
        "asyncio.Task[None]",
        entry.async_create_background_task(
            hass,
            _async_run_container_cleanup_when_persisted(
                hass, entry_id, jobs, min_modified_at=ticket.min_modified_at
            ),
            name=f"{DOMAIN} deferred container-login cleanup",
        ),
    )


async def _async_execute_container_cleanup(
    hass: HomeAssistant, jobs: list[PendingContainerCleanup]
) -> None:
    """Execute already claimed cleanup jobs (best effort, isolated per job).

    Every job is isolated: a failing delete must not skip the next job, and no
    failure here may turn a working setup into a failed one, because the watcher
    re-imports a surviving file.
    """

    for job in jobs:
        if job.imported_stable_key is not None or job.imported_digest is not None:
            try:
                await _async_delete_watched_secrets(
                    hass,
                    imported_stable_key=job.imported_stable_key,
                    imported_digest=job.imported_digest,
                )
            except Exception as err:  # noqa: BLE001 - cleanup must never fail setup
                _LOGGER.warning("Deferred watched-bundle cleanup failed: %s", err)


@_typed_callback
def async_discard_pending_container_cleanup(
    hass: HomeAssistant, *, unique_id: str | None, entry_id: str | None = None
) -> int:
    """Drop the staged cleanup jobs of an entry *without* executing them.

    For setup paths that end in a **final** ``return False`` (currently the
    duplicate-account guard in ``async_setup_entry``), which never reach the
    cleanup scheduler at the end of the function. Without this the staged job
    would sit in ``hass.data`` for the whole process lifetime and the staging
    area could grow without bound.

    Discards exactly the one ticket this entry would have claimed, using the
    same selection rule as :func:`_async_claim_container_cleanup`: a failed
    setup of entry A must not throw away the cleanup that a concurrent flow B
    staged for its own, still pending entry.

    Discarding, not running, is the deliberate choice: the entry is not being
    set up, so the credentials must stay where they are. The watched
    ``secrets.json`` copies are kept on disk and the secrets watcher re-imports
    them on its next scan.

    This must NOT be called for a retryable abort such as
    ``ConfigEntryNotReady``: Home Assistant retries that setup, and the job has
    to survive until the retry succeeds.

    Args:
        hass: The Home Assistant instance holding the staging bucket.
        unique_id: The unique id of the entry whose jobs are dropped.
        entry_id: The id of that entry, so an existing-entry update ticket
            addressed to it is dropped in preference to an uncorrelated one.

    Returns:
        The number of discarded jobs (0 when nothing was staged).
    """

    # Reuses the claim helper on purpose: the ticket selection and the
    # bookkeeping (account-less fallback, dropping the empty container) live
    # there and must not be duplicated.
    return len(
        _async_claim_container_cleanup(hass, unique_id=unique_id, entry_id=entry_id)
    )


def _secrets_key_status(parsed: dict[str, Any]) -> tuple[bool, bool]:
    """Report which E2EE keys are present in a parsed secrets bundle.

    The single-key rule: ``has_shared`` is the only gate that decides whether a
    bundle is importable. A bundle without a ``shared_key`` is a non-renewable
    dead end (the owner key can only be refreshed with the shared key), so it
    must be blocked regardless of the owner key. ``has_owner`` does NOT gate the
    validation; it only selects the wording of the D2 seeding warning (an
    owner-only bundle decrypts own-device locations until the owner key rotates,
    whereas a bundle with neither key decrypts nothing).

    A value counts as present only when it is a non-empty string after stripping
    surrounding whitespace; no further structural assumptions are made about the
    bundle.

    Args:
        parsed: A parsed (and normally already whitespace-normalized) bundle.

    Returns:
        A ``(has_shared, has_owner)`` tuple of booleans.
    """

    def _present(value: Any) -> bool:
        return isinstance(value, str) and bool(value.strip())

    return _present(parsed.get("shared_key")), _present(parsed.get("owner_key"))


def _reject_if_shared_key_missing(
    parsed: dict[str, Any], errors: dict[str, str], *, slot: str = "base"
) -> bool:
    """Apply the single-key import gate to ``parsed``; report blocked bundles.

    The single-key rule (see :func:`_secrets_key_status`) must hold at *every*
    import surface. This is the one named guard the reauth confirm flow uses at
    both of its persist sub-paths so the rule has a single call form rather than
    being re-inlined per branch. When the bundle has no usable ``shared_key`` the
    guard writes ``keys_missing`` into ``errors[slot]`` and returns ``True`` so
    the caller can fall through to re-show the form without persisting.

    Args:
        parsed: An already-normalized secrets bundle.
        errors: The flow's error mapping, mutated in place when blocked.
        slot: The error slot to populate (``"base"`` for surfaces without a
            dedicated field, like reauth).

    Returns:
        ``True`` if the bundle was rejected (caller must not persist), else
        ``False``.
    """
    has_shared, _has_owner = _secrets_key_status(parsed)
    if has_shared:
        return False
    errors[slot] = "keys_missing"
    return True


# ---------------------------
# API probing helpers (signature-robust)
# ---------------------------
async def _try_probe_devices(
    api: GoogleFindMyAPI, *, email: str, token: str
) -> list[dict[str, Any]]:
    """Call the API to fetch a basic device list using defensive signatures."""
    caller = cast(
        Callable[..., Awaitable[list[dict[str, Any]]]],
        api.async_get_basic_device_list,
    )
    try:
        return await caller(username=email, token=token)
    except TypeError:
        pass
    try:
        return await caller(email=email, token=token)
    except TypeError:
        pass
    try:
        return await caller(email=email)
    except TypeError:
        pass
    return await caller()


async def _async_new_api_for_probe(
    hass: HomeAssistant,
    email: str,
    token: str,
    *,
    secrets_bundle: dict[str, Any] | None = None,
) -> GoogleFindMyAPI:
    """Create a fresh, ephemeral API instance for pre-flight validation."""
    factory = cast(Callable[..., "GoogleFindMyAPI"], await _async_import_api(hass))
    try:
        return factory(
            oauth_token=token,
            google_email=email,
            secrets_bundle=secrets_bundle,
        )
    except TypeError:
        try:
            return factory(
                token=token,
                email=email,
                secrets_bundle=secrets_bundle,
            )
        except TypeError:
            return factory()


async def async_pick_working_token(
    hass: HomeAssistant,
    email: str,
    candidates: list[tuple[str, str]],
    *,
    secrets_bundle: dict[str, Any] | None = None,
) -> str | None:
    """Try the candidate tokens in order until one passes a minimal online validation."""
    for source, token in candidates:
        try:
            api = await _async_new_api_for_probe(
                hass,
                email=email,
                token=token,
                secrets_bundle=secrets_bundle,
            )
            await _try_probe_devices(api, email=email, token=token)
            _LOGGER.debug(
                "Token probe succeeded.",
                extra={
                    "token_source": _probe_source_label(source),
                },
            )
            return token
        except DependencyNotReady:
            raise
        except Exception as err:  # noqa: BLE001
            if _is_multi_entry_guard_error(err):
                _LOGGER.debug(
                    (
                        "Token probe guarded but accepted; deferring to entry-scoped "
                        "caches for multi-account setup."
                    ),
                    extra={
                        "token_source": _probe_source_label(source),
                    },
                )
                return token
            key = _map_api_exc_to_error_key(err)
            _LOGGER.debug(
                "Token probe failed; mapped error key.",
                extra={
                    "token_source": _probe_source_label(source),
                    "error_key": key,
                },
                exc_info=err,
            )
            continue
    return None


# Candidate source names that may appear in a log line. Any other name is logged
# as "other": discovery payloads can bring their own names (mapping keys, a
# ``label`` field), and the source travels in the same tuple as the token.
_PROBE_SOURCE_NAMES: tuple[str, ...] = (
    "Auth",
    "OAuthToken",
    "aas_token",
    "access_token",
    "admToken",
    "adm_token",
    "candidate_tokens",
    "candidates",
    "fcm_installation",
    "fcm_registration",
    "manual",
    "oauthToken",
    "oauth_token",
    "token",
    "tokens",
)


def _probe_source_label(source: object) -> str:
    """Return a fixed log name for a token candidate source.

    The name is taken from ``_PROBE_SOURCE_NAMES`` by comparison, so no data
    flows from the candidate tuple into the log line. Indexed discovery names
    (``tokens_0``) keep their prefix; everything else is ``other``.
    """

    for name in _PROBE_SOURCE_NAMES:
        if source == name:
            return name
    if isinstance(source, str):
        for name in _PROBE_SOURCE_NAMES:
            suffix = source.removeprefix(f"{name}_")
            if suffix != source and suffix.isdigit():
                return f"{name}_n"
    return "other"


def _cand_labels(candidates: list[tuple[str, str]]) -> str:
    """Return a redacted, human-readable list of token candidate sources."""
    sources = {_probe_source_label(source) for source, _token in candidates if source}
    if not sources:
        return "none"
    return ", ".join(sorted(sources))


def _log_token_validation_failure(*, candidates: list[tuple[str, str]]) -> None:
    """Log a sanitized token validation failure with candidate metadata."""

    _LOGGER.warning(
        "Token validation failed; no working tokens among candidates. Please re-enter your credentials to refresh expired tokens.",
        extra={
            "candidate_sources": _cand_labels(candidates),
        },
    )


# ---------------------------
# Shared interpreter for either/or credential choice (initial flow & options)
# ---------------------------
def _interpret_credentials_choice(
    user_input: dict[str, Any],
    *,
    secrets_field: str,
    token_field: str,
    email_field: str,
) -> tuple[str | None, str | None, list[tuple[str, str]] | None, str | None]:
    """Normalize flow input into a single authentication method.

    Returns:
        (method, email, token_candidates, error_key)
        - method: "secrets" | "manual" | None
        - email: normalized email string or None
        - token_candidates: list[(source_label, token)] in preference order
        - error_key: translation key if a validation error is detected
    """
    secrets_json = (user_input.get(secrets_field) or "").strip()
    oauth_token = (user_input.get(token_field) or "").strip()
    google_email = (user_input.get(email_field) or "").strip()

    has_secrets = bool(secrets_json)
    has_token = bool(oauth_token)
    has_email = bool(google_email)

    # Disallow mixing; require exactly one path.
    if has_secrets and (has_token or has_email):
        return None, None, None, "choose_one"
    if not has_secrets and not (has_token and has_email):
        return None, None, None, "choose_one"

    if has_secrets:
        try:
            parsed = json.loads(secrets_json)
            if not isinstance(parsed, dict):
                raise TypeError()
            parsed = normalize_secrets_bundle(parsed)
        except (json.JSONDecodeError, TypeError):
            return "secrets", None, None, "invalid_json"

        has_shared, _has_owner = _secrets_key_status(parsed)
        if not has_shared:
            # Single-key rule: a bundle without a shared_key is a non-renewable
            # dead end (the owner key can never be refreshed) and is blocked,
            # regardless of whether an owner_key is present.
            return "secrets", None, None, "keys_missing"

        email = _extract_email_from_secrets(parsed) or ""
        cands = _extract_oauth_candidates_from_secrets(parsed)
        if not (_email_valid(email) and cands):
            return "secrets", None, None, "invalid_token"
        return "secrets", email, cands, None

    # Manual path: basic plausibility and shape-based negative checks
    if not (_email_valid(google_email) and _token_plausible(oauth_token)):
        return "manual", None, None, "invalid_token"
    if _disqualifies_for_persistence(oauth_token):  # only rejects JWT now
        return "manual", None, None, "invalid_token"

    return "manual", google_email, [("manual", oauth_token)], None


# ---------------------------
# Reauth-specific helpers
# ---------------------------
_REAUTH_FIELD_SECRETS = "secrets_json"
_REAUTH_FIELD_TOKEN = "new_oauth_token"

# Mutually exclusive credential inputs of the options form, in the order they
# appear. ``new_oauth_token`` stays listed although its UI input is commented out
# (broken manual path, see agents/config_flow/AGENTS.md): if it is ever
# re-enabled the exclusivity check must already cover it. The reauth form has no
# counterpart any more: it offers exactly one field, so there is nothing to be
# exclusive about.
_OPTIONS_CREDENTIAL_FIELDS: tuple[str, ...] = (
    "new_secrets_json",
    "new_oauth_token",
)


def _interpret_reauth_choice(
    user_input: dict[str, Any],
) -> tuple[str | None, Any | None, str | None]:
    """Interpret reauth input where the email is fixed by the entry."""
    secrets_raw = (user_input.get(_REAUTH_FIELD_SECRETS) or "").strip()
    token_raw = (user_input.get(_REAUTH_FIELD_TOKEN) or "").strip()

    has_secrets = bool(secrets_raw)
    has_token = bool(token_raw)

    if (has_secrets and has_token) or (not has_secrets and not has_token):
        return None, None, "choose_one"

    if has_secrets:
        try:
            parsed = json.loads(secrets_raw)
            if not isinstance(parsed, dict):
                raise TypeError()
            parsed = normalize_secrets_bundle(parsed)
        except (json.JSONDecodeError, TypeError):
            return None, None, "invalid_json"

        email = _extract_email_from_secrets(parsed)
        candidates = _extract_oauth_candidates_from_secrets(parsed)
        if not (email and _email_valid(email) and candidates):
            return None, None, "invalid_token"
        return "secrets", parsed, None

    # The manual *reauth* token path is gone, not hidden (see
    # agents/config_flow/AGENTS.md); the initial-setup and options surfaces are
    # still merely hidden. ``_REAUTH_FIELD_TOKEN`` is still read above so that a
    # submission carrying both halves is rejected instead of letting the bundle
    # half win. For a token-only submission the outcome is the same either way:
    # it ends here with ``choose_one``, and without the read it would end at the
    # exclusivity check above with the same key. That half of the read is
    # inertia, not protection. Either way it is a rejection and not a path: no
    # ``return`` in this function can produce ``"manual"``, which is what
    # ``test_manual_reauth_removal_guard.py`` pins.
    return None, None, "choose_one"


def _resolve_entry_email_for_lookup(
    entry: ConfigEntry,
) -> tuple[str | None, str | None]:
    """Return the raw and normalized email associated with ``entry``."""

    global _RESOLVE_ENTRY_EMAIL
    if _RESOLVE_ENTRY_EMAIL is None:
        try:
            integration = import_integration_package()

            candidate = getattr(integration, "_resolve_entry_email")
        except Exception:  # pragma: no cover - fallback for stubs
            candidate = None

        if not callable(candidate):

            def _fallback(entry: ConfigEntry) -> tuple[str | None, str | None]:
                raw_email: str | None = None
                for container in (
                    getattr(entry, "data", {}),
                    getattr(entry, "options", {}),
                ):
                    if not isinstance(container, CollMapping):
                        continue
                    candidate_email = container.get(CONF_GOOGLE_EMAIL)
                    if isinstance(candidate_email, str) and candidate_email.strip():
                        raw_email = candidate_email.strip()
                        break
                normalized_email = normalize_email(raw_email)
                return raw_email, normalized_email

            _RESOLVE_ENTRY_EMAIL = _fallback
        else:
            _RESOLVE_ENTRY_EMAIL = cast(_ResolveEntryEmailCallable, candidate)

    resolver = _RESOLVE_ENTRY_EMAIL
    raw_email: str | None
    normalized_email: str | None
    try:
        raw_email, normalized_email = resolver(entry)
    except Exception as err:  # pragma: no cover - defensive guard
        _LOGGER.debug(
            "Failed to resolve email for entry %s during lookup: %s",
            getattr(entry, "entry_id", "<unknown>"),
            err,
        )
        return None, None
    return raw_email, normalized_email


def _find_entry_by_email(hass: HomeAssistant, email: str) -> ConfigEntry | None:
    """Return an existing entry that matches the normalized email, if any."""

    target = normalize_email(email)
    if not target:
        return None

    for candidate in hass.config_entries.async_entries(DOMAIN):
        _, normalized = _resolve_entry_email_for_lookup(candidate)
        if normalized and normalized == target:
            return candidate
    return None


#: Credential keys an account may or may not carry. They are removed from the
#: entry when the freshly discovered credentials do not carry them, which a flat
#: merge cannot do on its own -- hence the full-data payload built by
#: :func:`_merge_credential_updates`, and hence the absence check in
#: :func:`_entry_carries_credentials`.
#:
#: The removal has a second, deferred half: the entry-scoped token cache mirrors
#: these keys, and ``async_setup_entry`` would seed a removed one straight back
#: from that mirror. It therefore reads the same constant from ``const.py``,
#: which is the single source of truth for both halves -- a local copy here and
#: a local copy there is precisely how the two could drift apart.
_OPTIONAL_CREDENTIAL_KEYS: Final = OPTIONAL_CREDENTIAL_KEYS


def _entry_carries_credentials(entry: ConfigEntry, updates: Mapping[str, Any]) -> bool:
    """Whether ``entry`` holds exactly the credentials ``updates`` describes.

    The evidence that Home Assistant's unique-id guard performed the write, read
    off the entry instead of inferred from the exception that followed. The
    guard writes ``{**entry.data, **updates}``, so a landed write means every
    pair in ``updates`` is present verbatim; a guard that returned early leaves
    the entry untouched and fails this test.

    Presence alone is not enough, though. ``updates`` is the intended entry data
    in full, and :func:`_merge_credential_updates` expresses a *removal* by
    leaving an optional credential key out of it. A flat merge cannot act on
    that: the guard leaves the superseded ``secrets_data`` or ``aas_token``
    behind, where the reload would seed it into the entry-scoped token cache
    next to the new credentials. So every optional key missing from ``updates``
    must also be missing from the entry; where it is not, the caller's wholesale
    write is still owed.

    Callers must not treat ``AbortFlow`` alone as proof: the guard returns
    silently when the flow has no unique id or when no entry claims it (a legacy
    entry without ``unique_id``, or one whose account identity has moved), and
    :meth:`_async_prepare_account_context` then raises its own ``AbortFlow``
    without anything having been written.
    """

    data = getattr(entry, "data", None)
    if not isinstance(data, Mapping):
        return False
    if not all(key in data and data[key] == value for key, value in updates.items()):
        return False
    return all(
        key not in data for key in _OPTIONAL_CREDENTIAL_KEYS if key not in updates
    )


def _claim_entry_reload(hass: HomeAssistant, entry_id: str) -> bool:
    """Return whether this flow has to schedule the reload of ``entry_id`` itself.

    Every path here that writes credentials also goes through
    ``async_update_entry``, which notifies the integration's update listener, and
    that listener reloads the entry so the new credentials take effect *where it
    exists*: an entry that is not loaded has no listener any more, Home Assistant
    removes it on unload, which is why every writing path schedules its own reload
    rather than relying on it. Without an agreement on one owner, two of them
    produce two consecutive unload/setup cycles -- ``async_schedule_reload`` does
    not coalesce.
    The latch in the integration package is that agreement; see
    ``claim_pending_entry_reload``.

    Fails **open**: without an entry id, against an integration package that does
    not carry the latch, and when consulting it raises, this returns ``True`` and
    the caller reloads as before. One reload too many is a nuisance, a missing one
    leaves written credentials ineffective. That is deliberately the opposite
    answer to ``claim_pending_entry_reload``, which reports "no latch for you" for
    an empty id; the question differs (may I reload versus did I get the latch).
    """

    if not entry_id:
        return True
    try:
        integration = import_integration_package()
        claim = getattr(integration, "claim_pending_entry_reload", None)
        if not callable(claim):
            return True
        return bool(claim(hass, entry_id))
    except Exception:  # noqa: BLE001 - never let bookkeeping block a reload
        _LOGGER.debug(
            "Could not consult the reload latch for entry %s; reloading anyway",
            entry_id,
            exc_info=True,
        )
        return True


def _discard_entry_reload(hass: HomeAssistant, entry_id: str) -> None:
    """Release the reload latch of ``entry_id`` after a claim came to nothing.

    A claim is a promise to reload. Where that promise cannot be kept -- the
    scheduling call is missing or raises -- the latch has to go back, otherwise it
    stays set for the lifetime of the process and swallows every later reload of
    that entry: the release points (unload, setup, removal) all presuppose that a
    reload actually arrived.
    """

    if not entry_id:
        return
    try:
        integration = import_integration_package()
        discard = getattr(integration, "discard_pending_entry_reload", None)
        if callable(discard):
            discard(hass, entry_id)
    except Exception:  # noqa: BLE001 - bookkeeping must not raise into a flow
        _LOGGER.debug(
            "Could not release the reload latch for entry %s",
            entry_id,
            exc_info=True,
        )


def _schedule_claimed_reload(hass: HomeAssistant, entry_id: str) -> bool:
    """Schedule the one reload of ``entry_id`` and report whether it was ours.

    Single owner for the paths that route through it, which is no longer only
    the credential paths: the semantic-location, subentry repair and device
    visibility steps go through here too. Their fallbacks differ, and the
    difference decides what a stand-down actually costs. Visibility is the one
    with no fallback at all: un-ignoring a device reverses a registry removal,
    and the monotone per-setup known-sets in the platforms mean no poll rebuilds
    the entity. A stand-down there costs the whole thing, not a half. The credential update listener is a fallback for
    neither of them, because it returns early on an unchanged fingerprint. The
    semantic steps have a stronger one in its place: ``_apply_semantic_mapping``
    reads ``entry.options`` live on every payload, and ``async_update_entry``
    writes into the very entry object the coordinator holds, so the mapping is
    effective without any reload and the reload only pulls the first application
    forward through the first refresh at the end of the setup. The subentry
    repair steps are the ones that need it, and only for one half: their metadata
    still reaches the coordinator at the next poll, because
    ``_refresh_subentry_index`` re-reads ``entry.subentries`` there as well, but
    the entity-to-subentry binding is handed out at platform setup and changes on
    a reload only.

    It is deliberately **not** the only claimant in this module. Three paths
    still take ``_claim_entry_reload`` and call ``async_reload`` themselves: the
    non-interactive discovery update, ``_async_reload_entry_after_reconfigure``
    and ``_finalize_success``. All three run the same hopeless-state check
    before their claim, so none of them bypasses the gate below. What still
    differs is what they do with the *result*: the discovery update evaluates it
    and hands the latch back at a dead end, the credentials finalizer registers a
    done callback, and the reconfigure path delegates a falsy result to the core
    scheduler instead of releasing.
    Routing them here is a separate change with its own regression surface;
    until then a latch they leak also blocks the steps routed here, so do not
    read this docstring as "every path in this flow". Order matters twice: the
    availability of
    ``async_schedule_reload`` is checked **before** the claim, so an old core
    without it does not burn the latch, and the claim happens **last**, so the
    window between claim and scheduling stays as short as it can be. If the
    scheduling call still raises, the latch is released again instead of being
    left behind.

    Returns ``True`` when this call scheduled the reload, ``False`` when another
    owner already has it or scheduling was not possible.
    """

    schedule_reload = getattr(
        getattr(hass, "config_entries", None), "async_schedule_reload", None
    )
    if not callable(schedule_reload):
        _LOGGER.debug(
            "async_schedule_reload is unavailable; no reload was scheduled for "
            "entry %s, so whatever about this write needs a setup stays pending "
            "until the entry is next set up successfully",
            entry_id,
        )
        return False

    if _entry_reload_is_hopeless(hass, entry_id):
        # No second lookup to name the state: for two of the three reasons the
        # state would actively mislead (a disabled or ignored entry sits in a
        # perfectly recoverable ``NOT_LOADED``), and an entry that vanished in
        # between would produce a "state=None" line that contradicts the verdict
        # above it. The message names the reason set instead.
        _LOGGER.debug(
            "Not claiming the reload latch for entry %s: it is disabled, ignored "
            "or in a terminal state, so a reload would not reach a setup and "
            "nothing would hand the latch back",
            entry_id,
        )
        return False

    if not _claim_entry_reload(hass, entry_id):
        _LOGGER.debug(
            "Reload of entry %s not scheduled here; a reload is already on its way",
            entry_id,
        )
        return False

    try:
        schedule_reload(entry_id)
    except Exception:  # noqa: BLE001 - the write itself already landed
        _LOGGER.exception(
            "Failed to schedule the reload of entry %s after writing to it",
            entry_id,
        )
        _discard_entry_reload(hass, entry_id)
        return False
    return True


def _release_claim_when_reload_fails(
    hass: HomeAssistant, entry_id: str, task: Any
) -> None:
    """Hand the reload latch back when ``task`` ends without having reloaded.

    ``_schedule_claimed_reload`` only covers the synchronous half of the
    promise: a scheduling call that raises gives the latch back right there. A
    path that reloads *directly* keeps the promise open for the whole lifetime
    of its task, and that task can still end without a reload -- Home
    Assistant's ``async_unload`` raises ``OperationNotAllowed`` for an entry in
    a lifecycle state that forbids it, and the flow task dies with it.

    The release points (unload, setup, entry removal) all presuppose that a
    reload actually arrived, so a latch left behind here is permanent: every
    later credential write sees the stale claim, stands down, and its newly
    stored credentials stay ineffective until the entry is restarted or
    removed. A cancelled task counts as "no reload" for the same reason, and so
    does a task that merely *returns* falsy: ``async_reload`` reports a failed
    unload, and a component it could not set up, by returning ``False`` rather
    than by raising. A *disabled* entry is deliberately not on that list -- it
    returns the truthy unload result without ever reaching a setup, so only the
    check before the claim (``_entry_reload_is_hopeless``) catches it.

    Releasing once too often only risks one reload too many, which is the
    direction this latch deliberately fails towards (see
    ``_claim_entry_reload``).
    """

    add_done_callback = getattr(task, "add_done_callback", None)
    if not callable(add_done_callback):
        return

    def _on_done(finished: Any) -> None:
        try:
            failure: Any = True if finished.cancelled() else finished.exception()
        except Exception:  # noqa: BLE001 - a stub future need not answer at all
            return
        if not failure:
            # No exception is not the same as "reloaded". ``async_reload``
            # returns ``False`` without raising when the unload failed and when
            # the component could not be set up; the release points do not run
            # in those cases either, so the latch would stay behind just as
            # permanently as after a raising task. (A disabled entry returns the
            # truthy unload result instead and is caught before the claim.)
            result = getattr(finished, "result", None)
            if not callable(result):
                # A double without a result protocol says nothing either way,
                # and a latch is never released on a guess.
                return
            try:
                reloaded = result()
            except Exception:  # noqa: BLE001 - same reason as above
                return
            if reloaded:
                return
            # Falsy has three causes and only one of them leaves the latch
            # behind; the shared helper carries that distinction, so the direct
            # reload paths and this callback cannot drift apart on it.
            if not _falsy_reload_left_the_latch_behind(hass, entry_id):
                return
            failure = True
        # Warned about, not whispered: the flow has already told the user that
        # the change was applied, and the credentials it wrote stay ineffective
        # until the entry is reloaded by something else or Home Assistant
        # restarts. Taking ``exception()`` above also consumes it, so Python's
        # own "Task exception was never retrieved" no longer fires -- this line
        # is the only remaining trace.
        _LOGGER.warning(
            "Reload of entry %s ended without reloading; its new credentials "
            "stay ineffective until the entry is reloaded. Releasing the latch "
            "so a later write can schedule one",
            entry_id,
            exc_info=failure if isinstance(failure, BaseException) else None,
        )
        _discard_entry_reload(hass, entry_id)

    with suppress(Exception):  # a stub task need not accept a callback
        add_done_callback(_on_done)


async def _async_coalesce_account_entries(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> ConfigEntry | None:
    """Invoke the integration's coalesce helper to merge duplicate entries."""

    global _COALESCE_ENTRIES
    if _COALESCE_ENTRIES is None:
        integration = import_integration_package()

        candidate = getattr(integration, "async_coalesce_account_entries", None)

        async def _noop(_: HomeAssistant, __: ConfigEntry) -> ConfigEntry | None:
            return None

        if callable(candidate):

            async def _wrapped(
                hass_obj: HomeAssistant, entry_obj: ConfigEntry
            ) -> ConfigEntry | None:
                return await cast(
                    Callable[..., Awaitable[ConfigEntry | None]],
                    candidate,
                )(hass_obj, canonical_entry=entry_obj)

            _COALESCE_ENTRIES = _wrapped
        else:
            _COALESCE_ENTRIES = _noop

    coalesce = _COALESCE_ENTRIES
    try:
        return await coalesce(hass, entry)
    except Exception as err:  # pragma: no cover - defensive best-effort
        _LOGGER.debug(
            "Coalesce helper failed for entry %s: %s",
            getattr(entry, "entry_id", "<unknown>"),
            err,
        )
        return None


def _ensure_optional_entry_attributes(entry: ConfigEntry) -> None:
    """Ensure optional ConfigEntry attributes exist before flow helpers access them."""

    if entry is None:
        return

    sentinel = object()
    optional_defaults: Mapping[str, Any] = {
        "source": None,
    }

    for attribute, default in optional_defaults.items():
        if getattr(entry, attribute, sentinel) is sentinel:
            try:
                setattr(entry, attribute, default)
            except Exception:  # pragma: no cover - stub compatibility guard
                # Some stubs may define __slots__ or otherwise block new attributes.
                # In that case the attribute remains unavailable and the caller must
                # guard access via getattr(..., None).
                continue


# ---------------------------
# Discovery helpers
# ---------------------------


class DiscoveryFlowError(HomeAssistantErrorBase):
    """Raised when a discovery payload cannot be processed."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(slots=True)
class CloudDiscoveryData:
    """Normalized discovery payload shared by cloud setup hooks."""

    email: str
    unique_id: str
    candidates: tuple[tuple[str, str], ...]
    secrets_bundle: Mapping[str, Any] | None
    title: str | None = None
    # Which producer assembled the payload (``discovery_source``). Kept because
    # the flow context does not survive as a discriminator: discovery.py
    # downgrades every non-Home-Assistant source to plain ``discovery``, so the
    # tracker rescan and a genuine credential import arrive indistinguishable
    # unless the payload itself says who sent it. ``None`` means the payload
    # carried no marker, which is treated as a credential import: asking one
    # question too many beats replacing working credentials unasked.
    source: str | None = None


def _normalize_and_validate_discovery_payload(
    payload: Mapping[str, Any] | None,
) -> CloudDiscoveryData:
    """Normalize raw discovery metadata into a structured payload."""

    if not isinstance(payload, Mapping):
        raise DiscoveryFlowError("invalid_discovery_info")

    payload_dict = dict(payload)
    email_raw = payload_dict.get(CONF_GOOGLE_EMAIL) or payload_dict.get("email")
    if isinstance(email_raw, str):
        email_candidate = email_raw.strip()
    else:
        email_candidate = ""

    secrets_bundle: Mapping[str, Any] | None = None
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _add_candidate(label: str, token: Any) -> None:
        if isinstance(token, str) and _token_plausible(token) and token not in seen:
            candidates.append((label, token))
            seen.add(token)

    secrets_raw = (
        payload_dict.get(DATA_SECRET_BUNDLE)
        or payload_dict.get("secrets_json")
        or payload_dict.get("secrets")
    )
    if isinstance(secrets_raw, str):
        try:
            secrets_raw = json.loads(secrets_raw)
        except json.JSONDecodeError as err:
            raise DiscoveryFlowError("invalid_discovery_info") from err

    if isinstance(secrets_raw, Mapping):
        # Normalize whitespace here -- not only in the str-decode branch above --
        # so already-parsed mapping payloads from in-repo cloud discovery
        # (discovery.py forwards DATA_SECRET_BUNDLE as a dict) receive the same
        # cleanup. A stray space in owner_key/shared_key breaks AES-GCM
        # decryption. normalize_secrets_bundle is idempotent and copies its input.
        secrets_dict = dict(normalize_secrets_bundle(secrets_raw))
        # Single-key rule (parity with the paste/options import surfaces): a
        # discovered bundle without a shared_key is a non-renewable dead end and
        # is rejected here instead of being accepted as a valid discovery.
        has_shared, _has_owner = _secrets_key_status(secrets_dict)
        if not has_shared:
            raise DiscoveryFlowError("keys_missing")
        secrets_bundle = MappingProxyType(secrets_dict)
        email_from_secrets = _extract_email_from_secrets(secrets_dict)
        if email_from_secrets:
            email_candidate = email_candidate or email_from_secrets
        for label, token in _extract_oauth_candidates_from_secrets(secrets_dict):
            _add_candidate(label, token)

    for key in (
        "candidate_tokens",
        "candidates",
        "tokens",
    ):
        value = payload_dict.get(key)
        if isinstance(value, str):
            _add_candidate(key, value)
        elif isinstance(value, Mapping):
            for label, token in value.items():
                _add_candidate(str(label), token)
        elif isinstance(value, CollIterable):
            for idx, token in enumerate(value):
                if isinstance(token, Mapping):
                    label = str(token.get("label") or token.get("source") or key)
                    _add_candidate(label, token.get("token"))
                else:
                    _add_candidate(f"{key}_{idx}", token)

    for direct_key, label in (
        (CONF_OAUTH_TOKEN, CONF_OAUTH_TOKEN),
        ("oauth_token", "oauth_token"),
        ("token", "token"),
        ("aas_token", "aas_token"),
    ):
        _add_candidate(label, payload_dict.get(direct_key))

    if not (_email_valid(email_candidate) and email_candidate):
        raise DiscoveryFlowError("invalid_discovery_info")

    if not candidates:
        raise DiscoveryFlowError("cannot_connect")

    normalized_email = normalize_email(email_candidate)
    if not normalized_email:
        raise DiscoveryFlowError("invalid_discovery_info")
    title = payload_dict.get("title") or payload_dict.get("name")
    unique_id = unique_account_id(normalized_email)
    if unique_id is None:
        raise DiscoveryFlowError("invalid_discovery_info")

    source = payload_dict.get("discovery_source")
    return CloudDiscoveryData(
        email=email_candidate,
        unique_id=unique_id,
        candidates=tuple(candidates),
        secrets_bundle=secrets_bundle,
        title=str(title) if isinstance(title, str) else None,
        source=source if isinstance(source, str) and source else None,
    )


def _merge_credential_updates(
    entry_data: Mapping[str, Any], auth_data: Mapping[str, Any]
) -> dict[str, Any]:
    """Merge the discovered credential delta onto ``entry_data``.

    The result is the *entry data itself*, flat: that is what
    ``_abort_if_unique_id_configured(updates=...)`` merges into the entry
    (``data={**entry.data, **updates}``) and what
    :meth:`_async_write_entry_credentials` writes verbatim. A full payload is
    required rather than the bare delta because a flat merge can only add or
    replace keys, never drop them, and credentials that no longer include a
    secrets bundle or an ``aas_et/`` token must not leave the old ones behind.

    Callers must apply this as late as possible -- against the entry data as it
    is when the write happens, not as it was when a form was shown. A payload
    built before a confirmation dialog is a snapshot, and writing a snapshot
    rolls back whatever else touched the entry meanwhile (the options flow
    refreshes credentials through ``_async_options_container_persist``, for
    one).
    """

    merged = {**entry_data, **auth_data}
    for key in _OPTIONAL_CREDENTIAL_KEYS:
        if key not in auth_data:
            merged.pop(key, None)
    return merged


async def _ingest_discovery_credentials(
    flow: ConfigFlow,
    discovery: CloudDiscoveryData,
    *,
    existing_entry: ConfigEntry | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Validate discovery credentials and prepare flow + entry payloads.

    The second element is the ``updates`` payload for
    ``_abort_if_unique_id_configured``. Home Assistant merges it *flat* into the
    entry (``data={**entry.data, **updates}``, ``config_entries.py``), so it must
    be the entry data itself, not ``{"data": ...}``: a nested payload would leave
    every credential on its old value and add a stray ``data`` key inside
    ``entry.data`` instead.

    That second element is only good for an immediate write. It is merged from
    ``existing_entry`` as it is *now*; a caller that shows a form before writing
    must carry the first element (the delta) across it and merge again through
    :func:`_merge_credential_updates`, or it will roll back whatever touched the
    entry meanwhile.
    """

    candidates = list(discovery.candidates)
    secrets_bundle = (
        dict(discovery.secrets_bundle) if discovery.secrets_bundle is not None else None
    )

    hass = cast(HomeAssistant, getattr(flow, "hass", None))
    if hass is None:
        raise DiscoveryFlowError("unknown")

    try:
        chosen = await async_pick_working_token(
            hass,
            discovery.email,
            candidates,
            secrets_bundle=secrets_bundle,
        )
    except DependencyNotReady as err:
        raise DiscoveryFlowError("dependency_not_ready") from err
    except Exception as err:  # noqa: BLE001
        raise DiscoveryFlowError(_map_api_exc_to_error_key(err)) from err

    if not chosen:
        raise DiscoveryFlowError("cannot_connect")

    to_persist = chosen
    alt_candidate = next(
        (
            token
            for _label, token in candidates
            if not _disqualifies_for_persistence(token)
        ),
        None,
    )
    if _disqualifies_for_persistence(to_persist) and alt_candidate:
        to_persist = alt_candidate

    auth_method = _AUTH_METHOD_SECRETS if secrets_bundle else _AUTH_METHOD_INDIVIDUAL
    auth_data: dict[str, Any] = {
        DATA_AUTH_METHOD: auth_method,
        CONF_GOOGLE_EMAIL: discovery.email,
        CONF_OAUTH_TOKEN: to_persist,
    }
    if secrets_bundle:
        auth_data[DATA_SECRET_BUNDLE] = secrets_bundle
    else:
        auth_data.pop(DATA_SECRET_BUNDLE, None)

    if isinstance(to_persist, str) and to_persist.startswith("aas_et/"):
        auth_data[DATA_AAS_TOKEN] = to_persist
    else:
        auth_data.pop(DATA_AAS_TOKEN, None)

    if existing_entry is not None:
        updates: dict[str, Any] | None = _merge_credential_updates(
            existing_entry.data, auth_data
        )
    else:
        updates = None

    return auth_data, updates


class _ContainerLoginMixin:
    """Shared cleanup-ticket helpers for the config and options flows.

    Both ``ConfigFlow`` and ``OptionsFlowHandler`` can stage a deferred delete
    for a ``secrets.json`` copy they imported off disk, so the ticket bookkeeping
    lives here as the single source.

    The annotated slot below is a *declaration*, not a default: the concrete
    handlers create it in their ``__init__``, and every read here goes through
    ``getattr`` with a default so a directly instantiated handler cannot trip
    over a missing attribute.
    """

    hass: HomeAssistant
    _container_cleanup_ticket_id: str | None

    @_typed_callback
    def _async_cleanup_ticket_id(self) -> str:
        """Return a stable, per-flow id for this flow's cleanup ticket.

        Prefers Home Assistant's own ``flow_id``, which is unique per flow and
        therefore separates two concurrent flows for the same account. Falls
        back to a random id for a directly instantiated handler (a flow that
        never went through the flow manager has no ``flow_id``); the fallback
        must stay per-instance, otherwise the fallback itself would re-merge the
        tickets it exists to keep apart.

        Lives on the mixin because both ``ConfigFlow`` and ``OptionsFlowHandler``
        stage cleanup tickets, and resolves through ``getattr``/``setattr`` so it
        does not depend on either class's ``__init__``.
        """

        ticket_id = getattr(self, "_container_cleanup_ticket_id", None)
        if not isinstance(ticket_id, str) or not ticket_id:
            flow_id = getattr(self, "flow_id", None)
            ticket_id = flow_id if isinstance(flow_id, str) and flow_id else uuid4().hex
            self._container_cleanup_ticket_id = ticket_id
        return ticket_id

    @_typed_callback
    def _async_discard_own_cleanup_ticket(self) -> int:
        """Drop this flow's own, still uncorrelated ticket (abort path).

        Bound to :meth:`data_entry_flow.FlowHandler.async_remove` by the two
        concrete handlers. It cannot be *defined* as ``async_remove`` here:
        ``FlowHandler`` sits before this mixin in both MROs, so its no-op
        default would win.
        """

        return _async_discard_cleanup_ticket_for_flow(
            getattr(self, "hass", None), self._async_cleanup_ticket_id()
        )

    @_typed_callback
    def _async_mark_own_cleanup_ticket_entry_promised(self) -> int:
        """Protect this flow's ticket from the removal hook (create path).

        Thin binding of :func:`_async_mark_cleanup_ticket_entry_promised` to the
        flow's own ticket id, mirroring
        :meth:`_async_discard_own_cleanup_ticket`. Lives on the mixin because
        the ticket id resolution does, and resolves ``hass`` through ``getattr``
        for the same reason.
        """

        return _async_mark_cleanup_ticket_entry_promised(
            getattr(self, "hass", None), self._async_cleanup_ticket_id()
        )


# ---------------------------
# Config Flow
# ---------------------------
class _DomainAwareConfigFlow(config_entries.ConfigFlow):  # type: ignore[misc]
    """Config flow base that allows metaclass keywords for type checking."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Propagate keyword arguments, including ``domain``, to the parent."""

        super().__init_subclass__(**kwargs)


class ConfigFlow(
    _DomainAwareConfigFlow,
    _ConfigFlowMixin,
    _ContainerLoginMixin,
    domain=DOMAIN,
):
    """Handle the initial config flow for Google Find My Device."""

    domain: ClassVar[str] = DOMAIN
    VERSION = CONFIG_ENTRY_VERSION

    def _async_update_entry_and_abort(
        self,
        *,
        entry: ConfigEntry,
        data: Mapping[str, Any],
        reason: str,
    ) -> FlowResult:
        """Persist ``data`` on ``entry`` and abort, without reloading from here.

        Replaces ``async_update_reload_and_abort``. Home Assistant deprecates
        the combination of an update listener on the entry with a reloading
        config-flow method (warning from 2026.6, error from 2026.12): with a
        listener present, the core's reload duplicates the one the listener
        causes. This integration keeps its listener, because it adopts changed
        watch paths at runtime, so the update happens reload-free here.

        The reload that changed credentials still need is not lost, it moves:
        this helper schedules it directly, under the shared latch, so the update
        listener in ``__init__.py`` stands down for the same change. Storing
        credentials without a reload would leave them written but ineffective,
        because the token cache is seeded in ``async_setup_entry``.

        Scheduling here rather than leaving it to the listener alone is not
        belt-and-braces, it is the whole point for the case that matters most: an
        entry whose credentials expired is in ``SETUP_ERROR``, and Home Assistant
        runs ``_async_process_on_unload`` on a failed setup, which removes that
        very listener. Relying on it would make a successful reauth report success
        while the integration stays down until the next restart. ``SETUP_ERROR``
        has no retry timer either. The unconditional schedule also keeps the
        behaviour of ``async_update_reload_and_abort``, whose
        ``reload_even_if_entry_is_unchanged`` defaults to ``True``: re-importing
        the same credentials still rebuilds the entry.

        ``ConfigFlow.async_update_and_abort`` is deliberately not used: it
        exists only from HA 2025.11.0, while the declared minimum is 2025.9.1
        (``hacs.json``), where the method lives on ``ConfigSubentryFlow`` alone.
        Both building blocks used here exist in every supported version.
        """

        self.hass.config_entries.async_update_entry(entry, data=dict(data))
        _schedule_claimed_reload(self.hass, entry.entry_id)
        return self.async_abort(reason=reason)

    def __init__(self) -> None:
        """Initialize transient flow state."""
        self._auth_data: dict[str, Any] = {}
        self._available_devices: list[tuple[str, str]] = []
        self._subentry_key_core_tracking = TRACKER_SUBENTRY_KEY
        self._subentry_key_service = SERVICE_SUBENTRY_KEY
        self._pending_discovery_payload: CloudDiscoveryData | None = None
        # The credential *delta*, not a merged entry payload: what a form
        # carries across a user's think time must not be a snapshot of another
        # object's state (see _merge_credential_updates).
        self._pending_discovery_auth: dict[str, Any] | None = None
        self._pending_discovery_entry_exists = False
        # Survives _clear_discovery_confirmation_state on purpose: the overwrite
        # question is asked *after* the discovery card has been confirmed and
        # that state torn down.
        self._pending_overwrite_payload: CloudDiscoveryData | None = None
        self._pending_overwrite_auth: dict[str, Any] | None = None
        # One-shot marker for the local-bundle preflight scan in
        # ``async_step_user``; see _async_preflight_local_bundle. Lives here and
        # not on _ContainerLoginMixin because the preflight is a *setup* step
        # and the mixin is shared with OptionsFlowHandler.
        self._local_bundle_preflight_done = False
        # Bundle offered by ``found_local_bundle`` (path + scan result), kept so
        # the form can be re-shown with its placeholders after an error.
        self._local_bundle_candidate: tuple[Path, _SecretsScanResult] | None = None
        # Delete-after-import job of an accepted preflight bundle. Staged (not
        # executed) once device_selection actually creates the entry.
        self._local_bundle_pending_cleanup: PendingContainerCleanup | None = None
        # Identity of this flow's cleanup ticket (see _StagedCleanupTicket).
        # Resolved lazily because Home Assistant assigns ``flow_id`` only when
        # the flow manager starts the flow, i.e. after ``__init__``.
        self._container_cleanup_ticket_id: str | None = None

    @_typed_callback
    def async_remove(self) -> None:
        """Drop this flow's staged cleanup when Home Assistant removes the flow.

        Overrides the no-op hook of ``data_entry_flow.FlowHandler``. Home
        Assistant calls it for *every* ending of a flow, but only ever after
        ``async_finish_flow`` has run, so the two endings are distinguishable by
        what is still staged:

        * Success: ``async_finish_flow`` created and added the entry. When its
          first ``async_setup_entry`` succeeded, that run already claimed this
          flow's ticket and there is nothing left to drop. When it raised
          ``ConfigEntryNotReady``, the entry exists, Home Assistant will retry
          the setup and the ticket is still staged -- which is why the create
          path marks it ``entry_promised`` and this hook keeps it.
        * Abort -- including the abort Home Assistant fires on a competing
          in-progress flow when a same-account flow wins -- there is no entry
          and never will be, so the ticket is still staged *and* unmarked.
          Dropping it here is what stops the winning entry from claiming a
          ticket that was never about it and acking credentials that belong to
          the aborted flow.

        Update-path tickets carry an ``entry_id`` and are deliberately kept
        (see :func:`_async_discard_cleanup_ticket_for_flow`); those flows abort
        on purpose after a successful update.
        """

        discarded = self._async_discard_own_cleanup_ticket()
        if discarded:
            _LOGGER.debug(
                "Discarded %s staged container-login cleanup job(s) of a removed "
                "config flow; credential files are kept on disk",
                discarded,
            )

    async def async_step_subentry(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the flow initiated by the '+' icon on the integration card.

        Home Assistant does not populate the config entry context when the
        integration implements a custom subentry step. Determine the target hub
        before launching the existing reconfigure flow so the device visibility
        dialog has the required `entry_id`.
        """

        if self.context.get("entry_id"):
            return await self.async_step_reconfigure(user_input=user_input)

        return await self.async_step_select_hub_for_visibility(user_input)

    async def async_step_select_hub_for_visibility(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Prompt the user to choose the hub whose device visibility to edit."""

        hass_obj = getattr(self, "hass", None)
        if not isinstance(hass_obj, HomeAssistant):
            return self.async_abort(reason="unknown")
        hass = cast(HomeAssistant, hass_obj)

        entries = [
            entry
            for entry in hass.config_entries.async_entries(DOMAIN)
            # Two guards, two reasons. `getattr(entry, "source", None)` keeps
            # discovery-update stubs without a `.source` attribute inside the
            # Home Assistant contract. The module constant is resolved the same
            # way `_entry_reload_is_hopeless` does, and what that default guards
            # is an import world, not a core version: `SOURCE_IGNORE` predates
            # the declared minimum by years. Line 85 binds the *package
            # attribute* (`from homeassistant import config_entries`), and in an
            # ordinary test run that attribute is the installed module, because
            # `pytest_homeassistant_custom_component` imports it before
            # `tests/conftest.py` swaps the `sys.modules` entry for its stub and
            # the attribute survives the swap. There the constant is present and
            # the default never fires. It fires where that attribute is absent
            # and the import falls back to `sys.modules`: the conftest stub
            # carries `SOURCE_DISCOVERY` and `SOURCE_RECONFIGURE` and nothing
            # else, so a direct attribute access would raise `AttributeError`
            # there. Reproduce that world with `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`.
            # Of those two names only `SOURCE_DISCOVERY` is required at import
            # time (it raises); `SOURCE_RECONFIGURE` already reads via a default.
            if getattr(entry, "source", None)
            != getattr(config_entries, "SOURCE_IGNORE", "ignore")
        ]

        if not entries:
            return self.async_abort(reason="no_hubs_configured")

        def _entry_label(entry: ConfigEntry) -> str:
            if isinstance(entry.title, str):
                label = entry.title.strip()
                if label:
                    return label
            raw_email, _ = _resolve_entry_email_for_lookup(entry)
            if raw_email:
                return raw_email
            return cast(str, entry.entry_id)

        hub_choices = {entry.entry_id: _entry_label(entry) for entry in entries}

        if len(hub_choices) == 1:
            self.context["entry_id"] = next(iter(hub_choices))
            return await self.async_step_reconfigure(None)

        if user_input is not None:
            selected = user_input.get(_FIELD_VISIBILITY_HUB)
            if isinstance(selected, str) and selected in hub_choices:
                self.context["entry_id"] = selected
                return await self.async_step_reconfigure(None)

        default_choice = next(iter(hub_choices))
        schema = vol.Schema(
            {
                vol.Required(
                    _FIELD_VISIBILITY_HUB,
                    default=default_choice,
                ): vol.In(hub_choices)
            }
        )

        return self.async_show_form(
            step_id="select_hub_for_visibility",
            data_schema=schema,
        )

    async def _async_prepare_account_context(
        self,
        *,
        email: str,
        preferred_unique_id: str | None = None,
        updates: Mapping[str, Any] | None = None,
        coalesce: bool = True,
        abort_on_duplicate: bool = True,
    ) -> ConfigEntry | None:
        """Set the flow unique_id and abort if ``email`` already has an entry."""

        hass_obj = getattr(self, "hass", None)
        if hass_obj is None or not hasattr(hass_obj, "config_entries"):
            return None
        hass = cast(HomeAssistant, hass_obj)

        normalized = normalize_email(email)
        unique_id = preferred_unique_id or unique_account_id(normalized)
        if unique_id:
            await self.async_set_unique_id(unique_id, raise_on_progress=False)

        existing_entry: ConfigEntry | None = None
        if normalized:
            existing_entry = _find_entry_by_email(hass, normalized)

        if existing_entry is None:
            return None

        context_entry_id: str | None = None
        context_obj = getattr(self, "context", None)
        if isinstance(context_obj, Mapping):
            raw_context_entry = context_obj.get("entry_id")
            if isinstance(raw_context_entry, str) and raw_context_entry:
                context_entry_id = raw_context_entry

        bound_entry_id: str | None = None
        bound_entry = getattr(self, "config_entry", None)
        if isinstance(bound_entry, ConfigEntry):
            bound_entry_id = bound_entry.entry_id

        if (context_entry_id and existing_entry.entry_id == context_entry_id) or (
            bound_entry_id and existing_entry.entry_id == bound_entry_id
        ):
            if coalesce:
                await _async_coalesce_account_entries(hass, existing_entry)
            return existing_entry

        if not abort_on_duplicate:
            if coalesce:
                await _async_coalesce_account_entries(hass, existing_entry)
            return existing_entry

        _ensure_optional_entry_attributes(existing_entry)

        try:
            # reload_on_update=False: this flow class carries an update listener
            # (see __init__.py), and Home Assistant deprecates that combination
            # with a reloading config-flow method (warning from 2026.6, error
            # from 2026.12) because both would reload. The reload the changed
            # credentials still need normally comes from that listener, which
            # fires on the merge this call performs. That listener is not a
            # guarantee, though: an entry that is not loaded has none, Home
            # Assistant removes it on unload. Callers that pass ``updates`` here
            # therefore have to schedule the reload themselves once they have
            # established that the write landed (both do). The core's elif branch
            # for a SETUP_RETRY entry from a discovery source is untouched by
            # this and keeps working: it carries no report_usage.
            self._abort_if_unique_id_configured(updates=updates, reload_on_update=False)
        except data_entry_flow.AbortFlow:
            if coalesce:
                await _async_coalesce_account_entries(hass, existing_entry)
            raise

        if coalesce:
            await _async_coalesce_account_entries(hass, existing_entry)

        raise data_entry_flow.AbortFlow("already_configured")

    async def async_step_migrate(self, entry: ConfigEntry) -> FlowResult:
        """Migrate legacy config entries to the subentry-aware structure."""

        from . import (
            _clear_duplicate_account_issue,
            _extract_email_from_entry,
            _log_duplicate_and_raise_repair_issue,
            _resolve_entry_email,
        )

        _LOGGER.info(
            "Starting migration for %s from version %s to %s",
            entry.entry_id,
            entry.version,
            self.VERSION,
        )

        setattr(self, "config_entry", entry)

        context = getattr(self, "context", None)
        if not isinstance(context, dict):
            context = {}
            setattr(self, "context", context)
        context.setdefault("entry_id", entry.entry_id)

        normalized_email = normalize_email_or_default(entry.data.get(CONF_GOOGLE_EMAIL))
        placeholders = dict(context.get("title_placeholders", {}) or {})
        if normalized_email:
            placeholders["email"] = normalized_email
        if placeholders:
            context["title_placeholders"] = placeholders

        if entry.version >= self.VERSION:
            _LOGGER.debug(
                "Config entry %s already matches target version %s; performing consistency check.",
                entry.entry_id,
                self.VERSION,
            )

        old_data = dict(getattr(entry, "data", {}) or {})
        old_options = dict(getattr(entry, "options", {}) or {})

        options_payload: dict[str, Any] = dict(DEFAULT_OPTIONS)
        all_option_keys = OPTION_KEYS

        for key in all_option_keys:
            if key is None:
                continue
            if key in old_options and old_options[key] is not None:
                options_payload[key] = old_options[key]
            elif key in old_data and old_data[key] is not None:
                options_payload[key] = old_data[key]

        if OPT_IGNORED_DEVICES in options_payload:
            ignored_mapping, _changed = coerce_ignored_mapping(
                options_payload[OPT_IGNORED_DEVICES]
            )
            options_payload[OPT_IGNORED_DEVICES] = ignored_mapping

        options_payload[OPT_OPTIONS_SCHEMA_VERSION] = 2

        subentry_context = self._ensure_subentry_context()

        try:
            await self._async_sync_feature_subentries(
                entry,
                options_payload=options_payload,
                defaults=dict(DEFAULT_OPTIONS),
                context_map=subentry_context,
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.error(
                "Migration failed while creating subentries for %s: %s",
                entry.entry_id,
                err,
            )
            return self.async_abort(reason="migration_failed")

        allowed_data_keys = (
            DATA_AUTH_METHOD,
            CONF_OAUTH_TOKEN,
            CONF_GOOGLE_EMAIL,
            DATA_SECRET_BUNDLE,
            DATA_AAS_TOKEN,
        )
        new_data: dict[str, Any] = {
            key: value
            for key in allowed_data_keys
            if (value := old_data.get(key)) is not None
        }

        resolved_raw_email: str | None
        resolved_normalized_email: str | None
        resolved_raw_email, resolved_normalized_email = _resolve_entry_email(entry)
        if resolved_normalized_email:
            new_data[CONF_GOOGLE_EMAIL] = resolved_normalized_email
        elif resolved_raw_email:
            new_data[CONF_GOOGLE_EMAIL] = resolved_raw_email

        existing_title = (
            entry.title.strip()
            if isinstance(entry.title, str) and entry.title.strip()
            else None
        )

        title_update: str | None = resolved_raw_email if resolved_raw_email else None
        if resolved_normalized_email:
            if (
                existing_title
                and existing_title.lower() == resolved_normalized_email
                and existing_title != resolved_normalized_email
            ):
                title_update = existing_title
            elif title_update is None:
                title_update = existing_title or normalized_email
        elif title_update is None and existing_title:
            title_update = existing_title

        manager = getattr(self.hass, "config_entries", None)
        others: list[ConfigEntry] = []
        if manager is not None:
            try:
                candidates = manager.async_entries(DOMAIN)
            except TypeError:  # pragma: no cover - legacy signature
                candidates = manager.async_entries()
            for candidate in candidates:
                if getattr(candidate, "entry_id", None) == entry.entry_id:
                    continue
                others.append(candidate)

        conflict: ConfigEntry | None = None
        if normalized_email:
            for candidate in others:
                if _extract_email_from_entry(candidate) == normalized_email:
                    conflict = candidate
                    break

        if conflict and normalized_email:
            _log_duplicate_and_raise_repair_issue(
                self.hass,
                entry,
                normalized_email,
                cause="pre_migration_duplicate",
                conflicts=[conflict],
            )

        update_kwargs: dict[str, Any] = {
            "data": new_data,
            "options": options_payload,
            "version": self.VERSION,
        }

        if title_update and entry.title != title_update:
            update_kwargs["title"] = title_update

        unique_id: str | None = None
        if normalized_email:
            unique_id = unique_account_id(normalized_email)
        applied_unique_id = None
        if (
            unique_id
            and getattr(entry, "unique_id", None) != unique_id
            and conflict is None
        ):
            update_kwargs["unique_id"] = unique_id
            applied_unique_id = unique_id

        current_data = dict(getattr(entry, "data", {}) or {})
        current_options = dict(getattr(entry, "options", {}) or {})

        need_update = False
        if current_data != new_data:
            need_update = True
        else:
            update_kwargs.pop("data", None)

        if current_options != options_payload:
            need_update = True
        else:
            update_kwargs.pop("options", None)

        if "title" in update_kwargs:
            need_update = True

        if applied_unique_id is not None:
            need_update = True

        if getattr(entry, "version", None) != self.VERSION:
            need_update = True
        else:
            update_kwargs.pop("version", None)

        if need_update and update_kwargs:
            try:
                self.hass.config_entries.async_update_entry(entry, **update_kwargs)
            except TypeError:
                fallback_kwargs = dict(update_kwargs)
                fallback_kwargs.pop("version", None)
                if fallback_kwargs:
                    self.hass.config_entries.async_update_entry(
                        entry, **fallback_kwargs
                    )
                setattr(entry, "version", self.VERSION)
            except ValueError:
                if normalized_email:
                    _log_duplicate_and_raise_repair_issue(
                        self.hass,
                        entry,
                        normalized_email,
                        cause="unique_id_conflict",
                    )
                update_kwargs.pop("unique_id", None)
                applied_unique_id = None
                if update_kwargs:
                    self.hass.config_entries.async_update_entry(entry, **update_kwargs)
                setattr(entry, "version", self.VERSION)
            else:
                if "version" in update_kwargs:
                    setattr(entry, "version", self.VERSION)

        if getattr(entry, "version", None) != self.VERSION:
            setattr(entry, "version", self.VERSION)

        setattr(entry, "data", new_data)
        setattr(entry, "options", options_payload)
        if title_update:
            entry.title = title_update
        if applied_unique_id:
            setattr(entry, "unique_id", applied_unique_id)

        if conflict is None:
            _clear_duplicate_account_issue(self.hass, entry)

        placeholders = dict(context.get("title_placeholders", {}) or {})
        email_candidate = normalize_email_or_default(new_data.get(CONF_GOOGLE_EMAIL))
        if email_candidate:
            placeholders["email"] = email_candidate
        if placeholders:
            context["title_placeholders"] = placeholders

        _LOGGER.info(
            "Config entry %s migrated successfully to version %s",
            entry.entry_id,
            self.VERSION,
        )

        return await self._async_resolve_flow_result(
            self.async_show_form(step_id="migrate_complete")
        )

    async def async_step_migrate_complete(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Display a confirmation screen once migration completes."""

        if user_input is not None:
            return await self._async_resolve_flow_result(
                self.async_abort(reason="migration_successful")
            )

        context_obj = getattr(self, "context", None)
        placeholders: dict[str, str] = {}
        if isinstance(context_obj, dict):
            raw_placeholders = context_obj.get("title_placeholders", {}) or {}
            if isinstance(raw_placeholders, Mapping):
                placeholders = {
                    key: str(value)
                    for key, value in raw_placeholders.items()
                    if isinstance(key, str) and value is not None
                }

        if "email" not in placeholders:
            candidate_entry = getattr(self, "config_entry", None)
            email_placeholder: str | None = None
            if isinstance(candidate_entry, ConfigEntry):
                email_placeholder = normalize_email_or_default(
                    candidate_entry.data.get(CONF_GOOGLE_EMAIL)
                )
                if not email_placeholder:
                    email_placeholder = normalize_email_or_default(
                        candidate_entry.title
                        if isinstance(candidate_entry.title, str)
                        else None
                    )
                if not email_placeholder:
                    email_placeholder = candidate_entry.entry_id
            if email_placeholder:
                placeholders["email"] = email_placeholder

        return await self._async_resolve_flow_result(
            self.async_show_form(
                step_id="migrate_complete",
                data_schema=vol.Schema({}),
                description_placeholders=placeholders,
            )
        )

    async def _async_resolve_flow_result(
        self, result: FlowResult | Awaitable[FlowResult]
    ) -> FlowResult:
        """Return a flow result, awaiting if the stub returns a coroutine."""

        if inspect.isawaitable(result):
            awaited = await cast(Any, result)
            return cast(FlowResult, awaited)
        return cast(FlowResult, result)

    def _clear_discovery_confirmation_state(self) -> None:
        """Reset cached discovery confirmation state.

        The base `ConfigFlow` helper `_set_confirm_only()` toggles the
        `context["confirm_only"]` flag so the UI renders a confirmation form.
        This reset helper must clear the same flag whenever we dismiss the
        prompt to keep the state machine in sync with subsequent submissions.
        """

        self._pending_discovery_payload = None
        self._pending_discovery_auth = None
        self._pending_discovery_entry_exists = False
        context = getattr(self, "context", None)
        if isinstance(context, dict):
            context.pop("confirm_only", None)

    @staticmethod
    @_typed_callback
    def async_get_options_flow(config_entry: ConfigEntry) -> config_entries.OptionsFlow:
        """Return the options flow for an existing config entry."""
        return OptionsFlowHandler()

    @classmethod
    @_typed_callback
    def async_get_supported_subentry_types(
        cls,
        _config_entry: ConfigEntry,
    ) -> dict[str, type[ConfigSubentryFlow]]:
        """Return an empty mapping to hide subentry UI elements.

        Subentries (hub, service, tracker feature groups) are provisioned
        programmatically by the integration coordinator, NOT manually by users.
        Returning an empty dict prevents Home Assistant from displaying
        "Add subentry" buttons (+ Add hub feature group, + Add service feature
        group) in the config entry UI.

        The async_step_hub entry point (for "Hub hinzufügen" / "Add Hub")
        instantiates handlers directly without relying on this mapping.
        """
        return {}

    async def async_step_discovery(
        self, discovery_info: Mapping[str, Any] | None
    ) -> FlowResult:
        """Handle cloud-triggered discovery payloads.

        Entry point only. Home Assistant calls this with the payload; the
        confirmation the form asks for comes back in
        :meth:`async_step_discovery_confirm`, because the form carries its own
        ``step_id``. A step that showed its form under ``step_id="discovery"``
        would be re-entered *here* on submit and would have to tell a submit
        apart from a fresh payload by itself.
        """

        context_obj = getattr(self, "context", None)
        context_source: str | None = None
        if isinstance(context_obj, Mapping):
            context_source = context_obj.get("source")

        payload_keys: list[str] = []
        if isinstance(discovery_info, Mapping):
            payload_keys = sorted(str(key) for key in discovery_info.keys())

        _LOGGER.info(
            "Flow start: async_step_discovery (context_source=%s, payload_keys=%s)",
            context_source,
            payload_keys,
        )
        _LOGGER.debug(
            "discovery: context_source=%s, payload_keys=%s",
            context_source,
            payload_keys,
        )

        if _is_discovery_update_info(context_obj):
            _LOGGER.info(
                "Routing discovery payload to discovery-update-info handler "
                "(context_source=%s)",
                context_source,
            )
            return await self.async_step_discovery_update_info(discovery_info)

        # A fresh payload supersedes a confirmation that is still pending: the
        # user has not answered the old card, and the new payload is the more
        # recent truth. Clearing here also drops ``confirm_only`` from the
        # context, which _set_confirm_only re-arms below for the new card.
        self._clear_discovery_confirmation_state()

        try:
            normalized = _normalize_and_validate_discovery_payload(discovery_info or {})
        except DiscoveryFlowError as err:
            _LOGGER.debug("Discovery ignored due to invalid payload: %s", err.reason)
            return self.async_abort(reason=err.reason)
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception(
                "Discovery ignored due to unexpected payload: %s",
                err,
            )
            return self.async_abort(reason="invalid_discovery_info")

        existing_entry = await self._async_prepare_account_context(
            email=normalized.email,
            preferred_unique_id=normalized.unique_id,
            abort_on_duplicate=False,
        )
        try:
            auth_data, updates = await _ingest_discovery_credentials(
                self, normalized, existing_entry=existing_entry
            )
        except DiscoveryFlowError as err:
            reason = err.reason
            if reason not in {
                "invalid_discovery_info",
                "cannot_connect",
                "invalid_auth",
                "dependency_not_ready",
            }:
                reason = (
                    "cannot_connect" if reason != "invalid_discovery_info" else reason
                )
            return self.async_abort(reason=reason)

        self._auth_data = auth_data

        placeholders = dict(self.context.get("title_placeholders", {}) or {})
        placeholders.setdefault("email", normalized.email)
        self.context["title_placeholders"] = placeholders
        self._pending_discovery_payload = normalized
        self._pending_discovery_auth = dict(auth_data)
        self._pending_discovery_entry_exists = updates is not None
        self._set_confirm_only()
        return self.async_show_form(
            step_id="discovery_confirm",
            description_placeholders=placeholders,
        )

    async def async_step_discovery_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the confirmation of the discovery card.

        Home Assistant routes here because :meth:`async_step_discovery` showed
        its form under ``step_id="discovery_confirm"``. That is the whole reason
        this method exists as a step of its own: ``discovery_confirm`` is the
        name Home Assistant reserves for a discovery confirmation form (and the
        only one of the two that may carry a translated text), so the submit no
        longer re-enters the entry point and no longer has to be told apart from
        an incoming payload.
        """

        pending_payload = self._pending_discovery_payload
        pending_auth = self._pending_discovery_auth
        entry_exists = self._pending_discovery_entry_exists
        if pending_payload is None:
            # No card is pending: the flow was resumed without one, or the
            # state was cleared by a newer payload. Nothing to confirm.
            return self.async_abort(reason="invalid_discovery_info")

        self._clear_discovery_confirmation_state()

        if entry_exists and pending_auth is not None:
            # The account is already configured and someone supplied
            # credentials. Do not write yet: ask first, because this replaces
            # working credentials. The old behaviour wrote unconditionally and
            # reported "already_configured", which said nothing about what had
            # happened to the credentials.
            #
            # Every payload reaching this point gets the question, including an
            # unmarked one. That direction is deliberate: replacing working
            # credentials unasked is the worse failure, so an unknown producer
            # is asked about rather than trusted. The former silent branch
            # existed for the tracker rescan, which no longer produces discovery
            # payloads at all (see device_tracker.py); a positive list of
            # trusted sources would invert the direction and break the
            # credential import itself the moment Home Assistant renames or adds
            # a source constant.
            self._pending_overwrite_payload = pending_payload
            self._pending_overwrite_auth = pending_auth
            return await self.async_step_discovery_overwrite()

        return await self._async_import_discovered_account(pending_payload)

    async def _async_import_discovered_account(
        self, payload: CloudDiscoveryData | None
    ) -> FlowResult:
        """Import ``payload`` as a new account and stage the bundle cleanup.

        Split out of :meth:`async_step_discovery` because a second caller needs
        exactly this behaviour: :meth:`async_step_discovery_overwrite` lands
        here when the entry it was going to overwrite has disappeared while its
        form was open, which turns the overwrite into a plain first import.

        ``payload`` stays optional because the confirmation path reaches the
        import with nothing staged as well; the cleanup helpers below resolve
        that to "stage no key", which is what the absent payload means.
        """

        result = await self.async_step_device_selection()
        # Delete-after-import: STAGED here, executed in
        # async_setup_entry. A CREATE_ENTRY FlowResult is only a
        # promise: Home Assistant adds the entry afterwards in
        # ConfigEntriesFlowManager.async_finish_flow, so deleting the
        # on-disk copies here would drop the credentials before they
        # are persisted. Aborted device selection stages nothing and
        # leaves the watched bundles in place so the flow can be
        # retried. Only the copies the import actually consumed are
        # removed (see the hook docstring): a co-located bundle of a
        # different account and a same-account bundle that got fresher
        # while the user was confirming both survive.
        if (
            isinstance(result, Mapping)
            and result.get("type") == data_entry_flow.FlowResultType.CREATE_ENTRY
        ):
            _async_stage_container_cleanup(
                getattr(self, "hass", None),
                flow_id=self._async_cleanup_ticket_id(),
                unique_id=self.unique_id,
                job=PendingContainerCleanup(
                    imported_stable_key=_stable_key_for_discovery_payload(payload),
                    imported_digest=_digest_for_discovery_payload(payload),
                ),
            )
            # This staging happens AFTER the CREATE_ENTRY result, so
            # the mark the create path set in async_step_device_selection
            # predates the ticket this call may just have created. Mark
            # again here, at the site that staged last: without it the
            # flow removal Home Assistant performs right after
            # async_finish_flow drops a ticket that belongs to an entry
            # which exists and whose setup may still be retrying.
            self._async_mark_own_cleanup_ticket_entry_promised()
        return result

    def _async_rebase_credential_updates(
        self, email: str, auth_data: Mapping[str, Any]
    ) -> tuple[ConfigEntry, dict[str, Any]] | None:
        """Resolve the configured entry *now* and merge the delta onto it.

        Both discovery forms (the card, then the overwrite question) sit between
        the moment the credentials are validated and the moment they are
        written. Anything merged before them is a snapshot: writing it back
        would undo whatever else changed the entry meanwhile, and the options
        flow does exactly that when it refreshes credentials through the login
        container. So the merge happens here, against the entry as it is, and
        only the delta travels across the forms.

        Returns ``None`` when no entry can be resolved -- no ``hass``, or the
        account is no longer configured -- which leaves the decision about that
        case to the caller, where it differs (import versus abort).

        This closes the window a form holds open, not the one between this call
        and the write a few statements later; that one is bounded by the event
        loop rather than by user think time.
        """

        hass_obj = getattr(self, "hass", None)
        if hass_obj is None or not hasattr(hass_obj, "config_entries"):
            return None

        entry = _find_entry_by_email(cast(HomeAssistant, hass_obj), email)
        if entry is None:
            return None

        entry_data = getattr(entry, "data", None)
        if not isinstance(entry_data, Mapping):
            entry_data = {}
        return entry, _merge_credential_updates(entry_data, auth_data)

    def _async_write_entry_credentials(
        self, entry: ConfigEntry, updates: Mapping[str, Any]
    ) -> bool:
        """Apply ``updates`` to ``entry`` and schedule its reload.

        For the cases Home Assistant's unique-id guard, which normally performs
        this write, does not cover: ``_async_prepare_account_context`` returned
        the entry instead of aborting through the guard; the guard returned
        before its write because no entry claimed the flow's unique id and the
        helper then aborted on its own; or the guard did write, but flat, which
        leaves behind the credential keys the payload means to drop.
        ``updates`` is the entry data itself, flat and already merged (see
        :func:`_ingest_discovery_credentials`), so it is written as-is: a
        second merge would resurrect the keys the ingest deliberately dropped,
        which is precisely the third case above.

        Returns whether the write happened, so the caller can report the
        outcome it actually produced instead of assuming one.
        """

        hass_obj = getattr(self, "hass", None)
        if hass_obj is None or not hasattr(hass_obj, "config_entries"):
            return False
        hass = cast(HomeAssistant, hass_obj)

        try:
            hass.config_entries.async_update_entry(entry, data=dict(updates))
        except Exception:  # noqa: BLE001 - surface, but do not claim success
            _LOGGER.exception(
                "Failed to write discovered sign-in data to entry %s",
                entry.entry_id,
            )
            return False

        # ``async_update_entry`` only mutates the in-memory entry and schedules
        # the debounced store save; the running integration keeps the old
        # credentials until it is reloaded. That write also notifies the update
        # listener, which reloads for exactly this reason, so the reload below is
        # the one that also covers an entry without a listener -- one that is not
        # set up, for instance because these very credentials had expired.
        # Claiming the latch keeps the two from unloading the entry twice in a row.
        _schedule_claimed_reload(hass, entry.entry_id)
        return True

    async def async_step_discovery_overwrite(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Ask before replacing the credentials of an already configured account.

        Reached from :meth:`async_step_discovery` once the discovery card has
        been confirmed and the account turns out to have an entry already. The
        answer decides between two aborts, both of which say what happened:
        ``credentials_updated`` or ``credentials_kept``. Neither is
        ``already_configured``, which used to end both outcomes and told the user
        nothing about their credentials.

        Declining writes nothing at all: the entry keeps the credentials it has,
        and nothing is staged for cleanup, so the discovered bundle stays on
        disk.

        Accepting reports ``credentials_updated`` only where credentials were
        actually written, and that is read off the entry rather than inferred
        from the abort that ended the guard: the guard also aborts without
        writing when no entry claims the flow's unique id, and where it does
        write it merges flat, which cannot drop a superseded bundle or token.
        Whatever the reason, an entry that does not hold exactly these
        credentials afterwards is written here explicitly. The entry can also disappear while this form is open; that
        case continues as a first import of the same credentials.

        Accepting also stages the delete-after-import cleanup of the bundle,
        exactly as the non-interactive update path does, so an accepted file is
        eventually consumed instead of being rediscovered on every restart.

        What crosses this form is the credential delta alone. The payload that
        is written is merged from the entry's *current* data once the answer is
        in (:meth:`_async_rebase_credential_updates`), because a payload merged
        before the question would carry an entry state that may be minutes old
        and would roll back, say, a credential refresh the options flow ran in
        the meantime.
        """

        payload = self._pending_overwrite_payload
        auth_data = self._pending_overwrite_auth
        if payload is None or auth_data is None:  # pragma: no cover - defensive
            return self.async_abort(reason="already_configured")

        if user_input is not None:
            self._pending_overwrite_payload = None
            self._pending_overwrite_auth = None

            if not user_input.get(_FIELD_OVERWRITE_CREDENTIALS, False):
                _LOGGER.info(
                    "Discovery for %s declined: stored credentials kept",
                    _mask_email_for_logs(payload.email),
                )
                return self.async_abort(reason="credentials_kept")

            rebased = self._async_rebase_credential_updates(payload.email, auth_data)
            if rebased is None:
                # No entry to overwrite any more: it was removed, or its account
                # identity changed, while this form was open. The credentials
                # are still in hand, so import them as a new account rather than
                # dropping them.
                _LOGGER.info(
                    "Discovery for %s confirmed, but the configured entry "
                    "is gone; importing the credentials as a new account",
                    _mask_email_for_logs(payload.email),
                )
                return await self._async_import_discovered_account(payload)

            updates = rebased[1]
            existing_entry: ConfigEntry | None = None
            written = False
            try:
                existing_entry = await self._async_prepare_account_context(
                    email=payload.email,
                    preferred_unique_id=payload.unique_id,
                    updates=updates,
                )
            except data_entry_flow.AbortFlow:
                # Two different aborts arrive here and only one of them wrote:
                # the core's unique-id guard applies ``updates`` and then aborts
                # to signal "this account is already configured", but it also
                # returns silently when no entry claims this unique id (a legacy
                # entry without one, or an account identity that moved), and
                # ``_async_prepare_account_context`` then raises its own abort
                # having written nothing. So the entry is resolved again -- the
                # exception skipped the assignment above -- and asked what it
                # actually holds. Resolving happens deliberately *after* the
                # write, because the cleanup ticket below needs the entry's
                # fresh ``modified_at`` as its durability watermark.
                hass_obj = getattr(self, "hass", None)
                if hass_obj is not None and hasattr(hass_obj, "config_entries"):
                    existing_entry = _find_entry_by_email(
                        cast(HomeAssistant, hass_obj), payload.email
                    )
                if existing_entry is not None and _entry_carries_credentials(
                    existing_entry, updates
                ):
                    written = True
                    # The entry now holds exactly these credentials, which is
                    # what the caller below reports and what makes the wholesale
                    # write unnecessary. Note the difference: this reads the
                    # *state*, it does not prove that this flow's guard call
                    # performed the write. It is also true when the merge was a
                    # no-op because the entry already held them, which happens
                    # after a restart, where the watcher rediscovers an unchanged
                    # bundle (``_settled_signatures`` is process-local). The
                    # reload is scheduled for that case too, deliberately: it is
                    # what redeems the cleanup ticket staged below, and without
                    # it the same bundle is offered again after every restart.
                    #
                    # The reload has to be scheduled here because the guard was
                    # asked not to (``reload_on_update=False``), on the grounds
                    # that the entry's update listener carries it. An entry that
                    # is not loaded has no listener left, Home Assistant removes
                    # it on unload, and that is the state expired credentials
                    # produce (``SETUP_ERROR`` after ``ConfigEntryAuthFailed``):
                    # precisely the entry an overwrite is meant to rescue. There
                    # the write would stay ineffective and the durability gate in
                    # ``async_setup_entry`` would never run, so the staged bundle
                    # would never be consumed. Claiming the latch keeps this from
                    # becoming a second unload/setup cycle where a listener does
                    # exist. ``hass_obj`` cannot be ``None`` here: resolving the
                    # entry a few lines above is what required it.
                    _schedule_claimed_reload(
                        cast(HomeAssistant, hass_obj), existing_entry.entry_id
                    )

            if not written:
                # The entry does not hold these credentials: the guard returned
                # the entry instead of aborting through it, or it aborted
                # without ever reaching its write, or it wrote flat and left a
                # superseded ``secrets_data``/``aas_token`` behind. Reporting
                # success here would name an event that did not happen, so the
                # remaining cases are resolved on their own terms.
                if existing_entry is None:
                    # The entry the rebase above resolved is gone again, removed
                    # between that resolution and this write. Same answer as for
                    # the wider window before the rebase: the credentials are
                    # still in hand, so import them as a new account rather than
                    # dropping them.
                    _LOGGER.info(
                        "Discovery for %s confirmed, but the configured entry "
                        "is gone; importing the credentials as a new account",
                        _mask_email_for_logs(payload.email),
                    )
                    return await self._async_import_discovered_account(payload)

                # The entry exists but does not hold the credentials: the flow
                # is bound to that very entry (``context["entry_id"]`` or an
                # attached ``config_entry``), which makes the guard return it
                # instead of aborting through it; or no entry claimed the
                # flow's unique id and the guard returned before its write; or
                # the guard's flat merge could not drop what the payload drops.
                # Write the payload wholesale here, which covers all three, so
                # the reported outcome is the one that happened.
                if not self._async_write_entry_credentials(existing_entry, updates):
                    _LOGGER.warning(
                        "Discovery for %s confirmed, but the credentials could "
                        "not be written to the configured entry",
                        _mask_email_for_logs(payload.email),
                    )
                    return self.async_abort(reason="already_configured")

            # Delete-after-import (update case), the same staging the
            # non-interactive update path performs: STAGED here, executed from
            # the durability gate in async_setup_entry, which the reload
            # scheduled above arms. Without it an accepted bundle is never
            # consumed -- the watcher only remembers it in the process-local
            # ``_settled_signatures``, so the next Home Assistant restart
            # rediscovers the unchanged file and asks this very question again,
            # for good. A declined bundle is deliberately left alone (see the
            # decline branch above), which is what keeps it available.
            #
            # No entry means no watermark, and ``entry=None`` would stage a
            # *create-path* ticket, i.e. authorise the irreversible delete on
            # "the file exists" alone. Staging nothing is the fail-safe answer:
            # the bundle stays on disk and the login container's TTL delete
            # takes over.
            if existing_entry is not None:
                _async_stage_container_cleanup_for(
                    getattr(self, "hass", None),
                    flow_id=self._async_cleanup_ticket_id(),
                    unique_id=getattr(existing_entry, "unique_id", None),
                    job=PendingContainerCleanup(
                        imported_stable_key=_stable_key_for_discovery_payload(payload),
                        imported_digest=_digest_for_discovery_payload(payload),
                    ),
                    entry=existing_entry,
                )

            _LOGGER.info(
                "Discovery for %s confirmed: stored credentials replaced",
                _mask_email_for_logs(payload.email),
            )
            return self.async_abort(reason="credentials_updated")

        return self.async_show_form(
            step_id="discovery_overwrite",
            data_schema=vol.Schema(
                {vol.Required(_FIELD_OVERWRITE_CREDENTIALS, default=True): bool}
            ),
            description_placeholders={
                # The account is the user's own and is shown to them in their own
                # UI, so it is spelled out here; only the *log* is redacted.
                "email": payload.email,
            },
        )

    async def async_step_discovery_update_info(
        self, discovery_info: Mapping[str, Any] | None
    ) -> FlowResult:
        """Handle discovery updates for already configured entries."""

        context_obj = getattr(self, "context", None)
        context_source: str | None = None
        if isinstance(context_obj, Mapping):
            context_source = context_obj.get("source")

        payload_keys: list[str] = []
        if isinstance(discovery_info, Mapping):
            payload_keys = sorted(str(key) for key in discovery_info.keys())

        _LOGGER.info(
            "Flow start: async_step_discovery_update_info (context_source=%s, payload_keys=%s)",
            context_source,
            payload_keys,
        )

        try:
            normalized = _normalize_and_validate_discovery_payload(discovery_info or {})
        except DiscoveryFlowError as err:
            _LOGGER.debug("Discovery update ignored: %s", err.reason)
            return self.async_abort(reason=err.reason)
        except Exception as err:  # noqa: BLE001
            _LOGGER.exception(
                "Discovery update invalid: %s",
                err,
            )
            return self.async_abort(reason="invalid_discovery_info")

        existing_entry = await self._async_prepare_account_context(
            email=normalized.email,
            preferred_unique_id=normalized.unique_id,
            abort_on_duplicate=False,
        )
        _LOGGER.debug(
            "discovery_update_info: normalized.email=%s, unique_id=%s, has_entry=%s",
            _mask_email_for_logs(normalized.email),
            normalized.unique_id,
            existing_entry is not None,
        )
        if existing_entry is None:
            _LOGGER.info(
                "No existing entry for update-info; rerouting to discovery (email=%s)",
                _mask_email_for_logs(normalized.email),
            )

            ctx: dict[str, Any]
            if isinstance(self.context, dict):
                ctx = self.context
            else:
                ctx = dict(getattr(self, "context", {}) or {})

            prev_source = ctx.get("source")

            ctx["source"] = SOURCE_DISCOVERY
            self.context = ctx
            _LOGGER.debug(
                "Context source temporarily overridden: %s -> %s",
                prev_source,
                SOURCE_DISCOVERY,
            )

            try:
                return await self.async_step_discovery(discovery_info)
            finally:
                if prev_source is not None:
                    ctx["source"] = prev_source
                else:
                    ctx.pop("source", None)
                self.context = ctx
                _LOGGER.debug(
                    "Context restored after discovery reroute: source=%s",
                    prev_source,
                )

        try:
            auth_data, updates = await _ingest_discovery_credentials(
                self, normalized, existing_entry=existing_entry
            )
        except DiscoveryFlowError as err:
            reason = err.reason
            if reason not in {
                "invalid_discovery_info",
                "cannot_connect",
                "invalid_auth",
            }:
                reason = (
                    "cannot_connect" if reason != "invalid_discovery_info" else reason
                )
            return self.async_abort(reason=reason)

        self._auth_data = auth_data

        if updates is None:
            updates = dict(existing_entry.data)

        _LOGGER.info(
            "Handling discovery-update-info flow for %s",  # noqa: G004 - logging mask helper
            _mask_email_for_logs(normalized.email),
        )

        hass_obj = getattr(self, "hass", None)
        updates_to_apply = deepcopy(updates)

        abort_raised = False
        try:
            await self._async_prepare_account_context(
                email=normalized.email,
                preferred_unique_id=normalized.unique_id,
                updates=updates,
            )
        except data_entry_flow.AbortFlow:
            abort_raised = True

        if hass_obj is not None and hasattr(hass_obj, "config_entries"):
            hass = cast(HomeAssistant, hass_obj)
        else:
            hass = None

        if hass is not None:
            # ``updates_to_apply`` *is* the entry data, flat, the same payload
            # ``_abort_if_unique_id_configured`` merged above. It is applied
            # again here because that call only reaches the entry when the core
            # aborts; unpacking a nested ``data`` key would be wrong now and was
            # what hid the nesting defect before.
            hass.config_entries.async_update_entry(
                existing_entry, data=updates_to_apply
            )

            # Delete-after-import (update case): STAGED here, executed from the
            # durability gate in async_setup_entry. `async_update_entry` only
            # mutates the in-memory entry and schedules Home Assistant's
            # DEBOUNCED store save, so deleting the imported bundle here would
            # drop the credentials inside the save window: a crash there would
            # bring back the old credentials with the new bundle already gone.
            # The ticket therefore names this entry and carries the entry's
            # fresh `modified_at`, and the reload scheduled a few lines below
            # runs the very async_setup_entry that arms the gate. Only the copies
            # the import actually consumed are removed (see
            # _async_delete_watched_secrets): a co-located bundle of a different
            # account and a newer same-account bundle are kept.
            _async_stage_container_cleanup_for(
                hass,
                flow_id=self._async_cleanup_ticket_id(),
                unique_id=getattr(existing_entry, "unique_id", None),
                job=PendingContainerCleanup(
                    imported_stable_key=_stable_key_for_discovery_payload(normalized),
                    imported_digest=_digest_for_discovery_payload(normalized),
                ),
                entry=existing_entry,
            )

            def _normalize_tracking_lists() -> None:
                updated_attr = getattr(hass.config_entries, "updated", None)
                if isinstance(updated_attr, list) and len(updated_attr) > 1:
                    updated_attr[:] = updated_attr[-1:]

                reloaded_attr = getattr(hass.config_entries, "reloaded", None)
                if isinstance(reloaded_attr, list) and reloaded_attr:
                    seen_reload = False
                    trimmed_reload: list[Any] = []
                    for entry_id in reloaded_attr:
                        if entry_id == existing_entry.entry_id:
                            if seen_reload:
                                continue
                            seen_reload = True
                        trimmed_reload.append(entry_id)
                    if len(trimmed_reload) != len(reloaded_attr):
                        reloaded_attr[:] = trimmed_reload

            _normalize_tracking_lists()

            # The write above already notified the update listener, which reloads
            # for the changed credentials. Whichever side gets the latch reloads;
            # the other stands down instead of unloading the entry twice in a row.
            #
            # Ask first whether a reload can reach a setup at all. For a disabled
            # or ignored entry, and for a terminal state, ``async_reload`` returns
            # a truthy unload result without ever calling ``async_setup``, so no
            # release point fires and the claim would be stranded. The result
            # cannot tell that case from a reload that landed, which is why the
            # question belongs *before* the claim. No data is lost: the write has
            # already landed and the next setup reads ``entry.data`` afresh.
            reload_task: Any = None
            if _entry_reload_is_hopeless(hass, existing_entry.entry_id, existing_entry):
                _LOGGER.warning(
                    "Not reloading entry %s after a discovery update (state=%s, "
                    "disabled_by=%s, source=%s); the credentials are stored and "
                    "take effect the next time the entry is set up successfully",
                    existing_entry.entry_id,
                    getattr(existing_entry, "state", None),
                    getattr(existing_entry, "disabled_by", None),
                    getattr(existing_entry, "source", None),
                )
            elif _claim_entry_reload(hass, existing_entry.entry_id):
                reload_task = hass.config_entries.async_reload(existing_entry.entry_id)

            if inspect.isawaitable(reload_task):
                reload_coro = reload_task

                async def _reload_and_normalize() -> None:
                    try:
                        reloaded = await reload_coro
                    except BaseException:
                        # The claim above is a promise to reload. If the reload
                        # never lands -- ``OperationNotAllowed`` for an entry in
                        # a state that forbids unloading, or a cancelled task --
                        # none of the release points (unload, setup, removal)
                        # runs, and the latch would swallow every later reload
                        # of this entry.
                        _discard_entry_reload(hass, existing_entry.entry_id)
                        raise
                    else:
                        # No exception is not the same as "reloaded":
                        # ``async_reload`` reports a failed unload, a setup that
                        # returned ``False`` and a component it could not set up
                        # by *returning* falsy rather than by raising. Only the
                        # last of the three leaves the claim behind, and the
                        # shared helper is what knows the difference -- deciding
                        # it here as well would be a second truth that drifts.
                        # No retry is scheduled, deliberately and unlike
                        # reconfigure: this step ends in an abort anyway, a
                        # reload pushed against a setup that just failed would
                        # loop, and the next setup reads ``entry.data`` afresh.
                        if not reloaded and _falsy_reload_left_the_latch_behind(
                            hass, existing_entry.entry_id, existing_entry
                        ):
                            _LOGGER.warning(
                                "Reload of entry %s after a discovery update "
                                "ended without reloading; its new credentials "
                                "stay ineffective until the entry is reloaded. "
                                "Releasing the latch so a later write can "
                                "schedule one",
                                existing_entry.entry_id,
                            )
                            _discard_entry_reload(hass, existing_entry.entry_id)
                    finally:
                        _normalize_tracking_lists()

                create_task = getattr(hass, "async_create_task", None)
                task_name = f"{getattr(existing_entry, 'domain', DOMAIN)}.reload_after_discovery_update"
                if callable(create_task):
                    try:
                        create_task(_reload_and_normalize(), name=task_name)
                    except TypeError:
                        create_task(_reload_and_normalize())
                else:
                    loop = getattr(hass, "loop", None)
                    if loop is not None:
                        loop.create_task(
                            _reload_and_normalize(),
                            name=f"{DOMAIN}.reload_after_discovery_update",
                        )
            else:
                _normalize_tracking_lists()

        current_entries_callable = getattr(self, "_async_current_entries", None)
        if callable(current_entries_callable):
            try:
                current_entries_callable(include_ignore=False)
            except TypeError:  # Legacy signatures
                current_entries_callable()

        if abort_raised:
            return self.async_abort(reason="already_configured")

        return self.async_abort(reason="already_configured")

    async def async_step_discovery_update(
        self, discovery_info: Mapping[str, Any] | None
    ) -> FlowResult:
        """Provide legacy discovery-update entry point used by the helper."""

        return await self.async_step_discovery_update_info(discovery_info)

    async def async_step_hub(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle Add Hub flows by delegating to the standard user step."""

        context_obj = getattr(self, "context", None)
        entry_id: str | None = None
        if isinstance(context_obj, Mapping):
            raw_entry = context_obj.get("entry_id")
            if isinstance(raw_entry, str) and raw_entry:
                entry_id = raw_entry

        hass_obj = getattr(self, "hass", None)
        if hass_obj is None or not hasattr(hass_obj, "config_entries"):
            _LOGGER.error(
                "Add Hub flow invoked without Home Assistant context; aborting"
            )
            return self.async_abort(reason="unknown")
        hass = cast(HomeAssistant, hass_obj)

        config_entry_obj = getattr(self, "config_entry", None)
        if (
            config_entry_obj is None or not hasattr(config_entry_obj, "entry_id")
        ) and entry_id:
            config_entry_obj = hass.config_entries.async_get_entry(entry_id)

        if config_entry_obj is None or not hasattr(config_entry_obj, "entry_id"):
            _LOGGER.warning(
                "Add Hub flow missing config entry context (entry_id=%s); aborting",
                entry_id or "<unknown>",
            )
            return self.async_abort(reason="unknown")

        config_entry = cast(ConfigEntry, config_entry_obj)

        # Instantiate HubSubentryFlowHandler directly - we don't use
        # async_get_supported_subentry_types here because that method
        # intentionally returns {} to hide subentry UI buttons.
        # The "Add Hub" flow is a special entry point that bypasses the
        # normal HA subentry flow manager.
        handler = HubSubentryFlowHandler(config_entry)
        _LOGGER.info(
            "Add Hub flow requested; provisioning hub subentry (entry_id=%s)",
            config_entry.entry_id,
        )
        # Provide runtime context expected by ConfigSubentryFlow methods
        setattr(handler, "hass", hass)
        setattr(handler, "context", {"entry_id": config_entry.entry_id})
        result = handler.async_step_user(user_input)
        return await self._async_resolve_flow_result(result)

    # ------------------ Step: choose authentication path ------------------
    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Ask the user to choose how to provide credentials."""

        context_obj = getattr(self, "context", None)
        context_snapshot: dict[str, Any]
        context_entry_id: str | None = None
        context_source: str | None = None
        if isinstance(context_obj, Mapping):
            context_snapshot = {str(key): context_obj[key] for key in context_obj}
            raw_context_entry = context_obj.get("entry_id")
            if isinstance(raw_context_entry, str) and raw_context_entry:
                context_entry_id = raw_context_entry
            context_source = context_obj.get("source")
        else:
            context_snapshot = {}
        _LOGGER.info("Flow start: async_step_user (context=%s)", context_snapshot)

        bound_entry = getattr(self, "config_entry", None)
        bound_entry_id = (
            bound_entry.entry_id if isinstance(bound_entry, ConfigEntry) else None
        )

        config_entries = getattr(self.hass, "config_entries", None)
        async_entries = getattr(config_entries, "async_entries", None)

        existing_entries = async_entries(DOMAIN) if callable(async_entries) else []
        matching_entry = (
            next(
                (
                    entry
                    for entry in existing_entries
                    if entry.entry_id == context_entry_id
                ),
                None,
            )
            if context_entry_id
            else None
        )

        is_reconfigure_context = (
            context_source == SOURCE_RECONFIGURE
            or matching_entry is not None
            or (bound_entry_id is not None and bound_entry_id == context_entry_id)
        )
        bound_to_existing_entry = bool(bound_entry_id or context_entry_id)

        if existing_entries and not is_reconfigure_context:
            _LOGGER.debug(
                "async_step_user: Existing entries detected (found %d); deferring duplicate checks until credentials are available",
                len(existing_entries),
            )

        if is_reconfigure_context:
            if matching_entry is not None:
                self.context["entry_id"] = matching_entry.entry_id
            elif bound_entry_id is not None:
                self.context.setdefault("entry_id", bound_entry_id)
            return await self.async_step_reconfigure(None)

        # Do NOT check for duplicates here; self._auth_data is not yet populated.

        if user_input is not None:
            method = user_input.get("auth_method")
            _LOGGER.debug("User step: method selected = %s", method)
            if method == _AUTH_METHOD_SECRETS:
                return await self.async_step_secrets_json()
            if method == _AUTH_METHOD_INDIVIDUAL:
                return await self.async_step_individual_tokens()
            if (
                method is None
                and self._auth_data.get(CONF_OAUTH_TOKEN)
                and self._auth_data.get(CONF_GOOGLE_EMAIL)
            ):
                _LOGGER.debug(
                    "User step: confirm-only submission detected; proceeding to device selection.",
                )

                # CRITICAL FIX: Check for duplicates *after* auth data is present.
                email = cast(str, self._auth_data.get(CONF_GOOGLE_EMAIL))
                try:
                    await self._async_prepare_account_context(
                        email=email,
                        abort_on_duplicate=not bound_to_existing_entry,
                    )
                except data_entry_flow.AbortFlow:
                    return self.async_abort(reason="already_configured")

                return await self.async_step_device_selection()

        # Preflight (Track A for fresh installs): a bundle that is already lying
        # at one of the default watch paths must not be re-typed. The watcher
        # cannot offer it here -- it is armed in ``async_setup``, which a first
        # install without a config entry never runs, because Home Assistant only
        # imports the ``config_flow`` module to start this flow. So the flow
        # scans once, itself. One shot: a rejected offer returns to *this* form,
        # and the marker is what stops that from looping.
        #
        # ``not is_reconfigure_context`` is a deliberately redundant second gate:
        # the reconfigure branch above returns unconditionally, so this condition
        # cannot currently be reached with a reconfigure context and no test can
        # single it out (removing it keeps the suite green). It stays as a local
        # guard should that early return ever gain a fall-through -- the failure
        # it prevents, a preflight form hijacking a reconfigure dialog, is not
        # one to rediscover in the field.
        if (
            user_input is None
            and not is_reconfigure_context
            and not self._local_bundle_preflight_done
        ):
            preflight = await self._async_preflight_local_bundle(existing_entries)
            if preflight is not None:
                return preflight

        _LOGGER.debug("User step: presenting auth method selection form.")
        return self.async_show_form(step_id="user", data_schema=STEP_USER_DATA_SCHEMA)

    async def _async_preflight_local_bundle(
        self, existing_entries: CollIterable[ConfigEntry]
    ) -> FlowResult | None:
        """Scan the default watch paths once and offer an unconfigured bundle.

        Returns the ``found_local_bundle`` form, or ``None`` when the user step
        should just show its own form (no bundle, no executor, or every bundle
        belongs to an account that already has a config entry).

        The scan runs through ``hass.async_add_executor_job`` because reading and
        ``stat``-ing files in the event loop is forbidden; that hop is the only
        thing borrowed from :func:`_async_delete_watched_secrets`. Its *path*
        source is explicitly not borrowed: that one reads
        ``hass.data[DOMAIN]["discovery_manager"].watch_paths``, and on a fresh
        install there is no such manager yet, which is precisely the situation
        this preflight exists for. :func:`discovery._default_watch_paths` is the
        same list the manager would be built from.

        "Already configured" is decided per account, not per installation: with
        several accounts a second, still unknown bundle must still be offered.
        The account identity is derived through the same
        ``normalize_email`` -> ``unique_account_id`` chain that
        :meth:`_async_prepare_account_context` sets as the flow's ``unique_id``,
        so the comparison against the existing entries' ``unique_id`` cannot
        drift from the duplicate check that follows later in the flow.

        Deliberately age-blind: an old file is not a wrong file, and the login
        container may have produced it long before the user got around to adding
        the integration. The age is shown, never used as a filter.
        """

        self._local_bundle_preflight_done = True

        hass_obj = getattr(self, "hass", None)
        executor = getattr(hass_obj, "async_add_executor_job", None)
        if not callable(executor):
            _LOGGER.debug(
                "Local-bundle preflight skipped: no executor available on hass"
            )
            return None

        # Imported here (not at module level) to avoid a circular import:
        # discovery imports config_flow.
        from . import discovery as discovery_module

        def _scan() -> list[tuple[Path, _SecretsScanResult]]:
            return discovery_module.scan_secrets_bundles(
                discovery_module._default_watch_paths()
            )

        try:
            scanned: list[tuple[Path, _SecretsScanResult]] = await executor(_scan)
        except OSError as err:  # Unreadable mount: never block the setup form.
            _LOGGER.debug("Local-bundle preflight scan failed: %s", err)
            return None

        if not scanned:
            return None

        configured_ids = {
            unique_id
            for entry in existing_entries
            if isinstance(unique_id := getattr(entry, "unique_id", None), str)
            and unique_id
        }

        for path, scan in scanned:
            account_id = unique_account_id(normalize_email(scan.email))
            if account_id and account_id in configured_ids:
                _LOGGER.debug(
                    "Local-bundle preflight: skipping bundle of an already "
                    "configured account (%s)",
                    discovery_module._redact_account_for_log(
                        scan.email, scan.stable_key
                    ),
                )
                continue
            _LOGGER.info(
                "Local-bundle preflight: offering a bundle found on disk (%s)",
                discovery_module._redact_account_for_log(scan.email, scan.stable_key),
            )
            self._local_bundle_candidate = (path, scan)
            return await self.async_step_found_local_bundle()

        return None

    async def async_step_found_local_bundle(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Offer the bundle the preflight found, or fall back to the user step.

        Rejection (``use_found_bundle`` unset) returns to the auth-method form;
        the one-shot marker set by :meth:`_async_preflight_local_bundle` keeps
        that from bouncing straight back here.

        Every import failure -- an unusable token, a bundle without shared keys,
        no network -- re-shows *this* form with the error rather than aborting
        the flow, so the offer stays retryable and, above all, the rejection
        stays reachable.
        """

        errors: dict[str, str] = {}
        candidate = self._local_bundle_candidate
        if candidate is None:  # pragma: no cover - defensive
            return await self.async_step_user()
        path, scan = candidate

        if user_input is not None:
            if not user_input.get(_FIELD_USE_FOUND_BUNDLE, False):
                _LOGGER.debug("Local-bundle preflight: offer declined by the user")
                self._local_bundle_candidate = None
                return await self.async_step_user()
            imported = await self._async_import_local_bundle(scan, errors)
            if imported is not None:
                return imported

        return self.async_show_form(
            step_id="found_local_bundle",
            data_schema=vol.Schema(
                {vol.Required(_FIELD_USE_FOUND_BUNDLE, default=True): bool}
            ),
            errors=errors,
            description_placeholders={
                "bundle_path": str(path),
                # The account is the user's own and is shown to them in their own
                # UI, so it is spelled out here; only the *log* is redacted.
                "email": scan.email,
                "bundle_age": _format_bundle_age(scan.mtime),
            },
        )

    async def _async_import_local_bundle(
        self, scan: _SecretsScanResult, errors: dict[str, str]
    ) -> FlowResult | None:
        """Validate and persist a preflight bundle; ``None`` means "re-show form".

        Runs the same validation chain as the pasted-secrets path
        (``normalize_secrets_bundle`` -> single-key gate -> token selection), so
        the preflight cannot become a second, laxer import surface.

        On success the delete-after-import job is *staged* on the flow, not
        executed: it is the same :class:`PendingContainerCleanup` the watcher
        path stages, so both entry points delete through
        :func:`_async_delete_watched_secrets` behind the same durability gate.
        ``device_selection`` hands it over once the entry is actually created.
        """

        parsed = normalize_secrets_bundle(dict(scan.bundle))
        if _reject_if_shared_key_missing(parsed, errors):
            return None

        email = normalize_email(_extract_email_from_secrets(parsed))
        if not email:
            errors["base"] = "invalid_token"
            return None

        cands = _extract_oauth_candidates_from_secrets(parsed)
        if not cands:
            errors["base"] = "invalid_token"
            return None

        try:
            chosen = await async_pick_working_token(
                self.hass, email, cands, secrets_bundle=parsed
            )
        except (DependencyNotReady, ImportError) as exc:
            _register_dependency_error(errors, exc)
            return None

        if not chosen:
            _log_token_validation_failure(candidates=cands)
            errors["base"] = "cannot_connect"
            return None

        to_persist = chosen
        if _disqualifies_for_persistence(to_persist):
            alt = next(
                (v for (_src, v) in cands if not _disqualifies_for_persistence(v)),
                None,
            )
            if alt:
                to_persist = alt

        try:
            await self._async_prepare_account_context(email=email)
        except data_entry_flow.AbortFlow:
            return self.async_abort(reason="already_configured")

        self._auth_data = {
            DATA_AUTH_METHOD: _AUTH_METHOD_SECRETS,
            CONF_OAUTH_TOKEN: to_persist,
            CONF_GOOGLE_EMAIL: email,
        }
        self._auth_data.update(_persist_secrets_bundle(parsed, to_persist))
        self._local_bundle_pending_cleanup = PendingContainerCleanup(
            imported_stable_key=scan.stable_key,
            imported_digest=scan.digest,
        )
        return await self.async_step_device_selection()

    @_typed_callback
    def _async_stage_local_bundle_cleanup(self) -> None:
        """Hand a preflight import's delete-after-import job to the gate.

        A CREATE_ENTRY FlowResult is a promise, not a stored entry, so the
        irreversible delete waits behind the durability gate that
        ``async_setup_entry`` arms. Clearing the slot first makes a second call
        a no-op.
        """

        pending = self._local_bundle_pending_cleanup
        if pending is None:
            return
        self._local_bundle_pending_cleanup = None
        _async_stage_container_cleanup(
            getattr(self, "hass", None),
            flow_id=self._async_cleanup_ticket_id(),
            unique_id=getattr(self, "unique_id", None),
            job=pending,
        )

    # ------------------ Step: secrets.json path ------------------
    async def async_step_secrets_json(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Collect and validate secrets.json content, with failover and guard handling."""
        errors: dict[str, str] = {}

        schema = STEP_SECRETS_DATA_SCHEMA
        if selector is not None:
            schema = vol.Schema(
                {vol.Required("secrets_json"): selector({"text": {"multiline": True}})}
            )

        if user_input is not None:
            raw = user_input.get("secrets_json") or ""
            _LOGGER.debug("Secrets step: received input (chars=%d).", len(raw))
            parsed_secrets: dict[str, Any] | None = None
            try:
                parsed_candidate = json.loads(raw)
                if isinstance(parsed_candidate, dict):
                    parsed_secrets = normalize_secrets_bundle(parsed_candidate)
                else:
                    raise TypeError()
            except (json.JSONDecodeError, TypeError):
                parsed_secrets = None
            method, email, cands, err = _interpret_credentials_choice(
                user_input,
                secrets_field="secrets_json",
                token_field=CONF_OAUTH_TOKEN,
                email_field=CONF_GOOGLE_EMAIL,
            )
            if err:
                if err == "invalid_json":
                    errors["secrets_json"] = "invalid_json"
                else:
                    errors["base"] = err
            else:
                assert method == "secrets" and email and cands
                await self._async_prepare_account_context(email=email)

                try:
                    chosen = await async_pick_working_token(
                        self.hass,
                        email,
                        cands,
                        secrets_bundle=parsed_secrets,
                    )
                except (DependencyNotReady, ImportError) as exc:
                    _register_dependency_error(errors, exc)
                    return self.async_abort(reason="dependency_not_ready")
                else:
                    if not chosen:
                        _log_token_validation_failure(candidates=cands)
                        errors["base"] = "cannot_connect"
                    else:
                        # Persist validated token; prefer non-JWT candidate when possible
                        to_persist = chosen
                        bad_reason = _disqualifies_for_persistence(to_persist)
                        if bad_reason:
                            alt = next(
                                (
                                    v
                                    for (_src, v) in cands
                                    if not _disqualifies_for_persistence(v)
                                ),
                                None,
                            )
                            if alt:
                                to_persist = alt

                        self._auth_data = {
                            DATA_AUTH_METHOD: _AUTH_METHOD_SECRETS,
                            CONF_OAUTH_TOKEN: to_persist,
                            CONF_GOOGLE_EMAIL: email,
                        }
                        if parsed_secrets is not None:
                            self._auth_data.update(
                                _persist_secrets_bundle(parsed_secrets, to_persist)
                            )
                        return await self.async_step_device_selection()

        return self.async_show_form(
            step_id="secrets_json", data_schema=schema, errors=errors
        )

    # ------------------ Step: manual token + email ------------------
    async def async_step_individual_tokens(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Collect manual token and Google email, then validate."""
        errors: dict[str, str] = {}
        if user_input is not None:
            method, email, cands, err = _interpret_credentials_choice(
                user_input,
                secrets_field="secrets_json",
                token_field=CONF_OAUTH_TOKEN,
                email_field=CONF_GOOGLE_EMAIL,
            )
            if err:
                errors["base"] = err
            else:
                assert method == "manual" and email and cands
                await self._async_prepare_account_context(email=email)

                try:
                    chosen = await async_pick_working_token(self.hass, email, cands)
                except (DependencyNotReady, ImportError) as exc:
                    _register_dependency_error(errors, exc)
                    return self.async_abort(reason="dependency_not_ready")
                else:
                    if not chosen:
                        _log_token_validation_failure(candidates=cands)
                        errors["base"] = "cannot_connect"
                    else:
                        auth_method = _AUTH_METHOD_INDIVIDUAL
                        self._auth_data = {
                            CONF_OAUTH_TOKEN: chosen,
                            CONF_GOOGLE_EMAIL: email,
                        }
                        if isinstance(chosen, str) and chosen.startswith("aas_et/"):
                            auth_method = _AUTH_METHOD_SECRETS
                            self._auth_data[DATA_AAS_TOKEN] = chosen
                        else:
                            self._auth_data.pop(DATA_AAS_TOKEN, None)
                        self._auth_data[DATA_AUTH_METHOD] = auth_method
                        self._auth_data.pop(DATA_SECRET_BUNDLE, None)
                        return await self.async_step_device_selection()

        return self.async_show_form(
            step_id="individual_tokens",
            data_schema=STEP_INDIVIDUAL_DATA_SCHEMA,
            errors=errors,
        )

    # ------------------ Shared: build API for final probe ------------------
    async def _async_build_api_and_username(self) -> tuple[GoogleFindMyAPI, str, str]:
        """Construct an ephemeral API client from transient flow credentials."""
        email = self._auth_data.get(CONF_GOOGLE_EMAIL)
        oauth = self._auth_data.get(CONF_OAUTH_TOKEN)
        if not (email and oauth):
            raise HomeAssistantError("Missing credentials in setup flow.")
        api = await _async_new_api_for_probe(
            self.hass,
            email=email,
            token=oauth,
            secrets_bundle=self._auth_data.get(DATA_SECRET_BUNDLE),
        )
        return api, email, oauth

    # ------------------ Step: device selection & non-secret options ------------------
    async def async_step_device_selection(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Finalize the initial setup: optional device probe + non-secret options."""
        errors: dict[str, str] = {}

        # Ensure unique_id is set (should already be done)
        email_for_account = self._auth_data.get(CONF_GOOGLE_EMAIL)
        if isinstance(email_for_account, str) and email_for_account:
            await self._async_prepare_account_context(email=email_for_account)

        # Try a single probe (optional; setup will re-validate anyway)
        if not self._available_devices:
            try:
                api, username, token = await self._async_build_api_and_username()
                devices = await _try_probe_devices(api, email=username, token=token)
                if devices:
                    self._available_devices = [
                        (d.get("name") or d.get("id") or "", d.get("id") or "")
                        for d in devices
                    ]
            except (DependencyNotReady, ImportError) as exc:
                _register_dependency_error(errors, exc)
            except Exception as err:  # noqa: BLE001
                if not _is_multi_entry_guard_error(err):
                    key = _map_api_exc_to_error_key(err)
                    errors["base"] = key

        # Build options schema dynamically
        schema_fields: dict[Any, Any] = {
            vol.Optional(OPT_LOCATION_POLL_INTERVAL): vol.All(
                vol.Coerce(int), vol.Range(min=60, max=3600)
            ),
            vol.Optional(OPT_DEVICE_POLL_DELAY): vol.All(
                vol.Coerce(int), vol.Range(min=1, max=60)
            ),
            vol.Optional(OPT_MAP_VIEW_TOKEN_EXPIRATION): bool,
            vol.Optional(OPT_MAP_VIEW_ENABLED): bool,
        }
        if OPT_GOOGLE_HOME_FILTER_ENABLED is not None:
            schema_fields[vol.Optional(OPT_GOOGLE_HOME_FILTER_ENABLED)] = bool
        if OPT_GOOGLE_HOME_FILTER_KEYWORDS is not None:
            schema_fields[vol.Optional(OPT_GOOGLE_HOME_FILTER_KEYWORDS)] = str
        if OPT_ENABLE_STATS_ENTITIES is not None:
            schema_fields[vol.Optional(OPT_ENABLE_STATS_ENTITIES)] = bool

        base_schema = vol.Schema(schema_fields)

        # Defaults
        defaults: dict[str, Any] = {
            OPT_LOCATION_POLL_INTERVAL: DEFAULT_LOCATION_POLL_INTERVAL,
            OPT_DEVICE_POLL_DELAY: DEFAULT_DEVICE_POLL_DELAY,
            OPT_MAP_VIEW_TOKEN_EXPIRATION: DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
            OPT_MAP_VIEW_ENABLED: DEFAULT_MAP_VIEW_ENABLED,
            OPT_DELETE_CACHES_ON_REMOVE: DEFAULT_DELETE_CACHES_ON_REMOVE,
        }
        if (
            OPT_GOOGLE_HOME_FILTER_ENABLED is not None
            and DEFAULT_GOOGLE_HOME_FILTER_ENABLED is not None
        ):
            defaults[OPT_GOOGLE_HOME_FILTER_ENABLED] = (
                DEFAULT_GOOGLE_HOME_FILTER_ENABLED
            )
        if (
            OPT_GOOGLE_HOME_FILTER_KEYWORDS is not None
            and DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS is not None
        ):
            defaults[OPT_GOOGLE_HOME_FILTER_KEYWORDS] = (
                DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS
            )
        if (
            OPT_ENABLE_STATS_ENTITIES is not None
            and DEFAULT_ENABLE_STATS_ENTITIES is not None
        ):
            defaults[OPT_ENABLE_STATS_ENTITIES] = DEFAULT_ENABLE_STATS_ENTITIES

        reconfigure_defaults = self.context.get("reconfigure_options")
        if isinstance(reconfigure_defaults, Mapping):
            for key, value in reconfigure_defaults.items():
                if key is None:
                    continue
                if value is not None:
                    defaults[key] = value

        schema_with_defaults = self.add_suggested_values_to_schema(
            base_schema, defaults
        )

        if user_input is not None:
            # Data = credentials; options = runtime settings
            data_payload: dict[str, Any] = {
                DATA_AUTH_METHOD: self._auth_data.get(DATA_AUTH_METHOD),
                # We persist AAS master tokens as well; they are required to mint service tokens.
                CONF_OAUTH_TOKEN: self._auth_data.get(CONF_OAUTH_TOKEN),
                CONF_GOOGLE_EMAIL: self._auth_data.get(CONF_GOOGLE_EMAIL),
                DATA_SUBENTRY_KEY: None,
            }
            if DATA_SECRET_BUNDLE in self._auth_data:
                data_payload[DATA_SECRET_BUNDLE] = self._auth_data[DATA_SECRET_BUNDLE]
            aas_token = self._auth_data.get(DATA_AAS_TOKEN)
            if isinstance(aas_token, str) and aas_token:
                data_payload[DATA_AAS_TOKEN] = aas_token

            options_payload: dict[str, Any] = {}
            managed_option_keys: set[str] = set()
            for marker in schema_fields.keys():
                # `marker` may be a voluptuous wrapper; retrieve the underlying key
                schema_attr = getattr(marker, "schema", marker)
                if isinstance(schema_attr, str):
                    real_key = schema_attr
                elif isinstance(schema_attr, CollIterable) and not isinstance(
                    schema_attr, (bytes, bytearray)
                ):
                    real_key = next(iter(schema_attr))
                else:
                    real_key = cast(str, schema_attr)
                managed_option_keys.add(real_key)
                options_payload[real_key] = user_input.get(
                    real_key, defaults.get(real_key)
                )
            options_payload[OPT_OPTIONS_SCHEMA_VERSION] = (
                2  # bump schema version at creation
            )

            entry_for_update: ConfigEntry | None = None
            entry_id = self.context.get("entry_id")
            if isinstance(entry_id, str):
                entry_for_update = self.hass.config_entries.async_get_entry(entry_id)
            if self.context.get("is_reconfigure") and entry_for_update is not None:
                subentry_context = self._reset_reconfigure_subentry_context(
                    entry_for_update
                )
            else:
                subentry_context = self._ensure_subentry_context()
            if entry_for_update is not None:
                await self._async_trigger_core_subentry_repair(
                    self.hass, entry_for_update
                )
                await self._async_sync_feature_subentries(
                    entry_for_update,
                    options_payload=options_payload,
                    defaults=defaults,
                    context_map=subentry_context,
                )
                if self.context.get("is_reconfigure"):
                    merged_data = dict(getattr(entry_for_update, "data", {}) or {})
                    for removable in (
                        DATA_AUTH_METHOD,
                        CONF_OAUTH_TOKEN,
                        CONF_GOOGLE_EMAIL,
                        DATA_SECRET_BUNDLE,
                        DATA_AAS_TOKEN,
                        DATA_SUBENTRY_KEY,
                    ):
                        merged_data.pop(removable, None)
                    for key, value in data_payload.items():
                        if value is not None:
                            merged_data[key] = value

                    existing_options = dict(
                        getattr(entry_for_update, "options", {}) or {}
                    )
                    for managed in managed_option_keys | {OPT_OPTIONS_SCHEMA_VERSION}:
                        existing_options.pop(managed, None)
                    existing_options.update(options_payload)

                    try:
                        self.hass.config_entries.async_update_entry(
                            entry_for_update,
                            data=merged_data,
                            options=existing_options,
                        )
                    except TypeError:
                        fallback_options = dict(existing_options)
                        fallback_payload = dict(merged_data)
                        fallback_payload.update(fallback_options)
                        self.hass.config_entries.async_update_entry(
                            entry_for_update,
                            data=fallback_payload,
                        )
                        setattr(entry_for_update, "options", fallback_options)

                    await self._async_cleanup_stale_subentries(
                        entry_for_update, subentry_context
                    )

                    self.context.pop("is_reconfigure", None)
                    self.context.pop("reauth_success_reason_override", None)
                    self.context.pop("reconfigure_options", None)
                    await self._async_reload_entry_after_reconfigure(entry_for_update)
                    return self.async_abort(reason="reconfigure_successful")
            else:
                subentry_context.setdefault(self._subentry_key_core_tracking, None)
                subentry_context.setdefault(self._subentry_key_service, None)

            create_entry = cast(Callable[..., FlowResult], self.async_create_entry)
            try:
                created = create_entry(
                    # **Change**: title is always the email for clear multi-account display
                    title=self._auth_data.get(CONF_GOOGLE_EMAIL)
                    or "Google Find My Device",
                    data=data_payload,
                    options=options_payload,
                )
            except TypeError:
                # Older HA cores: merge options into data
                shadow = dict(data_payload)
                shadow.update(options_payload)
                created = create_entry(
                    title=self._auth_data.get(CONF_GOOGLE_EMAIL)
                    or "Google Find My Device",
                    data=shadow,
                )
            # Entry *promised*, not yet stored: the bundle the user accepted in
            # ``found_local_bundle`` is deleted from disk only after the entry
            # has been observed in storage (two-phase delete, F4/P2).
            self._async_stage_local_bundle_cleanup()
            # ... and pin the ticket to that promise BEFORE Home Assistant runs
            # async_finish_flow. From here on the flow's removal no longer means
            # "no entry": the first async_setup_entry runs inside
            # ConfigEntries.async_add, so a ConfigEntryNotReady leaves the entry
            # created, the ticket unclaimed and the flow removed all the same.
            # Staging above, marking here: a ticket only exists once a job was
            # staged. The mark is idempotent, so a caller that stages a further
            # job after this result (async_step_discovery does) marks again at
            # its own site rather than relying on this one.
            self._async_mark_own_cleanup_ticket_entry_promised()
            return created

        return self.async_show_form(
            step_id="device_selection", data_schema=schema_with_defaults, errors=errors
        )

    async def _async_reload_entry_after_reconfigure(
        self, entry_for_update: ConfigEntry
    ) -> None:
        """Reload the updated entry, deferring when the core forbids it."""

        hass_data = getattr(self.hass, "data", None)
        if not isinstance(hass_data, dict):
            hass_data = {}
            with suppress(Exception):
                setattr(self.hass, "data", hass_data)

        domain_bucket = cast(dict[str, Any], hass_data.setdefault(DOMAIN, {}))
        pending_refresh = domain_bucket.setdefault(
            "pending_reconfigure_device_list_refresh", set()
        )
        if isinstance(pending_refresh, set):
            pending_refresh.add(entry_for_update.entry_id)

        markers = domain_bucket.setdefault("recent_reconfigure_markers", {})
        reconfigure_ts = time.time()
        if isinstance(markers, dict):
            markers[entry_for_update.entry_id] = reconfigure_ts

        runtime_entries = domain_bucket.get("entries")
        runtime = (
            runtime_entries.get(entry_for_update.entry_id)
            if isinstance(runtime_entries, dict)
            else None
        )
        coordinator = getattr(runtime, "coordinator", None)
        mark_reconfigure = getattr(coordinator, "mark_recent_reconfigure", None)
        if callable(mark_reconfigure):
            mark_reconfigure(reconfigure_ts)
        request_list_refresh = getattr(coordinator, "request_device_list_refresh", None)
        if callable(request_list_refresh):
            request_list_refresh(reason="reconfigure")

        # Set once a hand-off to ``async_schedule_reload`` has been accepted.
        # From that moment a core-owned task carries the reload, so a later dead
        # end on *this* path is no longer a dead end for the entry, and giving
        # the latch back would open the window the latch exists to close.
        manager_owns_reload = False

        def _give_up_on_reload() -> None:
            """No further reload is coming from here; hand the latch back.

            This path claims the shared latch and then reloads *directly*, so
            the promise stays open until a reload actually arrives. Every dead
            end below therefore releases it: the release points (unload, setup,
            entry removal) never run without a reload, and a latch left behind
            would suppress every later reload of this entry.

            Only at *dead ends*, though, and only while this path still owns the
            promise. A failed hand-off is a dead end exactly where nothing
            follows it, which is the three result-driven hand-offs; each of them
            reaches this helper through ``_give_up_after_a_falsy_reload``, which
            first asks whether the latch can still be ours -- a falsy reload
            result may mean one of our lifecycle hooks already released it. The
            one exception is the hand-off in the ``OperationNotAllowed`` clause:
            the deferred retry below still follows it, so a refusal there is not
            the end of the path and is deliberately left unchecked. The dead ends
            that carry no result (exception, cancellation, an unarmable retry, a
            task handle that reports no outcome) call this helper *directly*:
            there is no result to classify, so the unconditional release is the
            only safe direction. A *successful*
            hand-off is no dead end either, and it is the sharper case: the core
            task it created will
            reach the unload that releases the latch anyway, so releasing here
            as well would leave the entry unlatched while its reload is still on
            its way, and the next claimant would queue a second teardown. If
            that core task dies without reloading, the latch is stuck -- the
            known residual described in the runtime-patterns contract, not
            something this release can repair.
            """

            if manager_owns_reload:
                _LOGGER.debug(
                    "Reload of entry %s is already owned by a scheduled reload; "
                    "keeping the latch",
                    entry_for_update.entry_id,
                )
                return
            _discard_entry_reload(self.hass, entry_for_update.entry_id)

        def _give_up_after_a_falsy_reload() -> None:
            """Give the latch back only if it can still be ours.

            The three dead ends that follow a **falsy reload result** differ from
            every other dead end on this path: there a reload actually ran, and
            how it ended decides who holds the latch now. Two of the falsy
            outcomes mean one of our lifecycle hooks already handed the latch
            back -- a failed unload releases at ``async_unload_entry``, a failed
            setup at the head of ``async_setup_entry`` -- so between that release
            and this line another writer may have claimed it. Releasing blindly
            would discard *their* promise, and the next caller would queue the
            second teardown this latch exists to prevent. Since
            ``discard_pending_entry_reload`` is a bare ``set.discard`` with no
            notion of ownership, that release cannot tell whose claim it drops.

            So ask the one shared classifier instead of guessing, the same one
            the discovery update and ``_release_claim_when_reload_fails``
            consult; a second, private answer here would drift from theirs. It
            fails towards releasing, which is the right direction: where the
            entry or the component list cannot be read, one reload too many
            beats a promise nobody redeems.

            Only for falsy *results*. The exception, cancellation and
            arming-failure dead ends keep releasing unconditionally: there no
            result exists, the post-reload state proves nothing, and a latch left
            behind would swallow every later reload of this entry for good.
            """

            if not _falsy_reload_left_the_latch_behind(
                self.hass, entry_for_update.entry_id, entry_for_update
            ):
                _LOGGER.debug(
                    (
                        "Reload of entry %s returned falsy after a lifecycle "
                        "release; leaving the latch to whoever holds it now"
                    ),
                    entry_for_update.entry_id,
                )
                return
            _give_up_on_reload()

        def _schedule_reload_via_manager(reason: str) -> bool:
            """Hand the reload to the core scheduler; report whether it took it."""

            nonlocal manager_owns_reload

            schedule_reload = getattr(
                self.hass.config_entries, "async_schedule_reload", None
            )
            if not callable(schedule_reload):
                _LOGGER.debug(
                    "Reload after reconfigure (%s) not scheduled for entry %s; helper missing",
                    reason,
                    entry_for_update.entry_id,
                )
                return False

            try:
                schedule_reload(entry_for_update.entry_id)
            except Exception:  # noqa: BLE001 - surface scheduler failures
                _LOGGER.exception(
                    "Failed to schedule reload after reconfigure (%s) for entry %s",
                    reason,
                    entry_for_update.entry_id,
                )
                return False

            manager_owns_reload = True
            return True

        def _log_failed_reload(result: Any, *, deferred: bool) -> None:
            if result is False:
                _LOGGER.warning(
                    (
                        "Reload%s after reconfigure for entry %s returned False; "
                        "entities may remain unavailable until the next attempt"
                    ),
                    " (deferred)" if deferred else "",
                    entry_for_update.entry_id,
                )

        def _handle_reports_its_outcome(task: Any) -> bool:
            """Whether ``task`` implements the part of the future protocol we use.

            The two call sites take whatever ``hass.async_create_task`` (or the
            loop) hands back. A real ``asyncio.Task`` always answers both, but a
            handle that carries only ``add_done_callback`` would make the
            callback raise ``AttributeError`` into the loop and leave the latch
            claimed -- the very state this path exists to avoid. Ask for both,
            and stand down together with the callback if either is missing.
            """

            return callable(getattr(task, "add_done_callback", None)) and callable(
                getattr(task, "cancelled", None)
            )

        def _log_task_result(task: asyncio.Future[Any]) -> None:
            if task.cancelled():
                # A cancelled task is a dead end like any other: ``result()``
                # would raise ``CancelledError``, which derives from
                # ``BaseException`` and would sail past the handler below,
                # leaving the promise open. Ask before taking the result.
                _LOGGER.debug(
                    "Deferred reload after reconfigure for entry %s was cancelled",
                    entry_for_update.entry_id,
                )
                _give_up_on_reload()
                return

            try:
                task_result = task.result()
            except Exception:  # noqa: BLE001 - log unexpected task failures
                _LOGGER.exception(
                    "Deferred reload after reconfigure for entry %s raised an exception",
                    entry_for_update.entry_id,
                )
                _give_up_on_reload()
                return

            _log_failed_reload(task_result, deferred=True)

            if task_result is False:
                # A refused hand-off is the end of this path: nothing else
                # follows the deferred task, so the promise has to go back --
                # but only if it is still ours to give (see
                # ``_give_up_after_a_falsy_reload``).
                if not _schedule_reload_via_manager(
                    "reload_returned_false_deferred_task"
                ):
                    _give_up_after_a_falsy_reload()

        async def _async_call_reload() -> Any:
            reload_result = self.hass.config_entries.async_reload(
                entry_for_update.entry_id
            )
            if inspect.isawaitable(reload_result):
                reload_result = await reload_result

            return reload_result

        # Ask first whether a reload can reach a setup at all, for the same
        # reason as the discovery update above: a disabled or ignored entry, and
        # an entry in a terminal state, makes ``async_reload`` return a truthy
        # unload result without calling ``async_setup``, and no release point
        # fires. Standing down here costs nothing -- the reconfigured data is
        # already written and the next setup reads it afresh -- while claiming
        # would strand the promise.
        #
        # This narrows the window, it does not close it: the entry can be
        # disabled *between* this check and the reload, because
        # ``async_set_disabled_by`` sets the field before it reloads and the
        # flow-abort it triggers filters on ``SOURCE_REAUTH``. Closing that
        # remainder needs a latch that knows its owner, which this one does not;
        # see the runtime-patterns contract.
        if _entry_reload_is_hopeless(
            self.hass, entry_for_update.entry_id, entry_for_update
        ):
            _LOGGER.warning(
                "Not reloading entry %s after a reconfigure (state=%s, "
                "disabled_by=%s, source=%s); the reconfigured data is stored and "
                "takes effect the next time the entry is set up successfully",
                entry_for_update.entry_id,
                getattr(entry_for_update, "state", None),
                getattr(entry_for_update, "disabled_by", None),
                getattr(entry_for_update, "source", None),
            )
            return

        # One owner for the reload: the entry update this helper follows notifies
        # the credential update listener, and a reconfigure rewrites exactly the
        # keys that listener watches. Whoever claims the latch first reloads; the
        # other side stands down instead of tearing the entry down twice.
        if not _claim_entry_reload(self.hass, entry_for_update.entry_id):
            _LOGGER.debug(
                "Reload after reconfigure for entry %s not scheduled; a reload is "
                "already on its way",
                entry_for_update.entry_id,
            )
            return

        try:
            reload_result = await _async_call_reload()
        except OperationNotAllowed:
            _schedule_reload_via_manager("operation_not_allowed")

            def _schedule_reload(_: Any) -> None:
                try:
                    reload_result_inner = self.hass.config_entries.async_reload(
                        entry_for_update.entry_id
                    )
                except OperationNotAllowed:
                    _LOGGER.warning(
                        (
                            "Deferred reload after reconfigure for entry %s was "
                            "rejected by Home Assistant"
                        ),
                        entry_for_update.entry_id,
                    )
                    _give_up_on_reload()
                    return
                except Exception:  # noqa: BLE001 - logged for visibility
                    _LOGGER.exception(
                        "Deferred reload after reconfigure for entry %s failed",
                        entry_for_update.entry_id,
                    )
                    _give_up_on_reload()
                    return

                if inspect.isawaitable(reload_result_inner):
                    create_task = getattr(self.hass, "async_create_task", None)
                    if callable(create_task):
                        task = create_task(reload_result_inner)
                        if _handle_reports_its_outcome(task):
                            task.add_done_callback(_log_task_result)
                        else:
                            # The docstring above promises to stand down together
                            # with the callback. Without it nobody watches how the
                            # reload ends, so a task that ends without reloading
                            # would keep the promise open for good.
                            _give_up_on_reload()
                        return

                    loop = getattr(self.hass, "loop", None)
                    if loop is not None:
                        task = loop.create_task(
                            reload_result_inner,
                            name=f"{DOMAIN}.deferred_reload_after_reconfigure",
                        )
                        if _handle_reports_its_outcome(task):
                            task.add_done_callback(_log_task_result)
                        else:
                            _give_up_on_reload()
                        return

                    if inspect.iscoroutine(reload_result_inner):
                        reload_result_inner.close()
                    _LOGGER.error(
                        (
                            "Deferred reload after reconfigure for entry %s could "
                            "not be scheduled; no task runner available"
                        ),
                        entry_for_update.entry_id,
                    )
                    _give_up_on_reload()
                    return

                _log_failed_reload(reload_result_inner, deferred=True)

                if reload_result_inner is False:
                    # Last chance on this path: the deferred retry has already
                    # run, so a refused hand-off leaves nobody to reload. Still
                    # only release a claim that can still be ours.
                    if not _schedule_reload_via_manager(
                        "reload_returned_false_deferred"
                    ):
                        _give_up_after_a_falsy_reload()

            # Inside a handler: an exception raised here is NOT caught by the
            # sibling ``except BaseException`` below (Python does not consult
            # further clauses for an error raised from within one). Without this
            # guard a hass whose loop is already closed -- shutdown during a
            # reconfigure -- would leave the claim behind for good.
            try:
                async_call_later(self.hass, 0, _schedule_reload)
            except Exception:  # noqa: BLE001 - the retry is the last chance
                _LOGGER.exception(
                    "Deferred reload after reconfigure for entry %s could not be armed",
                    entry_for_update.entry_id,
                )
                _give_up_on_reload()
            return
        except BaseException:
            # Anything other than the rejection handled above ends this path
            # without a reload, and the flow task dies with it. Unreachable for
            # ``OperationNotAllowed``: that clause always returns.
            _give_up_on_reload()
            raise

        _log_failed_reload(reload_result, deferred=False)

        if reload_result is False:
            # End of this path. If the core scheduler refuses the hand-off there
            # is no deferred retry behind it, so keeping the claim would strand
            # the promise. (The ``operation_not_allowed`` hand-off above is
            # deliberately unchecked: the deferred retry still follows it, which
            # is why ``_give_up_on_reload`` calls a refused hand-off "no dead
            # end" there.)
            if not _schedule_reload_via_manager("reload_returned_false"):
                _give_up_after_a_falsy_reload()

    # ------------------ Reauthentication ------------------
    async def async_step_reauth(self, entry_data: dict[str, Any]) -> FlowResult:
        """Start a reauthentication flow linked to an existing entry context."""
        return await self.async_step_reauth_confirm()

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle manual reconfiguration initiated from the config entry UI."""

        entry_id = self.context.get("entry_id")
        if not isinstance(entry_id, str):
            return self.async_abort(reason="unknown")

        entry = self.hass.config_entries.async_get_entry(entry_id)
        if entry is None:
            return self.async_abort(reason="unknown")

        placeholders = dict(self.context.get("title_placeholders", {}) or {})
        email = normalize_email_or_default(entry.data.get(CONF_GOOGLE_EMAIL))
        if email:
            placeholders["email"] = email
        if placeholders:
            self.context["title_placeholders"] = placeholders

        self._auth_data = {}
        for key in (
            DATA_AUTH_METHOD,
            CONF_OAUTH_TOKEN,
            CONF_GOOGLE_EMAIL,
            DATA_SECRET_BUNDLE,
            DATA_AAS_TOKEN,
        ):
            value = entry.data.get(key)
            if value is not None:
                self._auth_data[key] = value
        if CONF_GOOGLE_EMAIL not in self._auth_data and email:
            self._auth_data[CONF_GOOGLE_EMAIL] = email

        existing_unique_id = getattr(entry, "unique_id", None)
        if existing_unique_id:
            await self.async_set_unique_id(existing_unique_id, raise_on_progress=False)

        defaults = dict(DEFAULT_OPTIONS)
        entry_options = getattr(entry, "options", {}) or {}
        if isinstance(entry_options, Mapping):
            for opt_key, opt_value in entry_options.items():
                if opt_value is not None:
                    defaults[opt_key] = opt_value

        for opt_key in (
            OPT_LOCATION_POLL_INTERVAL,
            OPT_DEVICE_POLL_DELAY,
            OPT_MAP_VIEW_TOKEN_EXPIRATION,
            OPT_MAP_VIEW_ENABLED,
            OPT_CONTRIBUTOR_MODE,
            OPT_GOOGLE_HOME_FILTER_ENABLED,
            OPT_GOOGLE_HOME_FILTER_KEYWORDS,
            OPT_ENABLE_STATS_ENTITIES,
        ):
            if opt_key is None:
                continue
            if opt_key not in defaults and opt_key in entry.data:
                defaults[opt_key] = entry.data[opt_key]

        self.context["reconfigure_options"] = defaults
        self.context["is_reconfigure"] = True

        subentry_context = self._ensure_subentry_context()
        subentries = getattr(entry, "subentries", None)
        if isinstance(subentries, Mapping):
            for subentry in subentries.values():
                data = getattr(subentry, "data", {}) or {}
                group_key = data.get("group_key")
                if isinstance(group_key, str) and group_key in subentry_context:
                    subentry_context[group_key] = getattr(subentry, "subentry_id", None)

        if user_input is None:
            form_result = self.async_show_form(
                step_id="reconfigure",
                data_schema=vol.Schema({}),
                description_placeholders=placeholders or None,
            )
            if inspect.isawaitable(form_result):
                return await form_result
            return form_result

        oauth_token = self._auth_data.get(CONF_OAUTH_TOKEN)
        flow_result: FlowResult | Awaitable[FlowResult]
        if oauth_token:
            flow_result = await self.async_step_device_selection()
        else:
            self.context["reauth_success_reason_override"] = "reconfigure_successful"
            flow_result = await self.async_step_reauth_confirm()

        if not isinstance(flow_result, dict):
            return await flow_result
        return flow_result

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Collect and validate new credentials for this entry, then update+reload."""
        errors: dict[str, str] = {}

        entry = self.hass.config_entries.async_get_entry(self.context["entry_id"])
        assert entry is not None
        raw_email = entry.data.get(CONF_GOOGLE_EMAIL)
        fixed_email = normalize_email_or_default(raw_email)

        if selector is not None:
            schema = vol.Schema(
                {
                    vol.Optional(_REAUTH_FIELD_SECRETS): selector(
                        {"text": {"multiline": True}}
                    ),
                }
            )
        else:
            schema = vol.Schema(
                {
                    vol.Optional(_REAUTH_FIELD_SECRETS): str,
                }
            )

        if user_input is not None:
            method, payload, err = _interpret_reauth_choice(user_input)
            if err:
                if err == "invalid_json":
                    errors[_REAUTH_FIELD_SECRETS] = "invalid_json"
                else:
                    errors["base"] = err
            else:
                try:
                    if method == "secrets":
                        if not isinstance(payload, Mapping):
                            errors["base"] = "invalid_token"
                        else:
                            # Normalize locally so the gate, the email/token
                            # extraction, and the persisted bundle all operate on
                            # the same whitespace-normalized, scoped-key-promoted
                            # bundle, instead of depending on an implicit
                            # invariant established by an upstream caller.
                            # ``normalize_secrets_bundle`` is idempotent, so this
                            # is a no-op when ``payload`` was already normalized.
                            parsed: dict[str, Any] = normalize_secrets_bundle(
                                dict(payload)
                            )
                            extracted_email = normalize_email(
                                _extract_email_from_secrets(parsed)
                            )
                            cands = _extract_oauth_candidates_from_secrets(parsed)

                            if extracted_email and extracted_email != fixed_email:
                                existing = _find_entry_by_email(
                                    self.hass, extracted_email
                                )
                                if existing is not None:
                                    return self.async_abort(reason="already_configured")
                                errors["base"] = "email_mismatch"
                            # Single-key rule: a shared_key-less bundle is a
                            # non-renewable dead end. Gate it BEFORE the token
                            # probe, mirroring the initial/options/discovery
                            # surfaces where the gate always dominates token
                            # validation. This sits INSIDE the email-matches
                            # branch so a foreign/already-configured bundle keeps
                            # its email_mismatch/already_configured precedence
                            # above. Hoisting the gate makes a shared_key-less
                            # bundle with a dead token return the deterministic
                            # ``keys_missing`` instead of masking it as
                            # ``cannot_connect`` from a failed probe.
                            elif not _reject_if_shared_key_missing(parsed, errors):
                                try:
                                    chosen = await async_pick_working_token(
                                        self.hass,
                                        fixed_email,
                                        cands,
                                        secrets_bundle=parsed,
                                    )
                                except (DependencyNotReady, ImportError) as exc:
                                    _register_dependency_error(errors, exc)
                                else:
                                    if not chosen:
                                        _log_token_validation_failure(candidates=cands)
                                        errors["base"] = "cannot_connect"
                                    else:
                                        # Prefer non-JWT if available
                                        to_persist = chosen
                                        bad_reason = _disqualifies_for_persistence(
                                            to_persist
                                        )
                                        if bad_reason:
                                            alt = next(
                                                (
                                                    v
                                                    for (_src, v) in cands
                                                    if not _disqualifies_for_persistence(
                                                        v
                                                    )
                                                ),
                                                None,
                                            )
                                            if alt:
                                                to_persist = alt
                                        updated_data = {
                                            **entry.data,
                                            DATA_AUTH_METHOD: _AUTH_METHOD_SECRETS,
                                            **_persist_secrets_bundle(
                                                parsed, to_persist
                                            ),
                                        }
                                        if not (
                                            isinstance(to_persist, str)
                                            and to_persist.startswith("aas_et/")
                                        ):
                                            updated_data.pop(DATA_AAS_TOKEN, None)
                                        await self._async_clear_cached_aas_token(entry)
                                        # The single-key gate above already
                                        # guaranteed a usable shared_key, so the
                                        # persist runs unconditionally here; record
                                        # the key for operators.
                                        _LOGGER.info(
                                            "Reauth for %s: shared_key present in secrets bundle",
                                            _mask_email_for_logs(fixed_email),
                                        )
                                        success_reason = self.context.get(
                                            "reauth_success_reason_override",
                                            "reauth_successful",
                                        )
                                        return self._async_update_entry_and_abort(
                                            entry=entry,
                                            data=updated_data,
                                            reason=success_reason,
                                        )
                except Exception as err2:  # noqa: BLE001
                    if _is_multi_entry_guard_error(err2):
                        # Defer: accept first candidate and reload
                        if method == "secrets" and isinstance(payload, Mapping):
                            # Normalize once and gate on the SAME object that is
                            # persisted, so the single-key gate and the stored
                            # bundle can never disagree. An entry-scope guard
                            # error must not become a bypass for a shared_key-less
                            # (or whitespace-corrupted) bundle.
                            parsed = normalize_secrets_bundle(dict(payload))
                            if not _reject_if_shared_key_missing(parsed, errors):
                                cands = _extract_oauth_candidates_from_secrets(parsed)
                                token_first = cands[0][1] if cands else ""
                                updated_data = {
                                    **entry.data,
                                    DATA_AUTH_METHOD: _AUTH_METHOD_SECRETS,
                                    **_persist_secrets_bundle(parsed, token_first),
                                }
                                if not (
                                    isinstance(token_first, str)
                                    and token_first.startswith("aas_et/")
                                ):
                                    updated_data.pop(DATA_AAS_TOKEN, None)
                                await self._async_clear_cached_aas_token(entry)
                                return self._async_update_entry_and_abort(
                                    entry=entry,
                                    data=updated_data,
                                    reason="reauth_successful",
                                )
                        elif method == "secrets":
                            # payload is not a Mapping: malformed deferral input.
                            errors["base"] = "invalid_token"
                    # Fall back to the generic mapped error only when the
                    # deferral branch above did not already classify the failure
                    # (for example the single-key gate setting ``keys_missing``);
                    # do not clobber a more specific error.
                    errors.setdefault("base", _map_api_exc_to_error_key(err2))

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=schema,
            errors=errors,
            description_placeholders={"email": fixed_email},
        )

    async def _async_clear_cached_aas_token(self, entry: ConfigEntry) -> None:
        """Best-effort removal of the cached AAS token when credentials are replaced.

        Called from the ``secrets.json`` reauth branches and from the options-flow
        credential refresher, so the stale AAS token cannot outlive the OAuth token
        it was minted from.
        """

        cache = self._get_entry_cache(entry)
        if cache is None:
            return

        for attr in ("async_set_cached_value", "set"):
            setter = getattr(cache, attr, None)
            if not callable(setter):
                continue
            try:
                result = setter(DATA_AAS_TOKEN, None)
                if inspect.isawaitable(result):
                    await result
                return
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "Clearing cached AAS token failed.",
                    extra={
                        "setter": attr,
                        "entry_id": getattr(entry, "entry_id", None),
                    },
                    exc_info=err,
                )
        _LOGGER.debug(
            "No compatible cache setter found to clear the cached AAS token.",
            extra={"entry_id": getattr(entry, "entry_id", None)},
        )

    def _get_entry_cache(self, entry: ConfigEntry) -> Any | None:
        """Return the TokenCache (or equivalent) for this entry if available.

        Returns None if the cache is closed (unusable state) to allow
        self-healing during reauth flows.
        """
        cache = None

        rd = getattr(entry, "runtime_data", None)
        if rd is not None:
            for attr in ("token_cache", "cache", "_cache"):
                if hasattr(rd, attr):
                    try:
                        cache = getattr(rd, attr)
                        break
                    except Exception:  # pragma: no cover
                        pass

        if cache is None:
            runtime_container = getattr(self.hass, "data", {}) if self.hass else {}
            runtime_bucket = runtime_container.get(DOMAIN, {}).get("entries", {})
            runtime_entry = runtime_bucket.get(entry.entry_id)
            if runtime_entry is not None:
                for attr in ("_cache", "cache"):
                    if hasattr(runtime_entry, attr):
                        try:
                            cache = getattr(runtime_entry, attr)
                            break
                        except Exception:  # pragma: no cover
                            pass
                if cache is None and isinstance(runtime_entry, dict):
                    cache = runtime_entry.get("cache") or runtime_entry.get("_cache")

        # Check if cache is closed (unusable) - return None to allow self-healing
        if cache is not None and getattr(cache, "_closed", False):
            _LOGGER.debug(
                "Cache instance for entry '%s' is closed; returning None to allow "
                "self-healing",
                getattr(entry, "entry_id", "unknown"),
            )
            return None

        return cache

    @staticmethod
    async def _async_trigger_core_subentry_repair(
        hass: HomeAssistant | None, entry: ConfigEntry | None
    ) -> None:
        """Ensure core tracker/service subentries exist before presenting forms."""

        if hass is None or entry is None:
            return

        coordinator: Any | None = None
        subentry_manager: Any | None = None

        runtime = getattr(entry, "runtime_data", None)
        if runtime is not None:
            coordinator = getattr(runtime, "coordinator", None) or getattr(
                runtime, "data", None
            )
            subentry_manager = getattr(runtime, "subentry_manager", None)

        if coordinator is None or subentry_manager is None:
            domain_bucket: Any = getattr(hass, "data", {}).get(DOMAIN)
            if isinstance(domain_bucket, Mapping):
                entries_bucket = domain_bucket.get("entries")
                if isinstance(entries_bucket, Mapping):
                    runtime_candidate = entries_bucket.get(entry.entry_id)
                    if runtime_candidate is not None:
                        if coordinator is None:
                            coordinator = getattr(
                                runtime_candidate, "coordinator", None
                            )
                            if coordinator is None and isinstance(
                                runtime_candidate, Mapping
                            ):
                                coordinator = runtime_candidate.get("coordinator")
                        if subentry_manager is None:
                            subentry_manager = getattr(
                                runtime_candidate, "subentry_manager", None
                            )
                            if subentry_manager is None and isinstance(
                                runtime_candidate, Mapping
                            ):
                                subentry_manager = runtime_candidate.get(
                                    "subentry_manager"
                                )

        if coordinator is None or subentry_manager is None:
            return

        attach_manager = getattr(coordinator, "attach_subentry_manager", None)
        if callable(attach_manager):
            try:
                attach_manager(subentry_manager)
            except Exception as err:  # pragma: no cover - defensive guard
                _LOGGER.debug(
                    "Skipping core subentry repair attachment due to error: %s", err
                )

        builder = getattr(coordinator, "_build_core_subentry_definitions", None)
        if not callable(builder):
            return

        try:
            definitions = builder()
        except Exception as err:  # pragma: no cover - defensive guard
            _LOGGER.debug("Core subentry repair builder failed: %s", err)
            return

        if not definitions:
            return

        sync_method = getattr(subentry_manager, "async_sync", None)
        if not callable(sync_method):
            return

        try:
            await sync_method(definitions)
        except Exception as err:  # pragma: no cover - defensive guard
            _LOGGER.debug("Core subentry repair via options flow failed: %s", err)
            return

        refresher = getattr(coordinator, "_refresh_subentry_index", None)
        if callable(refresher):
            try:
                refresher()
            except Exception as err:  # pragma: no cover - defensive guard
                _LOGGER.debug(
                    "Core subentry metadata refresh after repair failed: %s", err
                )

        ensure_device = getattr(coordinator, "_ensure_service_device_exists", None)
        if callable(ensure_device):
            try:
                ensure_device(entry)
            except Exception as err:  # pragma: no cover - defensive guard
                _LOGGER.debug(
                    "Service device ensure after core subentry repair failed: %s", err
                )

        get_service_subentry = getattr(subentry_manager, "get", None)
        service_config_subentry_id: str | None = None
        if callable(get_service_subentry):
            service_obj = get_service_subentry(SERVICE_SUBENTRY_KEY)
            if service_obj is not None:
                service_config_subentry_id = getattr(service_obj, "subentry_id", None)

        # Unlike the steps above this one is not wrapped: a failure here means
        # the service device did not reach the subentry the repair just created,
        # and a repair step that reports success on a half-applied state is worse
        # than one that surfaces the error. The coordinator path inside swallows
        # its own failure and falls back; what reaches here is the fallback's.
        ConfigFlow._ensure_service_device_binding(
            hass,
            entry,
            coordinator,
            service_config_subentry_id,
        )

    def _ensure_subentry_context(self) -> dict[str, str | None]:
        """Return (and initialize) the flow-scoped subentry identifier mapping."""

        current = self.context.get("subentry_ids")
        if isinstance(current, dict):
            return current
        mapping: dict[str, str | None] = {}
        mapping.setdefault(self._subentry_key_core_tracking, None)
        mapping.setdefault(self._subentry_key_service, None)
        self.context["subentry_ids"] = mapping
        return mapping

    def _reset_reconfigure_subentry_context(
        self, entry: ConfigEntry
    ) -> dict[str, str | None]:
        """Reseed subentry context for reconfigure flows.

        Config Subentry Handbook: keep the flow context aligned with the
        registry-backed service/tracker IDs before dispatchers are rebound so
        listeners never target stale config_subentry_id values.
        """

        mapping: dict[str, str | None] = {
            self._subentry_key_core_tracking: None,
            self._subentry_key_service: None,
        }

        subentries = getattr(entry, "subentries", None)
        if isinstance(subentries, Mapping):
            integration = import_integration_package()

            manager_cls: type[_SubentryManagerProto] | None = getattr(
                integration, "ConfigEntrySubEntryManager", None
            )
            if manager_cls is not None:
                managed = manager_cls(self.hass, entry)
                for group_key, managed_subentry in managed.managed_subentries.items():
                    target_key = group_key
                    if target_key not in mapping:
                        # Deliberately NOT ``_canonical_core_key_of`` here, and
                        # the reason is measured rather than assumed. A legacy
                        # per-account tracker group is adopted as the core
                        # tracking group on this path, but the fold that does it
                        # is ``ConfigEntrySubEntryManager``'s: it keys every
                        # tracker-typed subentry under ``TRACKER_SUBENTRY_KEY``,
                        # so such a group arrives here already named
                        # ``core_tracking`` and never reaches this branch.
                        # Rewriting this branch therefore does not fix that
                        # defect; it would only let a legacy ``hub`` win the
                        # service slot ahead of a real service subentry. That
                        # half is now decided rather than open: beside a real
                        # ``service`` subentry the hub is the deterministic
                        # loser of ``_candidate_score``'s literal-owner field,
                        # where it used to depend on manager iteration order.
                        # What is left is the **fold**, which still leaves a
                        # ``hub`` on its stored key and is where the remaining
                        # defect lives.
                        subentry_type = getattr(managed_subentry, "subentry_type", None)
                        if subentry_type == SUBENTRY_TYPE_SERVICE:
                            target_key = SERVICE_SUBENTRY_KEY
                        elif subentry_type == SUBENTRY_TYPE_TRACKER:
                            target_key = self._subentry_key_core_tracking
                    if target_key not in mapping or mapping[target_key] is not None:
                        continue

                    subentry_id = getattr(managed_subentry, "subentry_id", None)
                    mapping[target_key] = (
                        subentry_id if isinstance(subentry_id, str) else None
                    )

            for subentry in subentries.values():
                data = getattr(subentry, "data", {}) or {}
                group_key_candidate = data.get("group_key")
                subentry_id = getattr(subentry, "subentry_id", None)
                if (
                    isinstance(group_key_candidate, str)
                    and group_key_candidate in mapping
                    and mapping[group_key_candidate] is None
                ):
                    mapping[group_key_candidate] = (
                        subentry_id if isinstance(subentry_id, str) else None
                    )

        self.context["subentry_ids"] = mapping
        return mapping

    @staticmethod
    def _ensure_service_device_binding(
        hass: HomeAssistant,
        entry: ConfigEntry,
        coordinator: Any | None,
        service_config_subentry_id: str | None,
    ) -> None:
        """Move the service device into the subentry the mapping now names.

        ``service_config_subentry_id`` is where the device shall sit; ``None``
        means the entry root (the hub link). Both are ordinary targets of the
        same MOVE intent, and the keywords that express it are chosen from the
        installed registry signature, never written out here. Contract:
        ``agents/config_flow/AGENTS.md``, "Device ownership in flow code".
        """

        if hass is None or entry is None:
            return

        # Imported here and not at module scope: ``coordinator/__init__.py``
        # eagerly imports the API with its crypto and network dependencies, and
        # the config flow must stay importable on the flow-only path even when
        # that graph does not (``agents/config_flow/AGENTS.md``, "Integration
        # module imports"). A local import keeps the names visible to mypy.
        from .coordinator.helpers.registry import (
            OwnershipIntent,
            detect_device_registry_capabilities,
            execute_ownership_plan,
            plan_device_ownership,
            resolve_device_by_identifiers,
        )

        dev_reg = dr.async_get(hass)
        update_call = getattr(dev_reg, "async_update_device", None)
        if not callable(update_call):
            return

        # Per-entry identifier first, the subentry-scoped one second, because the
        # stable form survives a subentry change and the scoped one goes stale
        # with it. Both are registered today (``service_device_info`` emits the
        # scoped one for eight platform sites). From Core 2026.8 the resolver
        # walks that order and scopes each lookup to the entry; below it there is
        # no replacement API, so it still passes the set on and the old
        # iteration-order behaviour remains. The order is therefore a guarantee
        # on the newer core and an expression of intent on the older one, the
        # same wording as the coordinator's own lookup.
        candidates: tuple[tuple[str, str], ...] = (
            service_device_identifier(entry.entry_id),
        )
        if service_config_subentry_id is not None:
            candidates += (
                (DOMAIN, f"{entry.entry_id}:{service_config_subentry_id}:service"),
            )

        device = resolve_device_by_identifiers(
            dev_reg, candidates, entry_id=entry.entry_id
        )
        if device is None:
            return

        device_id = getattr(device, "id", None) or getattr(device, "device_id", None)
        if device_id is None:
            return

        # Binding the service device is an ownership change, so the flow states
        # an intent and lets ``plan_device_ownership`` choose the keywords for
        # the installed core. MOVE, not ENSURE: the caller may be clearing a
        # stale subentry link, and ENSURE with an empty target would leave that
        # link in place.
        apply_ownership = getattr(coordinator, "_apply_device_ownership", None)
        if callable(apply_ownership):
            try:
                apply_ownership(
                    dev_reg,
                    intent=OwnershipIntent.MOVE,
                    device_id=device_id,
                    entry_id=entry.entry_id,
                    target_subentry_id=service_config_subentry_id,
                    # ``device`` is deliberately not handed over, for the same
                    # reason ``current`` is not handed to the planner below: the
                    # coordinator would read the ownership from it and, on a
                    # non-single-owner capability profile, plan a
                    # ``remove_config_entry_id``/``remove_config_subentry_id``
                    # pair. MOVE does not need it either way -- the coordinator
                    # skips its own lookup for this intent.
                )
                return
            except Exception as err:  # noqa: BLE001 - defensive guard
                _LOGGER.debug(
                    "Service device binding via coordinator helper failed: %s", err
                )

        # Reached on two ways: no coordinator at all, or the coordinator path
        # raised above. The retry after a failure is safe because a MOVE plan is
        # exactly one operation and setting the same ownership twice is a no-op;
        # a multi-operation plan would have to be resumed rather than replanned
        # from the snapshot taken before the first attempt.
        #
        # Same translation, different executor. What
        # used to stand here was a hand-written ``TypeError`` retry that renamed
        # ``add_config_*`` to ``config_*``. That rename produced a keyword no
        # supported core has: ``async_update_device`` takes no bare
        # ``config_subentry_id`` at tag 2025.9.1, 2026.8.0 or 2026.9.0. The
        # planner asks the installed signature instead of guessing from an error
        # message. See ``agents/config_flow/AGENTS.md``, "Device ownership in
        # flow code".
        execute_ownership_plan(
            dev_reg,
            plan_device_ownership(
                OwnershipIntent.MOVE,
                caps=detect_device_registry_capabilities(update_call),
                device_id=device_id,
                entry_id=entry.entry_id,
                target_subentry_id=service_config_subentry_id,
                # ``current`` is deliberately not passed. On a single-owner core
                # MOVE does not read it, and on the declared minimum a device
                # entry describes no ownership, so it would be ``None`` anyway;
                # measured, it changes nothing on any supported core. What it
                # would change is the degraded case: an unreadable
                # ``async_update_device`` signature yields the all-false profile,
                # where a known ``current`` emits
                # ``remove_config_entry_id`` + ``remove_config_subentry_id``
                # while the adding half degrades to a bare
                # ``add_config_entry_id`` naming the entry that already owns the
                # device, which arms no pending move. On a 2026.8 registry the
                # unmatched removal pair is the deletion. This call site never
                # emitted a ``remove_*`` keyword before, and neither of its two
                # paths starts now.
            ),
        )

    async def _async_sync_feature_subentries(
        self,
        entry: ConfigEntry,
        *,
        options_payload: dict[str, Any],
        defaults: dict[str, Any],
        context_map: dict[str, str | None],
    ) -> None:
        """Ensure the service and tracker subentries match the latest toggles."""

        tracker_key = TRACKER_SUBENTRY_KEY
        service_key = SERVICE_SUBENTRY_KEY
        tracker_unique_id = f"{entry.entry_id}-{tracker_key}"
        service_unique_id = f"{entry.entry_id}-{service_key}"

        entry_title = entry.title or (
            self._auth_data.get(CONF_GOOGLE_EMAIL) or "Google Find My Device"
        )
        tracker_title = "Google Find My devices"
        service_title = "Google Find Hub Service"
        tracker_translation_key = TRACKER_SUBENTRY_TRANSLATION_KEY
        service_translation_key = SERVICE_SUBENTRY_TRANSLATION_KEY

        has_filter, feature_flags = _derive_feature_settings(
            options_payload=options_payload,
            defaults=defaults,
        )

        def _resolve_existing(key: str) -> ConfigSubentry | None:
            """Return the subentry that is the ``key`` group, type axis included.

            Resolving by stored ``group_key`` alone handed the tracker group to
            a ``service``- or ``hub``-typed twin that still stores
            ``core_tracking``, which then received ``tracker_payload`` and, via
            the ``if not tracker_visible`` fallback below, every probed device
            id. The mirror direction lost data: a ``tracker``-typed subentry
            storing the service key was overwritten with the id-less
            ``service_payload``. The axis therefore guards **both** branches,
            the seeded context map and the fallback scan, because the migration
            path only reaches the second and the reconfigure path decides in the
            first.

            Within the scan, an exact stored-key match wins over a folded legacy
            twin. That ordering is load-bearing rather than cosmetic: where a
            real service subentry and a mis-keyed twin both answer for the
            service group, preferring the exact match leaves the twin untouched
            instead of rewriting a stored identity nobody asked to change.

            Both exits are ordered, and neither ordering is decoration. Two
            candidates can reach the *same* exit: two folded twins with no exact
            match, or a ``service``- and a ``hub``-typed subentry both storing
            the service key, which is a shape the module itself produces because
            ``HubSubentryFlowHandler._group_key`` is ``SERVICE_SUBENTRY_KEY``.
            Taking whichever came first let the iteration order of
            ``entry.subentries`` decide which one is written and which one the
            cleanup then treats as a leftover. Measured, that turned an
            ``AbortFlow`` at the previous commit into a silent
            ``async_remove_subentry`` of the **canonical service group**, device
            and entity registry bindings included: a dead flow traded for a data
            defect, which is the exact trade ``_claim_unique_id`` below exists
            to prevent.

            The tie-break is therefore substantive before it is stable: the type
            that *literally* owns the key wins over one that only folds onto it.
            ``coordinator/subentry.py`` records why that distinction is real,
            the entity platforms match ``subentry_type == "service"`` literally,
            so a hub is the service group for the assignment predicate and the
            index but not for them. Only among equals does the lowest
            ``subentry_id`` decide; that value is arbitrary, and stability is
            all that is asked of it.

            The seed is a *tie-break inside* that ranking, not a precedence in
            front of it, and the difference is measured rather than tidy. A
            context map written by an earlier run names a specific subentry, and
            honouring it is what keeps a single flow from re-homing the group
            between two of its own steps, so among candidates of equal rank the
            seeded one still wins. But the seeder resolves through the runtime
            manager, which folds *every* tracker-typed subentry onto
            ``TRACKER_SUBENTRY_KEY``. It can therefore name a subentry that does
            not carry the key in either pool: a legacy per-account tracker
            group, or a ``hub`` sitting beside the canonical ``service``
            subentry. Ranking such a seed ahead
            of the pool let it be rewritten onto the core key, displaced the
            canonical group's identity through ``_claim_unique_id`` and left
            that group out of the context map, so the cleanup swept it, device
            and entity registry bindings included. Measured against the previous
            commit, which raised ``AbortFlow`` and kept both groups: a dead flow
            traded for a data defect, in the direction ``_claim_unique_id``
            exists to prevent.

            A seed therefore only decides among candidates that qualify for the
            key. It still decides alone when no candidate does, which is the
            case a map naming a not-yet-keyed group depends on.

            ``ConfigEntrySubEntryManager._candidate_score`` now ranks a key
            collision on the same axes this pool does (exact stored key, then
            literal owner per ``LITERAL_CORE_KEY_OWNER``), which narrows *how
            often* the seeder can name an unqualified subentry but does not
            remove the case. Both shapes above are contested collisions and the
            manager now resolves them towards the canonical group. What it
            cannot resolve is the **uncontested** one: a legacy per-account
            group with no canonical sibling holds ``TRACKER_SUBENTRY_KEY``
            alone, so the manager names it because there is nothing to rank it
            against. That is the shape this ordering still exists for, and it
            is also why the manager's rank is not a reason to move the seed in
            front of the pool.
            """

            def _is_literal_owner(candidate: ConfigSubentry) -> bool:
                literal = _LITERAL_CORE_KEY_OWNER.get(key)
                return (
                    literal is not None
                    and getattr(candidate, "subentry_type", None) == literal
                )

            seeded: ConfigSubentry | None = None
            existing_id = context_map.get(key)
            if isinstance(existing_id, str):
                candidate = entry.subentries.get(existing_id)
                if candidate is not None and _may_answer_for(candidate, key):
                    seeded = candidate
            exact: list[ConfigSubentry] = []
            folded: list[ConfigSubentry] = []
            for candidate in entry.subentries.values():
                if not _may_answer_for(candidate, key):
                    continue
                data = getattr(candidate, "data", {}) or {}
                if data.get("group_key") == key:
                    exact.append(candidate)
                elif _canonical_core_key_of(candidate) == key:
                    folded.append(candidate)
            pool = exact or folded
            if not pool:
                return seeded
            return min(
                pool,
                key=lambda item: (
                    not _is_literal_owner(item),
                    item is not seeded,
                    str(getattr(item, "subentry_id", "")),
                ),
            )

        async def _claim_unique_id(desired: str, target: ConfigSubentry | None) -> None:
            """Free ``desired`` by displacing whichever subentry else holds it.

            The type axis above turns an update into a create wherever a twin
            stops answering for a core key, and a create is exactly where
            ``ConfigEntries.async_add_subentry`` calls
            ``_raise_if_subentry_unique_id_exists`` unconditionally; the update
            calls it whenever the id actually changes. Both raise
            ``AbortFlow("already_configured")``, and the reconfigure entry point
            has no ``try`` around this helper, so an unhandled collision would
            trade a data defect for a dead flow.

            The canonical id stays with the canonical group and the *legacy
            holder* is the one displaced, not the newcomer. That direction is
            not a preference: five sites outside this flow build their subentry
            definitions with ``f"{entry_id}-{key}"`` and reconcile against it
            (``__init__.py`` and ``coordinator/subentry.py``), so a core group
            carrying a derived id would collide with the runtime manager on the
            next sync and be resolved by its type-blind adoption path instead.

            The substitute is searched until it is free, for the same reason
            ``_unclaimed_fallback_key`` searches: a third subentry may hold the
            very substitute the first is displaced to. ``entry.subentries`` is
            finite, so it terminates.
            """

            holder: ConfigSubentry | None = None
            for candidate in entry.subentries.values():
                if candidate is target:
                    continue
                if getattr(candidate, "unique_id", None) == desired:
                    holder = candidate
                    break
            if holder is None:
                return

            taken = {
                getattr(other, "unique_id", None)
                for other in entry.subentries.values()
                if other is not holder
            }
            replacement = f"{desired}-legacy"
            suffix = 2
            while replacement in taken:
                replacement = f"{desired}-legacy-{suffix}"
                suffix += 1

            await type(self)._async_update_subentry(
                self,
                entry,
                holder,
                data=dict(getattr(holder, "data", {}) or {}),
                title=str(getattr(holder, "title", "") or ""),
                unique_id=replacement,
            )

        def _existing_visible(subentry_obj: ConfigSubentry | None) -> tuple[str, ...]:
            if subentry_obj is None:
                return ()
            data = getattr(subentry_obj, "data", {}) or {}
            raw_visible = data.get("visible_device_ids")
            if isinstance(raw_visible, (list, tuple, set)):
                return tuple(_normalize_visible_ids(raw_visible))
            return ()

        def _carry_over_visible(
            payload: dict[str, Any], subentry_obj: ConfigSubentry | None
        ) -> None:
            """Keep a stored allow-list the payload does not carry.

            ``_async_update_subentry`` hands the core a full ``data`` mapping
            and the core replaces the stored one wholesale, so a payload built
            without ``visible_device_ids`` *deletes* the group's device
            assignment.

            **Only the tracker branch calls this, and the service branch
            deliberately does not.** The service branch's omission looks like
            the same defect and is the opposite: the service and hub groups are
            non-device groups (``_NON_DEVICE_SUBENTRY_KEYS``,
            ``_accepts_device_assignment``), so a list stored there is legacy
            residue rather than an assignment, and clearing it at the next run
            is what keeps a wrong assignment from becoming permanent. The
            tracker branch reads its list back through ``_existing_visible``
            and so looked immune, but that read normalises an empty list to
            nothing and ``_build_subentry_payload`` then omits the key, which
            loses it just as thoroughly.

            Deliberately **verbatim** rather than normalised. Normalising would
            not change which devices the group ends up with -- measured,
            ``_normalize_visible_ids`` strips each entry, drops non-strings and
            entries that end up empty, deduplicates and sorts, so an entry such
            as ``"prefix:"`` survives it, merely possibly reordered; the
            entry-shaped folding that turns such an entry into nothing happens
            later and elsewhere, on the reading side. The reason is narrower
            and holds anyway: a step whose job is to keep a value must not also
            rewrite it, because the reading side reports a reinterpretation
            from the **raw** stored value, and a rewrite here would quietly
            remove its evidence.

            Restricted to the sequence shapes the readers accept, and the
            **container type is preserved** along with the contents. That is
            not cosmetic: the core writes only when ``subentry.data != data``,
            and a ``list`` is never equal to a ``tuple`` holding the same
            items, so converting the shape here would make every single run
            write and fire the update listeners on a group nothing changed
            about. Both shapes occur in stored data -- ``_normalize_visible_ids``
            and ``_migrate_entry_identifier_namespaces`` write a ``list``, the
            options assignment writes a ``tuple`` -- and after a restart the
            core hands back whatever JSON can carry, which is a ``list``.
            A ``set`` is the one exception, accepted for symmetry with
            ``_existing_visible`` but **sorted** into a ``tuple``:
            ``tuple(some_set)`` has no defined order, so carrying it as-is
            would rewrite the stored order on every run for the same reason.
            """

            if subentry_obj is None:
                return
            if "visible_device_ids" in payload:
                return
            data = getattr(subentry_obj, "data", None)
            if not isinstance(data, Mapping):
                return
            raw_visible = data.get("visible_device_ids")
            if isinstance(raw_visible, list):
                payload["visible_device_ids"] = list(raw_visible)
            elif isinstance(raw_visible, tuple):
                payload["visible_device_ids"] = tuple(raw_visible)
            elif isinstance(raw_visible, set):  # pragma: no cover - unreachable
                # Accepted for symmetry with ``_existing_visible``, which takes
                # the same three shapes, and sorted because ``tuple(some_set)``
                # has no defined order and would make the write non-idempotent.
                # Marked rather than tested: no writer in the tree stores a set
                # (every one produces ``list`` or ``tuple``, measured), and the
                # core persists entry data as JSON, which cannot carry one. A
                # fixture for it would model a state the core does not reach.
                # Kept rather than dropped because dropping the shape would omit
                # the key, and an absent key reads as unrestricted.
                payload["visible_device_ids"] = tuple(sorted(raw_visible))

        def _stores_visible(subentry_obj: ConfigSubentry | None) -> bool:
            """Return whether the subentry stores the key *at all*.

            Deliberately not ``bool(_existing_visible(...))``: that folds "no
            key stored" and "key stored, empty" into the same answer, and those
            two carry different meanings. A missing key means the group was
            never assigned anything, an empty one means it was assigned
            nothing.
            """

            if subentry_obj is None:
                return False
            data = getattr(subentry_obj, "data", None)
            if not isinstance(data, Mapping):
                return False
            return isinstance(data.get("visible_device_ids"), (list, tuple, set))

        tracker_subentry = _resolve_existing(tracker_key)
        tracker_visible = _existing_visible(tracker_subentry)
        # The fill-up is what makes the initial sync the only writer that ever
        # *adds* to the tracker allow-list, and it stays -- but only for a
        # group that never carried the key. Filling up a group whose stored
        # list is present and empty would silently turn "this group shows
        # nothing" back into "this group shows everything", which is the same
        # fail-open direction the empty-allow-list fix closed on the reading
        # side, arriving here through the very flow the user just used to
        # empty it.
        if (
            not tracker_visible
            and self._available_devices
            and not _stores_visible(tracker_subentry)
        ):
            tracker_visible = tuple(
                _normalize_visible_ids(
                    device_id for _, device_id in self._available_devices
                )
            )

        service_payload = _build_subentry_payload(
            group_key=service_key,
            features=_SERVICE_FEATURE_PLATFORMS,
            entry_title=entry_title,
            has_google_home_filter=has_filter,
            feature_flags=feature_flags,
        )

        tracker_payload = _build_subentry_payload(
            group_key=tracker_key,
            features=_TRACKER_FEATURE_PLATFORMS,
            entry_title=tracker_title,
            has_google_home_filter=has_filter,
            feature_flags=feature_flags,
            visible_device_ids=tracker_visible,
        )

        service_subentry = _resolve_existing(service_key)

        context_map.setdefault(
            service_key, getattr(service_subentry, "subentry_id", None)
        )
        context_map.setdefault(
            tracker_key, getattr(tracker_subentry, "subentry_id", None)
        )

        await _claim_unique_id(service_unique_id, service_subentry)
        if service_subentry is None:
            created_service = await type(self)._async_create_subentry(
                self,
                entry,
                data=service_payload,
                title=service_title,
                unique_id=service_unique_id,
                subentry_type=SUBENTRY_TYPE_SERVICE,
                translation_key=service_translation_key,
            )
            if created_service is not None:
                context_map[service_key] = created_service.subentry_id
        else:
            # No carry-over here, and the asymmetry with the tracker branch
            # below is the point rather than an oversight: this payload's
            # missing ``visible_device_ids`` clears a legacy assignment on a
            # group that must not hold one, which is the repair
            # ``test_service_subentry_visibility_is_cleared_by_the_setup_
            # roundtrip`` measures.
            await type(self)._async_update_subentry(
                self,
                entry,
                service_subentry,
                data=service_payload,
                title=service_title,
                unique_id=service_unique_id,
                translation_key=service_translation_key,
            )
            context_map[service_key] = service_subentry.subentry_id

        tracker_subentry = _resolve_existing(tracker_key)
        await _claim_unique_id(tracker_unique_id, tracker_subentry)
        if tracker_subentry is None:
            created_tracker = await type(self)._async_create_subentry(
                self,
                entry,
                data=tracker_payload,
                title=tracker_title,
                unique_id=tracker_unique_id,
                subentry_type=SUBENTRY_TYPE_TRACKER,
                translation_key=tracker_translation_key,
            )
            if created_tracker is not None:
                context_map[tracker_key] = created_tracker.subentry_id
        else:
            # The tracker branch reads its stored list back through
            # ``_existing_visible``, so it looks immune -- and is, for a
            # non-empty list. A stored *empty* list normalises to nothing, and
            # ``_build_subentry_payload`` then omits the key entirely, so the
            # group loses it altogether. Measured on 2026-08-06 with an empty
            # probe set.
            _carry_over_visible(tracker_payload, tracker_subentry)
            await type(self)._async_update_subentry(
                self,
                entry,
                tracker_subentry,
                data=tracker_payload,
                title=tracker_title,
                unique_id=tracker_unique_id,
                translation_key=tracker_translation_key,
            )
            context_map[tracker_key] = tracker_subentry.subentry_id

    async def _async_cleanup_stale_subentries(
        self, entry: ConfigEntry, context_map: Mapping[str, str | None]
    ) -> None:
        """Remove stale tracker/service subentries before reloading."""

        if not isinstance(context_map, Mapping):
            return

        expected_ids = {
            subentry_id
            for subentry_id in context_map.values()
            if isinstance(subentry_id, str) and subentry_id
        }
        if not expected_ids:
            return

        subentries = getattr(entry, "subentries", None)
        if not isinstance(subentries, Mapping):
            return

        allowed_keys = {
            self._subentry_key_core_tracking,
            self._subentry_key_service,
        }
        stale_ids: list[str] = []
        for subentry_id, subentry in subentries.items():
            if not isinstance(subentry_id, str) or subentry_id in expected_ids:
                continue
            data = getattr(subentry, "data", {}) or {}
            group_key = data.get("group_key")
            if not isinstance(group_key, str) or group_key not in allowed_keys:
                continue
            subentry_type = getattr(subentry, "subentry_type", None)
            if subentry_type is not None and subentry_type != (
                _LITERAL_CORE_KEY_OWNER.get(group_key)
            ):
                # Only the type that *literally* owns a core key can be a
                # leftover copy of that core group; every other type sitting on
                # the key is a group of its own, and removing it takes its
                # device and entity registry bindings with it
                # (``async_remove_subentry`` clears both).
                #
                # Before the sync resolver read ``subentry_type``, such a
                # subentry was resolved by its stored key and therefore landed
                # in ``context_map`` -- unless a same-keyed sibling took the
                # slot -- which kept it out of this list by accident. The
                # resolver may now leave one alone deliberately, so the axis has
                # to be stated here too, and stated as *ownership* rather than
                # as "the type names a different key": a ``hub`` writes
                # ``SERVICE_SUBENTRY_KEY`` by design
                # (``HubSubentryFlowHandler._group_key``), so beside a real
                # service subentry it is not mis-keyed at all, yet the
                # literal-owner rank makes it the deterministic loser.
                #
                # ``.get`` cannot miss today: ``allowed_keys`` above is exactly
                # the key set of ``_LITERAL_CORE_KEY_OWNER``. It is written
                # defensively rather than indexed, so that widening one of the
                # two sets fails towards skipping. An untyped subentry is *not*
                # covered by this guard: ``_canonical_core_key_of`` treats a
                # missing type as "the stored key keeps deciding", which makes
                # such a subentry a candidate copy of the core group it stores,
                # and sweeping stale legacy copies is what this pass is for.
                continue
            stale_ids.append(subentry_id)

        if not stale_ids:
            return

        runtime = getattr(entry, "runtime_data", None)
        manager = getattr(runtime, "subentry_manager", None)
        if manager is not None:
            managed_subentries = getattr(manager, "managed_subentries", None)
            remove = getattr(manager, "async_remove", None)
            if isinstance(managed_subentries, Mapping) and callable(remove):
                for key, subentry in managed_subentries.items():
                    managed_id = getattr(subentry, "subentry_id", None)
                    if managed_id in stale_ids:
                        removal = remove(key)
                        if inspect.isawaitable(removal):
                            await removal
                        stale_ids.remove(managed_id)
                        if not stale_ids:
                            return

        remove_fn = getattr(self.hass.config_entries, "async_remove_subentry", None)
        if not callable(remove_fn):
            return

        for subentry_id in tuple(stale_ids):
            try:
                removal = remove_fn(entry, subentry_id)
            except TypeError:
                removal = remove_fn(entry, subentry_id=subentry_id)
            if inspect.isawaitable(removal):
                await removal

    async def _async_create_subentry(
        self,
        entry: ConfigEntry,
        *,
        data: dict[str, Any],
        title: str,
        unique_id: str | None,
        subentry_type: str,
        translation_key: str | None = None,
    ) -> ConfigSubentry | None:
        """Create a config entry subentry using the best available API."""

        manager = getattr(self.hass, "config_entries", None)
        if manager is None:
            return None

        create_fn = getattr(manager, "async_create_subentry", None)
        if callable(create_fn):
            create_kwargs: dict[str, Any] = {
                "data": data,
                "title": title,
                "unique_id": unique_id,
                "subentry_type": subentry_type,
            }
            if translation_key is not None:
                create_kwargs["translation_key"] = translation_key
            try:
                result = create_fn(entry, **create_kwargs)
            except TypeError:
                create_kwargs.pop("translation_key", None)
                result = create_fn(entry, **create_kwargs)
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, ConfigSubentry):
                return result

        add_fn = getattr(manager, "async_add_subentry", None)
        if (
            callable(add_fn) and ConfigSubentry is not None
        ):  # pragma: no cover - legacy fallback
            subentry_cls = cast(Callable[..., ConfigSubentry], ConfigSubentry)
            ctor_kwargs: dict[str, Any] = {
                "data": MappingProxyType(dict(data)),
                "title": title,
                "unique_id": unique_id,
                "subentry_type": subentry_type,
            }
            if translation_key is not None:
                ctor_kwargs["translation_key"] = translation_key
            try:
                subentry = subentry_cls(**ctor_kwargs)
            except TypeError:  # pragma: no cover - legacy signature
                ctor_kwargs.pop("translation_key", None)
                try:
                    subentry = subentry_cls(**ctor_kwargs)
                except TypeError:
                    ctor_kwargs.pop("subentry_type", None)
                    subentry = subentry_cls(**ctor_kwargs)
            add_fn(entry, subentry)
            return subentry

        return None

    async def _async_update_subentry(
        self,
        entry: ConfigEntry,
        subentry: ConfigSubentry,
        *,
        data: dict[str, Any],
        title: str,
        unique_id: str | None,
        translation_key: str | None = None,
    ) -> None:
        """Update an existing subentry if the API supports it."""

        manager = getattr(self.hass, "config_entries", None)
        if manager is None:
            return

        update_fn = getattr(manager, "async_update_subentry", None)
        if not callable(update_fn):
            return

        update_kwargs: dict[str, Any] = {
            "data": data,
            "title": title,
            "unique_id": unique_id,
        }
        if translation_key is not None:
            update_kwargs["translation_key"] = translation_key
        try:
            result = update_fn(
                entry,
                subentry,
                **update_kwargs,
            )
        except TypeError:
            update_kwargs.pop("translation_key", None)
            result = update_fn(
                entry,
                subentry,
                **update_kwargs,
            )
        if inspect.isawaitable(result):
            await result

    def _lookup_subentry(
        self, entry: ConfigEntry, group_key: str
    ) -> ConfigSubentry | None:
        """Return the first subentry matching the requested group key."""

        for candidate in entry.subentries.values():
            if candidate.data.get("group_key") == group_key:
                return candidate
        return None


# ---------------------------
# Subentry Flow Handlers
# ---------------------------


class _BaseSubentryFlow(ConfigSubentryFlow, _ConfigSubentryFlowMixin):  # type: ignore[misc]
    """Shared helpers for Google Find My config subentry flows."""

    _group_key: str
    _subentry_type: str
    _features: tuple[str, ...]
    _config_entry_cache: ConfigEntry | None

    def __init__(
        self,
        config_entry: ConfigEntry | None = None,
        subentry: ConfigSubentry | None = None,
    ) -> None:
        """Initialize the subentry flow handler.

        Home Assistant 2026.x may instantiate handlers without passing config_entry
        in the constructor. The flow manager sets up context (including access to
        the parent config entry via _get_entry()) after instantiation.

        We support both patterns:
        1. Direct instantiation with config_entry (legacy/manual usage)
        2. HA flow manager instantiation (config_entry accessed via _get_entry())
        """
        super_init = cast(Callable[..., None], super().__init__)
        self._config_entry_cache = None

        if config_entry is not None and subentry is not None:
            try:
                super_init(config_entry, subentry)
            except TypeError:
                try:
                    super_init(config_entry)
                except TypeError:  # pragma: no cover - legacy stub compatibility
                    try:
                        super_init()
                    except TypeError:
                        pass
                setattr(self, "subentry", subentry)
        elif config_entry is not None:
            try:
                super_init(config_entry)
            except TypeError:  # pragma: no cover - legacy stub compatibility
                try:
                    super_init()
                except TypeError:
                    pass
        else:
            try:
                super_init()
            except TypeError:
                pass

        if subentry is not None and not hasattr(self, "subentry"):
            setattr(self, "subentry", subentry)

        # Cache config_entry if provided directly; lazy resolution via
        # the config_entry property handles HA flow manager instantiation
        if config_entry is not None:
            self._config_entry_cache = config_entry

    @property
    def config_entry(self) -> ConfigEntry:
        """Return the parent config entry, resolving lazily if needed.

        Home Assistant 2026.x provides _get_entry() on ConfigSubentryFlow to
        access the parent config entry. We try multiple resolution strategies
        for compatibility across HA versions.
        """
        # Check cached value first
        if self._config_entry_cache is not None:
            return self._config_entry_cache

        # Try the instance attribute (may be set by HA or super().__init__)
        cached = getattr(self, "_config_entry", None)
        if cached is not None:
            self._config_entry_cache = cached
            return cached

        # Try HA 2026.x _get_entry() method
        get_entry_method = getattr(self, "_get_entry", None)
        if callable(get_entry_method):
            try:
                entry = get_entry_method()
                if entry is not None:
                    self._config_entry_cache = entry
                    return entry
            except Exception:  # noqa: BLE001 - defensive, HA internals may vary
                pass

        raise RuntimeError(
            f"{type(self).__name__} cannot resolve config_entry; "
            "ensure the handler is instantiated via Home Assistant's flow manager "
            "or provide config_entry in the constructor"
        )

    @config_entry.setter
    def config_entry(self, value: ConfigEntry) -> None:
        """Set the parent config entry."""
        self._config_entry_cache = value
        # Also set on instance for compatibility
        object.__setattr__(self, "_config_entry", value)

    @property
    def _entry_id(self) -> str:
        return getattr(self.config_entry, "entry_id", "")

    def _resolve_existing(self) -> ConfigSubentry | None:
        """Return the subentry this handler owns, type axis included.

        The scan asks ``_may_answer_for`` in addition to the stored key, for the
        same reason ``_async_sync_feature_subentries._resolve_existing`` does:
        the stored ``group_key`` alone hands a group to a twin whose *type*
        names a different core key. Measured against the real handlers rather
        than argued: a ``tracker``-typed subentry storing the service key was
        resolved by ``HubSubentryFlowHandler`` and then overwritten through
        ``async_step_user`` with the id-less service payload, the service title
        and ``unique_id = f"{entry_id}-service"``, dropping the device
        assignment it held; the mirror direction handed a ``hub``-typed twin
        storing ``core_tracking`` to ``TrackerSubentryFlowHandler``.

        What the axis deliberately does **not** touch is the branch above it. A
        ``subentry`` handed over by Home Assistant's flow manager identifies the
        subentry the user opened, so filtering it would make a mis-typed
        subentry unreconfigurable. That hand-over is unreachable today rather
        than merely rare, and the measurement says why: this integration's
        ``async_get_supported_subentry_types`` returns ``{}`` unconditionally,
        so core never registers these handlers with its subentry flow manager,
        and the one production entry point that builds one (``async_step_hub``)
        passes no ``subentry``. The branch is left open on purpose, so the
        asymmetry is already right the day core does hand one over.

        Two steps call this, and a refusal ends differently in each. In
        ``async_step_reconfigure`` a ``None`` is an abort with
        ``invalid_subentry`` and nothing is written. Only in
        ``async_step_user`` does the rejection of the sole key-matching
        candidate turn the update into a create. That create does not reach
        ``async_add_subentry`` at all: ``ConfigSubentryFlow.async_create_entry``
        raises ``ValueError("Source is None, expected user")`` first, because
        ``async_step_hub`` sets a context without ``source``. Measured against
        the real core, and **pre-existing**: the same call raises on a config
        entry that holds no subentry whatsoever, so the axis widened the set of
        inputs that reach the branch, it did not create the dead end. Both the
        collision the ``unique_id`` would cause behind it and the raise in front
        of it are tracked as ``U-27`` and ``U-28`` in
        ``PLAN_GFMY_SUBENTRY_FLOW_CREATE_PATH`` rather than repaired here, and
        are carried in the remainder register of
        ``agents/config_flow/AGENTS.md`` with the order between them: U-28
        first, because U-27 is unreachable while the raise stands.
        """

        candidate = getattr(self, "subentry", None)
        if isinstance(candidate, ConfigSubentry):
            return candidate
        for subentry in getattr(self.config_entry, "subentries", {}).values():
            if subentry.data.get("group_key") != self._group_key:
                continue
            if not _may_answer_for(subentry, self._group_key):
                continue
            return subentry
        return None

    def _current_options_payload(self) -> dict[str, Any]:
        payload = dict(getattr(self.config_entry, "options", {}))
        for key in (
            OPT_MAP_VIEW_TOKEN_EXPIRATION,
            OPT_MAP_VIEW_ENABLED,
            OPT_GOOGLE_HOME_FILTER_ENABLED,
            OPT_ENABLE_STATS_ENTITIES,
            OPT_CONTRIBUTOR_MODE,
        ):
            if key is not None and key not in payload and key in self.config_entry.data:
                payload[key] = self.config_entry.data[key]
        return payload

    def _defaults_for_entry(self) -> dict[str, Any]:
        defaults = dict(DEFAULT_OPTIONS)
        for key in (
            OPT_MAP_VIEW_TOKEN_EXPIRATION,
            OPT_MAP_VIEW_ENABLED,
            OPT_GOOGLE_HOME_FILTER_ENABLED,
            OPT_ENABLE_STATS_ENTITIES,
            OPT_CONTRIBUTOR_MODE,
        ):
            if key is not None and key in self.config_entry.data:
                defaults[key] = self.config_entry.data[key]
        return defaults

    def _entry_title(self) -> str:
        return getattr(self.config_entry, "title", None) or "Google Find My Device"

    def _visible_device_ids(self) -> tuple[str, ...]:
        subentry = self._resolve_existing()
        if subentry is None:
            return ()
        raw_visible = getattr(subentry, "data", {}).get("visible_device_ids")
        if isinstance(raw_visible, (list, tuple, set)):
            return tuple(_normalize_visible_ids(raw_visible))
        return ()

    def _build_payload(self) -> tuple[dict[str, Any], str, str]:
        options_payload = self._current_options_payload()
        defaults = self._defaults_for_entry()
        has_filter, feature_flags = _derive_feature_settings(
            options_payload=options_payload,
            defaults=defaults,
        )
        title = self._entry_title()
        visible_ids = self._visible_device_ids()
        payload = _build_subentry_payload(
            group_key=self._group_key,
            features=self._features,
            entry_title=title,
            has_google_home_filter=has_filter,
            feature_flags=feature_flags,
            visible_device_ids=visible_ids,
        )
        unique_id = f"{self._entry_id}-{self._group_key}"
        return payload, title, unique_id

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Provision or update the logical subentry without duplicating entries."""

        payload, title, unique_id = self._build_payload()

        existing = self._resolve_existing()
        if existing is not None:
            update_callable = self.async_update_and_abort
            update_kwargs = {
                "data": payload,
                "title": title,
                "unique_id": unique_id,
            }
            update_signature = inspect.signature(update_callable).parameters
            if "entry" in update_signature and "subentry" in update_signature:
                return update_callable(self.config_entry, existing, **update_kwargs)
            return update_callable(**update_kwargs)

        create_callable = self.async_create_entry
        create_kwargs: dict[str, Any] = {
            "title": title,
            "data": payload,
        }
        create_signature = inspect.signature(create_callable).parameters
        if "unique_id" in create_signature:
            create_kwargs["unique_id"] = unique_id
        if "subentry_type" in create_signature:
            create_kwargs["subentry_type"] = self._subentry_type

        return create_callable(**create_kwargs)

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        payload, title, unique_id = self._build_payload()
        update_callable = self.async_update_and_abort
        update_kwargs = {
            "data": payload,
            "title": title,
            "unique_id": unique_id,
        }
        update_signature = inspect.signature(update_callable).parameters
        if "entry" in update_signature and "subentry" in update_signature:
            subentry = self._resolve_existing()
            if subentry is None:
                return self.async_abort(reason="invalid_subentry")
            return update_callable(self.config_entry, subentry, **update_kwargs)
        return update_callable(**update_kwargs)


class HubSubentryFlowHandler(_BaseSubentryFlow):
    """Config subentry flow handler invoked from the Add Hub entry point."""

    _group_key = SERVICE_SUBENTRY_KEY
    _subentry_type = SUBENTRY_TYPE_HUB
    _features = _SERVICE_FEATURE_PLATFORMS

    def _visible_device_ids(self) -> tuple[str, ...]:
        return ()

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Provision or update the hub feature group when requested by the UI."""

        _LOGGER.info(
            "Hub subentry flow requested; provisioning service feature group (entry_id=%s)",
            self._entry_id or "<unknown>",
        )
        result = super().async_step_user(user_input)
        resolved = await _resolve_flow_result(result)
        return cast(FlowResult, resolved)


class ServiceSubentryFlowHandler(_BaseSubentryFlow):
    """Config subentry flow for the hub/service feature group."""

    _group_key = SERVICE_SUBENTRY_KEY
    _subentry_type = SUBENTRY_TYPE_SERVICE
    _features = _SERVICE_FEATURE_PLATFORMS

    def _visible_device_ids(self) -> tuple[str, ...]:
        return ()


class TrackerSubentryFlowHandler(_BaseSubentryFlow):
    """Config subentry flow for tracked device feature groups."""

    _group_key = TRACKER_SUBENTRY_KEY
    _subentry_type = SUBENTRY_TYPE_TRACKER
    _features = _TRACKER_FEATURE_PLATFORMS

    def _entry_title(self) -> str:
        return "Google Find My devices"


# ---------------------------
# Options Flow
# ---------------------------
class OptionsFlowHandler(OptionsFlowBase, _OptionsFlowMixin, _ContainerLoginMixin):  # type: ignore[misc, valid-type]
    """Options flow to update non-secret settings and optionally refresh credentials.

    Notes:
        - Device inclusion/exclusion is controlled by HA's device enable/disable.
          We no longer present a `tracked_devices` multi-select here.
        - Returning `async_create_entry` writes the options and nothing else.
          There is no automatic reload here on purpose (see `OptionsFlowBase`):
          a step that needs one asks for it through `_schedule_claimed_reload`,
          so every reload of this entry passes the single owner latch.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the options flow handler."""

        super().__init__(*args, **kwargs)
        self._semantic_location_editing: str | None = None
        # Identity of this flow's cleanup ticket (see _StagedCleanupTicket),
        # resolved lazily because Home Assistant assigns ``flow_id`` only when
        # the flow manager starts the flow, i.e. after ``__init__``.
        self._container_cleanup_ticket_id: str | None = None

    @_typed_callback
    def async_remove(self) -> None:
        """Drop this flow's staged cleanup when Home Assistant removes the flow.

        Same contract as ``ConfigFlow.async_remove``, and needed separately
        because ``data_entry_flow.FlowHandler`` precedes ``_ContainerLoginMixin``
        in this class's MRO, so a mixin-level override would never be reached.
        In practice this is a no-op for the options credential refresh, whose
        ticket names its entry and is therefore kept; it exists so that a future
        options path staging an uncorrelated ticket cannot leak it.
        """

        discarded = self._async_discard_own_cleanup_ticket()
        if discarded:
            _LOGGER.debug(
                "Discarded %s staged container-login cleanup job(s) of a removed "
                "options flow; credential files are kept on disk",
                discarded,
            )

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Display a small menu for settings, credentials refresh, or visibility."""
        return self.async_show_menu(
            step_id="init",
            menu_options=[
                "settings",
                "credentials",
                "visibility",
                "semantic_locations",
                "repairs",
            ],
        )

    # ---------- Helpers for live API/cache access ----------
    def _get_entry_cache(self, entry: ConfigEntry) -> Any | None:
        """Proxy to the ConfigFlow cache lookup helper."""

        return ConfigFlow._get_entry_cache(self, entry)

    async def _async_clear_cached_aas_token(self, entry: ConfigEntry) -> None:
        """Proxy to the ConfigFlow cache-clearing helper."""

        await ConfigFlow._async_clear_cached_aas_token(self, entry)

    async def _async_build_api_from_entry(self, entry: ConfigEntry) -> GoogleFindMyAPI:
        """Construct API object from the live entry context (cache-first)."""
        cache = self._get_entry_cache(entry)
        if cache is not None:
            session = async_get_clientsession(self.hass)
            api_ctor = cast(
                Callable[..., "GoogleFindMyAPI"],
                await _async_import_api(self.hass),
            )
            try:
                return api_ctor(cache=cache, session=session)
            except TypeError:
                return api_ctor(cache=cache)

        oauth = entry.data.get(CONF_OAUTH_TOKEN)
        email = entry.data.get(CONF_GOOGLE_EMAIL)
        if oauth and email:
            api_ctor = cast(
                Callable[..., "GoogleFindMyAPI"],
                await _async_import_api(self.hass),
            )
            try:
                return api_ctor(oauth_token=oauth, google_email=email)
            except TypeError:
                return api_ctor(token=oauth, email=email)

        raise RuntimeError(
            "GoogleFindMyAPI requires either `cache=` or minimal flow credentials."
        )

    # ---------- Shared subentry helpers ----------
    def _gather_subentry_options(self) -> list[_SubentryOption]:
        """Return ordered subentry options available for selection."""

        entry = self.config_entry
        options: list[_SubentryOption] = []
        key_counts: Counter[str] = Counter()

        subentries = getattr(entry, "subentries", None)
        if isinstance(subentries, Mapping):
            for subentry in subentries.values():
                data = dict(getattr(subentry, "data", {}) or {})
                raw_key = data.get("group_key")
                if isinstance(raw_key, str) and raw_key.strip():
                    stored_key: str | None = raw_key.strip()
                else:
                    stored_key = None
                key = stored_key or str(
                    getattr(subentry, "subentry_id", TRACKER_SUBENTRY_KEY)
                )
                label = (
                    getattr(subentry, "title", None)
                    or data.get("entry_title")
                    or key.replace("_", " ").title()
                )
                raw_visible = data.get("visible_device_ids")
                if isinstance(raw_visible, CollIterable) and not isinstance(
                    raw_visible, (str, bytes)
                ):
                    visible = tuple(
                        str(item)
                        for item in raw_visible
                        if isinstance(item, str) and item
                    )
                else:
                    visible = ()
                options.append(
                    _SubentryOption(
                        key=key,
                        label=str(label),
                        subentry=subentry,
                        visible_device_ids=visible,
                        stored_key=stored_key,
                    )
                )
                key_counts[key] += 1

        # The key must identify the option, and the stored ``group_key`` does
        # not: ``Subentry alias handling`` in ``agents/config_flow/AGENTS.md``
        # explicitly lets a legacy subentry keep an email-style ``group_key``
        # while being typed ``service``, so the *same* label can sit on a
        # service and a tracker subentry at once. That shape is pinned in
        # ``tests/test_config_flow_subentry_sync.py``. Every consumer downstream
        # treats this key as an identity: ``_subentry_choice_map`` and
        # ``_device_target_choice_map`` collapse it into a ``dict``, and
        # ``async_step_repairs_delete`` resolves both the deletion target and
        # the devices it hands on through that mapping. A duplicate key
        # therefore hides one subentry from every form, and it used to let
        # ``options.sort`` below decide which subentry an assignment wrote to.
        #
        # The rewrite is all-or-nothing on purpose. Moving only the duplicates
        # looks smaller but is not total: a subentry may store the very
        # ``subentry_id`` a duplicate is about to move to, which recreates the
        # collision one step further out, and chaining that a second time
        # defeats any fixed number of passes. Sending *every* option to its own
        # ``subentry_id`` as soon as one duplicate exists is injective in a
        # single pass, because ``subentry_id`` is the key under which
        # ``entry.subentries`` stores the subentry and is therefore unique by
        # construction. It also avoids picking a winner, which would leave the
        # outcome dependent on the ``options.sort`` below.
        #
        # The healthy entry is untouched: without a duplicate this loop does not
        # run, so the stored ``group_key`` keeps reaching the forms. Only an
        # entry that already carries drifting aliases sees opaque keys, and the
        # label the user reads is unaffected either way. Nothing new reaches
        # storage either: the options steps use ``option.key`` only to route a
        # choice to its subentry and never write it into ``group_key``.
        if any(count > 1 for count in key_counts.values()):
            for option in options:
                option.key = str(option.subentry_id or option.key)

        if not options:
            title = getattr(entry, "title", None) or "Core tracking"
            options.append(
                _SubentryOption(
                    key=TRACKER_SUBENTRY_KEY,
                    label=str(title),
                    subentry=None,
                    visible_device_ids=(),
                )
            )

        options.sort(key=lambda opt: opt.label.lower())
        return options

    def _subentry_choice_map(
        self,
    ) -> tuple[dict[str, str], dict[str, _SubentryOption]]:
        """Return mapping of subentry keys to labels and option objects."""

        options = self._gather_subentry_options()
        label_map = {opt.key: opt.label for opt in options}
        option_map = {opt.key: opt for opt in options}
        return label_map, option_map

    def _device_target_choice_map(
        self,
    ) -> tuple[dict[str, str], dict[str, _SubentryOption]]:
        """Return the subentry choices that can actually hold device assignments.

        Deliberately a second helper rather than a filter inside
        ``_subentry_choice_map``: that one fed all six steps before this
        change, and only the three that assign devices may narrow their
        choices. The two it still feeds must keep seeing every group:
        ``async_step_repairs`` only asks whether *any* subentry exists (a
        shared filter could send an entry whose sole subentry is the service
        group into ``repairs_no_subentries``), and ``async_step_repairs_delete``
        builds its ``removable_choices`` from it, because deletability is not
        assignability. (``async_step_settings`` and ``async_step_credentials``,
        which also targeted the service group, no longer offer a group.)
        """

        all_options = self._gather_subentry_options()
        options = [
            option for option in all_options if _accepts_device_assignment(option)
        ]
        if not options:
            # Same fallback as ``_gather_subentry_options``, for the same
            # reason: an entry whose only subentries are non-device groups
            # would otherwise hand ``vol.In`` an empty mapping, which renders a
            # form the user cannot submit.
            #
            # The key is taken from ``_unclaimed_fallback_key`` rather than
            # being ``TRACKER_SUBENTRY_KEY`` outright, and the difference is
            # not cosmetic: the two ``if not options:`` guards look alike but
            # test different sets. The one in ``_gather_subentry_options``
            # fires on the *unfiltered* list, so nothing exists that could hold
            # the key. This one fires on the *filtered* list while the
            # unfiltered one may well be non-empty, which is where a borrowed
            # identity becomes possible.
            title = getattr(self.config_entry, "title", None) or "Core tracking"
            options = [
                _SubentryOption(
                    key=_unclaimed_fallback_key(
                        {option.key for option in all_options}
                        | {opt.stored_key for opt in all_options if opt.stored_key}
                    ),
                    label=str(title),
                    subentry=None,
                    visible_device_ids=(),
                )
            ]

        label_map = {opt.key: opt.label for opt in options}
        option_map = {opt.key: opt for opt in options}
        return label_map, option_map

    @staticmethod
    def _default_subentry_key(
        choices: dict[str, str],
        option_map: Mapping[str, _SubentryOption],
    ) -> str:
        """Return the key of the group a form should preselect.

        This asks ``key`` a question about *meaning* -- "which of these is the
        tracker group?" -- and is therefore the second site that the collision
        rewrite in ``_gather_subentry_options`` silently disarms, the first
        being ``_accepts_device_assignment``. Once any two options collide,
        every key becomes a ``subentry_id``, a literal membership test can no
        longer match, and the helper preselects whichever group happens to sort
        first by label. A user who submits ``async_step_repairs_move`` without
        touching the target field then writes the devices to that group.

        ``option_map`` is how the caller supplies the meaning the key lost, and
        it is required rather than optional: every one of the three call sites
        obtains it from the same choice map it passes as ``choices``, so an
        optional parameter would only have kept a branch alive that nothing
        reaches, and a future caller who forgot the map would silently get the
        pre-fix behaviour back on *both* axes.

        The type axis is asked as well, because a key comparison alone is the
        very check ``agents/config_flow/AGENTS.md`` forbids: the alias rule
        lets a ``service``-typed subentry keep the tracker key. **What the
        axis buys here is an identity proxy, not damage prevention**, and the
        distinction is worth stating because an earlier version of this
        docstring got it wrong. It argued that preselecting such a group sends
        feature-flag writes to something that cannot hold devices -- but the
        contract named the service group as a legitimate target of the two
        steps that then used the unfiltered map (settings and credentials), so
        writing there was not the harm.
        The harm is that the key no longer identifies the tracker group and
        ``_accepts_device_assignment`` is the closest stand-in left: a group
        wearing the tracker key while refusing devices is, whatever else it
        is, not the tracker group the form means to offer first.

        **The second loop is bound to that case rather than run
        unconditionally**, and the boundary is the load-bearing part. Preferring
        any device-accepting option also fires on an ordinary legacy entry --
        an email-keyed tracker group beside a real service group, nothing
        parked -- where the pre-fix answer was ``next(iter(choices))``. That
        moved the preselection of ``async_step_settings`` and
        ``async_step_credentials`` (the steps that then offered a group) away
        from the service group they legitimately targeted, for every such installation and in both label
        orders. Measured, and pinned by
        ``::test_the_preselection_is_unchanged_where_nothing_is_parked``.

        **None of the three remaining call sites can reach that loop**:
        ``async_step_visibility``, ``async_step_repairs_move`` and
        ``async_step_repairs_delete`` pass ``_device_target_choice_map``, whose
        filter is ``_accepts_device_assignment`` itself -- a parked group never
        arrives there. The loop served ``async_step_settings`` and
        ``async_step_credentials``, which passed the unfiltered
        ``_subentry_choice_map`` until their group selector was removed (neither
        step stored anything account-wide by group). It is unreachable from production
        today and stays pinned by the tests that call this helper directly
        until its removal is decided.

        A key with no option behind it is skipped in **both** loops. The two
        used to disagree, the first skipping and the second returning it, so
        an unjudgeable choice outranked every judgeable one; ``choices`` and
        ``option_map`` come from one call today, which makes this a guard
        rather than a live path (``::test_a_choice_without_an_option_never_
        wins_the_preselection``).

        The *first* loop recognises the tracker group on the key axis alone,
        and that is a measurement rather than an omission. Adding the type
        axis there -- so that a ``tracker``-typed subentry on an email-style
        key is recognised as the tracker group -- turns
        ``::test_the_preselection_is_unchanged_where_nothing_is_parked`` red
        (``'owner@example.com'`` where it demands ``'service'``) and takes
        ``::test_the_preselected_group_survives_the_collision_rewrite`` with
        it: on an ordinary legacy entry nothing is parked, and moving a
        preselection off the service group is the regression that pin exists
        for.

        The type axis therefore sits *inside* the parked branch, where the
        boundary above already confines it: among the options that accept
        devices, one whose type says ``tracker`` wins over one that merely
        sorts first. It cannot route through ``_canonical_core_key_of``, which
        returns ``None`` for a tracker on its own key on purpose, so a direct
        type comparison is the only reader that can name such a group. The
        fallback to "the first device-accepting option" is kept, so the older
        guarantee holds where no candidate carries the tracker type at all
        (``::test_a_parked_tracker_key_prefers_the_typed_tracker_group``).

        The synthesised fallback of ``_device_target_choice_map`` stores
        nothing, so ``stored_key or key`` is its (possibly suffixed) key; it
        wins the first loop only when it *is* the tracker key, and it accepts
        devices, so the answer is the same either way.
        """

        parked_on_the_tracker_key = False
        for key in choices:
            option = option_map.get(key)
            if option is None:
                continue
            if (option.stored_key or option.key) != TRACKER_SUBENTRY_KEY:
                continue
            if _accepts_device_assignment(option):
                return key
            parked_on_the_tracker_key = True
        if parked_on_the_tracker_key:
            device_accepting: str | None = None
            for key in choices:
                option = option_map.get(key)
                if option is None:
                    continue
                if not _accepts_device_assignment(option):
                    continue
                if (
                    getattr(option.subentry, "subentry_type", None)
                    == SUBENTRY_TYPE_TRACKER
                ):
                    return key
                if device_accepting is None:
                    device_accepting = key
            if device_accepting is not None:
                return device_accepting
        return next(iter(choices), TRACKER_SUBENTRY_KEY)

    async def _async_assign_devices_to_subentry(
        self, entry: ConfigEntry, target_key: str, device_ids: list[str]
    ) -> set[str]:
        """Assign devices to the target subentry while removing from others."""

        if not device_ids:
            return set()

        changed: set[str] = set()
        options = self._gather_subentry_options()

        # Authoritative checkpoint for the invariant described at
        # ``_NON_DEVICE_SUBENTRY_KEYS``. Three production sites in this module
        # already write it that way -- both ``_visible_device_ids`` overrides
        # and the ``service_payload`` of ``_async_sync_feature_subentries`` --
        # and ``tests/test_config_flow_subentry_sync.py`` pins their result, but
        # none of them is a checkpoint. The guard sits here rather than in each
        # calling step because this is where every assignment in this module
        # passes, including any future caller.
        #
        # The service payload's omission is not merely defensive, it is also
        # the only thing that clears a legacy assignment on a group that must
        # not hold one, so it is load-bearing in both directions and must not
        # be "fixed" into a carry-over the way the tracker branch has one.
        # ``test_ap5_the_service_branch_still_clears_a_legacy_assignment`` and
        # ``test_service_subentry_visibility_is_cleared_by_the_setup_roundtrip``
        # pin that on the flow side and on the manager side respectively.
        #
        # The predicate is decided per *option* and must therefore be consumed
        # per option. Resolving the first holder of the key and then writing to
        # every holder is what let a service subentry receive ids while the
        # guard judged its tracker twin (or refused on behalf of a twin the user
        # never picked). ``_gather_subentry_options`` now hands out injective
        # keys, so a twin cannot arise in the first place; the identity
        # comparison in the loop below is the second, independent half, and it
        # holds even if that ever regresses.
        #
        # Standing down instead of stripping the ids: the request cannot take
        # effect, so changing nothing is safer than unassigning the devices from
        # the group that legitimately holds them.
        matching = [option for option in options if option.key == target_key]
        target_option = next(
            (option for option in matching if _accepts_device_assignment(option)), None
        )
        if matching and target_option is None:
            _LOGGER.warning(
                "Refusing to assign %d device(s) to subentry %r: this feature group "
                "never carries device visibility",
                len(device_ids),
                target_key,
            )
            return set()

        # No option carries the key at all. Inside this module the synthesised
        # fallback of ``_device_target_choice_map`` is what produces that shape;
        # a direct caller passing an unknown key produces it too, which is why
        # the check sits here and not in the steps. A
        # move needs a destination, so the loop below must not run: its
        # ``else`` branch would strip the ids from every group that holds them
        # and put them nowhere. That is not the requested move, it is a loss,
        # and it would be reported as a success, because a strip fills
        # ``changed``. Standing down keeps the ids where they legitimately are
        # and leaves ``changed`` empty, which is what the callers read.
        if target_option is None:
            return set()

        for option in options:
            subentry = option.subentry
            if subentry is None:
                continue

            data = dict(getattr(subentry, "data", {}) or {})
            raw_visible = data.get("visible_device_ids")
            if isinstance(raw_visible, CollIterable) and not isinstance(
                raw_visible, (str, bytes)
            ):
                visible = [
                    str(item) for item in raw_visible if isinstance(item, str) and item
                ]
            else:
                visible = list(option.visible_device_ids)

            before = list(visible)
            # Identity, not key equality: see the note above the guard. When no
            # option carries ``target_key`` at all -- the synthesised fallback of
            # ``_device_target_choice_map`` produces exactly that -- every option
            # takes the ``else`` branch, which is what this function did before
            # and what the empty arm of the narrowing relies on.
            if option is target_option:
                for dev_id in device_ids:
                    if dev_id not in visible:
                        visible.append(dev_id)
            else:
                visible = [dev for dev in visible if dev not in device_ids]

            if visible == before:
                continue

            data["visible_device_ids"] = tuple(sorted(dict.fromkeys(visible)))
            update_helper = cast(
                Callable[..., Awaitable[None] | None], ConfigFlow._async_update_subentry
            )
            result = update_helper(
                self,
                entry,
                subentry,
                data=data,
                title=getattr(subentry, "title", None),
                unique_id=getattr(subentry, "unique_id", None),
            )
            if inspect.isawaitable(result):
                await result
            changed.add(option.key)

        return changed

    async def _async_remove_subentry(
        self, entry: ConfigEntry, subentry_option: _SubentryOption
    ) -> bool:
        """Remove a subentry using the config entries API when available."""

        subentry_id = subentry_option.subentry_id
        if not subentry_id:
            return False

        manager = getattr(self.hass, "config_entries", None)
        remove_fn = getattr(manager, "async_remove_subentry", None)
        if not callable(remove_fn):
            return False

        result = remove_fn(entry, subentry_id)
        if inspect.isawaitable(result):
            result = await result
        return bool(result)

    def _device_choice_map(self) -> dict[str, str]:
        """Return mapping of device IDs to display labels for UI selectors."""

        entry = self.config_entry
        choices: dict[str, str] = {}

        runtime = getattr(entry, "runtime_data", None)
        coordinator = None
        if runtime is not None:
            coordinator = getattr(runtime, "coordinator", None) or getattr(
                runtime, "data", None
            )
        if coordinator is None:
            coordinator = getattr(entry, "runtime_data", None)

        datasets: list[CollIterable[Any]] = []
        if coordinator is not None:
            data_attr = getattr(coordinator, "data", None)
            if isinstance(data_attr, CollIterable):
                datasets.append(data_attr)

        for dataset in datasets:
            for candidate in dataset:
                if not isinstance(candidate, Mapping):
                    continue
                device_id = candidate.get("device_id") or candidate.get("id")
                if not isinstance(device_id, str) or not device_id:
                    continue
                name = candidate.get("name")
                if not isinstance(name, str) or not name.strip():
                    name = device_id
                choices.setdefault(device_id, name)

        if not choices:
            for option in self._gather_subentry_options():
                for device_id in option.visible_device_ids:
                    choices.setdefault(device_id, device_id)

        return dict(sorted(choices.items(), key=lambda item: item[1].lower()))

    # ---------- Semantic locations ----------
    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        """Return the float representation or ``None`` when conversion fails."""

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _semantic_locations(self) -> dict[str, dict[str, float]]:
        """Return a normalized mapping of semantic locations from options."""

        raw = self.config_entry.options.get(OPT_SEMANTIC_LOCATIONS) or {}
        if not isinstance(raw, Mapping):
            return {}

        normalized: dict[str, dict[str, float]] = {}
        for name, payload in raw.items():
            if not isinstance(name, str) or not isinstance(payload, Mapping):
                continue

            latitude_obj = payload.get("latitude")
            longitude_obj = payload.get("longitude")
            if latitude_obj is None or longitude_obj is None:
                continue

            latitude = self._coerce_float(latitude_obj)
            longitude = self._coerce_float(longitude_obj)
            if latitude is None or longitude is None:
                continue

            accuracy = self._coerce_float(payload.get("accuracy"))
            if accuracy is None or accuracy <= 0:
                # Use default radius for semantic zones without explicit accuracy.
                # Never use 0.0 as it's physically impossible for GPS.
                accuracy = DEFAULT_SEMANTIC_DETECTION_RADIUS

            normalized[name] = {
                "latitude": latitude,
                "longitude": longitude,
                "accuracy": accuracy,
            }

        return normalized

    def _home_zone_defaults(self) -> tuple[float, float, float]:
        """Return latitude/longitude/accuracy defaults from the Home zone."""

        latitude = getattr(self.hass.config, "latitude", None)
        longitude = getattr(self.hass.config, "longitude", None)
        accuracy = DEFAULT_SEMANTIC_DETECTION_RADIUS

        zone_state = self.hass.states.get("zone.home")
        if zone_state is not None:
            attrs = zone_state.attributes
            latitude = attrs.get("latitude", latitude)
            longitude = attrs.get("longitude", longitude)
            accuracy = attrs.get("radius", accuracy)

        try:
            lat_float = float(latitude) if latitude is not None else 0.0
        except (TypeError, ValueError):
            lat_float = 0.0

        try:
            lon_float = float(longitude) if longitude is not None else 0.0
        except (TypeError, ValueError):
            lon_float = 0.0

        try:
            acc_float = (
                float(accuracy)
                if accuracy is not None
                else DEFAULT_SEMANTIC_DETECTION_RADIUS
            )
        except (TypeError, ValueError):
            acc_float = DEFAULT_SEMANTIC_DETECTION_RADIUS

        acc_float = max(acc_float, DEFAULT_SEMANTIC_DETECTION_RADIUS)

        return lat_float, lon_float, acc_float

    async def async_step_semantic_locations(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """List semantic locations and expose add/edit/delete actions."""

        semantic_locations = self._semantic_locations()
        display: list[str] = []
        for name, payload in sorted(semantic_locations.items()):
            display.append(
                f"{name}: {payload.get('latitude')} "
                f"{payload.get('longitude')} (±{payload.get('accuracy')} m)"
            )

        menu_options = ["semantic_locations_add"]
        if semantic_locations:
            menu_options.extend(
                ["semantic_locations_edit", "semantic_locations_delete"]
            )

        return cast(
            FlowResult,
            cast(Any, self.async_show_menu)(
                step_id="semantic_locations",
                description_placeholders={
                    "semantic_locations": "\n".join(display)
                    if display
                    else "None configured",
                },
                menu_options=menu_options,
            ),
        )

    async def async_step_semantic_locations_add(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Add a semantic location mapping."""

        return await self._async_semantic_location_form(user_input)

    async def async_step_semantic_locations_edit(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Select and edit an existing semantic location mapping."""

        semantic_locations = self._semantic_locations()
        if not semantic_locations:
            return self.async_abort(reason="no_semantic_locations")

        choices = {name: name for name in sorted(semantic_locations)}
        schema = vol.Schema({vol.Required("semantic_location"): vol.In(choices)})

        if user_input is None:
            return self.async_show_form(
                step_id="semantic_locations_edit", data_schema=schema
            )

        selected = str(user_input.get("semantic_location", ""))
        if selected not in semantic_locations:
            return self.async_show_form(
                step_id="semantic_locations_edit",
                data_schema=schema,
                errors={"semantic_location": "invalid"},
            )

        self._semantic_location_editing = selected
        return await self._async_semantic_location_form(
            None, selected, semantic_locations[selected]
        )

    async def async_step_semantic_locations_edit_form(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle submission for editing a semantic location."""

        semantic_locations = self._semantic_locations()
        existing_name = self._semantic_location_editing
        existing = (
            semantic_locations.get(existing_name or "") if existing_name else None
        )
        return await self._async_semantic_location_form(
            user_input, existing_name, existing
        )

    async def async_step_semantic_locations_delete(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Delete one or more semantic location mappings."""

        semantic_locations = self._semantic_locations()
        if not semantic_locations:
            return self.async_abort(reason="no_semantic_locations")

        choices = {name: name for name in sorted(semantic_locations)}
        schema = vol.Schema(
            {vol.Required("semantic_locations", default=[]): cv.multi_select(choices)}
        )

        if user_input is None:
            return self.async_show_form(
                step_id="semantic_locations_delete", data_schema=schema
            )

        to_delete_raw = user_input.get("semantic_locations") or []
        if not isinstance(to_delete_raw, list):
            to_delete_raw = list(to_delete_raw)
        to_delete = [name for name in to_delete_raw if name in semantic_locations]
        if not to_delete:
            return self.async_show_form(
                step_id="semantic_locations_delete",
                data_schema=schema,
                errors={"base": "required"},
            )

        new_locations = {
            name: data
            for name, data in semantic_locations.items()
            if name not in to_delete
        }
        new_options = dict(self.config_entry.options)
        new_options[OPT_SEMANTIC_LOCATIONS] = new_locations

        self.hass.config_entries.async_update_entry(
            self.config_entry, options=new_options
        )
        # Standing down costs the reload, never the mapping. The options are in
        # the entry above, and ``_apply_semantic_mapping`` reads them live from
        # that same object on every payload, so the next poll or push applies
        # them whatever the foreign holder does; the reload only pulls that
        # forward. Do not restate the narrower claim this comment used to make
        # ("safe as long as the holder keeps its promise, one window is not
        # covered"). It rested on the holder reaching a setup, and there are
        # several ways it does not: a cancelled or refused reload task, a
        # scheduling call that raised, a failed unload, an entry disabled in
        # between, and the core-owned task after ``async_schedule_reload`` that
        # can die unobserved (see the residual paragraph in
        # ``agents/runtime_patterns/AGENTS.md``). None of them reaches this
        # write, which is why the live read and not the promise is the reason
        # standing down is safe here.
        _schedule_claimed_reload(self.hass, self.config_entry.entry_id)
        return await self.async_step_init()

    async def _async_semantic_location_form(
        self,
        user_input: dict[str, Any] | None,
        existing_name: str | None = None,
        existing: Mapping[str, Any] | None = None,
    ) -> FlowResult:
        """Handle add/edit form for a semantic location mapping."""

        if existing_name is None:
            self._semantic_location_editing = None

        semantic_default = existing_name or ""
        latitude_default = (
            self._coerce_float(existing.get("latitude")) if existing else None
        )
        longitude_default = (
            self._coerce_float(existing.get("longitude")) if existing else None
        )
        accuracy_default = (
            self._coerce_float(existing.get("accuracy")) if existing else None
        )

        if latitude_default is None or longitude_default is None:
            lat, lon, acc = self._home_zone_defaults()
            latitude_default = lat
            longitude_default = lon
            if accuracy_default is None:
                accuracy_default = acc
        elif accuracy_default is None:
            _, _, acc = self._home_zone_defaults()
            accuracy_default = acc

        schema = vol.Schema(
            {
                vol.Required("semantic_name", default=semantic_default): str,
                vol.Required("latitude", default=latitude_default): vol.All(
                    vol.Coerce(float), vol.Range(min=-90, max=90)
                ),
                vol.Required("longitude", default=longitude_default): vol.All(
                    vol.Coerce(float), vol.Range(min=-180, max=180)
                ),
                vol.Required("accuracy", default=accuracy_default): vol.All(
                    vol.Coerce(float), vol.Range(min=0)
                ),
            }
        )

        if user_input is None:
            return self.async_show_form(
                step_id="semantic_locations_add"
                if existing_name is None
                else "semantic_locations_edit_form",
                data_schema=schema,
            )

        errors: dict[str, str] = {}
        semantic_name = str(user_input.get("semantic_name", "")).strip()
        if not semantic_name:
            errors["semantic_name"] = "required"

        semantic_locations = self._semantic_locations()
        normalized_keys = {name.lower(): name for name in semantic_locations}
        semantic_name_key = semantic_name.lower()
        if (
            semantic_name
            and semantic_name_key in normalized_keys
            and normalized_keys[semantic_name_key] != existing_name
        ):
            errors["semantic_name"] = "duplicate_semantic_location"

        if errors:
            return self.async_show_form(
                step_id="semantic_locations_add"
                if existing_name is None
                else "semantic_locations_edit_form",
                data_schema=schema,
                errors=errors,
            )

        new_locations = dict(semantic_locations)
        if existing_name and existing_name in new_locations:
            new_locations.pop(existing_name)

        new_locations[semantic_name] = {
            "latitude": float(user_input["latitude"]),
            "longitude": float(user_input["longitude"]),
            "accuracy": float(user_input["accuracy"]),
        }

        new_options = dict(self.config_entry.options)
        new_options[OPT_SEMANTIC_LOCATIONS] = new_locations

        self.hass.config_entries.async_update_entry(
            self.config_entry, options=new_options
        )
        # Same reasoning as in ``async_step_semantic_locations_delete``: the new
        # mapping is in the entry before the claim and is read live from there,
        # so a stand-down delays the first application to the next poll or push
        # and loses nothing, whether or not the foreign holder reaches a setup.
        _schedule_claimed_reload(self.hass, self.config_entry.entry_id)
        self._semantic_location_editing = None
        return await self.async_step_init()

    # ---------- Settings (non-secret) ----------
    async def async_step_settings(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Update non-secret options in a single form."""
        await ConfigFlow._async_trigger_core_subentry_repair(
            self.hass, self.config_entry
        )
        entry = self.config_entry
        opt = cast(Mapping[str, object], entry.options)
        dat = cast(Mapping[str, object], entry.data)

        def _get(cur_key: str, default_val: object) -> object:
            return opt.get(cur_key, dat.get(cur_key, default_val))

        current: dict[str, object] = {
            OPT_LOCATION_POLL_INTERVAL: _get(
                OPT_LOCATION_POLL_INTERVAL, DEFAULT_LOCATION_POLL_INTERVAL
            ),
            OPT_DEVICE_POLL_DELAY: _get(
                OPT_DEVICE_POLL_DELAY, DEFAULT_DEVICE_POLL_DELAY
            ),
            OPT_MAP_VIEW_TOKEN_EXPIRATION: _get(
                OPT_MAP_VIEW_TOKEN_EXPIRATION, DEFAULT_MAP_VIEW_TOKEN_EXPIRATION
            ),
            OPT_MAP_VIEW_ENABLED: _get(OPT_MAP_VIEW_ENABLED, DEFAULT_MAP_VIEW_ENABLED),
            OPT_DELETE_CACHES_ON_REMOVE: _get(
                OPT_DELETE_CACHES_ON_REMOVE, DEFAULT_DELETE_CACHES_ON_REMOVE
            ),
            OPT_CONTRIBUTOR_MODE: _get(OPT_CONTRIBUTOR_MODE, DEFAULT_CONTRIBUTOR_MODE),
            OPT_STALE_THRESHOLD: _get(OPT_STALE_THRESHOLD, DEFAULT_STALE_THRESHOLD),
            OPT_SHOW_LOCATION_AGE: _get(
                OPT_SHOW_LOCATION_AGE, DEFAULT_SHOW_LOCATION_AGE
            ),
            OPT_SPEED_GATE_ENABLED: _get(
                OPT_SPEED_GATE_ENABLED, DEFAULT_SPEED_GATE_ENABLED
            ),
            OPT_ROUNDTRIP_CONFIRM: _get(
                OPT_ROUNDTRIP_CONFIRM, DEFAULT_ROUNDTRIP_CONFIRM
            ),
            OPT_ACCURACY_GATE_ENABLED: _get(
                OPT_ACCURACY_GATE_ENABLED, DEFAULT_ACCURACY_GATE_ENABLED
            ),
            # Advanced override (F3): additional secrets.json watch paths, one per
            # line. Empty by default; the zero-config container-data path is
            # watched automatically without this option. Rendered as a text block
            # (newline-joined) and parsed back to a list on submit.
            SECRETS_EXTRA_WATCH_PATHS: _extra_watch_paths_to_text(
                opt.get(SECRETS_EXTRA_WATCH_PATHS)
            ),
        }
        if (
            OPT_GOOGLE_HOME_FILTER_ENABLED is not None
            and DEFAULT_GOOGLE_HOME_FILTER_ENABLED is not None
        ):
            current[OPT_GOOGLE_HOME_FILTER_ENABLED] = _get(
                OPT_GOOGLE_HOME_FILTER_ENABLED, DEFAULT_GOOGLE_HOME_FILTER_ENABLED
            )
        if (
            OPT_GOOGLE_HOME_FILTER_KEYWORDS is not None
            and DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS is not None
        ):
            current[OPT_GOOGLE_HOME_FILTER_KEYWORDS] = _get(
                OPT_GOOGLE_HOME_FILTER_KEYWORDS, DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS
            )
        if (
            OPT_ENABLE_STATS_ENTITIES is not None
            and DEFAULT_ENABLE_STATS_ENTITIES is not None
        ):
            current[OPT_ENABLE_STATS_ENTITIES] = _get(
                OPT_ENABLE_STATS_ENTITIES, DEFAULT_ENABLE_STATS_ENTITIES
            )

        fields: dict[Any, Any] = {}
        option_markers: list[str] = []

        def _resolve_marker_key(marker: Any) -> str:
            obj: Any = marker
            seen: set[int] = set()
            while True:
                if isinstance(obj, str):
                    return obj
                obj_id = id(obj)
                if obj_id in seen:
                    break
                seen.add(obj_id)
                candidate = getattr(obj, "schema", None)
                if isinstance(candidate, Mapping):
                    try:
                        obj = next(iter(candidate))
                        continue
                    except StopIteration:
                        break
                if isinstance(candidate, CollIterable) and not isinstance(
                    candidate, (str, bytes, bytearray)
                ):
                    iterator = iter(candidate)
                    try:
                        obj = next(iterator)
                        continue
                    except StopIteration:
                        break
                if candidate is not None:
                    obj = candidate
                    continue
                if isinstance(obj, Mapping):
                    try:
                        obj = next(iter(obj))
                        continue
                    except StopIteration:
                        break
                if isinstance(obj, CollIterable) and not isinstance(
                    obj, (str, bytes, bytearray)
                ):
                    iterator = iter(obj)
                    try:
                        obj = next(iterator)
                        continue
                    except StopIteration:
                        break
                break
            return str(obj)

        def _register(marker: Any, validator: Any) -> None:
            fields[marker] = validator
            option_markers.append(_resolve_marker_key(marker))

        _register(
            vol.Optional(OPT_LOCATION_POLL_INTERVAL),
            vol.All(vol.Coerce(int), vol.Range(min=60, max=3600)),
        )
        _register(
            vol.Optional(OPT_DEVICE_POLL_DELAY),
            vol.All(vol.Coerce(int), vol.Range(min=1, max=60)),
        )
        _register(vol.Optional(OPT_MAP_VIEW_TOKEN_EXPIRATION), bool)
        _register(vol.Optional(OPT_MAP_VIEW_ENABLED), bool)
        _register(vol.Optional(OPT_DELETE_CACHES_ON_REMOVE), bool)
        if OPT_GOOGLE_HOME_FILTER_ENABLED is not None:
            _register(vol.Optional(OPT_GOOGLE_HOME_FILTER_ENABLED), bool)
        if OPT_GOOGLE_HOME_FILTER_KEYWORDS is not None:
            _register(vol.Optional(OPT_GOOGLE_HOME_FILTER_KEYWORDS), str)
        if OPT_ENABLE_STATS_ENTITIES is not None:
            _register(vol.Optional(OPT_ENABLE_STATS_ENTITIES), bool)
        if selector is not None:
            _register(
                vol.Optional(OPT_CONTRIBUTOR_MODE),
                selector(
                    {
                        "select": {
                            "options": [
                                CONTRIBUTOR_MODE_HIGH_TRAFFIC,
                                CONTRIBUTOR_MODE_IN_ALL_AREAS,
                            ],
                            "translation_key": "contributor_mode",
                        }
                    }
                ),
            )
        else:
            _register(
                vol.Optional(OPT_CONTRIBUTOR_MODE),
                vol.In([CONTRIBUTOR_MODE_HIGH_TRAFFIC, CONTRIBUTOR_MODE_IN_ALL_AREAS]),
            )
        _register(
            vol.Optional(OPT_STALE_THRESHOLD),
            vol.All(vol.Coerce(int), vol.Range(min=300, max=86400)),
        )
        _register(vol.Optional(OPT_SHOW_LOCATION_AGE), bool)
        _register(vol.Optional(OPT_SPEED_GATE_ENABLED), bool)
        _register(vol.Optional(OPT_ROUNDTRIP_CONFIRM), bool)
        _register(vol.Optional(OPT_ACCURACY_GATE_ENABLED), bool)
        # Advanced override (F3): extra secrets.json watch paths (one per line).
        if selector is not None:
            _register(
                vol.Optional(SECRETS_EXTRA_WATCH_PATHS),
                selector({"text": {"multiline": True}}),
            )
        else:
            _register(vol.Optional(SECRETS_EXTRA_WATCH_PATHS), str)

        base_schema = vol.Schema(fields)
        schema_with_defaults = self.add_suggested_values_to_schema(base_schema, current)

        if user_input is not None:
            new_options = dict(entry.options)
            for real_key in option_markers:
                if real_key in user_input:
                    new_options[real_key] = user_input[real_key]
                else:
                    new_options[real_key] = current.get(real_key)
            # Normalize the extra-watch-paths text block into a clean list
            # (one path per line). An empty value removes the override so the
            # zero-config defaults apply.
            parsed_extra = _parse_extra_watch_paths_text(
                new_options.get(SECRETS_EXTRA_WATCH_PATHS)
            )
            if parsed_extra:
                new_options[SECRETS_EXTRA_WATCH_PATHS] = parsed_extra
            else:
                new_options.pop(SECRETS_EXTRA_WATCH_PATHS, None)
            new_options[OPT_OPTIONS_SCHEMA_VERSION] = 2

            # These options are read at setup time (poll interval, map view),
            # so they need a reload to take effect. Until
            # the update listener arrived this step inherited one from
            # ``OptionsFlowWithReload``; now that the base is plain, it asks
            # for its own, through the same owner latch as its siblings. The
            # listener does not cover it: it reloads only when
            # credential-relevant keys changed, and this form writes none.
            # Ordering is safe for the reason spelled out in
            # ``async_step_visibility``: the unload half runs first and never
            # reads ``entry.options``.
            #
            # Only for a write that changes something, though, and that is
            # not a refinement of the inherited behaviour but a copy of it:
            # ``OptionsFlowManager.async_finish_flow`` scheduled on
            # ``async_update_entry(entry, options=result["data"]) and
            # automatic_reload``, and ``async_update_entry`` returns ``False``
            # for an unchanged payload. Probed against the real manager in
            # core 2026.1.3 and read in 2026.2.3: an identical payload
            # schedules nothing, a changed one schedules once. Claiming
            # unconditionally would turn confirming the form without an edit
            # into a full teardown of every entity of the account, and would
            # hold the single-owner latch while doing it.
            #
            # One comparison, because the options are the only thing this
            # submit branch writes. It mirrors the core's own test
            # (``entry.options != result["data"]``; ``new_options`` *is* that
            # payload, and ``MappingProxyType`` compares by content, so the
            # ``dict()`` is for mypy, not for semantics).
            options_changed = dict(entry.options) != new_options
            if not options_changed:
                _LOGGER.debug(
                    "Settings for entry %s were confirmed without a change; "
                    "no reload owed",
                    entry.entry_id,
                )
            elif not _schedule_claimed_reload(self.hass, entry.entry_id):
                _LOGGER.debug(
                    "Settings for entry %s were saved without scheduling a "
                    "reload; another owner holds the reload, or the entry "
                    "has no usable lever",
                    entry.entry_id,
                )

            return self.async_create_entry(title="", data=new_options)

        return self.async_show_form(
            step_id="settings",
            data_schema=schema_with_defaults,
        )

    # ---------- Visibility (restore ignored devices) ----------
    async def async_step_visibility(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Display ignored devices and allow restoring them (remove from OPT_IGNORED_DEVICES)."""
        entry = self.config_entry
        options = dict(entry.options)
        raw = (
            options.get(OPT_IGNORED_DEVICES)
            or entry.data.get(OPT_IGNORED_DEVICES)
            or {}
        )
        ignored_map, _migrated = coerce_ignored_mapping(raw)

        if not ignored_map:
            return self.async_abort(reason="no_ignored_devices")

        choices: dict[str, str]
        if callable(ignored_choices_for_ui):
            choices = dict(ignored_choices_for_ui(ignored_map))
        else:
            choices = {}
            for dev_id, meta in ignored_map.items():
                name_obj: object | None = None
                if isinstance(meta, CollMapping):
                    name_obj = meta.get("name")
                choices[dev_id] = dev_id if not isinstance(name_obj, str) else name_obj

        subentry_choices, subentry_option_map = self._device_target_choice_map()
        default_subentry = self._default_subentry_key(
            subentry_choices, subentry_option_map
        )

        schema = vol.Schema(
            {
                vol.Required(_FIELD_SUBENTRY, default=default_subentry): vol.In(
                    subentry_choices
                ),
                vol.Optional("unignore_devices", default=[]): cv.multi_select(choices),
            }
        )

        if user_input is not None:
            selected_key = str(user_input.get(_FIELD_SUBENTRY, default_subentry))
            if selected_key not in subentry_choices:
                return self.async_show_form(
                    step_id="visibility",
                    data_schema=schema,
                    errors={_FIELD_SUBENTRY: "invalid_subentry"},
                    description_placeholders=_SUBENTRY_PLACEHOLDERS,
                )

            raw_restore = user_input.get("unignore_devices") or []
            if not isinstance(raw_restore, list):
                raw_restore = list(raw_restore)
            to_restore = [
                str(dev_id) for dev_id in raw_restore if isinstance(dev_id, str)
            ]
            for dev_id in to_restore:
                ignored_map.pop(dev_id, None)

            new_options = dict(entry.options)
            new_options[OPT_IGNORED_DEVICES] = ignored_map
            new_options[OPT_OPTIONS_SCHEMA_VERSION] = 2

            if to_restore:
                await self._async_assign_devices_to_subentry(
                    entry, selected_key, to_restore
                )
                # Unignoring is not unhiding. When the device was ignored,
                # ``async_remove_config_entry_device`` returned True, so Home
                # Assistant dropped the device and its entities from the
                # registries; putting the id back into the options does not bring
                # them back. The platform listeners do run on the next poll, but
                # each of them guards on monotone known-sets that are built once
                # per setup and never lose an entry (``device_tracker`` alone
                # keeps two, ``known_ids`` and ``added_unique_ids``; ``sensor``,
                # ``binary_sensor`` and ``button`` keep their own), so
                # ``_build_entities`` sees the id, hits a ``continue`` and creates
                # nothing. Measured, not assumed: removing either guard on its own
                # leaves the characterisation test green, only removing both turns
                # it red. Only a reload rebuilds the sets
                # from empty, which is why the docstring of
                # ``_verify_pending_registry_entries`` calls it the only lever that
                # helps here. Routing through the single owner keeps a reload that
                # is already on its way from turning into two.
                #
                # The claim happens before ``async_create_entry`` stores the
                # options, and what makes that safe is narrower than it looks.
                # ``async_schedule_reload`` does **not** merely queue a task:
                # ``hass.async_create_task`` defaults to ``eager_start=True``, so
                # ``async_reload`` runs synchronously up to its first real
                # suspension, and the *unload* half is therefore already under way
                # before this step returns. That half does not read
                # ``entry.options``, so it is harmless. The *setup* half is the one
                # that reads them, and it sits behind the ``await asyncio.gather``
                # in the platform teardown, which yields; by the time it runs, the
                # step has returned and ``OptionsFlowManager.async_finish_flow`` has
                # written the options through ``async_update_entry`` (that path
                # itself holds no suspension point). It gets that far only because
                # this handler does not inherit from ``OptionsFlowWithReload``: with
                # that base and an update listener registered, the manager raises
                # before the write and this scheduled reload would tear the entry
                # down for a restore that never lands. So the invariant is: *some*
                # real await must remain in the unload path. Were the parent unload
                # ever to return synchronously for a loaded entry, the setup half
                # would read the options this step has not written yet and the
                # restore would be silently lost. Measured against core 2026.1.3
                # and 2026.2.3; a core bump puts the question again.
                #
                # Named residual, deliberately **not** closed here: the argument
                # above is scoped to a *loaded* entry, and it has to be.
                # ``ConfigEntry.async_unload`` returns at
                # ``if self.state is not ConfigEntryState.LOADED: ... return True``
                # for every other state, without running our
                # ``async_unload_entry`` and therefore without that suspension
                # (measured in both cores). Whether the setup half then reaches an
                # ``entry.options`` read before its own first suspension is **not
                # measured**, so whether this actually loses a write on a
                # ``SETUP_ERROR`` or ``SETUP_RETRY`` entry is open. If it does, it
                # is a property of all four steps that route through
                # ``_schedule_claimed_reload`` from a flow, not of this one, and a
                # not-loaded entry would lose nothing by standing down, because its
                # next setup reads the newly written options by itself. Fixing it
                # at one site only would be the punctual fix this repo's
                # error-class discipline forbids.
                if not _schedule_claimed_reload(self.hass, entry.entry_id):
                    # Falsy covers two situations the helper cannot tell apart for
                    # us, and neither of them leaks the latch (it returns before
                    # its claim in both). Either another owner already holds the
                    # reload, in which case that reload carries this write too and
                    # nothing is lost, or there is no usable lever: a terminal
                    # entry, or a core without ``async_schedule_reload``. The
                    # second case costs the whole thing here rather than a half,
                    # because the restored devices have no entity to fall back on.
                    # A warning rather than an abort reason, precisely because the
                    # two cases are indistinguishable from here; the credential
                    # path can report ``credentials_saved_not_reloaded`` because it
                    # runs the state check itself and knows which case it is in.
                    _LOGGER.warning(
                        "Restored %d device(s) from the ignore list for entry %s, "
                        "but no reload was scheduled by this step. If another "
                        "reload is already on its way the devices come back with "
                        "it; otherwise they stay without entities until the entry "
                        "is set up again",
                        len(to_restore),
                        entry.entry_id,
                    )

            return self.async_create_entry(title="", data=new_options)

        return self.async_show_form(
            step_id="visibility",
            data_schema=schema,
            description_placeholders=_SUBENTRY_PLACEHOLDERS,
        )

    async def async_step_repairs(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Entry point for subentry repair operations."""
        await ConfigFlow._async_trigger_core_subentry_repair(
            self.hass, self.config_entry
        )

        subentry_choices, _ = self._subentry_choice_map()
        if not subentry_choices:
            return self.async_abort(reason="repairs_no_subentries")

        return self.async_show_menu(
            step_id="repairs",
            menu_options=["repairs_move", "repairs_delete"],
        )

    async def async_step_repairs_move(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Assign selected devices to a subentry, removing them from others."""
        await ConfigFlow._async_trigger_core_subentry_repair(
            self.hass, self.config_entry
        )

        # A move needs a real destination, so the synthesised fallback is not a
        # candidate here. ``async_step_repairs_delete`` already draws that line
        # for its own fallback field through ``real_fallback_keys``; this is the
        # same rule, applied to the target field.
        #
        # Dropping it rather than letting it through is what keeps the report
        # honest. Without a backing subentry the assignment writes nothing, so
        # ``changed`` stays empty and the step below aborts with
        # ``subentry_move_success`` -- a success message for a move that could
        # not happen. Aborting on ``repairs_no_subentries`` instead says the
        # truth about such an entry with a string that already exists, so no
        # translation key is added.
        #
        # ``async_step_visibility`` deliberately keeps the fallback: there the
        # user's request is to leave the ignore list, which the option write and
        # the reload carry on their own, and an id no subentry claims is merged
        # into the tracker group by ``coordinator/subentry.py``.
        raw_choices, raw_option_map = self._device_target_choice_map()
        # Filter the labels and the options together. Handing the *unfiltered*
        # map to a helper that receives the filtered labels is harmless only
        # while every branch of that helper iterates ``choices``; the step
        # drops the synthesised fallback on purpose, so a future branch that
        # walked the map instead would hand it back as the preselection.
        subentry_options = {
            key: raw_option_map[key]
            for key in raw_choices
            if raw_option_map[key].subentry is not None
        }
        subentry_choices = {key: raw_choices[key] for key in subentry_options}
        if not subentry_choices:
            return self.async_abort(reason="repairs_no_subentries")

        default_subentry = self._default_subentry_key(
            subentry_choices, subentry_options
        )
        device_choices = self._device_choice_map()

        schema = vol.Schema(
            {
                vol.Required(_FIELD_REPAIR_TARGET, default=default_subentry): vol.In(
                    subentry_choices
                ),
                vol.Optional(_FIELD_REPAIR_DEVICES, default=[]): cv.multi_select(
                    device_choices
                ),
            }
        )

        if user_input is not None:
            target_key = str(user_input.get(_FIELD_REPAIR_TARGET, default_subentry))
            if target_key not in subentry_choices:
                return self.async_show_form(
                    step_id="repairs_move",
                    data_schema=schema,
                    errors={_FIELD_REPAIR_TARGET: "invalid_subentry"},
                )

            raw_devices = user_input.get(_FIELD_REPAIR_DEVICES) or []
            if not isinstance(raw_devices, list):
                raw_devices = list(raw_devices)
            device_ids = [
                str(dev_id) for dev_id in raw_devices if isinstance(dev_id, str)
            ]

            if not device_ids:
                return self.async_abort(reason="repair_no_devices")

            changed = await self._async_assign_devices_to_subentry(
                self.config_entry, target_key, device_ids
            )

            placeholders = {
                "subentry": subentry_choices[target_key],
                "count": str(len(device_ids)),
            }

            if not changed:
                return self.async_abort(
                    reason="subentry_move_success",
                    description_placeholders=placeholders,
                )

            # Two directions here, and only one of them is a nuisance. Towards a
            # second reload: unlike the semantic steps there is an ``await``
            # between the write and the claim
            # (``_async_assign_devices_to_subentry`` above), during which a
            # foreign reload may release the latch again, which costs one reload
            # too many and never the assignment. Towards no reload at all:
            # standing down leaves the assignment stored, and the coordinator
            # still picks its metadata up at the next poll, because
            # ``_refresh_subentry_index`` re-reads ``entry.subentries`` there.
            # What waits for a reload is the entity-to-subentry binding, which
            # platform setup hands out. If the holder ends in one of the dead
            # ends that release the latch without reloading, that half waits for
            # the next reload. ``async_step_visibility`` performs the same
            # assignment and now takes the same claim, so all three assignment
            # sites agree; what still differs is the price of standing down. There
            # it is the whole thing, because a device coming back from the ignore
            # list has no entity at all until a reload rebuilds the platform
            # known-sets. Here it is that one half.
            _schedule_claimed_reload(self.hass, self.config_entry.entry_id)
            return self.async_abort(
                reason="subentry_move_success", description_placeholders=placeholders
            )

        return self.async_show_form(step_id="repairs_move", data_schema=schema)

    async def async_step_repairs_delete(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Remove a subentry after optionally moving its devices to a fallback."""
        await ConfigFlow._async_trigger_core_subentry_repair(
            self.hass, self.config_entry
        )

        # Two maps on purpose, and only one of them is narrowed. What may be
        # *deleted* is a different question from what may *receive* devices: a
        # service subentry stays removable (the core repair path recreates a
        # missing one), while it must not be offered as the fallback target,
        # because the devices moved there would go nowhere.
        #
        # The two questions are still coupled, through ``fallback_key !=
        # target_key`` below: a group may only be offered for deletion while a
        # *different* group can inherit its devices. Narrowing the fallback
        # side without re-deriving the deletable side left the common shape
        # (one tracker group plus one service group) with a form whose single
        # fallback value was always the deletion target, so every submission
        # failed on ``invalid_subentry`` with no way out. Offering an action
        # that can never be carried out is the very defect this step was
        # narrowed to remove, one field higher up.
        subentry_choices, option_map = self._subentry_choice_map()
        fallback_choices, fallback_option_map = self._device_target_choice_map()
        # Synthesised entries do not count: the ``core_tracking`` placeholder
        # that ``_device_target_choice_map`` falls back to has no backing
        # subentry, so it cannot actually inherit anything.
        real_fallback_keys = {
            key
            for key, option in fallback_option_map.items()
            if option.subentry is not None
        }
        removable_choices = {
            key: label
            for key, label in subentry_choices.items()
            if option_map[key].subentry and (real_fallback_keys - {key})
        }
        if not removable_choices:
            return self.async_abort(reason="subentry_delete_invalid")

        schema = vol.Schema(
            {
                vol.Required(_FIELD_REPAIR_DELETE): vol.In(removable_choices),
                vol.Required(
                    _FIELD_REPAIR_FALLBACK,
                    default=self._default_subentry_key(
                        fallback_choices, fallback_option_map
                    ),
                ): vol.In(fallback_choices),
            }
        )

        if user_input is not None:
            errors: dict[str, str] = {}
            target_key = str(user_input.get(_FIELD_REPAIR_DELETE, ""))
            fallback_key = str(user_input.get(_FIELD_REPAIR_FALLBACK, ""))

            if target_key not in removable_choices:
                errors[_FIELD_REPAIR_DELETE] = "invalid_subentry"
            if fallback_key not in fallback_choices or fallback_key == target_key:
                errors[_FIELD_REPAIR_FALLBACK] = "invalid_subentry"

            if errors:
                return self.async_show_form(
                    step_id="repairs_delete", data_schema=schema, errors=errors
                )

            devices = list(option_map[target_key].visible_device_ids)
            if devices:
                await self._async_assign_devices_to_subentry(
                    self.config_entry, fallback_key, devices
                )

            removed = await self._async_remove_subentry(
                self.config_entry, option_map[target_key]
            )
            if not removed:
                return self.async_abort(reason="subentry_remove_failed")

            placeholders = {
                "subentry": subentry_choices[target_key],
                "fallback": fallback_choices[fallback_key],
                "count": str(len(devices)),
            }

            # Same two directions as in ``async_step_repairs_move``: the removal
            # is awaited above and done, and the core clears the device and
            # entity registry links of the subentry as part of it, so a latch
            # that changed hands in between costs at most a second reload, never
            # the change itself. In the other direction, a stand-down whose
            # holder never reaches a setup leaves the already loaded entities of
            # the removed subentry standing until the next reload.
            _schedule_claimed_reload(self.hass, self.config_entry.entry_id)
            return self.async_abort(
                reason="subentry_delete_success",
                description_placeholders=placeholders,
            )

        return self.async_show_form(step_id="repairs_delete", data_schema=schema)

    async def async_step_credentials(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Refresh credentials without exposing current ones.

        IMPORTANT CHANGE:
        - This step NO LONGER allows changing the Google email to avoid cross-account
          mutations that can break unique_id semantics. Use a new integration entry
          to add another account.
        """
        errors: dict[str, str] = {}

        if selector is not None:
            schema = vol.Schema(
                {
                    vol.Optional("new_secrets_json"): selector(
                        {"text": {"multiline": True}}
                    ),
                    # vol.Optional("new_oauth_token"): str,  # Disabled: broken manual token path stays hidden until fixed.
                }
            )
        else:
            schema = vol.Schema(
                {
                    vol.Optional("new_secrets_json"): str,
                    # vol.Optional("new_oauth_token"): str,  # Disabled: broken manual token path stays hidden until fixed.
                }
            )

        if user_input is not None:
            new_token = (user_input.get("new_oauth_token") or "").strip()
            has_token = bool(new_token)
            has_secrets = bool((user_input.get("new_secrets_json") or "").strip())
            supplied = _count_supplied_credential_methods(
                user_input, _OPTIONS_CREDENTIAL_FIELDS
            )
            if supplied != 1:
                # Exactly one credential method per submission. Zero is the
                # pre-existing "nothing entered" case; more than one must
                # fail here too, before anything is written.
                errors["base"] = "choose_one"
            else:
                try:
                    entry = self.config_entry
                    email = entry.data.get(CONF_GOOGLE_EMAIL)

                    async def _finalize_success(
                        updated_data: dict[str, Any],
                    ) -> FlowResult:
                        await self._async_clear_cached_aas_token(entry)
                        self.hass.config_entries.async_update_entry(
                            entry, data=updated_data
                        )
                        # A claim is a promise to reload, and the release
                        # points (unload, setup, removal) all presuppose
                        # that a reload arrives. Where it cannot, promising
                        # would strand the latch for the life of the
                        # process and every later credential write would
                        # stand down with its credentials ineffective.
                        if _entry_reload_is_hopeless(self.hass, entry.entry_id, entry):
                            # Names all three inputs rather than the state
                            # alone: for a disabled or ignored entry the
                            # state stays recoverable and would mislead.
                            _LOGGER.warning(
                                "Not reloading entry %s after a credential "
                                "write (state=%s, disabled_by=%s, "
                                "source=%s); the credentials are stored and "
                                "take effect the next time the entry is "
                                "set up successfully",
                                entry.entry_id,
                                getattr(entry, "state", None),
                                getattr(entry, "disabled_by", None),
                                getattr(entry, "source", None),
                            )
                            # Only this branch reports differently. The
                            # regular path keeps ``reconfigure_successful``,
                            # where that message is true.
                            return self.async_abort(
                                reason="credentials_saved_not_reloaded"
                            )
                        # The write above notifies the credential update
                        # listener, which reloads for exactly this change.
                        # Whichever side claims the latch first reloads.
                        if _claim_entry_reload(self.hass, entry.entry_id):
                            reload_task = self.hass.async_create_task(
                                self.hass.config_entries.async_reload(entry.entry_id)
                            )
                            # Fire-and-forget keeps the promise open for the
                            # task's whole lifetime; a task that dies before
                            # the unload/setup release points would leave
                            # the latch set for good.
                            _release_claim_when_reload_fails(
                                self.hass, entry.entry_id, reload_task
                            )
                        return self.async_abort(reason="reconfigure_successful")

                    if has_token:
                        try:
                            chosen = await async_pick_working_token(
                                self.hass,
                                email,
                                [("manual", new_token)],
                            )
                        except (DependencyNotReady, ImportError) as exc:
                            _register_dependency_error(errors, exc)
                        else:
                            if not chosen:
                                _log_token_validation_failure(
                                    candidates=[("manual", new_token)],
                                )
                                errors["base"] = "cannot_connect"
                            else:
                                if _disqualifies_for_persistence(chosen):
                                    _LOGGER.warning(
                                        "Options: token looks like a JWT; persisting anyway due to validation."
                                    )
                                updated_data = {
                                    **entry.data,
                                    DATA_AUTH_METHOD: _AUTH_METHOD_INDIVIDUAL,
                                    CONF_OAUTH_TOKEN: chosen,
                                }
                                updated_data.pop(DATA_SECRET_BUNDLE, None)
                                if isinstance(chosen, str) and chosen.startswith(
                                    "aas_et/"
                                ):
                                    updated_data[DATA_AAS_TOKEN] = chosen
                                else:
                                    updated_data.pop(DATA_AAS_TOKEN, None)
                                return await _finalize_success(updated_data)

                    if has_secrets and "new_secrets_json" in user_input:
                        try:
                            parsed = json.loads(user_input["new_secrets_json"])
                            if not isinstance(parsed, dict):
                                raise TypeError()
                            parsed = normalize_secrets_bundle(parsed)
                        except Exception:
                            errors["new_secrets_json"] = "invalid_json"
                        else:
                            has_shared, _has_owner = _secrets_key_status(parsed)
                            if not has_shared:
                                # Single-key rule: reject a shared_key-less
                                # bundle before any persist/reload. Field
                                # slot (like invalid_json) because it is a
                                # correctable paste, not a bundle/network
                                # state.
                                errors["new_secrets_json"] = "keys_missing"
                            elif not (
                                cands := _extract_oauth_candidates_from_secrets(parsed)
                            ):
                                errors["base"] = "invalid_token"
                            else:
                                try:
                                    chosen = await async_pick_working_token(
                                        self.hass,
                                        email,
                                        cands,
                                        secrets_bundle=parsed,
                                    )
                                except (DependencyNotReady, ImportError) as exc:
                                    _register_dependency_error(errors, exc)
                                else:
                                    if not chosen:
                                        _log_token_validation_failure(candidates=cands)
                                        errors["base"] = "cannot_connect"
                                    else:
                                        to_persist = chosen
                                        if _disqualifies_for_persistence(to_persist):
                                            alt = next(
                                                (
                                                    v
                                                    for (_src, v) in cands
                                                    if not _disqualifies_for_persistence(
                                                        v
                                                    )
                                                ),
                                                None,
                                            )
                                            if alt:
                                                to_persist = alt
                                        updated_data = {
                                            **entry.data,
                                            DATA_AUTH_METHOD: _AUTH_METHOD_SECRETS,
                                            **_persist_secrets_bundle(
                                                parsed, to_persist
                                            ),
                                        }
                                        if not (
                                            isinstance(to_persist, str)
                                            and to_persist.startswith("aas_et/")
                                        ):
                                            updated_data.pop(DATA_AAS_TOKEN, None)
                                        return await _finalize_success(updated_data)
                except Exception as err2:  # noqa: BLE001
                    # The deferral rebuilds the payload from the bundle, so
                    # it only applies to a bundle submission. ``has_secrets``
                    # is the guard, not a mere key check: it also rules out
                    # an empty string, which would reach ``json.loads`` and
                    # raise there instead. Both credential branches call
                    # ``_finalize_success`` inside this ``try``, so a guard
                    # error can arrive from a token-only submission too;
                    # that one falls through to the error mapping below.
                    # Defence in depth rather than a live user path: the
                    # token field is commented out of both schema branches
                    # and the flow manager rejects extra keys, so today
                    # only a direct call produces that shape.
                    if _is_multi_entry_guard_error(err2) and has_secrets:
                        entry = self.config_entry
                        parsed = normalize_secrets_bundle(
                            json.loads(user_input["new_secrets_json"])
                        )
                        cands = _extract_oauth_candidates_from_secrets(parsed)
                        token_first = cands[0][1] if cands else ""
                        updated_data = {
                            **entry.data,
                            DATA_AUTH_METHOD: _AUTH_METHOD_SECRETS,
                            **_persist_secrets_bundle(parsed, token_first),
                        }
                        if not (
                            isinstance(token_first, str)
                            and token_first.startswith("aas_et/")
                        ):
                            updated_data.pop(DATA_AAS_TOKEN, None)
                        return await _finalize_success(updated_data)
                    errors["base"] = _map_api_exc_to_error_key(err2)

        return self.async_show_form(
            step_id="credentials",
            data_schema=schema,
            errors=errors,
        )


# ---------- Custom exceptions ----------
class CannotConnect(HomeAssistantErrorBase):
    """Error to indicate we cannot connect to the remote service."""


class InvalidAuth(HomeAssistantErrorBase):
    """Error to indicate invalid authentication was provided."""


_LOGGER.debug(
    "ConfigFlow import OK; class=%s, class.domain=%s, const.DOMAIN=%s, class_id=%s",
    ConfigFlow.__name__,
    getattr(ConfigFlow, "domain", None),
    DOMAIN,
    id(ConfigFlow),
)
