# custom_components/googlefindmy/config_flow.py
"""Config flow for the Google Find My Device custom integration.

This module implements the complete configuration and options flows for the
integration, following Home Assistant best practices:

Key design decisions (Best Practice):
- Test-before-configure: We validate credentials *before* creating a config entry.
  If validation fails, no entry is created, the form is shown again with an error.
- Early unique_id: We set the config entry unique ID (normalized Google email)
  as soon as it is known, to avoid duplicate flows and duplicate entries.
- No persistence during the flow: We never write tokens/secrets to disk before
  `async_create_entry`. All flow-time validation uses ephemeral clients only.
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
- No secrets are persisted before `async_create_entry`.
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
import re
import sys
import time
from collections.abc import Awaitable, Callable, Mapping
from collections.abc import Iterable as CollIterable
from collections.abc import Mapping as CollMapping
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from types import MappingProxyType, ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
)

import voluptuous as vol
from homeassistant import config_entries, data_entry_flow
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_call_later

try:
    from homeassistant.config_entries import ConfigEntry, OperationNotAllowed
except ImportError:  # Pre-2025.5 HA builds do not expose the helper.
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
    DEFAULT_CONTRIBUTOR_MODE,
    DEFAULT_DELETE_CACHES_ON_REMOVE,
    DEFAULT_DEVICE_POLL_DELAY,
    DEFAULT_ENABLE_STATS_ENTITIES,
    # Defaults
    DEFAULT_LOCATION_POLL_INTERVAL,
    DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
    DEFAULT_OPTIONS,
    DEFAULT_SEMANTIC_DETECTION_RADIUS,
    DEFAULT_SHOW_LOCATION_AGE,
    DEFAULT_STALE_THRESHOLD,
    # Core domain & credential keys
    DOMAIN,
    OPT_CONTRIBUTOR_MODE,
    OPT_DELETE_CACHES_ON_REMOVE,
    OPT_DEVICE_POLL_DELAY,
    OPT_ENABLE_STATS_ENTITIES,
    OPT_IGNORED_DEVICES,
    # Options (non-secret runtime settings)
    OPT_LOCATION_POLL_INTERVAL,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,
    OPT_OPTIONS_SCHEMA_VERSION,
    OPT_SEMANTIC_LOCATIONS,
    OPT_SHOW_LOCATION_AGE,
    OPT_STALE_THRESHOLD,
    OPTION_KEYS,
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
from .email import normalize_email, normalize_email_or_default, unique_account_id
from .integration_modules import (
    import_integration_api_module,
    import_integration_package,
)

_ResolveEntryEmailCallable = Callable[[ConfigEntry], tuple[str | None, str | None]]
_CoalesceCallable = Callable[
    [HomeAssistant, ConfigEntry],
    Awaitable[ConfigEntry | None],
]

_RESOLVE_ENTRY_EMAIL: _ResolveEntryEmailCallable | None = None
_COALESCE_ENTRIES: _CoalesceCallable | None = None

if TYPE_CHECKING:
    from .api import GoogleFindMyAPI


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

if _discovery_flow_helper is None:  # pragma: no cover - legacy fallback

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

        try:
            from homeassistant.helpers.discovery_flow import (
                async_create_flow as _async_create_flow,
            )
        except Exception:  # noqa: BLE001
            flow_manager = cast(
                "ConfigEntriesFlowManager",
                getattr(hass.config_entries, "flow"),
            )
            init = getattr(flow_manager, "async_init", None)
            if not callable(init):
                return cast(
                    FlowResult,
                    {
                        "type": data_entry_flow.FlowResultType.ABORT,
                        "reason": "unknown",
                    },
                )
            try:
                init_result = await init(
                    domain,
                    context=context,
                    data=data,
                )
            except Exception:
                _LOGGER.error(
                    "Legacy discovery flow init failed (domain=%s, context=%s)",
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
                _LOGGER.error(
                    "Legacy discovery flow init returned None (domain=%s, context=%s)",
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

        create_flow: Callable[..., Awaitable[FlowResult] | FlowResult] = (
            _async_create_flow
        )
        if not callable(create_flow):
            _LOGGER.error(
                "Discovery flow helper 'async_create_flow' is not callable (domain=%s, context=%s)",
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

        try:
            result = create_flow(
                hass,
                domain,
                context,
                data,
                discovery_key=discovery_key,
            )
            result = await _resolve_flow_result(result)
        except Exception:
            _LOGGER.error(
                "Discovery flow creation failed (domain=%s, context=%s)",
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
        if result is None:
            _LOGGER.debug(
                "Discovery flow already in progress or skipped (domain=%s, context=%s)",
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
                _LOGGER.debug(
                    "Discovery flow helper returned None (domain=%s, context=%s) — treating as already in progress",
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
        if fallback_result is not None:
            return cast(FlowResult, fallback_result)
        _LOGGER.debug(
            "Fallback discovery flow helper returned None (domain=%s, context=%s) — treating as already in progress",
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

    def async_update_reload_and_abort(self, **kwargs: Any) -> FlowResult: ...

    def _abort_if_unique_id_configured(
        self, *, updates: Mapping[str, Any] | None = None
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


if hasattr(config_entries, "OptionsFlowWithReload"):
    OptionsFlowBase = cast(
        type[config_entries.OptionsFlow],
        getattr(config_entries, "OptionsFlowWithReload"),
    )
else:
    OptionsFlowBase = cast(type[config_entries.OptionsFlow], config_entries.OptionsFlow)


@dataclass(slots=True)
class _SubentryOption:
    """Lightweight representation of a selectable subentry."""

    key: str
    label: str
    subentry: ConfigSubentry | None
    visible_device_ids: tuple[str, ...]

    @property
    def subentry_id(self) -> str | None:
        """Return the backing Home Assistant subentry identifier when available."""

        if self.subentry is None:
            return None
        return getattr(self.subentry, "subentry_id", None)


_FIELD_SUBENTRY = "subentry"
_FIELD_REPAIR_TARGET = "target_subentry"
_FIELD_REPAIR_DELETE = "delete_subentry"
_FIELD_REPAIR_FALLBACK = "fallback_subentry"
_FIELD_VISIBILITY_HUB = "hub"
# Field identifiers used in options/visibility flows
_FIELD_REPAIR_DEVICES = "device_ids"

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


def _extract_fcm_credentials_from_secrets(
    data: dict[str, Any],
) -> dict[str, Any] | None:
    """Extract fcm_credentials from secrets.json if present."""

    try:
        fcm_creds = data.get("fcm_credentials")
        if isinstance(fcm_creds, dict):
            return fcm_creds
    except Exception:  # noqa: BLE001
        pass
    return None


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
                    "token_source": source,
                    "email": _mask_email_for_logs(email),
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
                        "token_source": source,
                        "email": _mask_email_for_logs(email),
                    },
                )
                return token
            key = _map_api_exc_to_error_key(err)
            _LOGGER.debug(
                "Token probe failed; mapped error key.",
                extra={
                    "token_source": source,
                    "error_key": key,
                    "email": _mask_email_for_logs(email),
                },
                exc_info=err,
            )
            continue
    return None


def _cand_labels(candidates: list[tuple[str, str]]) -> str:
    """Return a redacted, human-readable list of token candidate sources."""
    sources = {source for source, _token in candidates if source}
    if not sources:
        return "none"
    return ", ".join(sorted(sources))


def _log_token_validation_failure(
    *, email: str, candidates: list[tuple[str, str]]
) -> None:
    """Log a sanitized token validation failure with candidate metadata."""

    _LOGGER.warning(
        "Token validation failed; no working tokens among candidates. Please re-enter your credentials to refresh expired tokens.",
        extra={
            "email": _mask_email_for_logs(email),
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
        except (json.JSONDecodeError, TypeError):
            return "secrets", None, None, "invalid_json"

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
        except (json.JSONDecodeError, TypeError):
            return None, None, "invalid_json"

        email = _extract_email_from_secrets(parsed)
        candidates = _extract_oauth_candidates_from_secrets(parsed)
        if not (email and _email_valid(email) and candidates):
            return None, None, "invalid_token"
        return "secrets", parsed, None

    # Manual token path disabled: broken manual reauth entry remains hidden until fixed.
    # if not (
    #     _token_plausible(token_raw) and not _disqualifies_for_persistence(token_raw)
    # ):
    #     return None, None, "invalid_token"

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


def _discovery_payload_equivalent(
    first: CloudDiscoveryData, second: CloudDiscoveryData
) -> bool:
    """Return True when two normalized discovery payloads are equivalent."""

    if first.unique_id != second.unique_id or first.email != second.email:
        return False

    if first.candidates != second.candidates:
        return False

    if first.secrets_bundle is None or second.secrets_bundle is None:
        return first.secrets_bundle is None and second.secrets_bundle is None

    return dict(first.secrets_bundle) == dict(second.secrets_bundle)


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
        secrets_dict = dict(secrets_raw)
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

    return CloudDiscoveryData(
        email=email_candidate,
        unique_id=unique_id,
        candidates=tuple(candidates),
        secrets_bundle=secrets_bundle,
        title=str(title) if isinstance(title, str) else None,
    )


async def _ingest_discovery_credentials(
    flow: ConfigFlow,
    discovery: CloudDiscoveryData,
    *,
    existing_entry: ConfigEntry | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Validate discovery credentials and prepare flow + entry payloads."""

    candidates = list(discovery.candidates)
    secrets_bundle = (
        dict(discovery.secrets_bundle) if discovery.secrets_bundle is not None else None
    )
    fcm_credentials = (
        _extract_fcm_credentials_from_secrets(secrets_bundle)
        if secrets_bundle is not None
        else None
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
    if fcm_credentials is not None:
        auth_data["fcm_credentials"] = fcm_credentials

    if isinstance(to_persist, str) and to_persist.startswith("aas_et/"):
        auth_data[DATA_AAS_TOKEN] = to_persist
    else:
        auth_data.pop(DATA_AAS_TOKEN, None)

    if existing_entry is not None:
        updated = {**existing_entry.data, **auth_data}
        if not secrets_bundle:
            updated.pop(DATA_SECRET_BUNDLE, None)
        if not (isinstance(to_persist, str) and to_persist.startswith("aas_et/")):
            updated.pop(DATA_AAS_TOKEN, None)
        if fcm_credentials is not None:
            updated["fcm_credentials"] = fcm_credentials
        updates: dict[str, Any] | None = {"data": updated}
    else:
        updates = None

    return auth_data, updates


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
    domain=DOMAIN,
):
    """Handle the initial config flow for Google Find My Device."""

    domain: ClassVar[str] = DOMAIN
    VERSION = CONFIG_ENTRY_VERSION

    def __init__(self) -> None:
        """Initialize transient flow state."""
        self._auth_data: dict[str, Any] = {}
        self._available_devices: list[tuple[str, str]] = []
        self._subentry_key_core_tracking = TRACKER_SUBENTRY_KEY
        self._subentry_key_service = SERVICE_SUBENTRY_KEY
        self._pending_discovery_payload: CloudDiscoveryData | None = None
        self._pending_discovery_updates: dict[str, Any] | None = None
        self._pending_discovery_existing_entry: ConfigEntry | None = None
        self._discovery_confirm_pending = False

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
            # Guard source lookup so discovery-update stubs without `.source`
            # keep the Home Assistant contract intact.
            if getattr(entry, "source", None) != config_entries.SOURCE_IGNORE
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
            self._abort_if_unique_id_configured(updates=updates)
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
            "fcm_credentials",
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

        self._discovery_confirm_pending = False
        self._pending_discovery_payload = None
        self._pending_discovery_updates = None
        self._pending_discovery_existing_entry = None
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
        """Handle cloud-triggered discovery payloads."""

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
            "discovery: context_source=%s, pending_confirm=%s, payload_keys=%s",
            context_source,
            getattr(self, "_discovery_confirm_pending", False),
            payload_keys,
        )

        if _is_discovery_update_info(context_obj):
            _LOGGER.info(
                "Routing discovery payload to discovery-update-info handler "
                "(context_source=%s)",
                context_source,
            )
            return await self.async_step_discovery_update_info(discovery_info)

        if self._discovery_confirm_pending:
            pending_payload = self._pending_discovery_payload
            is_submission = not discovery_info
            if (
                not is_submission
                and isinstance(discovery_info, Mapping)
                and pending_payload is not None
            ):
                try:
                    normalized_candidate = _normalize_and_validate_discovery_payload(
                        discovery_info
                    )
                except Exception:  # noqa: BLE001
                    is_submission = False
                else:
                    is_submission = _discovery_payload_equivalent(
                        normalized_candidate, pending_payload
                    )

            if not is_submission:
                self._clear_discovery_confirmation_state()
            else:
                updates = self._pending_discovery_updates
                existing_entry = self._pending_discovery_existing_entry
                self._clear_discovery_confirmation_state()

                if updates is not None and pending_payload is not None:
                    try:
                        await self._async_prepare_account_context(
                            email=pending_payload.email,
                            preferred_unique_id=pending_payload.unique_id,
                            updates=updates,
                        )
                    except data_entry_flow.AbortFlow:
                        return self.async_abort(reason="already_configured")
                    return self.async_abort(reason="already_configured")

                return await self.async_step_device_selection()

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
        self._pending_discovery_updates = updates
        self._pending_discovery_existing_entry = existing_entry
        self._discovery_confirm_pending = True
        self._set_confirm_only()
        return self.async_show_form(
            step_id="discovery",
            description_placeholders=placeholders,
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
            updates = {"data": dict(existing_entry.data)}

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
            update_payload: dict[str, Any] = {}
            if "data" in updates_to_apply:
                update_payload["data"] = updates_to_apply["data"]
            if "options" in updates_to_apply:
                update_payload["options"] = updates_to_apply["options"]

            try:
                hass.config_entries.async_update_entry(existing_entry, **update_payload)
            except TypeError:  # Legacy cores without options support
                hass.config_entries.async_update_entry(
                    existing_entry,
                    data=update_payload.get("data", existing_entry.data),
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

            reload_task = hass.config_entries.async_reload(existing_entry.entry_id)
            if inspect.isawaitable(reload_task):
                reload_coro = reload_task

                async def _reload_and_normalize() -> None:
                    try:
                        await reload_coro
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
                        loop.create_task(_reload_and_normalize())
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

        _LOGGER.debug("User step: presenting auth method selection form.")
        return self.async_show_form(step_id="user", data_schema=STEP_USER_DATA_SCHEMA)

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
                    parsed_secrets = parsed_candidate
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
                        _log_token_validation_failure(email=email, candidates=cands)
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
                            self._auth_data[DATA_SECRET_BUNDLE] = parsed_secrets
                            fcm_credentials = _extract_fcm_credentials_from_secrets(
                                parsed_secrets
                            )
                            if fcm_credentials is not None:
                                self._auth_data["fcm_credentials"] = fcm_credentials
                        if isinstance(to_persist, str) and to_persist.startswith(
                            "aas_et/"
                        ):
                            self._auth_data[DATA_AAS_TOKEN] = to_persist
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
                        _log_token_validation_failure(email=email, candidates=cands)
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
            fcm_credentials = self._auth_data.get("fcm_credentials")
            if isinstance(fcm_credentials, Mapping):
                data_payload["fcm_credentials"] = dict(fcm_credentials)
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
                return create_entry(
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
                return create_entry(
                    title=self._auth_data.get(CONF_GOOGLE_EMAIL)
                    or "Google Find My Device",
                    data=shadow,
                )

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

        def _schedule_reload_via_manager(reason: str) -> None:
            schedule_reload = getattr(
                self.hass.config_entries, "async_schedule_reload", None
            )
            if not callable(schedule_reload):
                _LOGGER.debug(
                    "Reload after reconfigure (%s) not scheduled for entry %s; helper missing",
                    reason,
                    entry_for_update.entry_id,
                )
                return

            try:
                schedule_reload(entry_for_update.entry_id)
            except Exception:  # noqa: BLE001 - surface scheduler failures
                _LOGGER.exception(
                    "Failed to schedule reload after reconfigure (%s) for entry %s",
                    reason,
                    entry_for_update.entry_id,
                )

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

        def _log_task_result(task: asyncio.Future[Any]) -> None:
            try:
                task_result = task.result()
            except Exception:  # noqa: BLE001 - log unexpected task failures
                _LOGGER.exception(
                    "Deferred reload after reconfigure for entry %s raised an exception",
                    entry_for_update.entry_id,
                )
                return

            _log_failed_reload(task_result, deferred=True)

            if task_result is False:
                _schedule_reload_via_manager("reload_returned_false_deferred_task")

        async def _async_call_reload() -> Any:
            reload_result = self.hass.config_entries.async_reload(
                entry_for_update.entry_id
            )
            if inspect.isawaitable(reload_result):
                reload_result = await reload_result

            return reload_result

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
                    return
                except Exception:  # noqa: BLE001 - logged for visibility
                    _LOGGER.exception(
                        "Deferred reload after reconfigure for entry %s failed",
                        entry_for_update.entry_id,
                    )
                    return

                if inspect.isawaitable(reload_result_inner):
                    create_task = getattr(self.hass, "async_create_task", None)
                    if callable(create_task):
                        task = create_task(reload_result_inner)
                        if hasattr(task, "add_done_callback"):
                            task.add_done_callback(_log_task_result)
                        return

                    loop = getattr(self.hass, "loop", None)
                    if loop is not None:
                        task = loop.create_task(reload_result_inner)
                        if hasattr(task, "add_done_callback"):
                            task.add_done_callback(_log_task_result)
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
                    return

                _log_failed_reload(reload_result_inner, deferred=True)

                if reload_result_inner is False:
                    _schedule_reload_via_manager("reload_returned_false_deferred")

            async_call_later(self.hass, 0, _schedule_reload)
            return

        _log_failed_reload(reload_result, deferred=False)

        if reload_result is False:
            _schedule_reload_via_manager("reload_returned_false")

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
                    # vol.Optional(_REAUTH_FIELD_TOKEN): str,  # Disabled: manual reauth token path is intentionally hidden until fixed.
                }
            )
        else:
            schema = vol.Schema(
                {
                    vol.Optional(_REAUTH_FIELD_SECRETS): str,
                    # vol.Optional(_REAUTH_FIELD_TOKEN): str,  # Disabled: manual reauth token path is intentionally hidden until fixed.
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
                    if method == "manual":
                        token = str(payload)
                        try:
                            chosen = await async_pick_working_token(
                                self.hass,
                                fixed_email,
                                [("manual", token)],
                            )
                        except (DependencyNotReady, ImportError) as exc:
                            _register_dependency_error(errors, exc)
                        else:
                            if not chosen:
                                _log_token_validation_failure(
                                    email=fixed_email,
                                    candidates=[("manual", token)],
                                )
                                errors["base"] = "cannot_connect"
                            else:
                                if _disqualifies_for_persistence(chosen):
                                    _LOGGER.warning(
                                        "Reauth: token looks like a JWT; persisting anyway due to validation."
                                    )
                                updated_data = {
                                    **entry.data,
                                    DATA_AUTH_METHOD: _AUTH_METHOD_INDIVIDUAL,
                                    CONF_OAUTH_TOKEN: chosen,
                                }
                                if isinstance(chosen, str) and chosen.startswith(
                                    "aas_et/"
                                ):
                                    updated_data[DATA_AAS_TOKEN] = chosen
                                else:
                                    updated_data.pop(DATA_AAS_TOKEN, None)
                                updated_data.pop(DATA_SECRET_BUNDLE, None)
                                await self._async_clear_cached_aas_token(entry)
                                success_reason = self.context.get(
                                    "reauth_success_reason_override",
                                    "reauth_successful",
                                )
                                return self.async_update_reload_and_abort(
                                    entry=entry,
                                    data=updated_data,
                                    reason=success_reason,
                                )

                    elif method == "secrets":
                        if not isinstance(payload, Mapping):
                            errors["base"] = "invalid_token"
                        else:
                            parsed: dict[str, Any] = dict(payload)
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
                            else:
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
                                        _log_token_validation_failure(
                                            email=fixed_email, candidates=cands
                                        )
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
                                            CONF_OAUTH_TOKEN: to_persist,
                                            DATA_SECRET_BUNDLE: parsed,
                                        }
                                        fcm_credentials = (
                                            _extract_fcm_credentials_from_secrets(
                                                parsed
                                            )
                                        )
                                        if fcm_credentials is not None:
                                            updated_data["fcm_credentials"] = (
                                                fcm_credentials
                                            )
                                        if isinstance(
                                            to_persist, str
                                        ) and to_persist.startswith("aas_et/"):
                                            updated_data[DATA_AAS_TOKEN] = to_persist
                                        elif DATA_AAS_TOKEN in updated_data:
                                            updated_data.pop(DATA_AAS_TOKEN, None)
                                        await self._async_clear_cached_aas_token(entry)
                                        success_reason = self.context.get(
                                            "reauth_success_reason_override",
                                            "reauth_successful",
                                        )
                                        return self.async_update_reload_and_abort(
                                            entry=entry,
                                            data=updated_data,
                                            reason=success_reason,
                                        )
                except Exception as err2:  # noqa: BLE001
                    if _is_multi_entry_guard_error(err2):
                        # Defer: accept first candidate and reload
                        if method == "manual":
                            manual_token = str(payload)
                            updated_data = {
                                **entry.data,
                                DATA_AUTH_METHOD: _AUTH_METHOD_INDIVIDUAL,
                                CONF_OAUTH_TOKEN: manual_token,
                            }
                            if manual_token.startswith("aas_et/"):
                                updated_data[DATA_AAS_TOKEN] = manual_token
                            else:
                                updated_data.pop(DATA_AAS_TOKEN, None)
                            updated_data.pop(DATA_SECRET_BUNDLE, None)
                            await self._async_clear_cached_aas_token(entry)
                            return self.async_update_reload_and_abort(
                                entry=entry,
                                data=updated_data,
                                reason="reauth_successful",
                            )
                        if method == "secrets":
                            if not isinstance(payload, Mapping):
                                errors["base"] = "invalid_token"
                            else:
                                parsed = dict(payload)
                                cands = _extract_oauth_candidates_from_secrets(parsed)
                                token_first = cands[0][1] if cands else ""
                                updated_data = {
                                    **entry.data,
                                    DATA_AUTH_METHOD: _AUTH_METHOD_SECRETS,
                                    CONF_OAUTH_TOKEN: token_first,
                                    DATA_SECRET_BUNDLE: parsed,
                                }
                                fcm_credentials = _extract_fcm_credentials_from_secrets(
                                    parsed
                                )
                                if fcm_credentials is not None:
                                    updated_data["fcm_credentials"] = fcm_credentials
                                if isinstance(
                                    token_first, str
                                ) and token_first.startswith("aas_et/"):
                                    updated_data[DATA_AAS_TOKEN] = token_first
                                else:
                                    updated_data.pop(DATA_AAS_TOKEN, None)
                                await self._async_clear_cached_aas_token(entry)
                                return self.async_update_reload_and_abort(
                                    entry=entry,
                                    data=updated_data,
                                    reason="reauth_successful",
                                )
                    errors["base"] = _map_api_exc_to_error_key(err2)

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=schema,
            errors=errors,
            description_placeholders={"email": fixed_email},
        )

    async def _async_clear_cached_aas_token(self, entry: ConfigEntry) -> None:
        """Best-effort removal of the cached AAS token for a manual reauth entry."""

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
                "TokenCache for entry '%s' is closed; returning None to allow self-healing",
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
        """Ensure the service device metadata reflects the latest subentry mapping."""

        if hass is None or entry is None:
            return

        dev_reg = dr.async_get(hass)
        if not hasattr(dev_reg, "async_update_device"):
            return

        identifiers: set[tuple[str, str]] = {service_device_identifier(entry.entry_id)}
        if service_config_subentry_id is not None:
            identifiers.add(
                (DOMAIN, f"{entry.entry_id}:{service_config_subentry_id}:service")
            )

        get_device = getattr(dev_reg, "async_get_device", None)
        device: Any | None = None
        if callable(get_device):
            try:
                device = get_device(identifiers=identifiers)
            except TypeError:
                try:
                    device = get_device(identifiers)
                except TypeError:  # pragma: no cover - defensive guard
                    device = None

        if device is None:
            return

        device_id = getattr(device, "id", None) or getattr(device, "device_id", None)
        if device_id is None:
            return

        update_kwargs: dict[str, Any] = {
            "device_id": device_id,
            "config_subentry_id": service_config_subentry_id,
        }
        if service_config_subentry_id is not None:
            update_kwargs["add_config_entry_id"] = entry.entry_id

        call_api = getattr(coordinator, "_call_device_registry_api", None)
        if callable(call_api):
            try:
                call_api(dev_reg.async_update_device, base_kwargs=update_kwargs)
                return
            except Exception as err:  # noqa: BLE001 - defensive guard
                _LOGGER.debug(
                    "Service device binding via coordinator helper failed: %s", err
                )

        try:
            dev_reg.async_update_device(**update_kwargs)
        except TypeError as err:
            err_str = str(err)
            needs_fallback = False
            if (
                "add_config_entry_id" in update_kwargs
                and "add_config_entry_id" in err_str
            ):
                needs_fallback = True
            if (
                "add_config_subentry_id" in update_kwargs
                and "add_config_subentry_id" in err_str
            ):
                needs_fallback = True

            if not needs_fallback:
                raise

            fallback_kwargs = dict(update_kwargs)
            if "add_config_entry_id" in fallback_kwargs:
                fallback_kwargs["config_entry_id"] = fallback_kwargs.pop(
                    "add_config_entry_id"
                )
            if "add_config_subentry_id" in fallback_kwargs:
                fallback_kwargs["config_subentry_id"] = fallback_kwargs.pop(
                    "add_config_subentry_id"
                )

            _LOGGER.debug(
                "Retrying direct device registry update with legacy kwargs after %s",
                err,
            )
            dev_reg.async_update_device(**fallback_kwargs)

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
            existing_id = context_map.get(key)
            subentry_obj: ConfigSubentry | None = None
            if isinstance(existing_id, str):
                subentry_obj = entry.subentries.get(existing_id)
            if subentry_obj is None:
                for candidate in entry.subentries.values():
                    if candidate.data.get("group_key") == key:
                        subentry_obj = candidate
                        break
            return subentry_obj

        def _existing_visible(subentry_obj: ConfigSubentry | None) -> tuple[str, ...]:
            if subentry_obj is None:
                return ()
            data = getattr(subentry_obj, "data", {}) or {}
            raw_visible = data.get("visible_device_ids")
            if isinstance(raw_visible, (list, tuple, set)):
                return tuple(_normalize_visible_ids(raw_visible))
            return ()

        tracker_subentry = _resolve_existing(tracker_key)
        tracker_visible = _existing_visible(tracker_subentry)
        if not tracker_visible and self._available_devices:
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
            if isinstance(group_key, str) and group_key in allowed_keys:
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
        candidate = getattr(self, "subentry", None)
        if isinstance(candidate, ConfigSubentry):
            return candidate
        for subentry in getattr(self.config_entry, "subentries", {}).values():
            if subentry.data.get("group_key") == self._group_key:
                return subentry
        return None

    def _current_options_payload(self) -> dict[str, Any]:
        payload = dict(getattr(self.config_entry, "options", {}))
        for key in (
            OPT_MAP_VIEW_TOKEN_EXPIRATION,
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
class OptionsFlowHandler(OptionsFlowBase, _OptionsFlowMixin):  # type: ignore[misc, valid-type]
    """Options flow to update non-secret settings and optionally refresh credentials.

    Notes:
        - Device inclusion/exclusion is controlled by HA's device enable/disable.
          We no longer present a `tracked_devices` multi-select here.
        - Returning `async_create_entry` with the new options triggers a reload
          automatically when using `OptionsFlowWithReload` (if available).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the options flow handler."""

        super().__init__(*args, **kwargs)
        self._semantic_location_editing: str | None = None

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
        seen_keys: set[str] = set()

        subentries = getattr(entry, "subentries", None)
        if isinstance(subentries, dict):
            for subentry in subentries.values():
                data = dict(getattr(subentry, "data", {}) or {})
                raw_key = data.get("group_key")
                if isinstance(raw_key, str) and raw_key.strip():
                    key = raw_key.strip()
                else:
                    key = str(getattr(subentry, "subentry_id", "core_tracking"))
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
                    )
                )
                seen_keys.add(key)

        if not options:
            title = getattr(entry, "title", None) or "Core tracking"
            options.append(
                _SubentryOption(
                    key="core_tracking",
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

    @staticmethod
    def _default_subentry_key(choices: dict[str, str]) -> str:
        """Return the default subentry key for UI defaults."""

        if "core_tracking" in choices:
            return "core_tracking"
        return next(iter(choices), "core_tracking")

    async def _async_update_feature_group_subentry(
        self,
        entry: ConfigEntry,
        subentry_option: _SubentryOption,
        options_payload: Mapping[str, Any],
    ) -> None:
        """Update feature group metadata on the selected subentry."""

        subentry = subentry_option.subentry
        if subentry is None:
            return

        data = dict(getattr(subentry, "data", {}) or {})
        data.setdefault("group_key", subentry_option.key)

        raw_flags = data.get("feature_flags")
        if isinstance(raw_flags, Mapping):
            feature_flags = {str(key): raw_flags[key] for key in raw_flags}
        else:
            feature_flags = {}

        if OPT_ENABLE_STATS_ENTITIES is not None:
            if OPT_ENABLE_STATS_ENTITIES in options_payload:
                feature_flags[OPT_ENABLE_STATS_ENTITIES] = bool(
                    options_payload[OPT_ENABLE_STATS_ENTITIES]
                )
        if OPT_MAP_VIEW_TOKEN_EXPIRATION in options_payload:
            feature_flags[OPT_MAP_VIEW_TOKEN_EXPIRATION] = bool(
                options_payload[OPT_MAP_VIEW_TOKEN_EXPIRATION]
            )
        if OPT_GOOGLE_HOME_FILTER_ENABLED is not None and (
            OPT_GOOGLE_HOME_FILTER_ENABLED in options_payload
        ):
            feature_flags[OPT_GOOGLE_HOME_FILTER_ENABLED] = bool(
                options_payload[OPT_GOOGLE_HOME_FILTER_ENABLED]
            )
            data["has_google_home_filter"] = bool(
                options_payload[OPT_GOOGLE_HOME_FILTER_ENABLED]
            )
        if OPT_CONTRIBUTOR_MODE in options_payload:
            feature_flags[OPT_CONTRIBUTOR_MODE] = options_payload[OPT_CONTRIBUTOR_MODE]

        if feature_flags:
            data["feature_flags"] = feature_flags

        if "entry_title" in data or getattr(entry, "title", None):
            data["entry_title"] = getattr(entry, "title", None) or data.get(
                "entry_title"
            )

        update_helper = cast(
            Callable[..., Awaitable[None] | None], ConfigFlow._async_update_subentry
        )
        result = update_helper(
            self,
            entry,
            subentry,
            data=data,
            title=getattr(subentry, "title", None) or data.get("entry_title"),
            unique_id=getattr(subentry, "unique_id", None),
        )
        if inspect.isawaitable(result):
            await result

    async def _async_refresh_subentry_entry_title(
        self, entry: ConfigEntry, subentry_option: _SubentryOption
    ) -> None:
        """Ensure the subentry reflects the current entry title."""

        subentry = subentry_option.subentry
        if subentry is None:
            return

        data = dict(getattr(subentry, "data", {}) or {})
        new_entry_title = getattr(entry, "title", None)
        if not new_entry_title:
            return
        group_key = data.get("group_key") or subentry_option.key
        current_title = getattr(subentry, "title", None)
        default_title = _DEFAULT_SUBENTRY_TITLES.get(group_key)
        target_title = (
            current_title if default_title is not None else None
        ) or default_title
        if target_title is None:
            target_title = new_entry_title
        if (
            data.get("entry_title") == new_entry_title
            and getattr(subentry, "title", None) == target_title
        ):
            return
        data["entry_title"] = new_entry_title
        update_helper = cast(
            Callable[..., Awaitable[None] | None], ConfigFlow._async_update_subentry
        )
        result = update_helper(
            self,
            entry,
            subentry,
            data=data,
            title=target_title,
            unique_id=getattr(subentry, "unique_id", None),
        )
        if inspect.isawaitable(result):
            await result

    async def _async_assign_devices_to_subentry(
        self, entry: ConfigEntry, target_key: str, device_ids: list[str]
    ) -> set[str]:
        """Assign devices to the target subentry while removing from others."""

        if not device_ids:
            return set()

        changed: set[str] = set()
        options = self._gather_subentry_options()

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
            if option.key == target_key:
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
        self.hass.async_create_task(
            self.hass.config_entries.async_reload(self.config_entry.entry_id)
        )
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
        self.hass.async_create_task(
            self.hass.config_entries.async_reload(self.config_entry.entry_id)
        )
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
        errors: dict[str, str] = {}

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
            OPT_DELETE_CACHES_ON_REMOVE: _get(
                OPT_DELETE_CACHES_ON_REMOVE, DEFAULT_DELETE_CACHES_ON_REMOVE
            ),
            OPT_CONTRIBUTOR_MODE: _get(OPT_CONTRIBUTOR_MODE, DEFAULT_CONTRIBUTOR_MODE),
            OPT_STALE_THRESHOLD: _get(OPT_STALE_THRESHOLD, DEFAULT_STALE_THRESHOLD),
            OPT_SHOW_LOCATION_AGE: _get(OPT_SHOW_LOCATION_AGE, DEFAULT_SHOW_LOCATION_AGE),
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

        choices, option_map = self._subentry_choice_map()
        default_subentry = self._default_subentry_key(choices)

        fields: dict[Any, Any] = {
            vol.Required(_FIELD_SUBENTRY, default=default_subentry): vol.In(choices)
        }
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
                selector({
                    "select": {
                        "options": [CONTRIBUTOR_MODE_HIGH_TRAFFIC, CONTRIBUTOR_MODE_IN_ALL_AREAS],
                        "translation_key": "contributor_mode",
                    }
                }),
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

        base_schema = vol.Schema(fields)
        schema_with_defaults = self.add_suggested_values_to_schema(base_schema, current)

        if user_input is not None:
            selected_key = str(user_input.get(_FIELD_SUBENTRY, default_subentry))
            if selected_key not in choices:
                errors[_FIELD_SUBENTRY] = "invalid_subentry"
            else:
                new_options = dict(entry.options)
                for real_key in option_markers:
                    if real_key in user_input:
                        new_options[real_key] = user_input[real_key]
                    else:
                        new_options[real_key] = current.get(real_key)
                new_options[OPT_OPTIONS_SCHEMA_VERSION] = 2

                subentry_option = option_map.get(selected_key)
                if subentry_option is not None:
                    await self._async_update_feature_group_subentry(
                        entry, subentry_option, new_options
                    )

                return self.async_create_entry(title="", data=new_options)

        return self.async_show_form(
            step_id="settings",
            data_schema=schema_with_defaults,
            errors=errors,
            description_placeholders=_SUBENTRY_PLACEHOLDERS,
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

        subentry_choices, _ = self._subentry_choice_map()
        default_subentry = self._default_subentry_key(subentry_choices)

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

        subentry_choices, _ = self._subentry_choice_map()
        if not subentry_choices:
            return self.async_abort(reason="repairs_no_subentries")

        default_subentry = self._default_subentry_key(subentry_choices)
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

            self.hass.async_create_task(
                self.hass.config_entries.async_reload(self.config_entry.entry_id)
            )
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

        subentry_choices, option_map = self._subentry_choice_map()
        removable_choices = {
            key: label
            for key, label in subentry_choices.items()
            if option_map[key].subentry
        }
        if not removable_choices or len(removable_choices) <= 1:
            return self.async_abort(reason="subentry_delete_invalid")

        schema = vol.Schema(
            {
                vol.Required(_FIELD_REPAIR_DELETE): vol.In(removable_choices),
                vol.Required(
                    _FIELD_REPAIR_FALLBACK,
                    default=self._default_subentry_key(subentry_choices),
                ): vol.In(subentry_choices),
            }
        )

        if user_input is not None:
            errors: dict[str, str] = {}
            target_key = str(user_input.get(_FIELD_REPAIR_DELETE, ""))
            fallback_key = str(user_input.get(_FIELD_REPAIR_FALLBACK, ""))

            if target_key not in removable_choices:
                errors[_FIELD_REPAIR_DELETE] = "invalid_subentry"
            if fallback_key not in subentry_choices or fallback_key == target_key:
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
                "fallback": subentry_choices[fallback_key],
                "count": str(len(devices)),
            }

            self.hass.async_create_task(
                self.hass.config_entries.async_reload(self.config_entry.entry_id)
            )
            return self.async_abort(
                reason="subentry_delete_success",
                description_placeholders=placeholders,
            )

        return self.async_show_form(step_id="repairs_delete", data_schema=schema)

    # ---------- Credentials refresh ----------
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

        subentry_choices, option_map = self._subentry_choice_map()
        default_subentry = self._default_subentry_key(subentry_choices)

        if selector is not None:
            schema = vol.Schema(
                {
                    vol.Required(_FIELD_SUBENTRY, default=default_subentry): vol.In(
                        subentry_choices
                    ),
                    vol.Optional("new_secrets_json"): selector(
                        {"text": {"multiline": True}}
                    ),
                    # vol.Optional("new_oauth_token"): str,  # Disabled: broken manual token path stays hidden until fixed.
                }
            )
        else:
            schema = vol.Schema(
                {
                    vol.Required(_FIELD_SUBENTRY, default=default_subentry): vol.In(
                        subentry_choices
                    ),
                    vol.Optional("new_secrets_json"): str,
                    # vol.Optional("new_oauth_token"): str,  # Disabled: broken manual token path stays hidden until fixed.
                }
            )

        if user_input is not None:
            selected_key = str(user_input.get(_FIELD_SUBENTRY, default_subentry))
            if selected_key not in subentry_choices:
                errors[_FIELD_SUBENTRY] = "invalid_subentry"
            else:
                new_token = (user_input.get("new_oauth_token") or "").strip()
                has_token = bool(new_token)
                has_secrets = bool((user_input.get("new_secrets_json") or "").strip())
                if not has_secrets and not has_token:
                    errors["base"] = "choose_one"
                else:
                    try:
                        entry = self.config_entry
                        email = entry.data.get(CONF_GOOGLE_EMAIL)
                        selected_option = option_map.get(selected_key)

                        async def _finalize_success(
                            updated_data: dict[str, Any],
                        ) -> FlowResult:
                            await self._async_clear_cached_aas_token(entry)
                            self.hass.config_entries.async_update_entry(
                                entry, data=updated_data
                            )
                            if selected_option is not None:
                                await self._async_refresh_subentry_entry_title(
                                    entry, selected_option
                                )
                            self.hass.async_create_task(
                                self.hass.config_entries.async_reload(entry.entry_id)
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
                                        email=email,
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
                            except Exception:
                                errors["new_secrets_json"] = "invalid_json"
                            else:
                                cands = _extract_oauth_candidates_from_secrets(parsed)
                                if not cands:
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
                                            _log_token_validation_failure(
                                                email=email, candidates=cands
                                            )
                                            errors["base"] = "cannot_connect"
                                        else:
                                            to_persist = chosen
                                            if _disqualifies_for_persistence(
                                                to_persist
                                            ):
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
                                                CONF_OAUTH_TOKEN: to_persist,
                                                DATA_SECRET_BUNDLE: parsed,
                                            }
                                            fcm_credentials = (
                                                _extract_fcm_credentials_from_secrets(
                                                    parsed
                                                )
                                            )
                                            if fcm_credentials is not None:
                                                updated_data["fcm_credentials"] = (
                                                    fcm_credentials
                                                )
                                            if isinstance(
                                                to_persist, str
                                            ) and to_persist.startswith("aas_et/"):
                                                updated_data[DATA_AAS_TOKEN] = (
                                                    to_persist
                                                )
                                            else:
                                                updated_data.pop(DATA_AAS_TOKEN, None)
                                            return await _finalize_success(updated_data)
                    except Exception as err2:  # noqa: BLE001
                        if _is_multi_entry_guard_error(err2):
                            entry = self.config_entry
                            parsed = json.loads(user_input["new_secrets_json"])
                            cands = _extract_oauth_candidates_from_secrets(parsed)
                            token_first = cands[0][1] if cands else ""
                            updated_data = {
                                **entry.data,
                                DATA_AUTH_METHOD: _AUTH_METHOD_SECRETS,
                                CONF_OAUTH_TOKEN: token_first,
                                DATA_SECRET_BUNDLE: parsed,
                            }
                            fcm_credentials = _extract_fcm_credentials_from_secrets(
                                parsed
                            )
                            if fcm_credentials is not None:
                                updated_data["fcm_credentials"] = fcm_credentials
                            if isinstance(token_first, str) and token_first.startswith(
                                "aas_et/"
                            ):
                                updated_data[DATA_AAS_TOKEN] = token_first
                            else:
                                updated_data.pop(DATA_AAS_TOKEN, None)
                            return await _finalize_success(updated_data)
                        errors["base"] = _map_api_exc_to_error_key(err2)

        return self.async_show_form(
            step_id="credentials",
            data_schema=schema,
            errors=errors,
            description_placeholders=_SUBENTRY_PLACEHOLDERS,
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
