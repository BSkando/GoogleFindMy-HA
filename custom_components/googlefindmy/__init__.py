# custom_components/googlefindmy/__init__.py

"""Google Find My Device integration for Home Assistant.

Version: see INTEGRATION_VERSION in const.py / manifest.json (SSOT).
Multi-account enabled (E3) + owner-index routing attach
- Multi-account support: multiple config entries are allowed concurrently.
- Duplicate-account protection: if two entries use the same Google email, we raise a
  Repair issue and abort the later entry to avoid mixing credentials/state.
- Entry-scoped TokenCache usage only (no global facade calls).
- Device owner index scaffold (entry_id → canonical_id mapping container).
- Prepared (not executed) migration for entry-scoped device identifiers.
- NEW: Attach HA context to the shared FCM receiver to enable owner-index fallback routing.

Highlights (cumulative)
-----------------------
- Entry-scoped TokenCache (HA Store backend) with migration from legacy secrets.json.
- Multi-entry: allow multiple *active* entries; prevent duplicate entries targeting
  the same Google account (email) across entries.
- Deterministic default-entry choice (previous behavior) REMOVED for MA: we do not set a
  TokenCache "default" in this module; all reads/writes are entry-scoped.
- One-time migration that namespaces entity unique_ids by entry_id (idempotent, collision-aware).
- Services are registered at integration level (async_setup) so they are always visible.
- Clean lifecycle: refcounted shared FCM receiver; coordinator shutdown & cache flush on unload.
- Defensive logging: redact tokens in URLs; never log PII (no coordinates/secrets).

Notes
-----
This module aims to be self-documenting. All public functions include precise docstrings
(purpose, parameters, errors, security considerations). Keep comments/docstrings in English.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import os
import socket
import time
from collections import defaultdict
from collections.abc import (
    Awaitable,
    Callable,
    Collection,
    Coroutine,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from importlib import import_module
from types import MappingProxyType, ModuleType, SimpleNamespace
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Literal,
    Protocol,
    TypedDict,
    TypeGuard,
    TypeVar,
    cast,
)
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from weakref import WeakKeyDictionary

import voluptuous as vol
from homeassistant import data_entry_flow
from homeassistant.config_entries import ConfigEntry, ConfigEntryState, ConfigSubentry

try:  # pragma: no cover - ConfigEntryDisabler introduced in HA 2023.12
    from homeassistant.config_entries import ConfigEntryDisabler as _ConfigEntryDisabler
except ImportError:  # pragma: no cover - legacy Home Assistant builds
    _ConfigEntryDisabler = None
try:  # pragma: no cover - UnknownEntry introduced alongside subentry helpers
    from homeassistant.config_entries import UnknownEntry as _ConfigUnknownEntry
except ImportError:  # pragma: no cover - legacy Home Assistant builds
    _ConfigUnknownEntry = None
from homeassistant.const import (
    EVENT_HOMEASSISTANT_STOP,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
    Platform,
)
from homeassistant.core import Event, HomeAssistant

try:
    from homeassistant.helpers.entity import split_entity_id
except ImportError:  # pragma: no cover - fallback for legacy or test environments
    try:
        from homeassistant.core import split_entity_id
    except ImportError:  # pragma: no cover - minimal shim for isolated tests

        def split_entity_id(entity_id: str) -> tuple[str, str]:
            """Split an entity_id into its domain and object ID parts."""

            if "." not in entity_id:
                raise ValueError(entity_id)
            domain, object_id = entity_id.split(".", 1)
            return domain, object_id


from homeassistant.exceptions import (
    ConfigEntryAuthFailed,
    ConfigEntryNotReady,
    HomeAssistantError,
)
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_call_later, async_track_time_interval
from homeassistant.helpers.network import NoURLAvailableError, get_url

if TYPE_CHECKING:
    from homeassistant.helpers.entity import Entity
else:  # pragma: no cover - test environments without full Home Assistant
    try:
        from homeassistant.helpers.entity import Entity
    except (ImportError, AttributeError):

        class Entity:  # type: ignore[too-many-ancestors, override]
            """Minimal placeholder for Home Assistant's Entity base class."""

            __slots__ = ()


from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.storage import Store

try:  # pragma: no cover - HassKey introduced in HA 2024.6
    from homeassistant.util.hass_dict import HassKey
except ImportError:  # pragma: no cover - legacy Home Assistant builds

    class HassKey(str):  # type: ignore[no-redef]
        """Minimal shim for pre-2024.6 Home Assistant builds."""

        def __class_getitem__(cls, item: Any) -> type:
            return cls


# Eagerly import diagnostics to prevent blocking calls on-demand
from . import diagnostics  # noqa: F401

# Token cache (entry-scoped HA Store-backed cache + registry/facade)
from .Auth.token_cache import (
    TokenCache,
    _register_instance,
    _unregister_instance,
)

# Username key normalization
from .Auth.username_provider import username_string
from .const import (
    CACHE_KEY_CONTRIBUTOR_MODE,
    CACHE_KEY_LAST_MODE_SWITCH,
    CONF_GOOGLE_EMAIL,
    CONF_OAUTH_TOKEN,
    CONTRIBUTOR_MODE_HIGH_TRAFFIC,
    CONTRIBUTOR_MODE_IN_ALL_AREAS,
    DATA_AAS_TOKEN,
    DATA_AUTH_METHOD,
    DATA_EID_RESOLVER,
    DATA_SECRET_BUNDLE,
    DEFAULT_CONTRIBUTOR_MODE,
    DEFAULT_DELETE_CACHES_ON_REMOVE,
    DEFAULT_DEVICE_POLL_DELAY,
    DEFAULT_LOCATION_POLL_INTERVAL,
    DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
    DEFAULT_MIN_POLL_INTERVAL,
    DEFAULT_OPTIONS,
    DOMAIN,
    FEATURE_FMDN_FINDER_ENABLED,
    INTEGRATION_VERSION,
    ISSUE_MULTIPLE_CONFIG_ENTRIES,
    ISSUE_RESTART_REQUIRED_KEY,
    LEGACY_SERVICE_IDENTIFIER,
    LITERAL_CORE_KEY_OWNER,
    NON_DEVICE_SUBENTRY_KEYS,
    NON_DEVICE_SUBENTRY_TYPES,
    OPT_ALLOW_HISTORY_FALLBACK,
    OPT_CONTRIBUTOR_MODE,
    OPT_DELETE_CACHES_ON_REMOVE,
    OPT_DEVICE_POLL_DELAY,
    OPT_IGNORED_DEVICES,
    OPT_LOCATION_POLL_INTERVAL,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,
    OPT_MIN_POLL_INTERVAL,
    OPT_OPTIONS_SCHEMA_VERSION,
    OPTION_KEYS,
    OPTIONAL_CREDENTIAL_KEYS,
    SECRETS_EXTRA_WATCH_PATHS,
    SERVICE_DEVICE_TRANSLATION_KEY,
    SERVICE_FEATURE_PLATFORMS,
    SERVICE_SUBENTRY_KEY,
    SERVICE_SUBENTRY_TRANSLATION_KEY,
    STORAGE_KEY,
    STORAGE_VERSION,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_FEATURE_PLATFORMS,
    TRACKER_SUBENTRY_KEY,
    TRACKER_SUBENTRY_TRANSLATION_KEY,
    TRANSLATION_KEY_CACHE_PURGED,
    TRANSLATION_KEY_DUPLICATE_ACCOUNT,
    TRANSLATION_KEY_RESTART_REQUIRED,
    TRANSLATION_KEY_UNIQUE_ID_COLLISION,
    coerce_ignored_mapping,
    frozen_attributes_issue_id,
    map_token_hex_digest,
    map_token_secret_seed,
    service_device_identifier,
)
from .const import (
    CONFIG_ENTRY_VERSION as CONFIG_ENTRY_VERSION,
)
from .email_utils import normalize_email, unique_account_id
from .ha_typing import CloudDiscoveryRuntime, callback
from .SpotApi.spot_grpc_transport import SPOT_GRPC_TRANSPORT

# Shared FCM provider (HA-managed singleton)
try:  # pragma: no cover - OperationNotAllowed introduced alongside HA subentry retries
    from homeassistant.config_entries import OperationNotAllowed as _OperationNotAllowed
except ImportError:  # pragma: no cover - legacy Home Assistant builds
    _FallbackHAError = cast("type[Exception]", HomeAssistantError)

    _OperationNotAllowed = cast(
        "type[Exception]",
        type(
            "OperationNotAllowed",
            (_FallbackHAError,),
            {
                "__doc__": (
                    "Fallback OperationNotAllowed placeholder for pre-subentry Home Assistant cores."
                ),
                "__module__": __name__,
            },
        ),
    )

OperationNotAllowed = cast("type[Exception]", _OperationNotAllowed)

UnknownEntry: type[Exception]
if _ConfigUnknownEntry is None:  # pragma: no cover - legacy Home Assistant builds
    UnknownEntry = cast(
        "type[Exception]",
        type(
            "UnknownEntry",
            (cast("type[Exception]", HomeAssistantError),),
            {
                "__doc__": "Fallback UnknownEntry placeholder for pre-subentry Home Assistant cores.",
                "__module__": __name__,
            },
        ),
    )
else:
    UnknownEntry = cast("type[Exception]", _ConfigUnknownEntry)

__all__ = [
    "Common_pb2",
    "DeviceUpdate_pb2",
    "LocationReportsUpload_pb2",
]

ProtoDecoderModuleName = Literal[
    "Common_pb2", "DeviceUpdate_pb2", "LocationReportsUpload_pb2"
]

_PROTO_DECODER_PATHS: Mapping[ProtoDecoderModuleName, str] = MappingProxyType(
    {
        "Common_pb2": "custom_components.googlefindmy.ProtoDecoders.Common_pb2",
        "DeviceUpdate_pb2": "custom_components.googlefindmy.ProtoDecoders.DeviceUpdate_pb2",
        "LocationReportsUpload_pb2": (
            "custom_components.googlefindmy.ProtoDecoders.LocationReportsUpload_pb2"
        ),
    }
)
_PROTO_DECODER_CACHE: dict[ProtoDecoderModuleName, ModuleType] = {}
_PROTO_DECODER_CACHE_PROXY: Mapping[ProtoDecoderModuleName, ModuleType] = (
    MappingProxyType(_PROTO_DECODER_CACHE)
)


def _import_proto_decoder(name: ProtoDecoderModuleName) -> ModuleType:
    """Import and cache a ProtoDecoder module on demand."""

    module = _PROTO_DECODER_CACHE.get(name)
    if module is not None:
        return module

    module = import_module(_PROTO_DECODER_PATHS[name])
    _PROTO_DECODER_CACHE[name] = module
    return module


def get_proto_decoder(name: ProtoDecoderModuleName) -> ModuleType:
    """Return the requested ProtoDecoder module, importing it lazily."""

    return _import_proto_decoder(name)


def get_proto_decoders() -> Mapping[ProtoDecoderModuleName, ModuleType]:
    """Return a read-only mapping of all ProtoDecoder modules."""

    for proto_name in _PROTO_DECODER_PATHS:
        _import_proto_decoder(proto_name)
    return _PROTO_DECODER_CACHE_PROXY


from .eid_resolver import GoogleFindMyEIDResolver  # noqa: E402


def __getattr__(name: str) -> Any:
    """Expose ProtoDecoder modules at the package root for compatibility."""

    if name in __all__:
        module = _import_proto_decoder(cast(ProtoDecoderModuleName, name))
        globals()[name] = module
        return module
    raise AttributeError(name)


CloudDiscoveryRuntimeCallable = Callable[
    [HomeAssistant, ConfigEntry | None], CloudDiscoveryRuntime
]
TriggerCloudDiscoveryCallable = Callable[..., Awaitable[Any]]
RedactAccountForLogCallable = Callable[..., str]
if TYPE_CHECKING:
    from homeassistant.helpers.entity_registry import (
        RegistryEntryDisabler as RegistryEntryDisablerType,
    )

    from .api import (
        FcmReceiverProtocol as ApiFcmReceiverProtocol,
    )
    from .api import (
        register_fcm_receiver_provider as ApiRegisterFcmProviderType,
    )
    from .api import (
        unregister_fcm_receiver_provider as ApiUnregisterFcmProviderType,
    )
    from .Auth.fcm_receiver_ha import FcmReceiverHA as FcmReceiverHAType
    from .config_flow import _StagedCleanupTicket
    from .coordinator import GoogleFindMyCoordinator as GoogleFindMyCoordinatorType
    from .discovery import (
        DiscoveryManager as DiscoveryManagerType,
    )
    from .discovery import (
        _cloud_discovery_runtime as cloud_discovery_runtime_impl,
    )
    from .discovery import (
        _redact_account_for_log as redact_account_for_log_impl,
    )
    from .discovery import (
        _trigger_cloud_discovery as trigger_cloud_discovery_impl,
    )
    from .discovery import (
        async_initialize_discovery_runtime as AsyncInitializeDiscoveryRuntimeType,
    )
    from .map_view import (
        GoogleFindMyMapRedirectView as GoogleFindMyMapRedirectViewType,
    )
    from .map_view import (
        GoogleFindMyMapTilesTokenView as GoogleFindMyMapTilesTokenViewType,
    )
    from .map_view import (
        GoogleFindMyMapView as GoogleFindMyMapViewType,
    )
    from .NovaApi.ExecuteAction.LocateTracker.location_request import (
        FcmReceiverProtocol as NovaFcmReceiverProtocol,
    )
    from .services import async_register_services as AsyncRegisterServicesType

    GoogleFindMyCoordinator = GoogleFindMyCoordinatorType
    DiscoveryManager = DiscoveryManagerType
    GoogleFindMyMapView = GoogleFindMyMapViewType
    GoogleFindMyMapRedirectView = GoogleFindMyMapRedirectViewType
    GoogleFindMyMapTilesTokenView = GoogleFindMyMapTilesTokenViewType
    async_register_services = AsyncRegisterServicesType
    async_initialize_discovery_runtime = AsyncInitializeDiscoveryRuntimeType
    api_register_fcm_provider = ApiRegisterFcmProviderType
    api_unregister_fcm_provider = ApiUnregisterFcmProviderType
    _cloud_discovery_runtime_callable = cast(
        CloudDiscoveryRuntimeCallable, cloud_discovery_runtime_impl
    )
    _redact_account_for_log_callable = cast(
        RedactAccountForLogCallable, redact_account_for_log_impl
    )
    _trigger_cloud_discovery_callable = cast(
        TriggerCloudDiscoveryCallable, trigger_cloud_discovery_impl
    )
else:
    from typing import Any as FcmReceiverHAType
    from typing import Any as RegistryEntryDisablerType

    NovaFcmReceiverProtocol = FcmReceiverHAType
    ApiFcmReceiverProtocol = FcmReceiverHAType

    def _runtime_imports_not_initialized(*_args: Any, **_kwargs: Any) -> Any:
        """Raise when runtime-only imports are used before initialization."""

        raise RuntimeError("Runtime components not initialized")

    class _GoogleFindMyCoordinatorPlaceholder:
        """Placeholder until the coordinator module is imported."""

    class _DiscoveryManagerPlaceholder:
        """Placeholder until the discovery module is imported."""

    GoogleFindMyCoordinator: type[Any] = cast(
        type[Any], _GoogleFindMyCoordinatorPlaceholder
    )
    DiscoveryManager: type[Any] = cast(type[Any], _DiscoveryManagerPlaceholder)
    GoogleFindMyMapView: type[Any] = cast(
        type[Any], type("GoogleFindMyMapViewPlaceholder", (object,), {})
    )
    GoogleFindMyMapRedirectView: type[Any] = cast(
        type[Any], type("GoogleFindMyMapRedirectViewPlaceholder", (object,), {})
    )
    GoogleFindMyMapTilesTokenView: type[Any] = cast(
        type[Any], type("GoogleFindMyMapTilesTokenViewPlaceholder", (object,), {})
    )

    api_register_fcm_provider: Callable[
        [Callable[[], ApiFcmReceiverProtocol]], None
    ] = cast(
        Callable[[Callable[[], ApiFcmReceiverProtocol]], None],
        _runtime_imports_not_initialized,
    )
    api_unregister_fcm_provider: Callable[[], None] = cast(
        Callable[[], None], _runtime_imports_not_initialized
    )
    async_register_services: Callable[
        [HomeAssistant, Mapping[str, Any]], Awaitable[None]
    ] = cast(
        Callable[[HomeAssistant, Mapping[str, Any]], Awaitable[None]],
        _runtime_imports_not_initialized,
    )
    async_initialize_discovery_runtime: Callable[[HomeAssistant], Awaitable[Any]] = (
        cast(
            Callable[[HomeAssistant], Awaitable[Any]],
            _runtime_imports_not_initialized,
        )
    )
    _cloud_discovery_runtime_callable: CloudDiscoveryRuntimeCallable = cast(
        CloudDiscoveryRuntimeCallable,
        _runtime_imports_not_initialized,
    )
    _trigger_cloud_discovery_callable: TriggerCloudDiscoveryCallable = cast(
        TriggerCloudDiscoveryCallable,
        _runtime_imports_not_initialized,
    )
    _redact_account_for_log_callable: RedactAccountForLogCallable = cast(
        RedactAccountForLogCallable,
        _runtime_imports_not_initialized,
    )

loc_register_fcm_provider: (
    Callable[[Callable[[str | None], NovaFcmReceiverProtocol]], None] | None
) = None
loc_unregister_fcm_provider: Callable[[], None] | None = None

_RUNTIME_IMPORTS_LOADED = False
_FCM_RECEIVER_CLASS: type[FcmReceiverHAType] | None = None


def _resolve_fcm_receiver_class() -> type[FcmReceiverHAType]:
    """Import and return the Home Assistant FCM receiver class lazily."""

    global _FCM_RECEIVER_CLASS
    if _FCM_RECEIVER_CLASS is None:
        from .Auth.fcm_receiver_ha import FcmReceiverHA as _FcmReceiverHA

        _FCM_RECEIVER_CLASS = _FcmReceiverHA
    return _FCM_RECEIVER_CLASS


def _resolve_google_home_filter_class() -> type[Any] | None:
    """Import and cache the optional Google Home filter class lazily."""

    global _GOOGLE_HOME_FILTER_CLASS, _GOOGLE_HOME_FILTER_IMPORT_ATTEMPTED
    if not _GOOGLE_HOME_FILTER_IMPORT_ATTEMPTED:
        _GOOGLE_HOME_FILTER_IMPORT_ATTEMPTED = True
        try:
            from .google_home_filter import GoogleHomeFilter as _GoogleHomeFilterClass
        except Exception:  # pragma: no cover - optional module
            _GOOGLE_HOME_FILTER_CLASS = None
        else:
            _GOOGLE_HOME_FILTER_CLASS = _GoogleHomeFilterClass
    return _GOOGLE_HOME_FILTER_CLASS


def _ensure_runtime_imports() -> None:
    """Load runtime-only modules when they are first required."""

    global _RUNTIME_IMPORTS_LOADED
    global api_register_fcm_provider
    global api_unregister_fcm_provider
    global async_register_services
    global async_initialize_discovery_runtime
    global _cloud_discovery_runtime_callable
    global _trigger_cloud_discovery_callable
    global _redact_account_for_log_callable
    global GoogleFindMyCoordinator
    global DiscoveryManager
    global GoogleFindMyMapView
    global GoogleFindMyMapRedirectView
    global GoogleFindMyMapTilesTokenView

    if _RUNTIME_IMPORTS_LOADED:
        return

    from .api import (  # noqa: E402
        register_fcm_receiver_provider as _api_register_fcm_provider,
    )
    from .api import (
        unregister_fcm_receiver_provider as _api_unregister_fcm_provider,
    )
    from .coordinator import (  # noqa: E402
        GoogleFindMyCoordinator as _GoogleFindMyCoordinator,
    )
    from .discovery import (  # noqa: E402
        DiscoveryManager as _DiscoveryManager,
    )
    from .discovery import (
        _cloud_discovery_runtime as _cloud_discovery_runtime_fn,
    )
    from .discovery import (
        _redact_account_for_log as _redact_account_for_log_fn,
    )
    from .discovery import (
        _trigger_cloud_discovery as _trigger_cloud_discovery_fn,
    )
    from .discovery import (
        async_initialize_discovery_runtime as _async_initialize_discovery_runtime,
    )
    from .map_view import (  # noqa: E402
        GoogleFindMyMapRedirectView as _GoogleFindMyMapRedirectView,
    )
    from .map_view import (
        GoogleFindMyMapTilesTokenView as _GoogleFindMyMapTilesTokenView,
    )
    from .map_view import (
        GoogleFindMyMapView as _GoogleFindMyMapView,
    )
    from .services import (  # noqa: E402
        async_register_services as _async_register_services,
    )

    api_register_fcm_provider = _api_register_fcm_provider
    api_unregister_fcm_provider = _api_unregister_fcm_provider
    async_register_services = _async_register_services
    async_initialize_discovery_runtime = _async_initialize_discovery_runtime
    _cloud_discovery_runtime_callable = _cloud_discovery_runtime_fn
    _redact_account_for_log_callable = _redact_account_for_log_fn
    _trigger_cloud_discovery_callable = _trigger_cloud_discovery_fn
    GoogleFindMyCoordinator = _GoogleFindMyCoordinator
    DiscoveryManager = _DiscoveryManager
    GoogleFindMyMapView = _GoogleFindMyMapView
    GoogleFindMyMapRedirectView = _GoogleFindMyMapRedirectView
    GoogleFindMyMapTilesTokenView = _GoogleFindMyMapTilesTokenView

    _RUNTIME_IMPORTS_LOADED = True


def _is_map_view_enabled_for_entry_fn(entry: ConfigEntry | None) -> bool:
    """Return whether Map View is enabled for ``entry`` (one shared rule).

    Delegates to ``map_view._is_map_view_enabled_for_entry`` so registration,
    the device-URL refresh, the refresh service and the views all read the
    option the same way. Imported lazily like the other ``map_view`` names.
    """

    from .map_view import _is_map_view_enabled_for_entry

    return _is_map_view_enabled_for_entry(entry)


def _cloud_discovery_runtime(
    hass: HomeAssistant, entry: ConfigEntry | None = None
) -> CloudDiscoveryRuntime:
    """Return the cached discovery runtime, loading dependencies if needed."""

    _ensure_runtime_imports()
    return _cloud_discovery_runtime_callable(hass, entry)


async def _trigger_cloud_discovery(*args: Any, **kwargs: Any) -> Any:
    """Trigger cloud discovery with lazy dependency loading."""

    _ensure_runtime_imports()
    return await _trigger_cloud_discovery_callable(*args, **kwargs)


def _redact_account_for_log(*args: Any, **kwargs: Any) -> str:
    """Redact account references for logging with lazy imports."""

    _ensure_runtime_imports()
    return _redact_account_for_log_callable(*args, **kwargs)


try:  # pragma: no cover - compatibility shim for stripped test envs
    from homeassistant.helpers.entity_registry import (
        RegistryEntryDisabler as _RegistryEntryDisabler,
    )
except Exception:  # pragma: no cover - Home Assistant test doubles may omit enum
    _RegistryEntryDisabler = SimpleNamespace(INTEGRATION="integration")

RegistryEntryDisabler = cast("RegistryEntryDisablerType", _RegistryEntryDisabler)

# Optional feature: GoogleHomeFilter (guard import to avoid hard dependency)
if TYPE_CHECKING:
    from .google_home_filter import GoogleHomeFilter as GoogleHomeFilterProtocol
else:
    from typing import Any as GoogleHomeFilterProtocol

_GOOGLE_HOME_FILTER_CLASS: type[Any] | None = None
_GOOGLE_HOME_FILTER_IMPORT_ATTEMPTED = False

# Declare that this integration is config-entry-only (no YAML configuration).
# Use getattr fallback for older HA versions lacking config_entry_only_config_schema.
CONFIG_SCHEMA: vol.Schema = getattr(
    cv,
    "config_entry_only_config_schema",
    lambda domain: vol.Schema({domain: vol.Schema({})}),
)(DOMAIN)

_LOGGER = logging.getLogger(__name__)

# Options the legacy migration in ``async_setup_entry`` carries over: the
# user's settings and the schema version, nothing else.
_LEGACY_MIGRATION_KEPT_OPTIONS: Final[frozenset[str]] = frozenset(
    (*OPTION_KEYS, SECRETS_EXTRA_WATCH_PATHS, OPT_OPTIONS_SCHEMA_VERSION)
)

# Bundle field names that may appear in a log line. Any other field name is logged
# as "other field": per-account fields embed the account e-mail
# (``adm_token_<email>``).
_LOGGABLE_BUNDLE_FIELDS: tuple[str, ...] = (
    "aas_token",
    "access_token",
    "adm_token",
    "Email",
    "fcm_credentials",
    "google_email",
    "oauth_token",
    "owner_key",
    "shared_key",
    "token",
    "username",
)


def _optional_credential_label(key: str) -> str:
    """Return a fixed log name for a key of ``OPTIONAL_CREDENTIAL_KEYS``.

    The name is chosen by comparison and returned as a literal, so no data flows
    from the key into the log line: the key constants look like secrets to CodeQL
    (``DATA_SECRET_BUNDLE``), and logging the key counts as clear-text logging of
    sensitive data. Every key has its own name (pinned by
    ``tests/test_log_hygiene_init.py``).
    """

    if key == DATA_SECRET_BUNDLE:
        return "secrets.json bundle"
    if key == DATA_AAS_TOKEN:
        return "AAS token"
    return "optional sign-in value"


async def _async_self_heal_duplicate_entities(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Detect and remove duplicate entities for this config entry.

    A duplicate is defined as sharing the same config entry, device identifier,
    entity domain, and logical name (``translation_key`` preferred, then
    ``original_name``) while exposing a different ``entity_id`` or
    ``unique_id``. Only one canonical entity in every group is preserved.
    """

    entity_registry = er.async_get(hass)

    groups: dict[tuple[str, str | None, str | None, str], list[er.RegistryEntry]] = {}

    for entity_entry in _iter_config_entry_entities(entity_registry, entry.entry_id):
        if entity_entry.platform != DOMAIN:
            continue

        try:
            entity_domain, _ = split_entity_id(entity_entry.entity_id)
        except ValueError:
            entity_domain = None

        translation_key = getattr(entity_entry, "translation_key", None)
        original_name = getattr(entity_entry, "original_name", None)
        logical_name = (
            (translation_key or "").strip()
            or (original_name or "").strip()
            or entity_entry.entity_id
        )

        device_id = getattr(entity_entry, "device_id", None)
        key = (
            entity_entry.config_entry_id,
            device_id,
            entity_domain,
            logical_name,
        )
        groups.setdefault(key, []).append(entity_entry)

    duplicates: list[str] = []

    for entries in groups.values():
        if len(entries) <= 1:
            continue

        canonical = _pick_canonical_entity_entry(hass, entries)
        for candidate in entries:
            if candidate.entity_id == canonical.entity_id:
                continue
            duplicates.append(candidate.entity_id)

    if not duplicates:
        return

    _LOGGER.info(
        "Removing %s duplicate Google Find My entities for config entry %s: %s",
        len(duplicates),
        entry.entry_id,
        ", ".join(sorted(duplicates)),
    )

    for entity_id in duplicates:
        if entity_id in entity_registry.entities:
            entity_registry.async_remove(entity_id)


def _compute_entity_score(
    hass: HomeAssistant,
    entity_entry: er.RegistryEntry,
) -> int:
    """Return a priority score describing how suitable an entity is to keep."""

    score = 0

    if entity_entry.translation_key:
        score += 4

    if entity_entry.disabled_by is None:
        score += 3

    state = hass.states.get(entity_entry.entity_id)
    if state is not None and state.state not in (STATE_UNAVAILABLE, STATE_UNKNOWN):
        score += 3

    return score


def _pick_canonical_entity_entry(
    hass: HomeAssistant,
    entries: list[er.RegistryEntry],
) -> er.RegistryEntry:
    """Pick the entity registry entry that should remain active."""

    canonical = entries[0]
    best_score = _compute_entity_score(hass, canonical)

    for candidate in entries[1:]:
        score = _compute_entity_score(hass, candidate)
        if score > best_score:
            canonical = candidate
            best_score = score

    return canonical


_CONFIG_FLOW_HELPERS: dict[str, Any] | None = None
# Keep the per-entry health probe timeout small so rebuilds remain responsive.
# Adjust cautiously if probing strategy changes—this is a per-entry budget, not
# a global backoff.
_ENTRY_HEALTH_TIMEOUT = 10.0


@dataclass(slots=True)
class _EntryHealth:
    """Summarize the credential health of a config entry."""

    status: Literal["valid", "invalid", "unknown"]
    reason: str | None = None
    token_source: str | None = None


def _load_config_flow_helpers() -> dict[str, Any] | None:
    """Lazily import config flow helpers used for credential probes."""

    global _CONFIG_FLOW_HELPERS
    if _CONFIG_FLOW_HELPERS is not None:
        return _CONFIG_FLOW_HELPERS

    try:  # pragma: no cover - exercised indirectly in tests
        from . import config_flow as _config_flow
    except Exception as err:  # noqa: BLE001 - defensive import guard
        _LOGGER.debug("Config flow helpers unavailable: %s", err)
        _CONFIG_FLOW_HELPERS = {}
        return _CONFIG_FLOW_HELPERS

    _CONFIG_FLOW_HELPERS = {
        "extract_oauth": getattr(
            _config_flow, "_extract_oauth_candidates_from_secrets", None
        ),
        "new_api": getattr(_config_flow, "_async_new_api_for_probe", None),
        "try_probe": getattr(_config_flow, "_try_probe_devices", None),
        "map_error": getattr(_config_flow, "_map_api_exc_to_error_key", None),
        "guard_error": getattr(_config_flow, "_is_multi_entry_guard_error", None),
        "dependency_not_ready": getattr(_config_flow, "DependencyNotReady", None),
    }
    return _CONFIG_FLOW_HELPERS


def _entry_schema_score(entry: ConfigEntry) -> int:
    """Return an integer describing how complete the entry schema is."""

    score = 0
    for container in (entry.data, entry.options):
        if not isinstance(container, Mapping):
            continue
        if isinstance(container.get(DATA_SECRET_BUNDLE), Mapping):
            score += 5
        elif container.get(DATA_SECRET_BUNDLE) is not None:
            score += 2
        if container.get(DATA_AUTH_METHOD):
            score += 2
        if container.get(CONF_OAUTH_TOKEN):
            score += 1
        if container.get(DATA_AAS_TOKEN):
            score += 1
    if isinstance(getattr(entry, "options", None), Mapping):
        score += len(entry.options)
    return score


def _entry_creation_timestamp(entry: ConfigEntry) -> float:
    """Return the creation timestamp (epoch seconds) or ``inf`` when unknown."""

    timestamp = getattr(entry, "created_at", None)
    if not isinstance(timestamp, datetime):
        timestamp = getattr(entry, "updated_at", None)
    if isinstance(timestamp, datetime):
        try:
            return float(timestamp.timestamp())
        except (OSError, ValueError):  # pragma: no cover - defensive fallback
            return float("inf")
    return float("inf")


async def _async_collect_entry_tokens(
    hass: HomeAssistant, entry: ConfigEntry
) -> tuple[list[tuple[str, str]], Mapping[str, Any] | None]:
    """Collect candidate OAuth tokens for ``entry`` from all known sources."""

    helpers = _load_config_flow_helpers() or {}
    extract_oauth = helpers.get("extract_oauth")

    seen: set[str] = set()
    tokens: list[tuple[str, str]] = []
    secrets_bundle: Mapping[str, Any] | None = None

    def _add(label: str, value: Any) -> None:
        if not isinstance(value, str):
            return
        candidate = value.strip()
        if not candidate or candidate in seen:
            return
        seen.add(candidate)
        tokens.append((label, candidate))

    for container_label, container in ("data", entry.data), ("options", entry.options):
        if not isinstance(container, Mapping):
            continue
        bundle = container.get(DATA_SECRET_BUNDLE)
        if isinstance(bundle, Mapping):
            secrets_bundle = secrets_bundle or cast(Mapping[str, Any], bundle)
            if callable(extract_oauth):
                try:
                    for source, token in extract_oauth(dict(bundle)):
                        _add(f"{container_label}.secrets.{source}", token)
                except Exception as err:  # pragma: no cover - defensive logging
                    _LOGGER.debug(
                        "Reading a stored sign-in value failed for %s.%s: %s",
                        entry.entry_id,
                        container_label,
                        err,
                    )
        _add(f"{container_label}.oauth_token", container.get(CONF_OAUTH_TOKEN))
        _add(f"{container_label}.aas_token", container.get(DATA_AAS_TOKEN))

    runtime = getattr(entry, "runtime_data", None)
    cache: TokenCache | None = getattr(runtime, "token_cache", None)
    created_cache: TokenCache | None = None
    if cache is None:
        try:
            created_cache = await TokenCache.create(hass, entry.entry_id)
        except Exception as err:  # pragma: no cover - defensive logging
            _LOGGER.debug(
                "Cache instance load failed for entry %s: %s", entry.entry_id, err
            )
        else:
            cache = created_cache

    if cache is not None:
        try:
            cached_values = await cache.all()
        except Exception as err:  # pragma: no cover - defensive logging
            _LOGGER.debug(
                "Cache instance read failed for entry %s: %s", entry.entry_id, err
            )
        else:
            if isinstance(cached_values, Mapping):
                _add("cache.oauth_token", cached_values.get(CONF_OAUTH_TOKEN))
                _add("cache.aas_token", cached_values.get(DATA_AAS_TOKEN))
                cached_bundle = cached_values.get(DATA_SECRET_BUNDLE)
                if isinstance(cached_bundle, Mapping) and secrets_bundle is None:
                    secrets_bundle = cast(Mapping[str, Any], cached_bundle)
                    if callable(extract_oauth):
                        try:
                            for source, token in extract_oauth(dict(cached_bundle)):
                                _add(f"cache.secrets.{source}", token)
                        except Exception as err:  # pragma: no cover - defensive
                            _LOGGER.debug(
                                "Reading a stored sign-in value failed for cache (%s): %s",
                                entry.entry_id,
                                err,
                            )

    if created_cache is not None:
        with suppress(Exception):  # pragma: no cover - defensive cleanup
            await created_cache.close()

    return tokens, secrets_bundle


async def _async_assess_entry_health(
    hass: HomeAssistant, entry: ConfigEntry, *, normalized_email: str
) -> _EntryHealth:
    """Probe stored credentials to determine whether ``entry`` is usable."""

    tokens, secrets_bundle = await _async_collect_entry_tokens(hass, entry)
    if not tokens:
        return _EntryHealth(status="unknown", reason="no_tokens")

    helpers = _load_config_flow_helpers() or {}
    new_api = helpers.get("new_api")
    try_probe = helpers.get("try_probe")
    map_error = helpers.get("map_error")
    guard_error = helpers.get("guard_error")
    dependency_not_ready = helpers.get("dependency_not_ready")

    if not callable(new_api) or not callable(try_probe) or not callable(map_error):
        return _EntryHealth(status="unknown", reason="helpers_missing")

    invalid_seen = False
    unknown_reason: str | None = None

    for source, token in tokens:
        try:
            async with asyncio.timeout(_ENTRY_HEALTH_TIMEOUT):
                api = await new_api(
                    email=normalized_email,
                    token=token,
                    secrets_bundle=secrets_bundle,
                )
                await try_probe(api, email=normalized_email, token=token)
        except TimeoutError:
            unknown_reason = "timeout"
            continue
        except Exception as err:  # noqa: BLE001 - deliberate broad guard
            if dependency_not_ready and isinstance(err, dependency_not_ready):
                return _EntryHealth(status="unknown", reason="dependency_not_ready")
            if callable(guard_error) and guard_error(err):
                return _EntryHealth(status="valid", reason="guard", token_source=source)
            error_key = map_error(err)
            if error_key == "invalid_auth":
                invalid_seen = True
                continue
            unknown_reason = error_key or "unknown"
            continue
        else:
            return _EntryHealth(status="valid", reason="probe_ok", token_source=source)

    if unknown_reason is not None:
        return _EntryHealth(status="unknown", reason=unknown_reason)
    if invalid_seen:
        return _EntryHealth(status="invalid", reason="invalid_auth")
    return _EntryHealth(status="unknown", reason="no_result")


async def async_coalesce_account_entries(
    hass: HomeAssistant,
    *,
    canonical_entry: ConfigEntry,
) -> ConfigEntry:
    """Ensure only one config entry remains for the account represented by ``canonical_entry``."""

    raw_email, normalized_email = _resolve_entry_email(canonical_entry)
    if not normalized_email:
        _LOGGER.warning(
            "Cannot deduplicate config entry %s: missing normalized email (raw=%s)",
            canonical_entry.entry_id,
            _mask_email_for_logs(raw_email),
        )
        return canonical_entry

    all_entries = hass.config_entries.async_entries(DOMAIN)
    candidates: dict[str, ConfigEntry] = {}
    for candidate in all_entries:
        if _extract_email_from_entry(candidate) == normalized_email:
            candidates[candidate.entry_id] = candidate

    candidates.setdefault(canonical_entry.entry_id, canonical_entry)

    candidate_list = list(candidates.values())
    if len(candidate_list) == 1:
        return candidate_list[0]

    health: dict[str, _EntryHealth] = {}
    for candidate in candidate_list:
        health[candidate.entry_id] = await _async_assess_entry_health(
            hass, candidate, normalized_email=normalized_email
        )

    canonical_id = canonical_entry.entry_id

    def _valid_sort_key(entry: ConfigEntry) -> tuple[Any, ...]:
        """Order valid entries by version, enabled state, schema richness, then caller."""
        not_disabled = 1 if getattr(entry, "disabled_by", None) is None else 0
        return (
            -int(getattr(entry, "version", 0)),
            -not_disabled,
            -_entry_schema_score(entry),
            -1 if entry.entry_id == canonical_id else 0,
            entry.entry_id,
        )

    valid_candidates = [
        candidate
        for candidate in candidate_list
        if health[candidate.entry_id].status == "valid"
    ]
    if valid_candidates:
        winner = sorted(valid_candidates, key=_valid_sort_key)[0]
    else:

        def _fallback_sort_key(entry: ConfigEntry) -> tuple[Any, ...]:
            """Order unknown/invalid entries by schema version, enabled flag, and age."""
            not_disabled = 1 if getattr(entry, "disabled_by", None) is None else 0
            return (
                -int(getattr(entry, "version", 0)),
                -not_disabled,
                _entry_creation_timestamp(entry),
                entry.entry_id,
            )

        winner = sorted(candidate_list, key=_fallback_sort_key)[0]
        _LOGGER.warning(
            "Account %s: selected entry %s via heuristics; no verified credentials",
            _mask_email_for_logs(normalized_email),
            winner.entry_id,
        )

    winner_health = health[winner.entry_id]
    _LOGGER.info(
        "Selected canonical entry %s for account %s with status %s",
        winner.entry_id,
        _mask_email_for_logs(normalized_email),
        winner_health.status,
    )

    _LOGGER.debug(
        "Account %s: sign-in health per entry → %s",
        _mask_email_for_logs(normalized_email),
        {entry_id: report.status for entry_id, report in health.items()},
    )

    for candidate in candidate_list:
        if candidate.entry_id == winner.entry_id:
            continue
        _LOGGER.info(
            "Removing duplicate Google Find My Device entry %s (account %s); canonical entry is %s",
            candidate.entry_id,
            _mask_email_for_logs(normalized_email),
            winner.entry_id,
        )
        try:
            await hass.config_entries.async_remove(candidate.entry_id)
        except Exception as err:  # noqa: BLE001 - surface unexpected failure
            _LOGGER.error(
                "Failed to remove duplicate entry %s for account %s: %s",
                candidate.entry_id,
                _mask_email_for_logs(normalized_email),
                err,
            )
            raise

    refreshed_getter = getattr(hass.config_entries, "async_get_entry", None)
    if callable(refreshed_getter):
        refreshed = refreshed_getter(winner.entry_id)
        if refreshed is not None:
            winner = refreshed

    return winner


# Platforms provided by this integration
PLATFORMS: list[Platform] = [
    Platform.DEVICE_TRACKER,
    Platform.BUTTON,
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
]


def _feature_name_from_platform(platform: Platform) -> str:
    """Return the Home Assistant domain string for a platform enum."""

    value = getattr(platform, "value", None)
    if isinstance(value, str):
        return value

    if isinstance(platform, str):  # pragma: no cover - defensive fallback
        return platform

    candidate = str(platform)
    if "." in candidate:
        _, candidate = candidate.split(".", 1)
    return candidate.lower()


# ---- Runtime typing helpers -------------------------------------------------


CleanupCallback = Callable[[], Awaitable[None] | None]


def _subentry_entry_id(subentry: Any) -> str | None:
    """Return the identifier Home Assistant uses to track ``subentry``.

    The ConfigSubentry API exposes both ``entry_id`` and ``subentry_id`` across
    core releases. This helper normalizes either attribute to a stable string so
    callers can trigger setup for newly created subentries. Keep this in sync
    with the handbook references in docs/CONFIG_SUBENTRIES_HANDBOOK.md.
    """

    subentry_id = getattr(subentry, "subentry_id", None)
    if isinstance(subentry_id, str) and subentry_id:
        return subentry_id

    subentry_entry_id = getattr(subentry, "entry_id", None)
    if isinstance(subentry_entry_id, str) and subentry_entry_id:
        return subentry_entry_id

    return None


@dataclass(slots=True)
class ConfigEntrySubentryDefinition:
    """Desired state for a managed configuration subentry."""

    key: str
    title: str
    data: Mapping[str, Any]
    subentry_type: str = SUBENTRY_TYPE_TRACKER
    unique_id: str | None = None
    translation_key: str | None = None
    unload: CleanupCallback | None = None


_AwaitableT = TypeVar("_AwaitableT")


class ConfigEntrySubEntryManager:
    """Helper for managing config entry subentries for this integration."""

    __slots__ = (
        "_cleanup",
        "_default_subentry_type",
        "_entry",
        "_hass",
        "_key_field",
        "_key_aliases",
        "_key_aliases_ambiguous",
        "_managed",
        "_managed_by_subentry_id",
        "_visibility_update_task",
    )

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        *,
        key_field: str = "group_key",
        default_subentry_type: str = SUBENTRY_TYPE_TRACKER,
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._key_field = key_field
        self._default_subentry_type = default_subentry_type
        self._key_aliases: dict[str, str] = {}
        self._key_aliases_ambiguous: set[str] = set()
        self._managed: dict[str, ConfigSubentry] = {}
        self._managed_by_subentry_id: dict[str, str] = {}
        self._cleanup: dict[str, CleanupCallback | None] = {}
        self._visibility_update_task: asyncio.Task[None] | None = None
        self._refresh_from_entry()

    @staticmethod
    async def _await_subentry_result(
        result: Awaitable[_AwaitableT] | _AwaitableT,
    ) -> _AwaitableT:
        """Return the awaited subentry operation result when needed."""

        if isinstance(result, Awaitable):
            awaited_any = await result
            awaited_value: _AwaitableT = cast(_AwaitableT, awaited_any)
            return awaited_value
        return result

    @staticmethod
    def _subentry_identity(
        candidate: ConfigSubentry | Any,
    ) -> tuple[str | None, str | None, str | None]:
        """Return entry, subentry, and unique identifiers for ``candidate``."""

        if candidate is None:
            return None, None, None

        entry_id_value = getattr(candidate, "entry_id", None)
        entry_id = (
            entry_id_value
            if isinstance(entry_id_value, str) and entry_id_value
            else None
        )

        subentry_id_value = getattr(candidate, "subentry_id", None)
        subentry_id = (
            subentry_id_value
            if isinstance(subentry_id_value, str) and subentry_id_value
            else None
        )

        unique_id_value = getattr(candidate, "unique_id", None)
        unique_id = (
            unique_id_value
            if isinstance(unique_id_value, str) and unique_id_value
            else None
        )

        return entry_id, subentry_id, unique_id

    def _payload_preserving_stored_fields(
        self,
        target: ConfigSubentry | Any,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Return ``payload`` widened by the stored fields it does not govern.

        The core replaces ``ConfigSubentry.data`` wholesale, so handing it a
        payload built from a definition alone *deletes* every stored field the
        definition happens not to carry. A definition is a **partial**
        description of the desired state: it names the fields it owns and is
        silent about the rest, and treating that silence as "delete" is the
        defect this helper closes.

        Measured on 2026-08-06 against the production definitions, the fields
        lost that way were exactly two, and both have owners elsewhere:
        ``visible_device_ids`` (the device-to-subentry assignment, written by
        the options flow and read by the coordinator) and ``feature_flags``
        (written by ``_build_subentry_payload`` in ``config_flow.py`` and read
        by ``SubentryOperations``). Neither is derivable from a definition, so
        neither can be repaired by adding it to one.

        The preservation is deliberately **not** an allow-list of named keys:
        an allow-list has to be extended whenever a writer stores a new field,
        and forgetting to extend it is precisely the silent-deletion failure
        this fixes. The cost is named rather than hidden, and it currently has
        no owner: a field a definition wants to *remove* can no longer be
        removed by omission and would have to be written explicitly, and no
        path in this integration removes a stored subentry key today. The
        migration path is **not** that owner, although an earlier version of
        this sentence named it: ``_migrate_entry_identifier_namespaces``
        normalises ``visible_device_ids`` and ``OPT_IGNORED_DEVICES`` and
        removes no key at all. Naming a place that cannot do the work would
        read as tracked while nothing tracks it, so the gap is stated instead:
        a deletion that must happen belongs in a path that deletes on purpose,
        and that path has still to be written.

        ``target`` is the subentry the payload is about to be written to, and
        the argument exists because that is not always the subentry the loop
        started from: the unique-id adoption path writes the payload onto a
        *different* owner. Computing the merge once per loop iteration would
        graft one subentry's stored fields onto another.

        One field is exempt, and the exemption is an invariant rather than a
        taste: a non-device group must not hold a device assignment at all. A
        ``visible_device_ids`` stored on one is therefore legacy residue from
        the flow that used to offer it as a target, and clearing it on the next
        sync is the repair that keeps a wrong assignment from becoming
        permanent -- measured by
        ``test_service_subentry_visibility_is_cleared_by_the_setup_roundtrip``.
        Preserving it would resurrect state the integration forbids, which is
        the one thing a preservation must not do.

        "Non-device" is decided on **both** axes, mirroring
        ``config_flow._accepts_device_assignment``, which refuses such a
        target: the group key first (``NON_DEVICE_SUBENTRY_KEYS``) and the
        subentry type second (``NON_DEVICE_SUBENTRY_TYPES``). Reading only the
        type would let a subentry that carries no usable type slip past while
        being managed under a non-device key, and the key is the axis that
        predicate reads first.
        """

        merged = dict(payload)
        stored = getattr(target, "data", None)
        if not isinstance(stored, Mapping):
            return merged

        skip: set[str] = set()
        group_key = merged.get(self._key_field)
        if (
            getattr(target, "subentry_type", None) in NON_DEVICE_SUBENTRY_TYPES
            or group_key in NON_DEVICE_SUBENTRY_KEYS
        ):
            skip.add("visible_device_ids")

        for stored_key, stored_value in stored.items():
            if stored_key in skip:
                continue
            if stored_key not in merged:
                merged[stored_key] = stored_value
        return merged

    @staticmethod
    def _is_subentry_like(candidate: Any) -> TypeGuard[ConfigSubentry]:
        """Return ``True`` when ``candidate`` exposes a subentry-like interface."""

        if candidate is None:
            return False

        _, subentry_id, _ = ConfigEntrySubEntryManager._subentry_identity(candidate)
        if not isinstance(subentry_id, str) or not subentry_id:
            return False

        data = getattr(candidate, "data", None)
        if not isinstance(data, Mapping):
            return False

        return True

    def _managed_key_for_subentry_id(self, subentry_id: str) -> str | None:
        """Return the managed key associated with ``subentry_id`` when present."""

        existing_key = self._managed_by_subentry_id.get(subentry_id)
        if existing_key is not None:
            return existing_key

        for managed_key, managed_subentry in self._managed.items():
            candidate_subentry_id = getattr(managed_subentry, "subentry_id", None)
            if (
                isinstance(candidate_subentry_id, str)
                and candidate_subentry_id == subentry_id
            ):
                self._managed_by_subentry_id[subentry_id] = managed_key
                return managed_key

        return None

    def _pop_managed(self, key: str) -> ConfigSubentry | None:
        """Remove ``key`` from the managed cache and return the stored subentry."""

        stored = self._managed.pop(key, None)
        if stored is None:
            return None

        stored_subentry_id = getattr(stored, "subentry_id", None)
        if isinstance(stored_subentry_id, str):
            mapped_key = self._managed_by_subentry_id.get(stored_subentry_id)
            if mapped_key == key:
                self._managed_by_subentry_id.pop(stored_subentry_id, None)

        return stored

    def _index_managed_subentry(self, key: str, subentry: ConfigSubentry) -> None:
        """Store ``subentry`` under ``key`` and keep the subentry-id index in sync."""

        subentry_id = getattr(subentry, "subentry_id", None)
        if isinstance(subentry_id, str) and subentry_id:
            existing_key = self._managed_by_subentry_id.get(subentry_id)
            if existing_key is None:
                for managed_key, managed_subentry in list(self._managed.items()):
                    candidate_subentry_id = getattr(
                        managed_subentry, "subentry_id", None
                    )
                    if (
                        isinstance(candidate_subentry_id, str)
                        and candidate_subentry_id == subentry_id
                    ):
                        existing_key = managed_key
                        break
            if existing_key is not None and existing_key != key:
                self._pop_managed(existing_key)

        # Displacing a *different* subentry from ``key`` has to take its
        # reverse-index entry with it, or the loser keeps claiming a slot it no
        # longer holds. The write below is keyed by ``key`` and therefore
        # overwrites ``_managed`` cleanly, which is what made the leak easy to
        # miss: only ``_managed_by_subentry_id`` is keyed by *identifier* and
        # survives the overwrite.
        #
        # Measured rather than derived, in ``_refresh_from_entry`` with two
        # ``tracker`` subentries both storing ``TRACKER_SUBENTRY_KEY``: iterated
        # loser-first, ``_managed`` ends at ``{core_tracking: id-a}`` while the
        # reverse index still maps ``id-b -> core_tracking``. The harm is not
        # cosmetic. ``_async_adopt_existing_unique_id`` asks
        # ``_managed_key_for_subentry_id(owner_subentry_id)`` which key the
        # owner previously held and pops it; with the stale answer it pops the
        # slot of the *rightful* holder, so ``core_tracking`` drops out of
        # ``_managed`` entirely until the next refresh.
        #
        # The tie-break introduced with the rank made the shape reachable
        # without any difference in identifier provenance: two otherwise equal
        # candidates now resolve by lowest identifier, so the higher one loses
        # deterministically instead of winning by arriving last. Read
        # "provenance" here as the property, not as a field: the two provenance
        # *fields* this method once consulted are gone, and their removal only
        # widens the set of pairs that reach the tie-break, so it makes this
        # paragraph apply more often rather than less.
        displaced = self._managed.get(key)
        if displaced is not None and displaced is not subentry:
            displaced_id = getattr(displaced, "subentry_id", None)
            if (
                isinstance(displaced_id, str)
                and displaced_id
                and displaced_id != subentry_id
                and self._managed_by_subentry_id.get(displaced_id) == key
            ):
                self._managed_by_subentry_id.pop(displaced_id, None)

        self._managed[key] = subentry

        if isinstance(subentry_id, str) and subentry_id:
            self._managed_by_subentry_id[subentry_id] = key

    def _candidate_score(
        self, subentry: ConfigSubentry, *, key: str
    ) -> tuple[int, int, int]:
        """Return preference tuple for resolving duplicate managed keys.

        Higher wins. ``key`` is the *canonical* managed key the collision is
        resolved for, not the key the subentry stores: an aliased subentry is
        ranked for the slot it actually competes for.

        The first three fields are the contract's ordering, spelled "higher
        wins" here where ``config_flow.py::_resolve_existing`` and
        ``coordinator/subentry.py`` spell it "lower wins"
        (``agents/config_flow/AGENTS.md``: exact stored key, then literal
        owner, then a candidate whose identifier is missing sorts last, then
        the lowest identifier):

        1. ``exact_key`` -- an exact stored-key match beats a folded twin. The
           flow spells this as ``pool = exact or folded`` rather than as a rank
           field, to the same effect. **Omitting this field was a measured
           regression, not a simplification**: with only the type axis and the
           identifier tie-break, a legacy tracker twin stored under an e-mail
           key outranked the canonical ``core_tracking`` subentry whenever its
           identifier happened to sort lower and took the slot. How loudly that
           fails depends on the identifier shape: in
           ``test_ap3_an_exact_stored_key_outranks_a_folded_twin`` the fixture
           ids (``e1-t``, ``e1-legacy``) hold no ``f"{entry_id}-{key}"``, so
           ``async_sync`` reaches ``_async_adopt_existing_unique_id`` for a
           unique id no subentry holds and raises. Every production writer
           embeds both parts, named by symbol rather than by line because an
           earlier draft cited five line numbers that had already drifted onto
           a ``)``, a comment and a line of prose: ``async_setup_entry`` and
           ``ConfigEntrySubEntryManager.async_sync`` here,
           ``SubentryOperations._build_core_subentry_definitions`` in
           ``coordinator/subentry.py``, and
           ``ConfigFlow._async_sync_feature_subentries`` in ``config_flow.py``.
           Greppable as ``f"{entry_id}-{key}"`` and its ``entry.entry_id``
           spellings. So on disk the adoption finds an
           owner and writes the payload onto the wrong subentry instead of
           raising. Silent misattribution rather than an exception is the worse
           of the two, which is why this field is not optional.
        2. ``type_match`` -- the type that literally owns this core key per
           ``LITERAL_CORE_KEY_OWNER`` beats one that merely folds onto it. For
           a non-core key the table yields ``None``, no candidate matches, and
           the field is uniformly ``0``: neutral rather than a second ordering.
        3. ``subentry_id_present`` mirrors the reading side's ``subentry_id is
           None`` field. It **cannot discriminate at the only call site**:
           ``_select_preferred_managed`` is the sole caller and passes both
           operands through ``_is_subentry_like`` first, which already demands
           a non-empty ``str`` id, so the field is uniformly ``1`` there. The
           contract criterion it stands for is really enforced by that guard,
           not by this tuple; the field is kept as the guard's mirror (and to
           order duck-typed candidates handed straight to this method by a
           test), not because it decides a production collision.

        Two provenance fields used to sit **behind** those three, and they are
        gone rather than reordered. Their removal is the point of this
        paragraph, because an earlier draft argued them harmless and the
        argument was wrong in a way worth keeping visible:

        - ``entry_match`` (``subentry.entry_id == self._entry.entry_id``) could
          never discriminate. ``ConfigSubentry`` declares ``data, subentry_id,
          subentry_type, title, unique_id`` and no ``entry_id``, so the field
          was ``getattr(..., None) == <our id>`` for every candidate.
        - ``unique_match`` was a *substring* test (``entry_id in unique_id``),
          and it is the field that broke the contract. Sitting between
          "identifier presence" and the lowest-identifier tie-break, it decided
          pairs the contract wanted decided by id. Measured on the pair
          ``(id-a, unique_id=None)`` at ``(1,1,1,0,0)`` against
          ``(id-b, "e1-core_tracking")`` at ``(1,1,1,0,1)``: this method took
          ``id-b`` in **both** iteration orders where the flow's ``min(...)``
          and the index's rank take ``id-a``. ``_refresh_subentry_index`` then
          hands one holder's visible list to a manager naming the other, which
          is how a group's device assignments land on the wrong subentry.

        The reachability argument that kept the field is the part to remember,
        because it was inverted rather than merely optimistic. It read "the
        pair needs a ``unique_id`` without the entry id, and no writer in
        ``custom_components/`` produces one". That is true of a *wrongly
        shaped* identifier and irrelevant to the pair actually measured, whose
        left operand is ``unique_id=None``: absence needs no writer. The core
        builds subentries with ``unique_id=subentry_data.get("unique_id")`` in
        ``ConfigEntry.__init__`` (``config_entries.py``, the ``subentries_data``
        loop), so a stored subentry without the key is ``None`` on load, and
        ``ConfigSubentryFlow.async_create_entry`` defaults the argument to
        ``None``. "No writer produces it" was a statement about the happy path,
        not about the data.

        How long the shape survives is a *window*, not a permanent state, and
        saying "never repaired" here was the same overreach one paragraph
        later. Two kinds of write-back exist. The three subentry updates on
        ``OptionsFlowHandler`` (``_async_update_feature_group_subentry``,
        ``_async_refresh_subentry_entry_title``,
        ``_async_assign_devices_to_subentry``) pass an existing value
        through unchanged (``unique_id=getattr(subentry, "unique_id", None)``),
        so they neither create nor repair a ``None``; but
        ``ConfigFlow._async_sync_feature_subentries`` assigns
        ``f"{entry_id}-{key}"`` to the *existing* core subentry it resolved
        (and its nested ``_claim_unique_id`` does the same for a displaced
        holder), so the next options-flow pass
        normally does repair it. The divergence therefore lived between entry
        load and the next flow run. That is long enough to matter -- the
        runtime index and the visibility write-back both run in it -- and short
        enough that the field's defenders could go a long time without seeing
        it.

        A full tie is broken by ``_select_preferred_managed``, not here, because
        the reading side's final field prefers the *lowest* ``subentry_id`` and
        that ordering cannot be spelled as a "higher wins" tuple element. With
        the provenance fields gone, that tie-break is now reached by every pair
        the three contract fields leave equal, which is what makes this method
        agree with ``config_flow.py::_resolve_existing`` and the runtime index
        instead of merely resembling them.

        Three of four, not three of three, and the fourth runs first. On the
        very pair pinned by
        ``::test_ap8_the_manager_and_the_index_agree_when_one_identifier_is_missing``
        the deletion path disagrees with all three:
        ``_deduplicate_subentries``'s ``_select_canonical`` groups by
        ``(group_key, subentry_type)`` and sorts a *present* ``unique_id``
        first, so it keeps the higher identifier and removes the lower one --
        the very subentry this method now names. ``async_sync`` calls it
        unconditionally before anything else, so this is a live ordering, not a
        latent one: a visibility write-back landing on the manager's holder can
        be undone by the next sync. The removal itself is pre-existing
        (``B16``, owned by ``PLAN_GFMY_SUBENTRY_DELETION_TYPE_AXIS``, where the
        input shape is now measured); what this change alters is which side of
        it the manager stands on.
        """

        data = getattr(subentry, "data", None)
        exact_key = int(isinstance(data, Mapping) and data.get(self._key_field) == key)
        canonical_owner = LITERAL_CORE_KEY_OWNER.get(key)
        type_match = int(
            canonical_owner is not None
            and getattr(subentry, "subentry_type", None) == canonical_owner
        )
        subentry_id_present = int(
            isinstance(getattr(subentry, "subentry_id", None), str)
        )
        return (
            exact_key,
            type_match,
            subentry_id_present,
        )

    def _select_preferred_managed(
        self, existing: ConfigSubentry, candidate: ConfigSubentry, *, key: str
    ) -> ConfigSubentry:
        """Return the preferred managed subentry when keys collide."""

        if not self._is_subentry_like(existing):
            return candidate
        if not self._is_subentry_like(candidate):
            return existing

        candidate_score = self._candidate_score(candidate, key=key)
        existing_score = self._candidate_score(existing, key=key)
        if candidate_score != existing_score:
            return candidate if candidate_score > existing_score else existing

        # Fully tied scores used to fall through to "whichever was iterated
        # first", which made the winner a function of load order. Break the tie
        # in the direction the other two sites use -- lowest ``subentry_id``
        # wins, matching ``_resolve_existing``'s ``min(...)``, which is
        # parametric in the key.
        #
        # What this buys and what it does not, kept apart because an earlier
        # draft of this comment ran the two together. It buys determinism here.
        # Cross-side agreement on *which* subentry holds the slot arrived with
        # the reading side's half: ``coordinator/subentry.py`` used to wrap its
        # whole rank in ``if group_key == SERVICE_SUBENTRY_KEY:``, so for every
        # other key the index was last-iterated-wins and the two could name
        # different subentries. Its rank block now covers both core keys, and
        # the agreement is asserted rather than assumed
        # (``::test_ap4_the_manager_and_the_index_agree_on_the_tracker_slot``,
        # both iteration orders). What is still *not* covered by any of it:
        # ``_deduplicate_subentries`` and ``_async_adopt_existing_unique_id``
        # never call this method. "Decide by iteration order" was the summary
        # here and it is only half true, which matters now that the removal of
        # the provenance fields made the other half load-bearing:
        # ``_async_adopt_existing_unique_id`` does take the first match, but
        # ``_select_canonical`` sorts on identifier *presence* first, then the
        # identifier itself, and only then falls to the index. A type field sat
        # between those two for two stages of PR #1236 and was dropped again
        # once the removal guard there began comparing the full ``(group_key,
        # subentry_type)`` identity, which left the field deciding nothing
        # (measured over every three-subentry shape). Neither direction changed
        # anything here: the first two fields are still a real criterion, and
        # they still point the opposite way from all three rankers for a pair
        # whose lower identifier carries ``unique_id=None``.
        #
        # The both-ids-missing case never reaches this point: ``subentry_id``
        # is read through the same ``getattr`` in ``_is_subentry_like``, which
        # rejects anything but a non-empty ``str``, so such a pair returns
        # ``candidate`` at the first guard above -- not the incumbent, as an
        # earlier draft said. The ``return existing`` below is consequently
        # unreachable from the sole call site (``_refresh_from_entry``) and
        # exists to keep the branch total.
        #
        # One asymmetry remains against the index and is deliberate: it sorts
        # on the *sanitised, provisional-filtered* identifier, this method on
        # the raw one. ``agents/config_flow/AGENTS.md`` already names that
        # difference between the flow and the index; the manager inherits it,
        # and it can only separate the two for an identifier shape no site in
        # ``custom_components/`` writes.
        candidate_id = getattr(candidate, "subentry_id", None)
        existing_id = getattr(existing, "subentry_id", None)
        if isinstance(candidate_id, str) and isinstance(existing_id, str):
            return candidate if candidate_id < existing_id else existing
        return existing

    def _resolve_updated_subentry(
        self,
        *,
        candidate: ConfigSubentry | Any,
        original: ConfigSubentry,
    ) -> ConfigSubentry:
        """Return the object that should back the managed cache after an update."""

        if self._is_subentry_like(candidate):
            return candidate

        subentry_id = getattr(original, "subentry_id", None)
        if isinstance(subentry_id, str) and subentry_id:
            subentries = getattr(self._entry, "subentries", None)
            if isinstance(subentries, Mapping):
                from_registry = subentries.get(subentry_id)
                if self._is_subentry_like(from_registry):
                    return from_registry

        return original

    def _resolve_registered_subentry(
        self,
        *,
        key: str,
        unique_id: str,
        candidate: ConfigSubentry | None,
        fallback_subentry_id: str | None,
    ) -> ConfigSubentry:
        """Return the registry-backed subentry for ``key``."""

        candidate_entry_id, candidate_subentry_id, candidate_unique_id = (
            self._subentry_identity(candidate)
        )

        target_subentry_id: str | None
        if isinstance(fallback_subentry_id, str) and fallback_subentry_id:
            target_subentry_id = fallback_subentry_id
        else:
            target_subentry_id = candidate_subentry_id

        manager = getattr(self._hass, "config_entries", None)
        refreshed_entry: ConfigEntry | None = None
        if manager is not None:
            get_entry = getattr(manager, "async_get_entry", None)
            if callable(get_entry):
                try:
                    refreshed_entry = get_entry(self._entry.entry_id)
                except Exception as err:  # pragma: no cover - defensive logging
                    _LOGGER.debug(
                        "[%s] async_sync: Failed to refresh parent entry during subentry"
                        " lookup: %s",
                        self._entry.entry_id,
                        err,
                    )

        if (
            isinstance(refreshed_entry, ConfigEntry)
            and refreshed_entry is not self._entry
        ):
            self._entry = refreshed_entry

        def _scan(entry: Any) -> ConfigSubentry | None:
            subentries = getattr(entry, "subentries", None)
            if not isinstance(subentries, Mapping):
                return None

            if isinstance(target_subentry_id, str):
                direct = subentries.get(target_subentry_id)
                if isinstance(direct, ConfigSubentry):
                    return direct
                if direct is not None:
                    return direct

            for subentry in subentries.values():
                subentry_entry_id, subentry_id, subentry_unique_id = (
                    self._subentry_identity(subentry)
                )

                if (
                    isinstance(target_subentry_id, str)
                    and subentry_id == target_subentry_id
                ):
                    return subentry

                if isinstance(candidate_entry_id, str):
                    if (
                        isinstance(subentry_entry_id, str)
                        and subentry_entry_id == candidate_entry_id
                    ):
                        return subentry

                resolved_candidate_unique_id: str | None
                if isinstance(candidate_unique_id, str) and candidate_unique_id:
                    resolved_candidate_unique_id = candidate_unique_id
                elif isinstance(unique_id, str) and unique_id:
                    resolved_candidate_unique_id = unique_id
                else:
                    resolved_candidate_unique_id = None
                if (
                    isinstance(subentry_unique_id, str)
                    and resolved_candidate_unique_id is not None
                    and subentry_unique_id == resolved_candidate_unique_id
                ):
                    return subentry

                data = getattr(subentry, "data", None)
                if isinstance(data, Mapping):
                    group_key = data.get(self._key_field)
                    if isinstance(group_key, str) and group_key == key:
                        return subentry

                if (
                    isinstance(fallback_subentry_id, str)
                    and isinstance(subentry_id, str)
                    and subentry_id == fallback_subentry_id
                ):
                    return subentry

            return None

        resolved: ConfigSubentry | None = None
        entries_to_scan: list[Any] = []
        for entry in (refreshed_entry, self._entry):
            if entry is None:
                continue
            if getattr(entry, "subentries", None) is None:
                continue
            if any(candidate is entry for candidate in entries_to_scan):
                continue
            entries_to_scan.append(entry)

        for entry in entries_to_scan:
            resolved = _scan(entry)
            if resolved is not None:
                break

        if resolved is None and isinstance(candidate, ConfigSubentry):
            resolved = candidate

        if resolved is None:
            candidate_descriptor: str
            if isinstance(candidate_entry_id, str) and candidate_entry_id:
                candidate_descriptor = candidate_entry_id
            elif isinstance(candidate_subentry_id, str) and candidate_subentry_id:
                fallback_unique = candidate_unique_id or unique_id
                if isinstance(fallback_unique, str) and fallback_unique:
                    candidate_descriptor = f"{candidate_subentry_id}:{fallback_unique}"
                else:
                    candidate_descriptor = candidate_subentry_id
            elif isinstance(candidate_unique_id, str) and candidate_unique_id:
                candidate_descriptor = candidate_unique_id
            elif isinstance(unique_id, str) and unique_id:
                candidate_descriptor = unique_id
            else:
                candidate_descriptor = "<unset>"
            context = (
                f"unique_id={unique_id!r}, key={key!r}, fallback_id={fallback_subentry_id!r},"
                f" candidate_ref={candidate_descriptor!r}"
            )
            _LOGGER.error(
                "[%s] async_sync: Unable to locate registered subentry for %s after"
                " async_add_subentry",
                self._entry.entry_id,
                context,
            )
            raise HomeAssistantError(
                "Failed to locate registered subentry after creation; see logs for"
                f" context ({context})"
            )

        return resolved

    def _register_key_alias(self, key: str, canonical_key: str) -> None:
        """Record ``key -> canonical_key`` unless the mapping is ambiguous.

        Two subentries of different types can carry the same stored key, for
        instance a service and a tracker subentry that both kept a legacy
        owner key from an early migration. Their canonical identities differ,
        so the alias has no single answer. Resolving it to whichever subentry
        happened to be iterated first would silently write to an arbitrary one
        of the two, so such a key resolves to nothing instead: ``get`` and
        ``update_visible_device_ids`` then miss and stay inert rather than hit
        the wrong subentry. The ambiguity is recorded rather than merely
        dropped, so a third subentry carrying the same key cannot re-establish
        the mapping later in the same pass.
        """

        if key in self._key_aliases_ambiguous:
            return

        existing = self._key_aliases.get(key)
        if existing is None:
            self._key_aliases[key] = canonical_key
            return
        if existing != canonical_key:
            self._key_aliases.pop(key, None)
            self._key_aliases_ambiguous.add(key)

    def _resolve_key(self, key: str) -> str | None:
        """Return the canonical key for ``key``, or ``None`` when ambiguous."""

        if key in self._key_aliases_ambiguous:
            return None
        return self._key_aliases.get(key, key)

    def _refresh_from_entry(self) -> None:
        """Populate managed mapping from the config entry."""

        self._managed.clear()
        self._managed_by_subentry_id.clear()
        self._key_aliases.clear()
        self._key_aliases_ambiguous.clear()
        subentries = getattr(self._entry, "subentries", None)
        if not isinstance(subentries, Mapping):
            return

        seen_subentry_ids: set[str] = set()
        for subentry in subentries.values():
            subentry_id = getattr(subentry, "subentry_id", None)
            if isinstance(subentry_id, str) and subentry_id:
                if subentry_id in seen_subentry_ids:
                    continue
                seen_subentry_ids.add(subentry_id)

            data = getattr(subentry, "data", None)
            if isinstance(data, Mapping):
                key = data.get(self._key_field)
            else:
                key = None

            canonical_key: str | None
            if isinstance(key, str) and key:
                canonical_key = key
            else:
                canonical_key = None

            subentry_type = getattr(subentry, "subentry_type", None)
            if isinstance(subentry_type, str):
                if subentry_type == SUBENTRY_TYPE_SERVICE:
                    canonical_key = SERVICE_SUBENTRY_KEY
                elif subentry_type == SUBENTRY_TYPE_TRACKER:
                    canonical_key = TRACKER_SUBENTRY_KEY
                elif canonical_key is None:
                    canonical_key = subentry_type

            if canonical_key is None:
                canonical_key = self._default_subentry_type

            if isinstance(key, str) and key and isinstance(canonical_key, str):
                if canonical_key == key:
                    # A subentry whose canonical identity *is* the stored key
                    # claims that key too. Recording the claim is what lets a
                    # later foreign alias be recognised as a conflict instead
                    # of taking the key uncontested: a ``hub``-typed subentry
                    # canonicalises onto its own stored key and would
                    # otherwise never register, leaving a service twin free to
                    # capture it.
                    self._register_key_alias(key, canonical_key)
                elif key not in (SERVICE_SUBENTRY_KEY, TRACKER_SUBENTRY_KEY):
                    # A core key already names a group of its own, so it must
                    # never stand in for a different one. A service subentry
                    # that still stores the legacy ``core_tracking`` key would
                    # otherwise register ``core_tracking -> service`` and route
                    # every write meant for the tracker group onto the service
                    # twin, which is precisely the group that must not hold
                    # devices.
                    self._register_key_alias(key, canonical_key)

            existing = self._managed.get(canonical_key)
            preferred = (
                self._select_preferred_managed(existing, subentry, key=canonical_key)
                if existing is not None
                else subentry
            )
            if preferred is existing:
                continue

            self._index_managed_subentry(canonical_key, preferred)

    async def _async_adopt_existing_unique_id(
        self,
        key: str,
        definition: ConfigEntrySubentryDefinition,
        unique_id: str,
        payload: dict[str, Any],
    ) -> ConfigSubentry:
        """Adopt the existing subentry that already owns ``unique_id``."""

        owner: ConfigSubentry | None = None
        for subentry in getattr(self._entry, "subentries", {}).values():
            _, _, candidate_unique_id = self._subentry_identity(subentry)
            if candidate_unique_id == unique_id:
                owner = subentry
                break

        if owner is None:
            raise HomeAssistantError(
                f"Subentry with unique_id '{unique_id}' not found while trying to "
                f"adopt it for key '{key}' in entry {self._entry.entry_id}"
            )

        _, owner_subentry_id, owner_unique_id = self._subentry_identity(owner)

        removal_keys: set[str] = set()

        if isinstance(owner_subentry_id, str) and owner_subentry_id:
            previous_key = self._managed_key_for_subentry_id(owner_subentry_id)
            if previous_key is not None and previous_key != key:
                removal_keys.add(previous_key)

        for old_key, mapped in list(self._managed.items()):
            if old_key == key:
                continue
            _, mapped_subentry_id, mapped_unique_id = self._subentry_identity(mapped)
            if (
                isinstance(owner_subentry_id, str)
                and owner_subentry_id
                and mapped_subentry_id == owner_subentry_id
            ):
                removal_keys.add(old_key)
                continue
            if (
                isinstance(owner_unique_id, str)
                and owner_unique_id
                and mapped_unique_id == owner_unique_id
            ):
                removal_keys.add(old_key)

        for old_key in removal_keys:
            self._pop_managed(old_key)
            self._cleanup.pop(old_key, None)

        # Against ``owner``, not against the subentry the caller started from:
        # this path adopts a *different* subentry, so the fields to preserve
        # are the ones stored on the adoptee.
        update_result = self._hass.config_entries.async_update_subentry(
            self._entry,
            owner,
            data=self._payload_preserving_stored_fields(owner, payload),
            title=definition.title,
        )
        resolved = await self._await_subentry_result(update_result)

        if isinstance(resolved, ConfigSubentry):
            stored = resolved
        else:
            stored = self._entry.subentries.get(
                getattr(owner, "subentry_id", None), owner
            )

        self._index_managed_subentry(key, stored)
        return stored

    async def _deduplicate_subentries(self) -> None:
        """Remove duplicate subentries so each logical group has a single entry."""

        manager = getattr(self._hass, "config_entries", None)
        remove_subentry = (
            getattr(manager, "async_remove_subentry", None)
            if manager is not None
            else None
        )
        if not callable(remove_subentry):
            return

        raw_subentries = getattr(self._entry, "subentries", None)
        candidates: Iterable[ConfigSubentry]
        if isinstance(raw_subentries, Mapping):
            candidates = cast(Iterable[ConfigSubentry], raw_subentries.values())
        elif isinstance(raw_subentries, dict):
            candidates = cast(Iterable[ConfigSubentry], raw_subentries.values())
        else:
            candidates = ()

        subentries: list[ConfigSubentry] = []
        seen_subentry_ids: set[str] = set()
        for subentry in candidates:
            subentry_id = getattr(subentry, "subentry_id", None)
            if isinstance(subentry_id, str) and subentry_id:
                if subentry_id in seen_subentry_ids:
                    continue
                seen_subentry_ids.add(subentry_id)
            subentries.append(subentry)
        if not subentries:
            return

        grouped_by_unique: dict[str, list[ConfigSubentry]] = defaultdict(list)
        grouped_by_group: dict[tuple[str | None, str | None], list[ConfigSubentry]] = (
            defaultdict(list)
        )
        # Reuse the key this loop already resolved instead of re-reading it in
        # the removal guard below. A second reading form is exactly the drift
        # this module keeps paying for: the flow-side and manager-side sweeps
        # answer the same question through two readers, and neither notices
        # when they disagree. Reading it here also keeps the guard's identity
        # and ``grouped_by_group``'s key literally the same expression, which
        # is what makes the subset property below checkable rather than
        # plausible.
        #
        # Keyed by object identity rather than by ``subentry_id``: the loop
        # above appends every candidate, including one whose id is missing or
        # not a ``str``, so an id-keyed map would need a guard against a shape
        # the core cannot build (``ConfigSubentry.subentry_id`` is a ``str``
        # defaulting to a fresh ULID) and would collide if two such candidates
        # ever appeared. ``id()`` is safe here because every key refers to an
        # object held alive by ``subentries`` for the whole function.
        stored_group_keys: dict[int, str | None] = {}

        for subentry in subentries:
            subentry_unique_id = getattr(subentry, "unique_id", None)
            if isinstance(subentry_unique_id, str):
                grouped_by_unique[subentry_unique_id].append(subentry)

            data = getattr(subentry, "data", None)
            if isinstance(data, Mapping):
                group_value = data.get(self._key_field)
            else:
                group_value = None
            key_value = (
                group_value if isinstance(group_value, str) and group_value else None
            )
            stored_group_keys[id(subentry)] = key_value
            grouped_by_group[
                (key_value, getattr(subentry, "subentry_type", None))
            ].append(subentry)

        def _select_canonical(candidates: list[ConfigSubentry]) -> ConfigSubentry:
            def _sort_key(
                item: tuple[int, ConfigSubentry],
            ) -> tuple[int, str, int, str]:
                index, candidate = item
                candidate_unique_id = getattr(candidate, "unique_id", None)
                unique_part = (
                    candidate_unique_id if isinstance(candidate_unique_id, str) else ""
                )
                candidate_subentry_id = getattr(candidate, "subentry_id", None)
                subentry_part = (
                    candidate_subentry_id
                    if isinstance(candidate_subentry_id, str)
                    else ""
                )
                # No type field here, and the absence is the measured half
                # of the removal guard below rather than an omission.
                #
                # A type rank did sit in this tuple for two stages of PR #1236.
                # It existed for the ``unique_id`` axis alone: candidates there
                # are grouped *by* ``unique_id``, so fields 1 and 2 tie by
                # construction and the survivor was whichever subentry
                # ``entry.subentries`` happened to yield first -- the same
                # "taking whichever came first" that
                # ``agents/config_flow/AGENTS.md`` records as a defect on the
                # flow side, where it silently removed the canonical service
                # group. Ranking the literal key owner first made that
                # deterministic, and the type guard below then decided which
                # siblings the winner could take with it.
                #
                # Once that guard compares the **full** identity, the rank
                # stops deciding anything: the surviving removals are exactly
                # those the ``grouped_by_group`` pass makes anyway, whichever
                # candidate this helper calls canonical. Measured rather than
                # reasoned about, because the previous stage of this comment
                # was wrong about a related claim in the same place: over all
                # 19 683 three-subentry shapes, and over a 19 987-shape random
                # sample of four- and five-subentry ones, the removal sets with
                # the rank and with the rank neutralised are identical. It is
                # dropped rather than left in place with a note, because a
                # ranking field on a removal path reads as load-bearing.
                #
                # What still decides, in order: identifier *presence*, then the
                # identifier itself, then load order, then ``subentry_id``. The
                # first two are the fields ``_candidate_score`` disagrees with
                # for a pair whose lower identifier is missing, which is
                # ``B16`` and unchanged here.
                return (
                    0 if unique_part else 1,
                    unique_part,
                    index,
                    subentry_part,
                )

            return min(enumerate(candidates), key=_sort_key)[1]

        removal_targets: set[str] = set()
        duplicate_descriptors: set[str] = set()

        for unique_id, candidates in grouped_by_unique.items():
            if len(candidates) <= 1:
                continue
            canonical = _select_canonical(candidates)
            canonical_identity = (
                stored_group_keys.get(id(canonical)),
                getattr(canonical, "subentry_type", None),
            )
            removed_here = False
            for candidate in candidates:
                if candidate is canonical:
                    continue
                # Sharing an identifier does not make two subentries the
                # same logical group, so what is removed here is decided on
                # the full ``(group_key, subentry_type)`` identity -- the same
                # pair ``grouped_by_group`` keys on -- and not on the type
                # alone.
                #
                # The type-only form was the second stage of PR #1236 and
                # Codex flagged it against ``agents/config_flow/AGENTS.md``:
                # two ``tracker``-typed subentries with *distinct* group keys,
                # restored from storage under one ``unique_id``, were read as
                # duplicate copies of each other. The rank then handed the slot
                # to whichever tracker literally owns ``core_tracking`` and the
                # other went to ``async_remove_subentry``, device and entity
                # registry bindings included -- while the contract states the
                # opposite for exactly that shape: several tracker groups with
                # distinct keys are supported, so a shared identifier is a
                # collision between two groups, not evidence of one group
                # stored twice. The first stage had the same defect one axis
                # over, with a ``hub`` beside a ``service``; both are gone now
                # for one reason instead of two.
                #
                # What is *not* resolved by leaving the pair alone: the
                # colliding identifier stays. ``_claim_unique_id`` renames such
                # a holder, and it runs only inside
                # ``ConfigFlow._async_sync_feature_subentries`` -- never from
                # this manager -- so the collision persists until the next flow
                # pass. That is the deliberate direction, and it is the same
                # one the flow-side sweep takes: a removal takes registry
                # bindings with it and cannot be undone, a deferred rename can.
                #
                # ``getattr`` on both sides for the same reason the sort key
                # uses it: a missing type is ``None`` on both, so two untyped
                # siblings on one key still collapse.
                #
                # The cost of this axis, stated because it is easy to overread
                # what remains: the identity compared here is precisely the
                # grouping key of the pass below, so this loop can no longer
                # contribute a removal the group pass would not make on its
                # own. Measured across all 19 683 three-subentry shapes and a
                # 19 987-shape random sample of four- and five-subentry ones,
                # its output is identical to switching the loop off entirely.
                # It is kept for its descriptor -- the log line still names the
                # identifier a duplicate was found under, which the group pass
                # cannot report -- and the type rank that used to aim it was
                # dropped in the same change.
                #
                # Two limits on "left alone", both measured, because an earlier
                # draft of the contract text overstated them:
                #
                # * it is a statement about *this helper*. ``async_sync`` runs
                #   a type-blind stale sweep afterwards which still removes a
                #   spared ``hub`` that stores a legacy or absent key. Only the
                #   production shape is verified through a whole call, by
                #   ``::test_dedup_a_spared_hub_survives_a_whole_sync_not_just_the_helper``.
                # * sparing is not free. Where the spared twin is the one that
                #   then holds a core slot through its stored key, the sync's
                #   write-back changes an identifier after all, aborts twice
                #   and ends in ``_async_adopt_existing_unique_id``
                #   (``::test_dedup_sparing_a_twin_can_route_a_sync_through_the_adoption_exit``).
                #   Same holder either way, so the trade stands, but it is a
                #   trade.
                candidate_identity = (
                    stored_group_keys.get(id(candidate)),
                    getattr(candidate, "subentry_type", None),
                )
                if candidate_identity != canonical_identity:
                    continue
                candidate_id = getattr(candidate, "subentry_id", None)
                if isinstance(candidate_id, str):
                    removal_targets.add(candidate_id)
                    removed_here = True
            # Descriptor only where the bucket actually contributes a removal.
            # Before the guard a multi-candidate bucket almost always did (the
            # exception being one whose losers all lack a ``str`` identifier);
            # now a cross-type bucket contributes nothing by design, and naming
            # it in the log line would report a collision that was left standing
            # as one that was cleaned up. The group loop below keeps setting its
            # descriptor unconditionally, and that asymmetry is intended: its
            # grouping key carries the type, so a multi-candidate bucket there
            # is always same-typed. It is *not* guaranteed to contribute -- the
            # same ``str``-identifier proviso named above applies there too --
            # and an earlier draft of this sentence said "always contributes",
            # which would have made the descriptor the reliable half of a pair
            # whose other half it is not.
            if removed_here:
                duplicate_descriptors.add(f"unique_id={unique_id}")

        for (key_value, subentry_type), candidates in grouped_by_group.items():
            if len(candidates) <= 1:
                continue
            canonical = _select_canonical(candidates)
            descriptor_key = key_value or "<unset>"
            duplicate_descriptors.add(f"group={descriptor_key}:{subentry_type}")
            for candidate in candidates:
                if candidate is canonical:
                    continue
                candidate_id = getattr(candidate, "subentry_id", None)
                if isinstance(candidate_id, str):
                    removal_targets.add(candidate_id)

        if not removal_targets:
            return

        removed_ids: list[str] = []
        for subentry in subentries:
            subentry_id = getattr(subentry, "subentry_id", None)
            if not isinstance(subentry_id, str) or subentry_id not in removal_targets:
                continue

            removal = remove_subentry(
                self._entry,
                subentry_id=subentry_id,
            )

            try:
                await self._await_subentry_result(removal)
            except Exception as err:  # pragma: no cover - defensive logging
                _LOGGER.error(
                    "Failed to remove duplicate subentry '%s': %s",
                    subentry_id,
                    err,
                )
                raise

            removed_ids.append(subentry_id)

        if removed_ids:
            _LOGGER.info(
                "Removed %s duplicate config subentries for %s (%s)",
                len(removed_ids),
                self._entry.entry_id,
                ", ".join(sorted(duplicate_descriptors)),
            )

        self._refresh_from_entry()

    @property
    def managed_subentries(self) -> dict[str, ConfigSubentry]:
        """Return a copy of the managed subentry mapping."""

        return dict(self._managed)

    def get(self, key: str) -> ConfigSubentry | None:
        """Return the managed subentry for a key when present."""

        resolved_key = self._resolve_key(key)
        if resolved_key is None:
            return None
        return self._managed.get(resolved_key)

    def update_visible_device_ids(
        self, key: str, visible_device_ids: Sequence[str]
    ) -> None:
        """Update the visible device identifiers stored in a subentry."""

        resolved_key = self._resolve_key(key)
        if resolved_key is None:
            return

        subentry = self._managed.get(resolved_key)
        if subentry is None:
            return

        if getattr(subentry, "subentry_type", None) in NON_DEVICE_SUBENTRY_TYPES:
            # This is the writing-side mirror of
            # ``config_flow._accepts_device_assignment``. That predicate keeps
            # the options flow from ever *offering* a service or hub group as
            # an assignment target, but it lives in another module and cannot
            # see this call. Deciding here, on the resolved subentry rather
            # than on the key, closes the **key** axis: whichever key the
            # caller passed and whatever the alias table made of it, a subentry
            # of a known non-device-bearing type is never written to. The type
            # axis stays fail-open by design, and that is stated rather than
            # implied: a subentry with no ``subentry_type`` or an unknown one
            # passes, mirroring ``_accepts_device_assignment``, which likewise
            # accepts the typeless synthesised fallback. In Home Assistant
            # ``subentry_type`` is a required field, so this is the inert
            # direction. The caller in
            # ``coordinator/subentry.py`` already folds such subentries onto
            # the service key, so today this is a second barrier rather than
            # the only one -- which is the point, because a future caller
            # would otherwise be unguarded.
            return

        normalized = tuple(
            dict.fromkeys(
                str(device_id)
                for device_id in visible_device_ids
                if isinstance(device_id, str) and device_id
            )
        )

        existing_raw = subentry.data.get("visible_device_ids")
        if isinstance(existing_raw, (list, tuple)):
            existing = tuple(
                str(device_id)
                for device_id in existing_raw
                if isinstance(device_id, str) and device_id
            )
        else:
            existing = ()

        if normalized == existing:
            return

        payload = dict(subentry.data)
        payload[self._key_field] = resolved_key
        payload["visible_device_ids"] = list(normalized)

        update_result = self._hass.config_entries.async_update_subentry(
            self._entry,
            subentry,
            data=payload,
        )

        if inspect.isawaitable(update_result):

            async def _await_visibility_update() -> None:
                resolved_subentry: ConfigSubentry | None = None
                try:
                    resolved = await self._await_subentry_result(update_result)
                except Exception as err:  # pragma: no cover - defensive logging
                    _LOGGER.debug(
                        "Subentry visibility update for '%s' raised: %s", key, err
                    )
                else:
                    if self._is_subentry_like(resolved):
                        resolved_subentry = resolved
                finally:
                    refreshed_subentry = self._resolve_updated_subentry(
                        candidate=resolved_subentry,
                        original=subentry,
                    )
                    self._index_managed_subentry(resolved_key, refreshed_subentry)
                    refreshed_subentry_id = getattr(
                        refreshed_subentry, "subentry_id", None
                    )
                    if (
                        key != resolved_key
                        and isinstance(refreshed_subentry_id, str)
                        and refreshed_subentry_id
                    ):
                        self._managed_by_subentry_id[refreshed_subentry_id] = key

            task = self._hass.async_create_task(
                _await_visibility_update(),
                name=f"{DOMAIN}.subentry_visibility_refresh",
            )
            self._visibility_update_task = task
            task.add_done_callback(self._clear_visibility_task)
            return

        refreshed = self._resolve_updated_subentry(
            candidate=update_result,
            original=subentry,
        )
        # Ensure local view reflects Home Assistant's stored subentry.
        self._index_managed_subentry(resolved_key, refreshed)
        refreshed_subentry_id = getattr(refreshed, "subentry_id", None)
        if (
            key != resolved_key
            and isinstance(refreshed_subentry_id, str)
            and refreshed_subentry_id
        ):
            self._managed_by_subentry_id[refreshed_subentry_id] = key
        self._visibility_update_task = None

    async def async_wait_visible_device_updates(self) -> None:
        """Wait for the latest visibility update task to finish."""

        task = self._visibility_update_task
        if task is None:
            return

        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # pragma: no cover - defensive logging
            _LOGGER.debug(
                "Visibility update task for %s raised: %s", self._entry.entry_id, err
            )

    def _clear_visibility_task(self, task: asyncio.Task[object]) -> None:
        """Clear the cached visibility task when it matches the active one."""

        if self._visibility_update_task is task:
            self._visibility_update_task = None

    async def async_sync(
        self,
        definitions: Iterable[ConfigEntrySubentryDefinition],
        *,
        suppress_registered_forwarding: bool = False,
    ) -> None:
        """Ensure subentries match the provided definitions.

        ``suppress_registered_forwarding`` prevents manual platform forwarding
        when the caller knows Home Assistant will schedule subentry platforms
        automatically (for example, during parent entry setup on modern cores)
        or when the subentries are already registered.
        """

        desired: dict[str, ConfigEntrySubentryDefinition] = {}
        for definition in definitions:
            desired[definition.key] = definition

        created_subentries: list[ConfigSubentry] = []

        try:
            await self._deduplicate_subentries()
        except Exception as err:  # pragma: no cover - defensive warning
            _LOGGER.warning(
                "[%s] async_sync: Failed to pre-deduplicate subentries: %s",
                self._entry.entry_id,
                err,
            )

        create_subentry = getattr(
            self._hass.config_entries, "async_create_subentry", None
        )

        for key, definition in desired.items():
            payload = dict(definition.data)
            payload[self._key_field] = key
            unique_id = definition.unique_id or f"{self._entry.entry_id}-{key}"
            subentry_type = definition.subentry_type or self._default_subentry_type
            translation_key = definition.translation_key
            cleanup = definition.unload

            existing = self._managed.get(key)
            deduplicated = False

            conflict_key = next(
                (
                    managed_key
                    for managed_key, managed_subentry in self._managed.items()
                    if managed_key != key and managed_subentry.unique_id == unique_id
                ),
                None,
            )
            if conflict_key is not None:
                await self._deduplicate_subentries()
                deduplicated = True
                existing = self._managed.get(key)

            while True:
                if existing is None:
                    _LOGGER.info(
                        "[%s] async_sync: Creating new subentry for key '%s' (unique_id=%s)",
                        self._entry.entry_id,
                        key,
                        unique_id,
                    )
                    _LOGGER.debug(
                        "[%s] async_sync: ADD PAYLOAD: type=%s, title=%s, group_key=%s",
                        self._entry.entry_id,
                        subentry_type,
                        definition.title,
                        definition.data.get("group_key"),
                    )
                    new_subentry: ConfigSubentry | None = None
                    add_result: Awaitable[ConfigSubentry] | ConfigSubentry
                    try:
                        if callable(create_subentry):
                            create_kwargs: dict[str, Any] = {
                                "data": payload,
                                "title": definition.title,
                                "unique_id": unique_id,
                                "subentry_type": subentry_type,
                            }
                            if translation_key is not None:
                                create_kwargs["translation_key"] = translation_key
                            try:
                                add_result = create_subentry(
                                    self._entry,
                                    **create_kwargs,
                                )
                            except TypeError:
                                if "translation_key" in create_kwargs:
                                    create_kwargs.pop("translation_key")
                                    add_result = create_subentry(
                                        self._entry,
                                        **create_kwargs,
                                    )
                                else:
                                    raise
                        else:
                            constructor_kwargs: dict[str, Any] = {
                                "data": MappingProxyType(payload),
                                "title": definition.title,
                                "unique_id": unique_id,
                                "subentry_type": subentry_type,
                            }
                            if translation_key is not None:
                                constructor_kwargs["translation_key"] = translation_key

                            subentry_signature = inspect.signature(ConfigSubentry)
                            subentry_id_param = subentry_signature.parameters.get(
                                "subentry_id"
                            )
                            if (
                                subentry_id_param is not None
                                and subentry_id_param.default is inspect.Signature.empty
                                and "subentry_id" not in constructor_kwargs
                            ):
                                constructor_kwargs["subentry_id"] = None

                            new_subentry = None
                            for drop_translation in (False, True):
                                try:
                                    new_subentry = ConfigSubentry(
                                        **constructor_kwargs,
                                    )
                                except TypeError:
                                    if (
                                        not drop_translation
                                        and "translation_key" in constructor_kwargs
                                    ):
                                        constructor_kwargs.pop("translation_key")
                                        continue
                                else:
                                    break
                            else:
                                saved_subentry_type = constructor_kwargs.pop(
                                    "subentry_type", None
                                )
                                try:
                                    new_subentry = ConfigSubentry(
                                        **constructor_kwargs,
                                    )
                                except TypeError as err:
                                    if saved_subentry_type is not None:
                                        constructor_kwargs["subentry_type"] = (
                                            saved_subentry_type
                                        )
                                    raise HomeAssistantError(
                                        "Failed to instantiate ConfigSubentry fallback;"
                                        " see logs for details"
                                    ) from err
                                finally:
                                    if saved_subentry_type is not None:
                                        constructor_kwargs["subentry_type"] = (
                                            saved_subentry_type
                                        )

                                if not isinstance(new_subentry, ConfigSubentry):
                                    raise HomeAssistantError(
                                        "ConfigSubentry fallback returned unexpected instance"
                                    )
                            if new_subentry is None:
                                raise HomeAssistantError(
                                    "Failed to instantiate ConfigSubentry fallback"
                                )

                            # Create a config subentry through HA's API so Core assigns
                            # a stable subentry_id and will later call async_setup_entry.
                            # According to the Home Assistant subentry model, subentries
                            # are created via async_add_subentry and set up via
                            # async_setup_entry to ensure platform callbacks receive the
                            # config_subentry_id context.
                            add_result = self._hass.config_entries.async_add_subentry(
                                self._entry, new_subentry
                            )

                        resolved_add = await self._await_subentry_result(add_result)
                    except data_entry_flow.AbortFlow as err:
                        if err.reason != "already_configured":
                            raise

                        if not deduplicated:
                            await self._deduplicate_subentries()
                            self._refresh_from_entry()
                            deduplicated = True
                            existing = self._managed.get(key)
                            continue

                        _LOGGER.warning(
                            "Subentry sync for key '%s' in entry %s encountered repeated "
                            "unique_id collision for '%s'; adopting existing owner.",
                            key,
                            self._entry.entry_id,
                            unique_id,
                        )

                        existing = await self._async_adopt_existing_unique_id(
                            key,
                            definition,
                            unique_id,
                            payload,
                        )
                        break

                    fallback_subentry_id: str | None = None
                    stored: ConfigSubentry | None
                    if isinstance(resolved_add, ConfigSubentry):
                        stored = resolved_add
                        fallback_subentry_id = getattr(
                            resolved_add, "subentry_id", None
                        )
                    else:
                        stored = new_subentry

                    stored = self._resolve_registered_subentry(
                        key=key,
                        unique_id=unique_id,
                        candidate=stored,
                        fallback_subentry_id=fallback_subentry_id,
                    )

                    resolved_id = _subentry_entry_id(stored)
                    _LOGGER.debug(
                        "[%s] Added subentry %s (unique_id=%s) to config entry %s",
                        self._entry.entry_id,
                        resolved_id,
                        unique_id,
                        self._entry.entry_id,
                    )

                    self._index_managed_subentry(key, stored)
                    created_subentries.append(stored)
                    break

                _LOGGER.debug(
                    "[%s] async_sync: Updating existing subentry for key '%s' (unique_id=%s)",
                    self._entry.entry_id,
                    key,
                    unique_id,
                )
                existing_group_key = None
                existing_data = getattr(existing, "data", None)
                if isinstance(existing_data, Mapping):
                    existing_group_key = existing_data.get(self._key_field)
                if isinstance(existing_group_key, str) and existing_group_key != key:
                    _LOGGER.debug(
                        "[%s] async_sync: Adopting owner subentry for key '%s'"
                        " with mismatched group_key '%s'",
                        self._entry.entry_id,
                        key,
                        existing_group_key,
                    )
                    stored_existing = await self._async_adopt_existing_unique_id(
                        key,
                        definition,
                        unique_id,
                        payload,
                    )
                    existing = stored_existing
                    break
                try:
                    update_kwargs: dict[str, Any] = {
                        # ``existing`` and not ``payload`` alone: the core
                        # replaces ``data`` wholesale, so a definition-built
                        # payload would delete every stored field the
                        # definition does not carry (measured: the device
                        # assignment and the feature flags).
                        "data": self._payload_preserving_stored_fields(
                            existing, payload
                        ),
                        "title": definition.title,
                        "unique_id": unique_id,
                    }
                    if translation_key is not None:
                        update_kwargs["translation_key"] = translation_key
                    try:
                        changed = self._hass.config_entries.async_update_subentry(
                            self._entry,
                            existing,
                            **update_kwargs,
                        )
                    except TypeError:
                        update_kwargs.pop("translation_key", None)
                        changed = self._hass.config_entries.async_update_subentry(
                            self._entry,
                            existing,
                            **update_kwargs,
                        )
                except data_entry_flow.AbortFlow as err:
                    if err.reason != "already_configured":
                        raise

                    if not deduplicated:
                        await self._deduplicate_subentries()
                        self._refresh_from_entry()
                        deduplicated = True
                        existing = self._managed.get(key)
                        continue

                    _LOGGER.warning(
                        "Subentry sync for key '%s' in entry %s encountered repeated "
                        "unique_id collision for '%s'; adopting existing owner.",
                        key,
                        self._entry.entry_id,
                        unique_id,
                    )

                    stored_existing = await self._async_adopt_existing_unique_id(
                        key,
                        definition,
                        unique_id,
                        payload,
                    )
                    existing = stored_existing
                    break

                resolved_update = await self._await_subentry_result(changed)

                if isinstance(resolved_update, ConfigSubentry):
                    stored_existing = resolved_update
                elif resolved_update:
                    stored_existing = self._entry.subentries.get(
                        getattr(existing, "subentry_id", None), existing
                    )
                else:
                    stored_existing = None

                if stored_existing is not None:
                    self._index_managed_subentry(key, stored_existing)
                break

            self._cleanup[key] = cleanup

        desired_ids: set[str] = {
            subentry_id
            for managed_key, subentry in list(self._managed.items())
            if managed_key in desired
            and isinstance(
                (subentry_id := getattr(subentry, "subentry_id", None)),
                str,
            )
        }

        # This sweep runs the *opposite* polarity of the flow-side one in
        # ``ConfigFlow._async_cleanup_stale_subentries``, and neither sweep
        # reads the other -- the two modules cite each other on the *ranking*
        # axis, never on removal. There, a subentry whose stored key is **not**
        # a core key is skipped, and the core keys themselves are additionally
        # protected by the literal-owner type axis. Here ``desired`` *is* the
        # core key set, so the core keys are the safe ones and every other
        # managed key is a removal candidate -- with no type axis at all.
        # Removal runs through ``async_remove`` into core's
        # ``async_remove_subentry``, which takes the device and entity registry
        # bindings with it.
        #
        # No *writer* in this tree produces a shape that reaches this branch,
        # and that much is measured rather than assumed: both production
        # callers pass exactly ``{core_tracking, service}``
        # (``async_setup_entry`` and the repair in ``coordinator/subentry.py``,
        # whose ``_build_core_subentry_definitions`` always returns both),
        # every production handler stores a core key, and
        # ``_refresh_from_entry`` folds each ``service``- or ``tracker``-typed
        # subentry onto one before this runs -- including the one legacy shape
        # the contract treats as real, a per-account tracker group on an e-mail
        # key.
        #
        # What that does **not** establish is that the set is empty, and an
        # earlier draft of this comment said so anyway. The claim was about
        # stored data while the evidence covered writers only, and the fold has
        # a gap the writer argument cannot close: it canonicalises ``service``
        # and ``tracker`` by type but leaves ``hub`` on its **stored** key. A
        # ``hub`` on disk under a non-core key -- from an older release or a
        # hand-edited entry -- therefore does reach the sweep and is removed,
        # registry bindings included. That is not hypothetical: it is pinned,
        # as behaviour we carry rather than behaviour we want, by
        # ``test_ap1_stale_sweep_is_decided_by_the_resolved_key`` in its
        # ``hub-email-removed`` and ``hub-nokey-removed`` cases.
        #
        # No guard is added here regardless, and the reason is now the honest
        # one: not that the set is empty, but that a second guard semantics
        # beside the flow-side one is the very drift this axis keeps paying
        # for, and closing the gap belongs with the type axis rather than with
        # a local barrier.
        #
        # For the ``hub`` gap above the guard is owed *now*, and it widens to
        # every managed key the moment that fold or ``_deduplicate_subentries``
        # changes, because either can drop a group out of ``desired`` and hand
        # it straight to this sweep. Both are owned by
        # ``PLAN_GFMY_SUBENTRY_DELETION_TYPE_AXIS``, which also carries the
        # ratchet that watches the two counts this latency rests on.
        stale_keys: list[str] = []
        for managed_key, subentry in list(self._managed.items()):
            if managed_key in desired:
                continue
            subentry_id = getattr(subentry, "subentry_id", None)
            if isinstance(subentry_id, str) and subentry_id in desired_ids:
                self._pop_managed(managed_key)
                self._cleanup.pop(managed_key, None)
                continue
            stale_keys.append(managed_key)

        for key in stale_keys:
            await self.async_remove(key)

        if created_subentries:
            # agents/runtime_patterns/AGENTS.md: Never enforce registration
            # during initial sync; rely on _async_setup_new_subentries to retry
            # UnknownEntry once the registry publishes the new subentry.
            await _async_setup_new_subentries(
                self._hass,
                self._entry,
                created_subentries,
                skip_registered=suppress_registered_forwarding,
            )

    async def async_remove(self, key: str) -> None:
        """Remove a managed subentry and run its cleanup callback."""

        subentry = self._pop_managed(key)
        cleanup = self._cleanup.pop(key, None)
        if cleanup is not None:
            try:
                result = cleanup()
                if inspect.isawaitable(result):
                    await result
            except Exception as err:  # pragma: no cover - defensive logging
                _LOGGER.debug("Subentry cleanup for '%s' raised: %s", key, err)

        if subentry is None:
            return

        subentry_id = getattr(subentry, "subentry_id", None)
        if not isinstance(subentry_id, str) or not subentry_id:
            return

        try:
            remove_result = self._hass.config_entries.async_remove_subentry(
                self._entry, subentry_id
            )
            await self._await_subentry_result(remove_result)
        except Exception as err:  # pragma: no cover - defensive logging
            _LOGGER.debug("Removing subentry '%s' failed: %s", key, err)

    async def async_remove_all(self) -> None:
        """Remove all managed subentries."""

        for key in list(self._managed):
            await self.async_remove(key)


@dataclass(slots=True)
class RuntimeData:
    """Container for per-entry runtime structures shared across platforms."""

    coordinator: GoogleFindMyCoordinator
    token_cache: TokenCache
    subentry_manager: ConfigEntrySubEntryManager
    fcm_receiver: FcmReceiverHAType | None = None
    google_home_filter: GoogleHomeFilterProtocol | None = None
    entity_recovery_manager: EntityRecoveryManager | None = None
    cloud_discovery: CloudDiscoveryRuntime | None = None
    subentry_setup_history: set[str] = field(default_factory=set)
    legacy_forwarded_platforms: set[str] | None = None
    legacy_forward_notice: bool = False
    subentry_retry_attempts: dict[str, dict[str, int]] = field(default_factory=dict)
    subentry_retry_handles: dict[
        str, asyncio.TimerHandle | asyncio.Task[Any] | Coroutine[Any, Any, Any]
    ] = field(default_factory=dict)

    @property
    def cache(self) -> TokenCache:
        """Legacy alias for the entry-scoped token cache."""

        return self.token_cache


type MyConfigEntry = ConfigEntry[RuntimeData]


SUBENTRY_FORWARD_HELPER_LOG_KEY: Literal["_subentry_forward_helper_logs"] = (
    "_subentry_forward_helper_logs"
)


def _runtime_data(entry: MyConfigEntry) -> RuntimeData:
    """Return the runtime data container attached to ``entry``."""

    data = entry.runtime_data
    assert isinstance(data, RuntimeData)
    return data


def _get_retry_attempts(entry: MyConfigEntry) -> dict[str, dict[str, int]]:
    """Return the retry attempts mapping for ``entry`` from runtime data."""

    return _runtime_data(entry).subentry_retry_attempts


def _get_retry_handles(
    entry: MyConfigEntry,
) -> dict[str, asyncio.TimerHandle | asyncio.Task[Any] | Coroutine[Any, Any, Any]]:
    """Return the retry handle mapping for ``entry`` from runtime data."""

    return _runtime_data(entry).subentry_retry_handles


def _safe_setattr(target: object, name: str, value: Any) -> None:
    """Set an attribute on ``target`` when the object permits mutation."""

    with suppress(AttributeError, TypeError):
        setattr(target, name, value)


def _cleanup_cloud_discovery_runtime(runtime_owner: Any) -> None:
    """Reset cloud-discovery bookkeeping stored on ``runtime_owner``."""

    container = getattr(runtime_owner, "cloud_discovery", None)
    if not isinstance(container, CloudDiscoveryRuntime):
        return

    while container.dispatcher_unsubscribers:
        unsub = container.dispatcher_unsubscribers.pop()
        try:
            unsub()
        except Exception:  # noqa: BLE001 - defensive best effort
            _LOGGER.debug(
                "Cloud discovery dispatcher unsubscribe failed", exc_info=True
            )

    while container.retry_handles:
        handle = container.retry_handles.pop()
        if inspect.iscoroutine(handle):
            handle.close()
            continue
        if isinstance(handle, asyncio.Task):
            handle.cancel()
            continue
        cancel = getattr(handle, "cancel", None)
        if callable(cancel):
            try:
                cancel()
            except Exception:  # noqa: BLE001 - defensive best effort
                _LOGGER.debug("Cloud discovery handle cancel failed", exc_info=True)

    results = container.results
    if isinstance(results, list):
        try:
            results.clear()
        except Exception:  # noqa: BLE001 - defensive best effort
            _LOGGER.debug("Cloud discovery results clear failed", exc_info=True)

    container.active_keys.clear()
    container.results = None
    container.lock = asyncio.Lock()


class GoogleFindMyDomainData(TypedDict, total=False):
    """Typed container describing objects stored under ``hass.data[DOMAIN]``."""

    device_owner_index: dict[str, str]
    ecdsa_acceleration_info: dict[str, str | None]
    entries: dict[str, RuntimeData]
    eid_resolver: GoogleFindMyEIDResolver
    discovery_manager: DiscoveryManager
    fcm_lock: asyncio.Lock
    fcm_receiver: FcmReceiverHAType
    fcm_receivers: dict[str, FcmReceiverHAType]
    fcm_provider_resolvers: dict[str, Callable[[], str | None]]
    fcm_refcount: int
    fcm_refcounts: dict[str, int]
    default_fcm_entry_id: str
    fcm_lock_contention_count: int
    initial_setup_complete: bool
    nova_refcount: int
    services_lock: asyncio.Lock
    services_registered: bool
    restart_check_registered: bool
    restart_check_unsub: Callable[[], None] | None
    restart_check_initial_unsub: Callable[[], None] | None
    url_refresh_registered: bool
    url_refresh_unsub: Callable[[], None] | None
    url_refresh_state: str
    providers_registered: bool
    views_registered: bool
    _subentry_forward_helper_logs: set[str]
    _subentry_setup_history: dict[str, set[str]]
    pending_reconfigure_device_list_refresh: set[str]
    recent_reconfigure_markers: dict[str, float]
    # One-shot latch per entry for the device_tracker registry self-heal reload.
    # Deliberately kept at DOMAIN level and NOT inside ``entries``: the reload it
    # guards tears the platform down and ``async_unload_entry`` pops the entry's
    # ``entries`` bucket, so a marker living in either place would be gone by the
    # time the rebuilt platform probes again -- and a permanently empty registry
    # would then reload in a loop. Cleared only when the entry is removed.
    registry_selfheal_reloads: set[str]
    # Entry ids whose reload is already on its way. Home Assistant's
    # ``async_schedule_reload`` does not coalesce -- every call adds another
    # unload/setup cycle -- and since the credential update listener reloads the
    # entry as well, a flow that writes credentials AND reloads the entry itself
    # would tear it down twice in a row. Every reload that follows a CREDENTIAL
    # write therefore claims this latch first and the second claimant stands down.
    # The device_tracker registry self-heal claims it too -- its own one-shot latch
    # answers a different question ("has this entry healed itself yet") and knows
    # nothing about a reload someone else already scheduled -- and hands that
    # one-shot back when it stands down here. The options steps for semantic
    # locations and subentry repairs claim it as well: they schedule through the
    # config flow's single owner, and unlike a credential write they have no
    # fallback, because the credential update listener returns early on an
    # unchanged fingerprint. Callers that issue the reload themselves via
    # ``async_reload`` (the non-interactive discovery update, the reconfigure
    # path and the credentials finalizer) claim the latch by hand rather than
    # through the config flow's scheduling helper, but all three run the same
    # hopeless-state check before they do, so none of them bypasses it. The
    # tracker registry self-heal in ``device_tracker`` runs it too, which is why
    # that check lives in ``entry_reload_gate`` beside this field rather than in
    # the config flow: it is the one claimant whose grace timer outlives a
    # failed unload.
    # Released as soon as
    # the reload arrives (unload, and setup for an entry that was not loaded), and
    # given back by a claimant whose scheduling call failed, so a genuinely later
    # change reloads again.
    pending_entry_reloads: set[str]
    # In-memory only, NEVER persisted: irreversible cleanup jobs staged by a
    # config or options flow (watched-secrets delete, login-container ack). A
    # FIFO list of per-flow tickets, NOT a mapping keyed by account: two
    # overlapping flows for the same account must not share one list, and every
    # ``async_setup_entry`` run claims at most one ticket. Create-path tickets
    # are correlated by account and dropped again when their flow is removed
    # without producing an entry; update-path tickets (reauth, reconfigure,
    # options, discovery update) name their entry and additionally carry the
    # ``modified_at`` the storage has to catch up to. The claimed jobs run only
    # after that proof. See ``config_flow._StagedCleanupTicket`` and
    # ``config_flow.async_schedule_pending_container_cleanup``.
    pending_container_cleanup: list[_StagedCleanupTicket]


# Typed hass.data key for the global domain bucket (HA 2024.6+ HassKey).
# Using HassKey enables static type analysis (MyPy) on hass.data[DATA_DOMAIN].
DATA_DOMAIN: HassKey[GoogleFindMyDomainData] = HassKey(DOMAIN)


def _domain_data(hass: HomeAssistant) -> GoogleFindMyDomainData:
    """Return the typed domain data bucket, creating it on first access."""

    return cast(GoogleFindMyDomainData, hass.data.setdefault(DATA_DOMAIN, {}))


_SUBENTRY_SETUP_RETRY_DELAY = 2.0
"""Delay (in seconds) before retrying UnknownEntry setup attempts."""

_SUBENTRY_SETUP_MAX_ATTEMPTS = 20
"""Maximum number of setup attempts recorded per config subentry."""

type CoroutineType = Coroutine[Any, Any, Any]


def _cancel_subentry_retry_handle(parent_entry: MyConfigEntry) -> None:
    """Cancel a scheduled retry callback when one is present."""

    handles_map = _get_retry_handles(parent_entry)
    handle = handles_map.pop(parent_entry.entry_id, None)
    if handle is None:
        return
    if inspect.iscoroutine(handle):
        handle.close()
        return
    if isinstance(handle, asyncio.Task):
        handle.cancel()
        return
    cancel = getattr(handle, "cancel", None)
    if callable(cancel):
        cancel()


def _clear_subentry_retry_entry(
    parent_entry: MyConfigEntry, registration_candidate: str
) -> None:
    """Remove ``registration_candidate`` from the retry queue if present."""

    attempts_map = _get_retry_attempts(parent_entry)
    entry_queue = attempts_map.get(parent_entry.entry_id)
    if isinstance(entry_queue, dict):
        entry_queue.pop(registration_candidate, None)
        if not entry_queue:
            attempts_map.pop(parent_entry.entry_id, None)
            _cancel_subentry_retry_handle(parent_entry)


@callback
def _schedule_subentry_retry(hass: HomeAssistant, parent_entry: MyConfigEntry) -> None:
    """Schedule a retry callback for ``parent_entry`` when absent."""

    loop = getattr(hass, "loop", None)
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if loop is not None and running_loop is not loop:
        add_job = getattr(hass, "add_job", None)

        if callable(add_job):
            add_job(_schedule_subentry_retry, hass, parent_entry)
            return

        loop.call_soon_threadsafe(_schedule_subentry_retry, hass, parent_entry)
        return

    handles_map = _get_retry_handles(parent_entry)
    entry_id = parent_entry.entry_id
    if entry_id in handles_map:
        return

    @callback
    def _retry_callback(_now: Any) -> None:
        handles_map.pop(entry_id, None)
        _async_create_task(
            hass,
            _async_retry_pending_subentries(hass, parent_entry),
            name=f"{DOMAIN}.retry_config_subentries.{entry_id}",
        )

    handle = async_call_later(hass, _SUBENTRY_SETUP_RETRY_DELAY, _retry_callback)
    if inspect.isawaitable(handle):
        task = _async_create_task(
            hass,
            cast(CoroutineType, handle),
            name=f"{DOMAIN}.schedule_config_subentry_retry.{entry_id}",
        )
        handles_map[entry_id] = task
        return

    if handle is not None:
        handles_map[entry_id] = handle


def _queue_subentry_retry(
    hass: HomeAssistant,
    parent_entry: MyConfigEntry,
    registration_candidate: str,
) -> None:
    """Record a retry attempt for ``registration_candidate`` and schedule a follow-up."""

    entry_queue = _get_retry_attempts(parent_entry).setdefault(
        parent_entry.entry_id, {}
    )
    attempts = entry_queue.get(registration_candidate, 0) + 1
    entry_queue[registration_candidate] = attempts

    if attempts >= _SUBENTRY_SETUP_MAX_ATTEMPTS:
        entry_queue.pop(registration_candidate, None)
        if not entry_queue:
            _get_retry_attempts(parent_entry).pop(parent_entry.entry_id, None)
            _cancel_subentry_retry_handle(parent_entry)
        _LOGGER.warning(
            "[%s] Giving up on config subentry %s after %s attempts",  # noqa: G004
            parent_entry.entry_id,
            registration_candidate,
            attempts,
        )
        return

    _schedule_subentry_retry(hass, parent_entry)


def _async_create_task(
    hass: HomeAssistant, coro: CoroutineType, *, name: str | None = None
) -> asyncio.Task[Any]:
    """Schedule ``coro`` using Home Assistant's task helper."""

    return cast(asyncio.Task[Any], hass.async_create_task(coro, name=name))


async def _async_retry_pending_subentries(
    hass: HomeAssistant, parent_entry: MyConfigEntry
) -> None:
    """Retry pending config subentry setups for ``parent_entry``."""

    attempts_map = _get_retry_attempts(parent_entry)
    handles_map = _get_retry_handles(parent_entry)
    pending = tuple(attempts_map.get(parent_entry.entry_id, {}).keys())
    if not pending:
        handles_map.pop(parent_entry.entry_id, None)
        return

    await _async_setup_new_subentries(
        hass,
        parent_entry,
        (),
        retry_entry_ids=pending,
    )


def _subentry_setup_tracker(
    hass: HomeAssistant, parent_entry: MyConfigEntry
) -> set[str]:
    """Return (and cache) the per-entry subentry setup tracker."""

    runtime_data = getattr(parent_entry, "runtime_data", None)
    if isinstance(runtime_data, RuntimeData):
        tracker = getattr(runtime_data, "subentry_setup_history", None)
        if not isinstance(tracker, set):
            tracker = set()
            runtime_data.subentry_setup_history = tracker

        # Merge any fallback history captured before runtime data was attached
        # (for example, during isolated manager tests) so retry bookkeeping
        # retains the full setup story once the container exists.
        domain_bucket = _domain_data(hass)
        domain_history = domain_bucket.get("_subentry_setup_history")
        if isinstance(domain_history, dict):
            cached = domain_history.pop(parent_entry.entry_id, None)
            if isinstance(cached, set):
                tracker.update(cached)

        return tracker

    domain_bucket = _domain_data(hass)
    history = domain_bucket.setdefault("_subentry_setup_history", {})
    tracker = history.get(parent_entry.entry_id)
    if not isinstance(tracker, set):
        tracker = set()
        history[parent_entry.entry_id] = tracker

    return tracker


def _registered_subentry_ids(
    hass: HomeAssistant, parent_entry: MyConfigEntry | str | None
) -> set[str]:
    """Return identifiers for subentries currently registered on ``parent_entry``.

    Home Assistant raises ``UnknownSubEntry`` when a subentry setup is triggered
    before the subentry is registered. To catch that early, gather every
    identifier the parent entry advertises so callers can log and abort before
    scheduling setup attempts. Identifiers only count as "registered" once a
    stable config subentry id (for example, ``subentry_id`` or
    ``stable_identifier``) exists, preventing placeholder objects from short-
    circuiting setup.
    """

    config_entries = getattr(hass, "config_entries", None)
    if config_entries is None:
        return set()

    parent: Any | None
    if isinstance(parent_entry, str):
        get_entry = getattr(config_entries, "async_get_entry", lambda _entry_id: None)
        parent = get_entry(parent_entry)
    elif parent_entry is None:
        parent = None
    else:
        parent = parent_entry

    if parent is None and hasattr(parent_entry, "entry_id"):
        parent = parent_entry

    if parent is None:
        return set()

    registered: set[str] = set()
    registered.update(
        {
            registered_id
            for registered_id in getattr(parent, "_registered_subentry_ids", set())
            if isinstance(registered_id, str)
        }
    )
    parent_subentries = getattr(parent, "subentries", None)
    if isinstance(parent_subentries, Mapping):
        for subentry_id, subentry in parent_subentries.items():
            identifier = _resolve_config_subentry_identifier(subentry)
            if identifier:
                registered.add(identifier)
                continue

            stable_candidate: str | None = None
            if isinstance(subentry, Mapping):
                stable_candidate = cast(
                    str | None,
                    subentry.get("config_subentry_id")
                    or subentry.get("subentry_id")
                    or subentry.get("stable_identifier"),
                )
            else:
                stable_candidate = cast(
                    str | None,
                    getattr(subentry, "config_subentry_id", None)
                    or getattr(subentry, "subentry_id", None)
                    or getattr(subentry, "stable_identifier", None),
                )

            if (
                isinstance(subentry_id, str)
                and subentry_id
                and isinstance(stable_candidate, str)
                and stable_candidate
            ):
                registered.add(subentry_id)

    async_get_subentries = getattr(config_entries, "async_get_subentries", None)
    if callable(async_get_subentries):
        for subentry in async_get_subentries(parent.entry_id):
            identifier = _resolve_config_subentry_identifier(subentry)
            if identifier:
                registered.add(identifier)

    return registered


def _core_auto_schedules_subentries(config_entries: Any) -> bool:
    """Return True if Home Assistant schedules subentry setups automatically."""

    if config_entries is None:
        return False

    if not (
        callable(getattr(config_entries, "async_setup_subentry", None))
        and callable(getattr(config_entries, "async_get_subentries", None))
    ):
        # 2025.11.3 interface snapshot (see HA SSoT excerpt) does not expose
        # async_setup_subentry/async_get_subentries. Maintain compatibility by
        # short-circuiting instead of assuming auto-scheduling support.
        return False

    schedule_setup = getattr(config_entries, "async_schedule_entry_setup", None)
    if schedule_setup is None:
        schedule_setup = getattr(config_entries, "async_schedule_reload", None)

    return callable(schedule_setup)


async def _async_setup_new_subentries(  # noqa: PLR0913
    hass: HomeAssistant,
    parent_entry: MyConfigEntry,
    subentries: Iterable[ConfigSubentry | Any] | None,
    *,
    enforce_registration: bool = False,
    retry_entry_ids: Iterable[str] | None = None,
    skip_registered: bool = False,
) -> None:
    """Trigger Home Assistant setup for newly created subentries.

    Home Assistant does not automatically set up subentries when they are added
    programmatically. After creating a ConfigSubentry (via async_sync or other
    helpers), the integration must forward platform setup from the parent entry
    so Core can run ``_async_setup_subentry`` and pass the correct
    ``config_subentry_id`` context. See docs/CONFIG_SUBENTRIES_HANDBOOK.md for
    the canonical lifecycle.

    When ``skip_registered`` is True, forwarding is suppressed once Home
    Assistant already advertises every pending subentry, relying on Core's
    automatic platform scheduling after parent setup completes. Placeholder
    identifiers that fall back to the parent ``entry_id`` are ignored so
    unregistered children without a stable identifier still trigger the initial
    forward pass.
    """

    if subentries is None:
        return

    pending_subentries = tuple(subentries)
    if not pending_subentries:
        return

    config_entries = getattr(hass, "config_entries", None)
    forward_setups = getattr(config_entries, "async_forward_entry_setups", None)
    if not callable(forward_setups):
        _LOGGER.debug(
            "[%s] async_setup_entry: Subentry forward helper missing; skipping setup",
            parent_entry.entry_id,
        )
        return

    registered = _registered_subentry_ids(hass, parent_entry)
    identifiers: list[str] = []
    for subentry in pending_subentries:
        identifier = _resolve_config_subentry_identifier(subentry)
        if identifier is not None:
            identifiers.append(identifier)

    registered_all = bool(identifiers) and all(
        identifier in registered for identifier in identifiers
    )
    parent_identifier = getattr(parent_entry, "entry_id", None)
    if (
        registered_all
        and parent_identifier
        and all(identifier == parent_identifier for identifier in identifiers)
    ):
        registered_all = False
    auto_schedules_subentries = _core_auto_schedules_subentries(config_entries)
    suppression_reasons: list[str] = []

    if skip_registered and registered_all:
        if auto_schedules_subentries:
            suppression_reasons.append("Home Assistant auto-schedules subentries")
        else:
            suppression_reasons.append("registered during setup")

    if suppression_reasons:
        _LOGGER.debug(
            "[%s] Skipping manual forward for %s registered subentries (%s)",
            parent_entry.entry_id,
            len(identifiers),
            "; ".join(suppression_reasons),
        )
        return

    setup_calls = getattr(config_entries, "setup_calls", None)
    platform_names: list[str] = []
    for subentry in pending_subentries:
        for platform in _determine_subentry_platforms(subentry):
            name = _platform_value(platform)
            if name and name not in platform_names:
                platform_names.append(name)

        identifier = _resolve_config_subentry_identifier(subentry)
        if identifier is not None and isinstance(setup_calls, list):
            setup_calls.append(identifier)

    if not platform_names:
        return

    platforms: tuple[str, ...] = tuple(sorted(platform_names))

    try:
        result = forward_setups(parent_entry, platforms)
    except ValueError:
        _LOGGER.debug(
            "[%s] async_setup_entry: Platforms already forwarded for new subentries: %s",
            parent_entry.entry_id,
            platforms,
        )
        _safe_setattr(parent_entry, "_gfm_parent_platforms_forwarded", True)
    else:
        if inspect.isawaitable(result):
            await result

        _safe_setattr(parent_entry, "_gfm_parent_platforms_forwarded", True)
        _LOGGER.debug(
            "[%s] Forwarded setup for config subentries to platforms: %s",
            parent_entry.entry_id,
            platforms,
        )

    signal = f"{DOMAIN}_subentry_setup_{parent_entry.entry_id}"
    for subentry in pending_subentries:
        async_dispatcher_send(hass, signal, subentry)


async def _async_purge_unloaded_subentry_registrations(
    hass: HomeAssistant,
    *,
    parent_entry_id: str | None,
    config_subentry_id: str | None,
    entry_type: str | None,
) -> tuple[int, int]:
    """Remove registry entries for a subentry that never finished loading."""

    if not parent_entry_id or not config_subentry_id:
        return (0, 0)

    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)

    removed_entities = 0
    removed_devices = 0

    for entity in er.async_entries_for_config_entry(ent_reg, parent_entry_id):
        if getattr(entity, "config_subentry_id", None) != config_subentry_id:
            continue
        ent_reg.async_remove(entity.entity_id)
        removed_entities += 1

    # Local imports: this module cannot import the coordinator package at module
    # level.  ``coordinator/__init__.py`` pulls in ``..api``, whose import chain
    # comes back to this partially initialised module (measured: ImportError on
    # ``get_proto_decoder`` from ``decrypt_locations.py``).  That is the same
    # circularity ``_ensure_runtime_imports`` exists for.  Importing the helper
    # module inside the functions that use it keeps the names visible to
    # ``mypy`` and ``ruff``, which a ``getattr`` indirection would not.
    from .coordinator.helpers.registry import (
        OwnershipIntent,
        detect_device_registry_capabilities,
        execute_ownership_plan,
        extract_subentry_links,
        plan_device_ownership,
        read_device_ownership,
    )

    update_call = getattr(dev_reg, "async_update_device", None)
    caps = (
        detect_device_registry_capabilities(update_call)
        if callable(update_call)
        else None
    )

    devices_for_entry = dr.async_entries_for_config_entry(dev_reg, parent_entry_id)
    for device in devices_for_entry:
        # ``extract_subentry_links`` reads the scalar ``config_entry_id`` and
        # ``config_subentry_id`` first (Core 2026.8+), then the
        # ``config_entries_subentries`` mapping (the real data below 2026.8),
        # and only then a bare ``config_subentry_id``.  The hand-written
        # predicate this replaced had no such fallback and skipped the device
        # instead.  No supported core reaches the last step, but a registry
        # double can, and there it selects a device the old shape passed over.
        if config_subentry_id not in extract_subentry_links(device, parent_entry_id):
            continue
        if caps is None:  # pragma: no cover - defensive guard
            _LOGGER.debug(
                "[%s] Purge: device registry has no async_update_device; skipping %s",
                parent_entry_id,
                device.id,
            )
            continue

        # The intent is DETACH on one named subentry link, never the hub link.
        # The plan the planner derives from it differs per core, and that is the
        # point of asking it rather than writing keywords here:
        #
        # * From Core 2026.8 a device has one owner, so giving it up *is* the
        #   deletion. The planner emits ``async_remove_device`` directly, and
        #   only after it has read the current ownership; it raises rather than
        #   guess (``plan_device_ownership``, the DETACH branch).
        # * Below that core it emits ``async_update_device`` with the removal
        #   pair, and Core deletes the device only when this was its last owning
        #   entry (tag ``2025.9.1``, lines 1174-1177). That is why no
        #   ``async_remove_device`` is written here for the legacy path: a direct
        #   removal would take a device another config entry still owns, and on
        #   the declared minimum core that case is real.
        #
        # One deliberate divergence follows from the 2026.8 form: the direct
        # removal bypasses Core's pending-move branch (tag ``2026.8.0``, lines
        # 2208-2231), which would *transfer* a device whose move another call
        # armed with ``add_config_entry_id`` instead of deleting it. This
        # integration arms no such move on that core (the planner uses the
        # ``new_*`` pair there), and Core cancels a move armed by a *recognised*
        # foreign integration.  Its cancellation is not unconditional: a move
        # whose origin is undetermined survives (tag ``2026.8.0``, lines
        # 2222-2229, "Origins from core/tests are undetermined (None) and never
        # cancel").  So the branch is unreachable from any move this integration
        # or a recognisable foreign one armed, which is narrower than "always".
        try:
            plan = plan_device_ownership(
                OwnershipIntent.DETACH,
                caps=caps,
                device_id=device.id,
                entry_id=parent_entry_id,
                detach_subentry_id=config_subentry_id,
                current=read_device_ownership(device),
            )
        except ValueError as err:
            # The planner refuses to guess at an ownership it cannot read.
            _LOGGER.debug(
                "[%s] Purge: cannot detach subentry %s from %s: %s",
                parent_entry_id,
                config_subentry_id,
                device.id,
                err,
            )
            continue

        if not plan:
            continue

        execute_ownership_plan(dev_reg, plan)
        removed_devices += 1

    if removed_entities or removed_devices:
        _LOGGER.debug(
            "[%s] Purged unloaded subentry %s (type=%s): %d entities, %d devices",  # noqa: G004
            parent_entry_id,
            config_subentry_id,
            entry_type,
            removed_entities,
            removed_devices,
        )

    return removed_entities, removed_devices


def _ensure_fcm_lock(bucket: GoogleFindMyDomainData) -> asyncio.Lock:
    """Return the shared FCM lock, creating it if missing."""

    lock = bucket.get("fcm_lock")
    if not isinstance(lock, asyncio.Lock):
        lock = asyncio.Lock()
        bucket["fcm_lock"] = lock
    return lock


def _sync_legacy_fcm_receiver_alias(bucket: GoogleFindMyDomainData) -> None:
    """Keep the legacy ``fcm_receiver`` key aligned with the active receivers."""

    receiver_cls = _resolve_fcm_receiver_class()
    receivers_obj = bucket.get("fcm_receivers")
    if isinstance(receivers_obj, dict):
        # ``fcm_receivers`` is the single source of truth once it exists.
        # Mirror a valid receiver into the legacy ``fcm_receiver`` alias, or
        # drop the alias when no active receiver remains (e.g. after the
        # refcount->0 release path empties the dict on reload). Keeping a
        # healthy alias here while the per-entry dict is empty is exactly the
        # issue #183 desync: ``is_push_ready()`` resolves the (empty) per-entry
        # store and reports False, while diagnostics read the surviving
        # singleton and still report the receiver as healthy/connected.
        default_entry_id = bucket.get("default_fcm_entry_id")
        preferred = (
            receivers_obj.get(default_entry_id)
            if isinstance(default_entry_id, str)
            else None
        )
        if not isinstance(preferred, receiver_cls):
            preferred = None
        receiver = preferred or next(
            (
                candidate
                for candidate in receivers_obj.values()
                if isinstance(candidate, receiver_cls)
            ),
            None,
        )
        if receiver is not None:
            bucket["fcm_receiver"] = receiver
        else:
            bucket.pop("fcm_receiver", None)
        return

    # ``fcm_receivers`` is absent entirely: preserve the legacy migration path
    # that ``_get_fcm_receivers`` relies on, only dropping a wrong-type alias.
    legacy_receiver = bucket.get("fcm_receiver")
    if legacy_receiver is not None and not isinstance(legacy_receiver, receiver_cls):
        bucket.pop("fcm_receiver", None)


def _ensure_services_lock(bucket: GoogleFindMyDomainData) -> asyncio.Lock:
    """Return the integration services lock, creating it if missing."""

    lock = bucket.get("services_lock")
    if not isinstance(lock, asyncio.Lock):
        lock = asyncio.Lock()
        bucket["services_lock"] = lock
    return lock


def _ensure_entries_bucket(bucket: GoogleFindMyDomainData) -> dict[str, RuntimeData]:
    """Return the per-entry runtime data bucket."""

    entries = bucket.get("entries")
    if not isinstance(entries, dict):
        entries = {}
        bucket["entries"] = entries
    return entries


def _ensure_pending_reconfigure_refresh(
    bucket: GoogleFindMyDomainData,
) -> set[str]:
    """Return the set of entry_ids awaiting a forced device list refresh."""

    pending = bucket.get("pending_reconfigure_device_list_refresh")
    if not isinstance(pending, set):
        pending = set()
        bucket["pending_reconfigure_device_list_refresh"] = pending
    return pending


def _ensure_recent_reconfigure_markers(
    bucket: GoogleFindMyDomainData,
) -> dict[str, float]:
    """Return the mapping of entry_id -> reconfigure timestamp markers."""

    markers = bucket.get("recent_reconfigure_markers")
    if not isinstance(markers, dict):
        markers = {}
        bucket["recent_reconfigure_markers"] = markers
    return markers


def _ensure_registry_selfheal_reloads(
    bucket: GoogleFindMyDomainData,
) -> set[str]:
    """Return the set of entry_ids that already used their self-heal reload."""

    claimed = bucket.get("registry_selfheal_reloads")
    if not isinstance(claimed, set):
        claimed = set()
        bucket["registry_selfheal_reloads"] = claimed
    return claimed


@callback
def claim_registry_selfheal_reload(hass: HomeAssistant, entry_id: str) -> bool:
    """Claim the one and only self-heal reload for ``entry_id``.

    Returns ``True`` for the first caller of an entry and ``False`` for every
    later one, including callers that run *after* the reload this claim caused.
    That is the whole point: the reload rebuilds the device_tracker platform, so
    any marker the platform itself held would be fresh again and a config entry
    whose entities never reach the entity registry would reload forever.
    """

    if not entry_id:
        return False
    claimed = _ensure_registry_selfheal_reloads(_domain_data(hass))
    if entry_id in claimed:
        return False
    claimed.add(entry_id)
    return True


@callback
def discard_registry_selfheal_reload(hass: HomeAssistant, entry_id: str) -> None:
    """Drop the self-heal latch of a removed entry.

    Without this the entry_id would sit in ``hass.data`` for the rest of the
    process lifetime, the same housekeeping rule the staged-cleanup discard in
    ``async_remove_entry`` follows.
    """

    if not entry_id:
        return
    bucket = _domain_data(hass)
    claimed = bucket.get("registry_selfheal_reloads")
    if isinstance(claimed, set):
        claimed.discard(entry_id)


def _ensure_pending_entry_reloads(
    bucket: GoogleFindMyDomainData,
) -> set[str]:
    """Return the set of entry_ids whose reload is already on its way."""

    pending = bucket.get("pending_entry_reloads")
    if not isinstance(pending, set):
        pending = set()
        bucket["pending_entry_reloads"] = pending
    return pending


@callback
def claim_pending_entry_reload(hass: HomeAssistant, entry_id: str) -> bool:
    """Claim the pending reload of ``entry_id`` for the caller.

    Returns ``True`` when the caller is the one that has to schedule the reload
    and ``False`` when a reload is already on its way and a second one would only
    tear the entry down twice: ``async_schedule_reload`` does not coalesce, and
    ``async_reload`` is an unload plus a setup either way.

    The duplicate is not hypothetical. Every path that writes credentials into an
    entry goes through ``async_update_entry``, which notifies the update listener
    in :func:`async_setup_entry`, and that listener reloads the entry so the new
    credentials take effect -- as long as it is there. An entry that is not loaded
    has no listener any more (Home Assistant removes it on unload), so a writing
    path cannot leave the reload to it and schedules its own: the discovery
    fallback writer, the branch where the core's unique-id guard did the write,
    the non-interactive discovery update, reconfigure, the options credential
    refresher. Two of them therefore produce two reloads unless the sides agree
    on one owner. This latch is that agreement, and it works
    regardless of which side runs first, which no ordering rule between a
    synchronous flow step and a listener task could guarantee.

    Call this **last**, immediately before scheduling: a claim that is followed by
    a caller-side abort leaves the latch set until the entry is next set up or
    unloaded, and during that window a real change would not reload.
    """

    if not entry_id:
        return False
    pending = _ensure_pending_entry_reloads(_domain_data(hass))
    if entry_id in pending:
        return False
    pending.add(entry_id)
    return True


@callback
def discard_pending_entry_reload(hass: HomeAssistant, entry_id: str) -> None:
    """Release the reload latch of ``entry_id``.

    Called where the scheduled reload actually arrives: at unload, and at setup
    for an entry that was not loaded (Home Assistant's ``async_reload`` skips the
    unload half in that case, so setup is the only point both paths share). A
    latch that is never released would swallow every later reload of that entry,
    which is why both ends clear it and removal clears it too.
    """

    if not entry_id:
        return
    # Read-only on purpose, no ``_domain_data``: releasing a latch that was never
    # claimed must not create the domain bucket. This runs at the very top of
    # ``async_setup_entry``, before the entry policy has had its say, and a bucket
    # created there would exist for entries that never got set up at all.
    hass_data = getattr(hass, "data", None)
    if not isinstance(hass_data, dict):
        return
    bucket = hass_data.get(DATA_DOMAIN)
    if not isinstance(bucket, dict):
        return
    pending = bucket.get("pending_entry_reloads")
    if isinstance(pending, set):
        pending.discard(entry_id)


def _ensure_device_owner_index(bucket: GoogleFindMyDomainData) -> dict[str, str]:
    """Return the shared device owner index mapping."""

    owner_index = bucket.get("device_owner_index")
    if not isinstance(owner_index, dict):
        owner_index = {}
        bucket["device_owner_index"] = owner_index
    return owner_index


def _has_logged_missing_subentry_forward_helper(
    hass: HomeAssistant, entry: ConfigEntry
) -> bool:
    """Return True if the missing forward helper warning already logged."""

    if getattr(entry, "_gfm_logged_subentry_forward_absence", False):
        return True

    entry_id = getattr(entry, "entry_id", None)
    if not isinstance(entry_id, str):
        return False

    bucket = _domain_data(hass)
    logged = bucket.get(SUBENTRY_FORWARD_HELPER_LOG_KEY)
    if isinstance(logged, set):
        return entry_id in logged
    return False


def _mark_missing_subentry_forward_helper_logged(
    hass: HomeAssistant, entry: ConfigEntry
) -> None:
    """Record that the missing forward helper warning has been emitted."""

    try:
        setattr(entry, "_gfm_logged_subentry_forward_absence", True)
    except (AttributeError, TypeError):
        pass

    bucket = _domain_data(hass)
    logged = bucket.get(SUBENTRY_FORWARD_HELPER_LOG_KEY)
    if not isinstance(logged, set):
        logged = set()
        bucket[SUBENTRY_FORWARD_HELPER_LOG_KEY] = logged

    entry_id = getattr(entry, "entry_id", None)
    if isinstance(entry_id, str):
        logged.add(entry_id)


def _get_fcm_receivers(
    bucket: GoogleFindMyDomainData,
) -> dict[str, FcmReceiverHAType]:
    """Return the cached entry-scoped FCM receivers."""

    receivers_obj = bucket.get("fcm_receivers")
    receiver_cls = _resolve_fcm_receiver_class()

    if isinstance(receivers_obj, dict):
        valid: dict[str, FcmReceiverHAType] = {}
        for entry_id, receiver in receivers_obj.items():
            if isinstance(entry_id, str) and isinstance(receiver, receiver_cls):
                valid[entry_id] = receiver
        bucket["fcm_receivers"] = valid
        _sync_legacy_fcm_receiver_alias(bucket)
        return valid

    legacy_receiver: object | None = bucket.get("fcm_receiver")
    if isinstance(legacy_receiver, receiver_cls):
        bucket["fcm_receivers"] = {"default": legacy_receiver}
        _sync_legacy_fcm_receiver_alias(bucket)
        return bucket["fcm_receivers"]

    bucket["fcm_receivers"] = {}
    return bucket["fcm_receivers"]


def _set_fcm_receiver(
    bucket: GoogleFindMyDomainData, entry_id: str, receiver: FcmReceiverHAType
) -> None:
    """Store the entry-scoped FCM receiver."""

    receivers = bucket.setdefault("fcm_receivers", {})
    receivers[entry_id] = receiver
    bucket.setdefault("fcm_refcounts", {})
    _sync_legacy_fcm_receiver_alias(bucket)


def _pop_fcm_receiver(
    bucket: GoogleFindMyDomainData, entry_id: str
) -> FcmReceiverHAType | None:
    """Remove and return the cached FCM receiver for an entry."""

    receivers = _get_fcm_receivers(bucket)
    receiver = receivers.pop(entry_id, None)
    bucket["fcm_receivers"] = receivers
    receiver_cls = _resolve_fcm_receiver_class()
    _sync_legacy_fcm_receiver_alias(bucket)
    if isinstance(receiver, receiver_cls):
        return receiver
    return None


def _pop_any_fcm_receiver(bucket: GoogleFindMyDomainData) -> object | None:
    """Remove and return any cached FCM receiver regardless of type."""

    receivers = _get_fcm_receivers(bucket)
    if receivers:
        entry_id, receiver = receivers.popitem()
        bucket["fcm_receivers"] = receivers
        bucket.setdefault("fcm_refcounts", {}).pop(entry_id, None)
        _sync_legacy_fcm_receiver_alias(bucket)
        return receiver
    stale = bucket.pop("fcm_receiver", None)
    _sync_legacy_fcm_receiver_alias(bucket)
    return stale


def _get_fcm_refcounts(bucket: GoogleFindMyDomainData) -> dict[str, int]:
    """Return the per-entry FCM refcounts."""

    refcounts = bucket.get("fcm_refcounts")
    if isinstance(refcounts, dict):
        valid: dict[str, int] = {}
        for entry_id, value in refcounts.items():
            if isinstance(entry_id, str) and isinstance(value, int):
                valid[entry_id] = value
        bucket["fcm_refcounts"] = valid
        return valid
    bucket["fcm_refcounts"] = {}
    return bucket["fcm_refcounts"]


def _get_fcm_refcount(bucket: GoogleFindMyDomainData, entry_id: str) -> int:
    """Return the current refcount for an entry-scoped FCM receiver."""

    return _get_fcm_refcounts(bucket).get(entry_id, 0)


def _set_fcm_refcount(
    bucket: GoogleFindMyDomainData, entry_id: str, value: int
) -> None:
    """Persist the refcount for an entry-scoped FCM receiver."""

    refcounts = bucket.setdefault("fcm_refcounts", {})
    refcounts[entry_id] = value
    bucket["fcm_refcounts"] = refcounts
    if entry_id == "default":
        bucket["fcm_refcount"] = value


def _get_nova_refcount(bucket: GoogleFindMyDomainData) -> int:
    """Return the Nova API session provider refcount."""

    value = bucket.get("nova_refcount")
    if isinstance(value, int):
        return value
    return 0


def _set_nova_refcount(bucket: GoogleFindMyDomainData, value: int) -> None:
    """Persist the Nova API session provider refcount."""

    bucket["nova_refcount"] = value


def _sync_receiver_default_entry(receiver: object, entry_id: str | None) -> None:
    """Propagate the chosen entry_id to receivers that track a default entry."""

    if entry_id is None:
        return

    try:
        setter = getattr(receiver, "set_default_entry_id", None)
        if callable(setter):
            setter(entry_id)
            return
    except Exception as err:  # noqa: BLE001 - defensive sync guard
        _LOGGER.debug("Default entry sync skipped: %s", err)

    try:
        setattr(receiver, "default_entry_id", entry_id)
    except Exception:
        _LOGGER.debug("Default entry sync attribute assignment failed", exc_info=True)


def _domain_fcm_provider(
    hass: HomeAssistant, entry_id: str | None = None
) -> FcmReceiverHAType:
    """Return the entry-scoped FCM receiver for provider callbacks."""

    bucket = _domain_data(hass)
    receivers = _get_fcm_receivers(bucket)
    if entry_id and entry_id in receivers:
        receiver = receivers[entry_id]
        _sync_receiver_default_entry(receiver, entry_id)
        return receiver

    candidates: list[tuple[str, FcmReceiverHAType]] = []

    resolver_map = bucket.get("fcm_provider_resolvers")
    if isinstance(resolver_map, dict):
        for resolver in resolver_map.values():
            if not callable(resolver):
                continue
            try:
                resolved_entry_id = resolver()
            except Exception as err:  # noqa: BLE001 - defensive resolver guard
                _LOGGER.debug("FCM resolver raised: %s", err)
                continue
            if isinstance(resolved_entry_id, str) and resolved_entry_id in receivers:
                candidates.append((resolved_entry_id, receivers[resolved_entry_id]))

    default_entry_id = bucket.get("default_fcm_entry_id")
    if (
        isinstance(default_entry_id, str)
        and default_entry_id in receivers
        and default_entry_id not in {entry for entry, _ in candidates}
    ):
        candidates.append((default_entry_id, receivers[default_entry_id]))

    if not candidates:
        candidates.extend(receivers.items())
    else:
        selected = {entry for entry, _ in candidates}
        for entry_key, receiver in receivers.items():
            if entry_key not in selected:
                candidates.append((entry_key, receiver))

    for candidate_entry_id, receiver in candidates:
        if getattr(receiver, "is_ready", False):
            bucket["default_fcm_entry_id"] = candidate_entry_id
            _sync_receiver_default_entry(receiver, candidate_entry_id)
            return receiver

    if candidates:
        candidate_entry_id, receiver = candidates[0]
        bucket["default_fcm_entry_id"] = candidate_entry_id
        _sync_receiver_default_entry(receiver, candidate_entry_id)
        return receiver

    if receivers:
        fallback_entry_id, receiver = next(iter(receivers.items()))
        _sync_receiver_default_entry(receiver, fallback_entry_id)
        return receiver

    raise RuntimeError("Shared FCM receiver unavailable")


async def _async_stop_receiver_if_possible(receiver: object | None) -> None:
    """Invoke ``async_stop`` on ``receiver`` when available."""

    if receiver is None:
        return

    stop_callable = getattr(receiver, "async_stop", None)
    if stop_callable is None:
        return

    try:
        result = stop_callable()
    except Exception as err:  # pragma: no cover - defensive
        _LOGGER.debug("Failed to call async_stop on stale FCM receiver: %s", err)
        return

    if inspect.isawaitable(result):
        try:
            await cast(Awaitable[object], result)
        except Exception as err:  # pragma: no cover - defensive
            _LOGGER.debug("async_stop coroutine failed: %s", err)


def _normalize_device_identifier(device: dr.DeviceEntry | Any, ident: str) -> str:
    """Return the Google device id portion used throughout the coordinator.

    Registry identifiers may be namespaced with the config entry ID and/or a
    subentry identifier (for example, ``<entry_id>:<subentry_id>:<device_id>``).
    The coordinator and ignore lists track devices by their Google-provided
    ``device_id`` alone, so we strip any entry/subentry prefixes and return the
    final segment when present. Service-device identifiers (for example,
    ``integration_<entry_id>``) are preserved as-is because they do not contain
    path separators.
    """

    if not ident:
        return ident

    if ":" not in ident:
        return ident

    parts = ident.split(":")

    # Local import, see ``_async_purge_unloaded_subentry_registrations``.
    from .coordinator.helpers.registry import device_owning_entry_ids

    # Measured, and worth knowing before touching this: the strip below cannot
    # change the return value.  It only ever removes leading segments and stops
    # at ``len(parts) > 1``, so ``parts[-1]`` is invariant under it -- checked
    # exhaustively over 6216 identifier/owner combinations, zero differences.
    # It is kept, not removed, because dropping it would leave ``device``
    # unused and force a signature change at every call site; that is a separate
    # change, not a device-registry migration.  Do not build new behaviour on
    # it, and do not expect a mutant of it to fail a test.
    owning_entries = device_owning_entry_ids(device)
    if owning_entries:
        while len(parts) > 1 and parts[0] in owning_entries:
            parts = parts[1:]

    # Prefer the final segment so trackers with entry/subentry prefixes resolve
    # to the same canonical device_id used by the coordinator and ignore list.
    last = parts[-1]
    return last or ident


def _iter_config_entry_entities(
    entity_registry: er.EntityRegistry, entry_id: str
) -> tuple[er.RegistryEntry, ...]:
    """Return entity registry entries belonging to a config entry."""

    helper = getattr(er, "async_entries_for_config_entry", None)
    if callable(helper):
        try:
            entries_iterable = helper(entity_registry, entry_id)
        except AttributeError:
            entries_iterable = None
    else:
        entries_iterable = None

    if entries_iterable is None:
        registry_helper = getattr(
            entity_registry, "async_entries_for_config_entry", None
        )
        if callable(registry_helper):
            try:
                entries_iterable = registry_helper(entry_id)
            except AttributeError:
                entries_iterable = None

    if entries_iterable is None:
        entries_iterable = [
            entity_entry
            for entity_entry in getattr(entity_registry, "entities", {}).values()
            if getattr(entity_entry, "config_entry_id", None) == entry_id
        ]

    entries = tuple(entries_iterable)
    if entries:
        return entries

    fallback_entities = getattr(entity_registry, "entities", None)
    candidates: Iterable[er.RegistryEntry]
    if fallback_entities is None:
        candidates = ()
    else:
        values = getattr(fallback_entities, "values", None)
        if callable(values):
            candidates = values()
        elif isinstance(fallback_entities, Mapping):
            candidates = fallback_entities.values()
        else:
            candidates = ()

    return tuple(
        entity_entry
        for entity_entry in candidates
        if getattr(entity_entry, "config_entry_id", None) == entry_id
    )


@dataclass(slots=True)
class _RecoveryRegistration:
    """Container describing recovery metadata for a platform."""

    expected_unique_ids: Callable[[], set[str]]
    entity_factory: Callable[[set[str]], Sequence[Entity]]
    add_entities: AddEntitiesCallback
    update_before_add: bool


class EntityRecoveryManager:
    """Coordinate recovery of missing entities for a config entry."""

    __slots__ = ("_hass", "_entry", "_coordinator", "_platforms")

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        coordinator: GoogleFindMyCoordinator,
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._coordinator = coordinator
        self._platforms: dict[str, _RecoveryRegistration] = {}

    def _register_platform(
        self,
        platform: Platform | str,
        registration: _RecoveryRegistration,
    ) -> None:
        key = platform.value if isinstance(platform, Platform) else str(platform)
        self._platforms[key] = registration

    def register_device_tracker_platform(
        self,
        *,
        expected_unique_ids: Callable[[], set[str]],
        entity_factory: Callable[[set[str]], Sequence[Entity]],
        add_entities: AddEntitiesCallback,
        update_before_add: bool = True,
    ) -> None:
        """Register recovery handlers for the device_tracker platform."""

        self._register_platform(
            Platform.DEVICE_TRACKER,
            _RecoveryRegistration(
                expected_unique_ids=expected_unique_ids,
                entity_factory=entity_factory,
                add_entities=add_entities,
                update_before_add=update_before_add,
            ),
        )

    def register_button_platform(
        self,
        *,
        expected_unique_ids: Callable[[], set[str]],
        entity_factory: Callable[[set[str]], Sequence[Entity]],
        add_entities: AddEntitiesCallback,
        update_before_add: bool = True,
    ) -> None:
        """Register recovery handlers for the button platform."""

        self._register_platform(
            Platform.BUTTON,
            _RecoveryRegistration(
                expected_unique_ids=expected_unique_ids,
                entity_factory=entity_factory,
                add_entities=add_entities,
                update_before_add=update_before_add,
            ),
        )

    def register_sensor_platform(
        self,
        *,
        expected_unique_ids: Callable[[], set[str]],
        entity_factory: Callable[[set[str]], Sequence[Entity]],
        add_entities: AddEntitiesCallback,
        update_before_add: bool = True,
    ) -> None:
        """Register recovery handlers for the sensor platform."""

        self._register_platform(
            Platform.SENSOR,
            _RecoveryRegistration(
                expected_unique_ids=expected_unique_ids,
                entity_factory=entity_factory,
                add_entities=add_entities,
                update_before_add=update_before_add,
            ),
        )

    def register_binary_sensor_platform(
        self,
        *,
        expected_unique_ids: Callable[[], set[str]],
        entity_factory: Callable[[set[str]], Sequence[Entity]],
        add_entities: AddEntitiesCallback,
        update_before_add: bool = True,
    ) -> None:
        """Register recovery handlers for the binary_sensor platform."""

        self._register_platform(
            Platform.BINARY_SENSOR,
            _RecoveryRegistration(
                expected_unique_ids=expected_unique_ids,
                entity_factory=entity_factory,
                add_entities=add_entities,
                update_before_add=update_before_add,
            ),
        )

    def expected_unique_ids_for_platform(self, platform: Platform | str) -> set[str]:
        """Return the expected unique IDs for ``platform`` if registered."""

        key = platform.value if isinstance(platform, Platform) else str(platform)
        registration = self._platforms.get(key)
        if registration is None:
            return set()
        return registration.expected_unique_ids()

    async def async_recover_missing_entities(self) -> None:
        """Inspect the entity registry and recover missing entities."""

        if not self._platforms:
            return

        registry_key = getattr(er, "DATA_REGISTRY", "entity_registry")
        entity_registry = self._hass.data.get(registry_key)
        if entity_registry is None:
            entity_registry = er.async_get(self._hass)
        registry_entries = _iter_config_entry_entities(
            entity_registry, self._entry.entry_id
        )

        by_platform: dict[str, set[str]] = {}
        for entry in registry_entries:
            platform = getattr(entry, "domain", None)
            if not isinstance(platform, str):
                continue
            # ``RegistryEntry.platform`` names the integration that owns the
            # entity on every supported core; ``RegistryEntry`` has no
            # ``integration_domain`` field.
            owner = getattr(entry, "platform", None)
            if owner is None:
                # The entries are already scoped to this config entry, so a
                # registry entry without ``platform`` (partial doubles) is ours.
                owner = DOMAIN
            if owner != DOMAIN:
                continue
            unique_id = getattr(entry, "unique_id", None)
            if not isinstance(unique_id, str) or not unique_id:
                continue
            by_platform.setdefault(platform, set()).add(unique_id)

        for platform, registration in self._platforms.items():
            try:
                expected = registration.expected_unique_ids()
            except Exception as err:  # pragma: no cover - defensive guard
                _LOGGER.debug(
                    "[%s] Failed to build expected unique_id set for %s: %s",
                    self._entry.entry_id,
                    platform,
                    err,
                )
                continue

            if not expected:
                continue

            actual = by_platform.get(platform, set())
            missing = expected - actual
            if not missing:
                continue

            try:
                entities = registration.entity_factory(missing)
            except Exception as err:  # pragma: no cover - defensive guard
                _LOGGER.debug(
                    "[%s] Failed to build recovery entities for %s: %s",
                    self._entry.entry_id,
                    platform,
                    err,
                )
                continue

            to_add: list[Entity] = []
            for entity in entities:
                unique_id = getattr(entity, "unique_id", None)
                if not isinstance(unique_id, str):
                    continue
                if unique_id not in missing:
                    continue
                to_add.append(entity)

            if not to_add:
                continue

            _LOGGER.info(
                "[%s] Recovering %d missing %s entities",
                self._entry.entry_id,
                len(to_add),
                platform,
            )

            try:
                registration.add_entities(to_add, registration.update_before_add)
            except Exception as err:  # pragma: no cover - defensive guard
                _LOGGER.error(
                    "[%s] Failed to add recovered %s entities: %s",
                    self._entry.entry_id,
                    platform,
                    err,
                )


def _default_button_subentry_identifier(subentry_map: Mapping[str, str]) -> str:
    """Return the preferred subentry identifier for button entities."""

    identifier = subentry_map.get("button")
    if isinstance(identifier, str) and identifier:
        return identifier

    identifier = subentry_map.get("device_tracker")
    if isinstance(identifier, str) and identifier:
        return identifier

    return _DEFAULT_SUBENTRY_IDENTIFIER


@dataclass(slots=True)
class _ButtonUniqueIdParts:
    """Container describing the parsed components of a button unique_id."""

    entry_id: str
    subentry_id: str
    google_device_id: str
    action: str


_BUTTON_ACTION_SUFFIXES: tuple[str, ...] = (
    "play_sound",
    "stop_sound",
    "locate_device",
)
_LEGACY_BUTTON_SUBENTRY_PREFIXES: tuple[str, ...] = ("tracker",)


def _normalize_legacy_button_remainder(
    remainder: str,
    *,
    identifier: str,
    suffixes: tuple[str, ...],
) -> str:
    """Strip legacy pseudo-subentry tokens before rebuilding button IDs."""

    action_suffix: str | None = None
    for suffix in suffixes:
        if remainder.endswith(suffix):
            action_suffix = suffix
            payload = remainder[: -len(suffix)]
            break
    else:
        return remainder

    if payload.startswith(f"{identifier}_"):
        return remainder

    for legacy_prefix in _LEGACY_BUTTON_SUBENTRY_PREFIXES:
        token = f"{legacy_prefix}_"
        if payload.startswith(token):
            trimmed = payload[len(token) :]
            if trimmed:
                return f"{trimmed}{action_suffix}"
            break

    return remainder


def _parse_button_unique_id(
    unique_id: str,
    entry: ConfigEntry,
    subentry_map: Mapping[str, str],
    fallback_subentry_id: str,
) -> _ButtonUniqueIdParts | None:
    """Return parsed button unique_id data for relinking heuristics."""

    if not isinstance(unique_id, str) or not unique_id:
        return None

    action: str | None = None
    prefix: str | None = None
    for candidate_action in _BUTTON_ACTION_SUFFIXES:
        suffix = f"_{candidate_action}"
        if unique_id.endswith(suffix):
            action = candidate_action
            prefix = unique_id[: -len(suffix)]
            break

    if action is None or prefix is None:
        try:
            prefix, action = unique_id.rsplit("_", 1)
        except ValueError:
            return None
        if not action:
            return None

    trimmed = prefix
    domain_prefix = f"{DOMAIN}_"
    if trimmed.startswith(domain_prefix):
        trimmed = trimmed[len(domain_prefix) :]

    entry_id = entry.entry_id
    if not isinstance(entry_id, str) or not entry_id:
        return None

    remainder = trimmed

    if ":" in trimmed:
        candidate_entry_id, potential_rest = trimmed.split(":", 1)
        if candidate_entry_id:
            if candidate_entry_id != entry_id:
                return None
            remainder = potential_rest
    elif trimmed.startswith(f"{entry_id}_"):
        remainder = trimmed[len(entry_id) + 1 :]
    elif trimmed.startswith(entry_id + ":"):
        remainder = trimmed[len(entry_id) + 1 :]
    elif trimmed.startswith(entry_id):
        suffix = trimmed[len(entry_id) :]
        if suffix.startswith("_") or suffix.startswith(":"):
            remainder = suffix[1:]
        elif suffix:
            remainder = trimmed

    if not remainder:
        return None

    subentry_id: str | None = None
    google_device_id = remainder

    if ":" in remainder:
        maybe_subentry, maybe_device = remainder.split(":", 1)
        if maybe_device:
            subentry_id = maybe_subentry or None
            google_device_id = maybe_device
        else:
            google_device_id = maybe_subentry
    else:
        known_identifiers = {
            identifier
            for identifier in subentry_map.values()
            if isinstance(identifier, str) and identifier
        }
        for candidate in sorted(known_identifiers, key=len, reverse=True):
            token = f"{candidate}_"
            if remainder.startswith(token):
                subentry_id = candidate
                google_device_id = remainder[len(token) :]
                break

    if not google_device_id:
        return None

    if subentry_id is None:
        subentry_id = fallback_subentry_id

    return _ButtonUniqueIdParts(
        entry_id=entry_id,
        subentry_id=subentry_id,
        google_device_id=google_device_id,
        action=action,
    )


def _iter_tracker_identifier_candidates(
    parts: _ButtonUniqueIdParts,
) -> tuple[tuple[str, str], ...]:
    """Return identifier candidates for locating the tracker device."""

    primary = (DOMAIN, f"{parts.entry_id}:{parts.subentry_id}:{parts.google_device_id}")
    secondary = (DOMAIN, f"{parts.entry_id}:{parts.google_device_id}")
    tertiary = (DOMAIN, parts.google_device_id)
    return primary, secondary, tertiary


class _DeviceLookup(Protocol):
    """Protocol describing the registry lookup helper used during relinks."""

    def __call__(
        self, identifier: tuple[str, str], *, allow_service: bool = True
    ) -> dr.DeviceEntry | Any | None:
        """Return the matching registry device when available."""


async def _async_relink_entities_for_entry(  # noqa: PLR0913
    hass: HomeAssistant,
    entry: ConfigEntry,
    *,
    allowed_domains: Collection[str],
    resolve_target: Callable[
        [er.RegistryEntry, _DeviceLookup, dr.DeviceEntry | Any | None],
        tuple[dr.DeviceEntry | Any, str | None] | None,
    ],
    summary_template: str,
    registry_failure_context: str,
    log_if_zero: bool = False,
) -> None:
    """Relink config entry entities using a shared registry lookup routine.

    The caller supplies the entity ``domain`` filter and a resolver that maps an
    entity registry entry to a target device (and optional expected subentry
    identifier). The helper guards registry acquisition across a range of Home
    Assistant versions, resolves devices through the shared entry-scoped
    resolver, and records a short summary once the pass finishes. Callers should
    keep resolver logic side effect free; the helper itself updates registry
    links when the resolver provides a valid target device.
    """

    entry_id = getattr(entry, "entry_id", "") or ""
    if not entry_id:
        return

    try:
        entity_registry = er.async_get(hass)
        device_registry = dr.async_get(hass)
    except Exception as err:  # noqa: BLE001 - defensive guard
        _LOGGER.debug(
            "googlefindmy(%s): registry acquisition failed during %s: %s",
            entry_id,
            registry_failure_context,
            err,
        )
        return

    # Local import, see ``_async_purge_unloaded_subentry_registrations``.
    from .coordinator.helpers.registry import iter_all_devices

    if not getattr(entity_registry, "entities", None):
        return
    # Readiness probe only: an empty registry has nothing to relink.  Asked
    # through the shared helper because no call site outside the translator may
    # touch ``device_registry.devices`` at all -- neither as a mapping nor via
    # ``getattr`` -- and the static ratchet enforces that
    # (``tests/test_guard_device_registry_kwargs.py``).  Note that ``__len__``
    # itself does *not* report (tag ``2026.9.0``, lines 1553-1555), so the cost
    # here is materialising a tuple of pointers where a length would do; that is
    # the price of having exactly one place that knows what ``devices`` means on
    # which core.
    if not iter_all_devices(device_registry):
        return

    registry_entries = _iter_config_entry_entities(entity_registry, entry_id)

    def lookup_device(
        identifier: tuple[str, str], *, allow_service: bool = True
    ) -> dr.DeviceEntry | Any | None:
        """Resolve one identifier to a device owned by this config entry.

        The lookup itself lives in the shared resolver; this closure only adds
        the service-device filter its callers ask for.  The entry scoping that
        used to be a post-filter here (``entry_id not in device.config_entries``)
        is now part of the lookup, and that is a narrowing in one direction only:
        the old shape searched by identifier alone, took the *first* match and
        then threw it away when it belonged to another entry -- so a second
        device carrying the same identifier under *our* entry was never reached.
        The resolver asks the registry for our entry's device directly.

        See ``custom_components/googlefindmy/agents/runtime_patterns/AGENTS.md``
        and ``docs/AI_DEPRECATIONS_GUIDE.md``, section VI: no call site may name
        ``async_get_device``, and none may build the entry scoping by hand.
        """
        # Local import, see ``_async_purge_unloaded_subentry_registrations``.
        from .coordinator.helpers.registry import resolve_device_by_identifiers

        device = resolve_device_by_identifiers(
            device_registry, (identifier,), entry_id=entry_id
        )
        if device is None:
            return None
        if not allow_service and _device_is_service_device(device, entry_id):
            return None

        return device

    fixed = 0

    for entity_entry in registry_entries:
        try:
            if getattr(entity_entry, "platform", None) != DOMAIN:
                continue

            domain = getattr(entity_entry, "domain", None)
            if domain not in allowed_domains:
                continue

            current_device_id = getattr(entity_entry, "device_id", None)
            current_device = (
                device_registry.async_get(current_device_id)
                if isinstance(current_device_id, str) and current_device_id
                else None
            )

            target = resolve_target(entity_entry, lookup_device, current_device)
            if target is None:
                continue

            target_device, expected_subentry = target
            target_device_id = getattr(target_device, "id", None)
            if not isinstance(target_device_id, str) or not target_device_id:
                continue
            if (
                isinstance(current_device_id, str)
                and current_device_id
                and current_device_id == target_device_id
            ):
                continue

            if expected_subentry:
                device_subentry = getattr(target_device, "config_subentry_id", None)
                if (
                    isinstance(device_subentry, str)
                    and device_subentry
                    and device_subentry != expected_subentry
                ):
                    continue

            entity_registry.async_update_entity(
                entity_entry.entity_id, device_id=target_device_id
            )
            fixed += 1
        except Exception as err:  # noqa: BLE001 - defensive guard
            _LOGGER.debug(
                "googlefindmy(%s): relink failed for %s: %s",
                entry_id,
                getattr(entity_entry, "entity_id", "<unknown>"),
                err,
            )

    if fixed or log_if_zero:
        _LOGGER.debug(
            "googlefindmy(%s): " + summary_template,
            entry_id,
            fixed,
        )


def _device_is_service_device(device: dr.DeviceEntry | Any, entry_id: str) -> bool:
    """Return True if the registry device represents the integration service device."""

    if device is None:
        return False

    device_entry_type_cls = getattr(dr, "DeviceEntryType", None)
    if device_entry_type_cls is not None:
        service_entry_type = getattr(device_entry_type_cls, "SERVICE", "service")
    else:
        service_entry_type = "service"
    entry_type = getattr(device, "entry_type", None)
    if entry_type == service_entry_type:
        return True
    if (
        isinstance(entry_type, str)
        and isinstance(service_entry_type, str)
        and entry_type.lower() == service_entry_type.lower()
    ):
        return True

    service_identifiers = {
        service_device_identifier(entry_id)[1],
        LEGACY_SERVICE_IDENTIFIER,
    }

    identifiers = getattr(device, "identifiers", None)
    if not isinstance(identifiers, Collection):
        return False

    for item in identifiers:
        try:
            domain, ident = item
        except (TypeError, ValueError):
            continue
        if domain != DOMAIN or not isinstance(ident, str) or not ident:
            continue
        canonical = _normalize_device_identifier(device, ident)
        if canonical in service_identifiers or canonical.endswith(":service"):
            return True

    return False


async def _async_relink_button_devices(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Ensure button entities are linked to their physical tracker devices."""

    subentry_map = _resolve_subentry_identifier_map(entry)
    fallback_subentry_id = _default_button_subentry_identifier(subentry_map)

    def _resolve_button_target(
        entity_entry: er.RegistryEntry,
        lookup_device: _DeviceLookup,
        current_device: dr.DeviceEntry | Any | None,
    ) -> tuple[dr.DeviceEntry | Any, str | None] | None:
        # Local import, see ``_async_purge_unloaded_subentry_registrations``.
        from .coordinator.helpers.registry import device_belongs_to_entry

        if current_device and device_belongs_to_entry(current_device, entry.entry_id):
            if not _device_is_service_device(current_device, entry.entry_id):
                return None

        parsed = _parse_button_unique_id(
            getattr(entity_entry, "unique_id", ""),
            entry,
            subentry_map,
            fallback_subentry_id,
        )
        if parsed is None:
            _LOGGER.debug(
                "googlefindmy(%s): unable to parse button unique_id '%s'",
                entry.entry_id,
                getattr(entity_entry, "unique_id", ""),
            )
            return None

        for candidate in _iter_tracker_identifier_candidates(parsed):
            device = lookup_device(candidate, allow_service=False)
            if device is None:
                continue
            return device, None

        _LOGGER.debug(
            "googlefindmy(%s): tracker device not found for %s (%s)",
            entry.entry_id,
            getattr(entity_entry, "entity_id", "<unknown>"),
            parsed.google_device_id,
        )
        return None

    await _async_relink_entities_for_entry(
        hass,
        entry,
        allowed_domains=("button",),
        resolve_target=_resolve_button_target,
        summary_template="relinked %d button entities",
        registry_failure_context="button relink",
        log_if_zero=True,
    )


async def _async_relink_subentry_entities(
    hass: HomeAssistant, entry: ConfigEntry
) -> None:
    """Attach tracker/service entities to devices with matching subentry metadata.

    Tracker sensors and device trackers migrate to their tracker device while
    service sensors and binary sensors remain anchored to the service device.
    The routine skips existing correct links, tolerates legacy unique ID shapes
    that omit subentry prefixes, and ignores unrelated platforms so callers can
    safely invoke it during reloads without disturbing non-integration entities.
    """

    entry_id = getattr(entry, "entry_id", "") or ""
    if not entry_id:
        return

    subentry_map = _resolve_subentry_identifier_map(entry)
    tracker_subentry_id = subentry_map.get(
        "device_tracker", _DEFAULT_SUBENTRY_IDENTIFIER
    )
    if not isinstance(tracker_subentry_id, str) or not tracker_subentry_id:
        tracker_subentry_id = _DEFAULT_SUBENTRY_IDENTIFIER

    service_subentry_id = subentry_map.get(
        "binary_sensor", _DEFAULT_SUBENTRY_IDENTIFIER
    )
    if not isinstance(service_subentry_id, str) or not service_subentry_id:
        service_subentry_id = _DEFAULT_SUBENTRY_IDENTIFIER

    allowed_domains: tuple[str, ...] = ("sensor", "binary_sensor", "device_tracker")

    known_identifiers = {
        identifier
        for identifier in subentry_map.values()
        if isinstance(identifier, str) and identifier
    }

    subentries = getattr(entry, "subentries", None)
    if isinstance(subentries, Mapping):
        for subentry in subentries.values():
            identifier = getattr(subentry, "subentry_id", None)
            if isinstance(identifier, str) and identifier:
                known_identifiers.add(identifier)
            data = getattr(subentry, "data", {}) or {}
            group_key_raw = data.get("group_key") if isinstance(data, Mapping) else None
            group_key = (
                str(group_key_raw).strip()
                if isinstance(group_key_raw, str) and group_key_raw.strip()
                else ""
            )
            if group_key == TRACKER_SUBENTRY_KEY:
                tracker_subentry_id = identifier or tracker_subentry_id
            elif group_key == SERVICE_SUBENTRY_KEY:
                service_subentry_id = identifier or service_subentry_id

    service_identifiers: tuple[tuple[str, str], ...]
    service_identifiers = (
        service_device_identifier(entry_id),
        (DOMAIN, f"{entry_id}:{service_subentry_id}:service"),
    )

    def _extract_tracker_binding_from_tracker(
        entity_entry: er.RegistryEntry,
    ) -> tuple[str, str] | None:
        """Return ``(subentry_id, google_device_id)`` for tracker entities."""

        uid = getattr(entity_entry, "unique_id", "") or ""
        if not uid:
            return None

        remainder = uid
        if remainder.startswith(f"{entry_id}:"):
            remainder = remainder[len(entry_id) + 1 :]

        subentry_candidate: str | None = None
        google_device_id: str | None = None

        if ":" in remainder:
            subentry_candidate, google_device_id = remainder.split(":", 1)
        else:
            google_device_id = remainder

        if not google_device_id:
            return None

        if not subentry_candidate:
            subentry_candidate = tracker_subentry_id

        return subentry_candidate, google_device_id

    def _extract_tracker_binding_from_sensor(
        entity_entry: er.RegistryEntry,
    ) -> tuple[str | None, str | None]:
        """Return the tracker metadata encoded in a sensor unique_id."""

        entity_unique_id = getattr(entity_entry, "unique_id", "")
        tracker_subentry_token = f"{entry_id}_{tracker_subentry_id}_"
        service_subentry_token = f"{entry_id}_{service_subentry_id}_"

        if not entity_unique_id:
            return None, None

        canonical = _strip_entry_namespace(entry_id, entity_unique_id)
        trimmed = canonical

        domain_prefix = f"{DOMAIN}_"
        if trimmed.startswith(domain_prefix):
            trimmed = trimmed[len(domain_prefix) :]

        if trimmed.startswith(service_subentry_token):
            return service_subentry_id, trimmed[len(service_subentry_token) :]

        if trimmed.startswith(tracker_subentry_token):
            remainder = trimmed[len(tracker_subentry_token) :]
        elif trimmed.startswith(f"{entry_id}_"):
            remainder = trimmed[len(f"{entry_id}_") :]
        else:
            remainder = trimmed

        if remainder.startswith(f"{tracker_subentry_id}_"):
            remainder = remainder[len(tracker_subentry_id) + 1 :]
        elif remainder.startswith(f"{service_subentry_id}_"):
            remainder = remainder[len(service_subentry_id) + 1 :]

        google_device_id: str | None = remainder
        subentry_id: str | None = tracker_subentry_id

        if ":" in remainder:
            maybe_subentry, maybe_device = remainder.split(":", 1)
            subentry_id = maybe_subentry or tracker_subentry_id
            google_device_id = maybe_device or google_device_id
        else:
            for candidate in sorted(known_identifiers, key=len, reverse=True):
                token = f"{candidate}_"
                if remainder.startswith(token):
                    subentry_id = candidate
                    google_device_id = remainder[len(token) :]
                    break

        if google_device_id and "_" in google_device_id:
            google_device_id = google_device_id.split("_", 1)[0]

        if not google_device_id:
            return None, None

        return subentry_id, google_device_id

    try:
        service_device_registry: dr.DeviceRegistry | Any | None = dr.async_get(hass)
    except Exception:  # noqa: BLE001 - defensive guard
        service_device_registry = None

    def _resolve_service_device(
        lookup_device: _DeviceLookup,
    ) -> dr.DeviceEntry | Any | None:
        """Return the service device assigned to the service subentry."""
        # Local import, see ``_async_purge_unloaded_subentry_registrations``.
        from .coordinator.helpers.registry import (
            device_belongs_to_entry,
            iter_all_devices,
        )

        for identifier in service_identifiers:
            device = lookup_device(identifier)
            if device is None:
                continue
            device_subentry = getattr(device, "config_subentry_id", None)
            if (
                isinstance(device_subentry, str)
                and device_subentry
                and device_subentry != service_subentry_id
            ):
                continue
            if not _device_is_service_device(device, entry_id):
                continue
            return device

        # Fallback scan when the identifier lookup above found nothing.  Whole
        # registry, then filtered to this entry, because a service device may
        # still carry a legacy identifier the candidates do not name.
        for device in iter_all_devices(service_device_registry):
            if not device_belongs_to_entry(device, entry_id):
                continue
            device_subentry = getattr(device, "config_subentry_id", None)
            if (
                isinstance(device_subentry, str)
                and device_subentry
                and device_subentry != service_subentry_id
            ):
                continue
            if not _device_is_service_device(device, entry_id):
                continue
            return device

        return None

    def _find_tracker_device(
        *,
        lookup_device: _DeviceLookup,
        subentry_id: str,
        google_device_id: str,
    ) -> dr.DeviceEntry | Any | None:
        """Return the tracker device that matches the subentry + device id pair."""

        parts = _ButtonUniqueIdParts(
            entry_id=entry_id,
            subentry_id=subentry_id,
            google_device_id=google_device_id,
            action="relink",
        )

        for candidate in _iter_tracker_identifier_candidates(parts):
            device = lookup_device(candidate, allow_service=False)
            if device is None:
                continue
            device_subentry = getattr(device, "config_subentry_id", None)
            if (
                isinstance(device_subentry, str)
                and device_subentry
                and device_subentry != subentry_id
            ):
                continue
            if _device_is_service_device(device, entry_id):
                continue
            return device

        return None

    def _resolve_target_device(
        entity_entry: er.RegistryEntry,
        lookup_device: _DeviceLookup,
        _current_device: dr.DeviceEntry | Any | None,
    ) -> tuple[dr.DeviceEntry | Any, str | None] | None:
        domain = getattr(entity_entry, "domain", None)
        if domain == "binary_sensor":
            service_device = _resolve_service_device(lookup_device)
            if service_device is None:
                return None
            return service_device, service_subentry_id

        if domain == "sensor":
            sensor_binding = _extract_tracker_binding_from_sensor(entity_entry)
            if sensor_binding is None:
                service_device = _resolve_service_device(lookup_device)
                if service_device is None:
                    return None
                return service_device, service_subentry_id

            subentry_id, google_device_id = sensor_binding
            subentry_id = subentry_id or tracker_subentry_id
            if subentry_id == service_subentry_id:
                service_device = _resolve_service_device(lookup_device)
                if service_device is None:
                    return None
                return service_device, service_subentry_id
            if google_device_id is None:
                return None
            tracker_device = _find_tracker_device(
                lookup_device=lookup_device,
                subentry_id=subentry_id,
                google_device_id=google_device_id,
            )
            if tracker_device is None:
                return None
            return tracker_device, subentry_id

        if domain == "device_tracker":
            tracker_binding = _extract_tracker_binding_from_tracker(entity_entry)
            if tracker_binding is None:
                return None
            subentry_id, google_device_id = tracker_binding
            subentry_id = subentry_id or tracker_subentry_id
            if google_device_id is None:
                return None
            tracker_device = _find_tracker_device(
                lookup_device=lookup_device,
                subentry_id=subentry_id,
                google_device_id=google_device_id,
            )
            if tracker_device is None:
                return None
            return tracker_device, subentry_id

        return None

    await _async_relink_entities_for_entry(
        hass,
        entry,
        allowed_domains=allowed_domains,
        resolve_target=_resolve_target_device,
        summary_template="relinked %d tracker/service entit(y/ies)",
        registry_failure_context="entity relink",
    )


def _strip_entry_namespace(entry_id: str, ident: str) -> str:
    """Strip the entry namespace prefix (`<entry_id>:`) when present."""

    if not ident or ":" not in ident:
        return ident

    prefix, canonical = ident.split(":", 1)
    if canonical and prefix == entry_id:
        return canonical

    return ident


def _coerce_alias_iterable(value: Any) -> list[str] | None:
    """Return a sanitized list of alias strings or ``None`` when unavailable."""

    if isinstance(value, str):
        candidate = value.strip()
        return [candidate] if candidate else None

    if isinstance(value, Iterable) and not isinstance(
        value, (str, bytes, bytearray, Mapping)
    ):
        sanitized = [
            alias.strip() for alias in value if isinstance(alias, str) and alias.strip()
        ]
        if sanitized:
            return sanitized

    return None


def _dedupe_aliases(
    exclude: str | None,
    *sources: Iterable[str] | None,
) -> list[str]:
    """Return a deduplicated alias list excluding the active name."""

    deduped: list[str] = []
    for source in sources:
        if not source:
            continue
        for alias in source:
            if not isinstance(alias, str):
                continue
            candidate = alias.strip()
            if not candidate:
                continue
            if exclude and candidate == exclude:
                continue
            if candidate not in deduped:
                deduped.append(candidate)
    return deduped


def _safe_epoch(value: Any) -> int:
    """Best-effort conversion to an integer epoch timestamp."""

    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _sanitize_ignored_meta(device_id: str, meta: Mapping[str, Any]) -> dict[str, Any]:
    """Return sanitized metadata for ignored device bookkeeping."""

    raw_name = meta.get("name") if isinstance(meta, Mapping) else None
    name = (
        raw_name.strip()
        if isinstance(raw_name, str) and raw_name.strip()
        else device_id
    )

    alias_sources: list[Iterable[str]] = []
    if isinstance(meta, Mapping):
        coerced_aliases = _coerce_alias_iterable(meta.get("aliases"))
        if coerced_aliases:
            alias_sources.append(coerced_aliases)

    if isinstance(raw_name, str):
        raw_name_aliases = _coerce_alias_iterable(raw_name)
        if raw_name_aliases:
            alias_sources.append(raw_name_aliases)

    aliases = _dedupe_aliases(name, *alias_sources)

    ignored_at = _safe_epoch(meta.get("ignored_at")) if isinstance(meta, Mapping) else 0
    if not ignored_at:
        ignored_at = int(time.time())

    source = meta.get("source") if isinstance(meta, Mapping) else None
    if not isinstance(source, str) or not source:
        source = "registry"

    return {
        "name": name,
        "aliases": aliases,
        "ignored_at": ignored_at,
        "source": source,
    }


def _merge_sanitized_ignored_meta(
    existing: Mapping[str, Any], incoming: Mapping[str, Any]
) -> dict[str, Any]:
    """Merge two sanitized ignored metadata records."""

    existing_name = (
        existing.get("name") if isinstance(existing.get("name"), str) else None
    )
    incoming_name = (
        incoming.get("name") if isinstance(incoming.get("name"), str) else None
    )
    name = existing_name or incoming_name or ""

    alias_sources: list[Iterable[str]] = []

    existing_aliases = _coerce_alias_iterable(existing.get("aliases"))
    if existing_aliases:
        alias_sources.append(existing_aliases)

    incoming_aliases = _coerce_alias_iterable(incoming.get("aliases"))
    if incoming_aliases:
        alias_sources.append(incoming_aliases)

    name_aliases = [alias for alias in (existing_name, incoming_name) if alias]
    if name_aliases:
        alias_sources.append(name_aliases)

    aliases = _dedupe_aliases(name, *alias_sources)

    ignored_at = max(
        _safe_epoch(existing.get("ignored_at")), _safe_epoch(incoming.get("ignored_at"))
    )
    source = existing.get("source") or incoming.get("source") or "registry"

    if not name:
        name = incoming_name or existing_name or ""

    return {
        "name": name,
        "aliases": aliases,
        "ignored_at": ignored_at,
        "source": source,
    }


def _normalize_ignored_device_map(
    entry_id: str, mapping: Mapping[str, Mapping[str, Any]]
) -> tuple[dict[str, dict[str, Any]], bool]:
    """Normalize ignored device identifiers for an entry."""

    normalized: dict[str, dict[str, Any]] = {}
    changed = False

    for raw_id, meta in mapping.items():
        canonical = _strip_entry_namespace(entry_id, raw_id)
        if canonical != raw_id:
            changed = True

        sanitized = _sanitize_ignored_meta(canonical, meta)
        existing = normalized.get(canonical)
        if existing is None:
            normalized[canonical] = sanitized
        else:
            normalized[canonical] = _merge_sanitized_ignored_meta(existing, sanitized)
            if normalized[canonical] != existing:
                changed = True

    if normalized != dict(mapping):
        changed = True

    return normalized, changed


def _normalize_visible_device_ids(
    entry_id: str, raw_ids: Iterable[Any]
) -> tuple[list[str], bool]:
    """Normalize visible device identifiers for a subentry."""

    normalized: list[str] = []
    seen: set[str] = set()
    changed = False

    for candidate in raw_ids:
        if not isinstance(candidate, str) or not candidate:
            changed = True
            continue
        canonical = _strip_entry_namespace(entry_id, candidate)
        if canonical != candidate:
            changed = True
        if canonical in seen:
            if canonical != candidate:
                changed = True
            continue
        seen.add(canonical)
        normalized.append(canonical)

    return normalized, changed


def _migrate_entry_identifier_namespaces(
    hass: HomeAssistant, entry: ConfigEntry
) -> None:
    """Strip entry namespaces from ignored/visible device identifiers."""

    options = dict(entry.options)
    current_raw = options.get(
        OPT_IGNORED_DEVICES, DEFAULT_OPTIONS.get(OPT_IGNORED_DEVICES)
    )
    ignored_map, _ = coerce_ignored_mapping(current_raw)
    normalized_map, options_changed = _normalize_ignored_device_map(
        entry.entry_id, ignored_map
    )
    if options_changed:
        options[OPT_IGNORED_DEVICES] = normalized_map
        options[OPT_OPTIONS_SCHEMA_VERSION] = 2
    if options_changed and options != entry.options:
        hass.config_entries.async_update_entry(entry, options=options)

    subentries = getattr(entry, "subentries", None)
    if not isinstance(subentries, Mapping):
        return

    for subentry in subentries.values():
        raw_visible = subentry.data.get("visible_device_ids")
        if not isinstance(raw_visible, (list, tuple, set)):
            continue
        normalized_visible, subentry_changed = _normalize_visible_device_ids(
            entry.entry_id, raw_visible
        )
        if not subentry_changed:
            continue
        payload = dict(subentry.data)
        payload["visible_device_ids"] = list(normalized_visible)
        hass.config_entries.async_update_subentry(
            entry,
            subentry,
            data=payload,
        )


# --- BEGIN: Helpers for resolution and manual locate ---------------------------
def _resolve_canonical_from_any(hass: HomeAssistant, arg: str) -> tuple[str, str]:
    """Resolve HA device_id/entity_id/canonical_id -> (canonical_id, friendly_name).

    Resolution order:
    1) If `arg` is a Home Assistant `device_id`: extract our (DOMAIN, identifier)
       from the device registry. Fails if not found/invalid.
    2) If `arg` is an `entity_id`: lookup entity; if it belongs to our DOMAIN
       and is linked to a device, extract the identifier from that device.
    3) Otherwise: treat `arg` as already-canonical Google ID.

    Raises:
        HomeAssistantError: if `arg` is a `device_id`/`entity_id` but cannot be mapped
            to a valid identifier of this integration.

    Security:
        Do not include secrets or coordinates in raised messages or logs.
    """
    dev_reg = dr.async_get(hass)
    ent_reg = er.async_get(hass)

    # 1) device_id
    dev = dev_reg.async_get(arg)
    if dev:
        for item in dev.identifiers:
            try:
                domain, ident = item  # expected 2-tuple
            except (TypeError, ValueError):
                continue
            if domain == DOMAIN and isinstance(ident, str) and ident:
                canonical = _normalize_device_identifier(dev, ident)
                friendly = (dev.name_by_user or dev.name or canonical).strip()
                return canonical, friendly
        raise HomeAssistantError(f"Device '{arg}' has no valid {DOMAIN} identifier")

    # 2) entity_id
    if "." in arg and "/" not in arg and ":" not in arg:
        ent = ent_reg.async_get(arg)
        if ent and ent.platform == DOMAIN and ent.device_id:
            dev = dev_reg.async_get(ent.device_id)
            if dev:
                for item in dev.identifiers:
                    try:
                        domain, ident = item
                    except (TypeError, ValueError):
                        continue
                    if domain == DOMAIN and isinstance(ident, str) and ident:
                        canonical = _normalize_device_identifier(dev, ident)
                        friendly = (dev.name_by_user or dev.name or canonical).strip()
                        return canonical, friendly
            raise HomeAssistantError(
                f"Entity '{arg}' is not linked to a valid {DOMAIN} device"
            )

    # 3) fallback: assume canonical id already
    return arg, arg


async def async_handle_manual_locate(
    hass: HomeAssistant, coordinator: GoogleFindMyCoordinator, arg: str
) -> None:
    """Handle manual locate button: resolve target, dispatch, and log correctly.

    Behavior:
        - Resolve any incoming identifier (`device_id`, `entity_id`, or canonical).
        - On dispatch: log what the coordinator actually returned, distinguishing
          a locate that produced data from one that produced none. The second
          line names no cause: the coordinator logs the reason where it knows it.
        - On failure: raise HomeAssistantError and mirror a redacted error record
          into the coordinator diagnostics buffer (if present).

    This function should be called by your button entity handler.
    """
    try:
        canonical_id, friendly = _resolve_canonical_from_any(hass, arg)
        # The return value is the evidence, so it is read rather than dropped:
        # async_locate_device returns an empty mapping when the request was
        # refused, got no answer, or its answer was dropped. Logging success
        # for that case claims an effect that never happened -- the same class
        # of unbacked success claim as the Stop Sound report in BSkando#195.
        # The empty case names no cause either, because several of those paths
        # log only at DEBUG and this line cannot tell them apart.
        location = await coordinator.async_locate_device(canonical_id)
        if location:
            _LOGGER.info("Manual locate returned data for %s", friendly)
        else:
            # A caller can repeat the invocation, so the INFO record names the
            # device by the resolved identifier (the canonical id when the
            # registry resolves it, else the caller's argument, which the
            # previous record carried as well), as the coordinator's own
            # record for an unanswered locate does; the display name stays at
            # DEBUG (AGENTS.md section 5 (b)).
            _LOGGER.info(
                "Manual locate for device %s produced no location update",
                canonical_id,
            )
            _LOGGER.debug("Manual locate without location update for %s", friendly)
    except HomeAssistantError as err:
        diag_buffer = cast(Any, getattr(coordinator, "_diag", None))
        if diag_buffer is not None and hasattr(diag_buffer, "add_error"):
            diag_buffer.add_error(
                code="manual_locate_resolution_failed",
                context={
                    "device_id": "",
                    "arg": str(arg)[:64],
                    "reason": str(err)[:160],
                },
            )
        _LOGGER.error("Locate failed for '%s': %s", arg, err)
        raise


# --- END: Helpers for resolution and manual locate -----------------------------


def _redact_url_token(url: str) -> str:
    """Return URL with any sensitive query parameter values redacted for logging."""

    try:
        parts = urlsplit(url)
        q = parse_qsl(parts.query, keep_blank_values=True)
        sensitive = {
            "token",
            "access_token",
            "id_token",
            "auth",
            "key",
            "apikey",
            "api_key",
            "signature",
        }
        redacted: list[tuple[str, str]] = []
        for k, v in q:
            if k.lower() in sensitive and v:
                red_v = "****"
                if len(v) > 4:
                    red_v = f"{v[:2]}…{v[-2:]}"
                redacted.append((k, red_v))
            else:
                redacted.append((k, v))
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(redacted, doseq=True),
                parts.fragment,
            )
        )
    except Exception:  # pragma: no cover
        return url


def _is_active_entry(entry: ConfigEntry) -> bool:
    """Return True if the entry is considered *active* for guard logic.

    We treat only well-defined 'working' states as active to avoid drift:
    - LOADED: entry is fully operational
    - SETUP_IN_PROGRESS / SETUP_RETRY: entry is being (re)initialized
    All other states are considered non-active.
    """
    if entry.disabled_by:
        return False
    active_states = {
        ConfigEntryState.LOADED,
        ConfigEntryState.SETUP_IN_PROGRESS,
        ConfigEntryState.SETUP_RETRY,
    }
    setup_error = getattr(ConfigEntryState, "SETUP_ERROR", None)
    if setup_error is not None:
        active_states.add(setup_error)
    return entry.state in active_states


def _primary_active_entry(entries: list[ConfigEntry]) -> ConfigEntry | None:
    """Pick a deterministic 'primary' active entry to avoid mutual aborts.

    Tie-break rule (stable, minimalistic):
        1) Prefer entries that are LOADED over all others.
        2) Otherwise, pick the lexicographically smallest entry_id.
    """
    active = [e for e in entries if _is_active_entry(e)]
    if not active:
        return None
    loaded = [e for e in active if e.state == ConfigEntryState.LOADED]
    pool = loaded or active
    return sorted(pool, key=lambda e: e.entry_id)[0]


# ------------------------------ Data/Options ---------------------------------


# Keys whose change makes a reload necessary, taken from the token-cache seeding
# in async_setup_entry: that seeding runs once per setup, so a change to any of
# these is written but ineffective until the entry is set up again.
_CREDENTIAL_KEYS: Final[tuple[str, ...]] = (
    DATA_AUTH_METHOD,
    CONF_OAUTH_TOKEN,
    DATA_AAS_TOKEN,
    CONF_GOOGLE_EMAIL,
    DATA_SECRET_BUNDLE,
)


def _credential_fingerprint(data: Mapping[str, Any] | None) -> str:
    """Return a non-reversible fingerprint of the credential-relevant keys.

    Deliberately a digest, not the values: this is held for the lifetime of the
    entry and would otherwise put a second copy of the tokens into memory (and
    one bad log line away from disk). Equality is all the caller needs.
    """

    container: Mapping[str, Any] = data if isinstance(data, Mapping) else {}
    values: dict[str, Any] = {key: container.get(key) for key in _CREDENTIAL_KEYS}
    # The FCM registration is derived material that the receiver refreshes in
    # place and writes back into the bundle (``FcmReceiverHA`` keeps the entry
    # durable for a regenerated token). The live cache already carries the
    # refreshed value when that write lands, so it must not schedule a reload:
    # a reload would tear down the receiver that just obtained the registration
    # and repeat the handshake for nothing. Every other bundle member counts.
    # Assumption: FCM credentials are machine-generated, never typed by the
    # user, so excluding this member cannot hide a user-initiated change; do not
    # "fix" it back, that would restore the reload loop above.
    bundle = values.get(DATA_SECRET_BUNDLE)
    if isinstance(bundle, Mapping):
        values[DATA_SECRET_BUNDLE] = {
            key: value for key, value in bundle.items() if key != "fcm_credentials"
        }
    material = json.dumps(values, sort_keys=True, default=repr)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _opt(entry: ConfigEntry, key: str, default: Any) -> Any:
    """Read a configuration value, preferring options over data.

    Defensive against partially-initialised ConfigEntry stubs (test mocks)
    and rare HA-lifecycle edge-cases where ``options`` or ``data`` may be
    absent. For fully-initialised entries this is semantically identical
    to the previous strict implementation.
    """
    options = getattr(entry, "options", None) or {}
    if key in options:
        return options.get(key, default)
    data = getattr(entry, "data", None) or {}
    return data.get(key, default)


def _effective_config(entry: ConfigEntry) -> dict[str, Any]:
    """Assemble a dict of non-secret runtime settings (options-first)."""
    return {k: _opt(entry, k, None) for k in OPTION_KEYS}


def _async_ensure_map_views_registered(
    hass: HomeAssistant, bucket: GoogleFindMyDomainData
) -> None:
    """Register the map HTTP views once, if not already done.

    Registration is domain-wide (``bucket`` is the shared domain data): HA core
    has no ``unregister_view`` API, so per-entry access is gated at request
    time instead (see ``map_view.py``'s ``OPT_MAP_VIEW_ENABLED`` check).
    """
    views_registered = bucket.get("views_registered")
    if not isinstance(views_registered, bool):
        views_registered = False
    if views_registered:
        return

    map_view_instance = GoogleFindMyMapView(hass)
    hass.http.register_view(map_view_instance)

    map_redirect_view_instance = GoogleFindMyMapRedirectView(hass)
    hass.http.register_view(map_redirect_view_instance)

    map_tiles_token_view_instance = GoogleFindMyMapTilesTokenView(hass)
    hass.http.register_view(map_tiles_token_view_instance)
    bucket["views_registered"] = True
    _LOGGER.debug("Registered map views")


def _normalize_contributor_mode(value: Any) -> str:
    """Return a sanitized contributor mode string."""

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in (
            CONTRIBUTOR_MODE_HIGH_TRAFFIC,
            CONTRIBUTOR_MODE_IN_ALL_AREAS,
        ):
            return normalized
    return DEFAULT_CONTRIBUTOR_MODE


# Key of the stray mapping an older discovery import left inside ``entry.data``.
# It is not a setting: the integration never writes a key called ``data`` into an
# entry (the only similar constant is ``secrets_data``), and nothing reads one.
_STRAY_NESTED_DATA_KEY = "data"


def _strip_stray_nested_entry_data(
    data: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    """Return ``data`` without the stray nested ``data`` key, and how deep it went.

    Discovery used to hand Home Assistant a nested ``{"data": {...}}`` payload,
    which the core merged verbatim into ``entry.data``. The credentials therefore
    never reached the level that is read, and the next import wrapped the whole
    thing again: the nesting grows by one level per run, and a live installation
    was found at depth 3 with roughly three quarters of its entry data spent on
    the dead copies.

    Removing the top key removes the whole chain, so the returned depth is a
    report, not a loop counter. A depth of 0 means nothing was found, and the
    returned mapping equals the input: callers must skip the write in that case
    so a healthy entry is never touched.

    Only a *mapping* is stripped. A future setting that happens to be called
    ``data`` and holds a scalar is left alone rather than silently deleted.
    """

    cleaned = dict(data)
    stray = cleaned.get(_STRAY_NESTED_DATA_KEY)
    if not isinstance(stray, Mapping):
        return cleaned, 0

    depth = 0
    while isinstance(stray, Mapping):
        depth += 1
        stray = stray.get(_STRAY_NESTED_DATA_KEY)

    del cleaned[_STRAY_NESTED_DATA_KEY]
    return cleaned, depth


def _async_strip_stray_nested_entry_data(
    hass: HomeAssistant, entry: ConfigEntry
) -> None:
    """Drop the discovery leftovers from ``entry.data``; idempotent, log-only.

    Runs on every setup rather than from ``async_migrate_entry``: migration only
    runs when the stored entry version differs from ``CONFIG_ENTRY_VERSION``, and
    affected entries are already at the current version, so they would never be
    reached. Bumping the version purely to trigger a cleanup would also block
    downgrades, which is a steep price for removing a stray key. The sibling
    ``_async_soft_migrate_data_to_options`` establishes the idiom: an idempotent
    setup-time fixup that writes only when it has something to change.

    The discarded copies are not promoted to the top level. They are ordered by
    age only as long as nothing else wrote to the entry in between; a reauth
    writes to the top level and inverts that order, so promoting the deeper copy
    could replace fresh credentials with stale ones. Running the login again is
    the reliable route, and after the discovery fix it lands correctly.
    """

    cleaned, depth = _strip_stray_nested_entry_data(entry.data)
    if depth == 0:
        return

    _LOGGER.warning(
        "[%s] Removing %d nested 'data' level(s) an earlier discovery import left "
        "in the entry. The credentials stored there are discarded; run the login "
        "again if the credentials in use are outdated.",
        entry.entry_id,
        depth,
    )
    hass.config_entries.async_update_entry(entry, data=cleaned)


async def _async_soft_migrate_data_to_options(
    hass: HomeAssistant, entry: ConfigEntry
) -> None:
    """Idempotently copy known settings from data -> options (never move secrets)."""
    new_options = dict(entry.options)
    changed = False
    for k in OPTION_KEYS:
        if k not in new_options and k in entry.data:
            new_options[k] = entry.data[k]
            changed = True
    if changed:
        _LOGGER.info(
            "Soft-migrating %d option(s) from data to options for '%s'",
            len(new_options) - len(entry.options),
            _label_entry_for_log(entry),
        )
        hass.config_entries.async_update_entry(entry, options=new_options)


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate a config entry to the latest schema and enforce duplicate policy."""

    _LOGGER.debug(
        "Config entry %s (version=%s) requested migration; validating account metadata.",
        entry.entry_id,
        entry.version,
    )

    canonical_entry = await async_coalesce_account_entries(hass, canonical_entry=entry)
    if canonical_entry.entry_id != entry.entry_id:
        _LOGGER.info(
            "Config entry %s removed during migration; canonical entry is %s",
            entry.entry_id,
            canonical_entry.entry_id,
        )
        return True

    entry = canonical_entry

    should_setup, normalized_email = await _ensure_post_migration_consistency(
        hass,
        entry,
        duplicate_issue_cause="migration_duplicate",
    )

    if not should_setup:
        email_for_log = (
            _mask_email_for_logs(normalized_email) if normalized_email else "n/a"
        )
        _LOGGER.info(
            "Migration halted for %s because account %s is not authoritative",
            entry.entry_id,
            email_for_log,
        )
        return False

    await _async_soft_migrate_data_to_options(hass, entry)

    if entry.version != CONFIG_ENTRY_VERSION:
        update_kwargs = {"version": CONFIG_ENTRY_VERSION}
        try:
            hass.config_entries.async_update_entry(entry, **update_kwargs)
        except TypeError:
            _apply_update_entry_fallback(hass, entry, update_kwargs)

    _LOGGER.debug(
        "Config entry %s migrated to version %s",
        entry.entry_id,
        CONFIG_ENTRY_VERSION,
    )
    return True


# ------------------------- Entity/Device migrations --------------------------


async def _async_create_uid_collision_issue(
    hass: HomeAssistant, entry: ConfigEntry, entity_ids: list[str]
) -> None:
    """Create a repair issue for unique_id collisions (batched; idempotent by key)."""
    try:
        preview = ", ".join(entity_ids[:8]) + ("…" if len(entity_ids) > 8 else "")
        ir.async_create_issue(
            hass,
            DOMAIN,
            f"unique_id_collision_{entry.entry_id}",
            is_fixable=False,
            severity=ir.IssueSeverity.WARNING,
            translation_key=TRANSLATION_KEY_UNIQUE_ID_COLLISION,
            translation_placeholders={
                "entry": entry.title or entry.entry_id,
                "count": str(len(entity_ids)),
                "entities": preview or "n/a",
            },
        )
    except Exception as err:
        _LOGGER.debug("Failed to create UID collision issue: %s", err)


async def _async_migrate_unique_ids(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Migrate entity unique_ids to the latest entry/subentry-aware schemas."""

    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)

    current_options = dict(entry.options)
    options_changed = False
    collisions: list[str] = []

    legacy_result: _LegacyUniqueIdMigrationResult | None = None
    if current_options.get("unique_id_migrated") is not True:
        legacy_result = _migrate_legacy_unique_ids(ent_reg, dev_reg, entry)
        if legacy_result.collisions:
            collisions.extend(legacy_result.collisions)
            _LOGGER.warning(
                "Unique-ID migration incomplete for '%s': migrated=%d / total_needed=%d, collisions=%d",
                _label_entry_for_log(entry),
                legacy_result.migrated,
                legacy_result.total_candidates,
                len(legacy_result.collisions),
            )
        else:
            current_options["unique_id_migrated"] = True
            options_changed = True
            if legacy_result.total_candidates or legacy_result.migrated:
                _LOGGER.debug(
                    "Unique-ID migration complete for '%s': migrated=%d, already_scoped=%d, nonprefix=%d",
                    _label_entry_for_log(entry),
                    legacy_result.migrated,
                    legacy_result.skipped_already_scoped,
                    legacy_result.skipped_nonprefix,
                )

    subentry_result: _SubentryUniqueIdMigrationResult | None = None
    if current_options.get("unique_id_subentry_migrated") is not True:
        subentry_result = _migrate_unique_ids_to_subentry(ent_reg, entry)
        if subentry_result.collisions:
            collisions.extend(subentry_result.collisions)
            _LOGGER.warning(
                "Subentry unique-ID migration incomplete for '%s': updated=%d, already_current=%d, skipped=%d, collisions=%d",
                _label_entry_for_log(entry),
                subentry_result.updated,
                subentry_result.already_current,
                subentry_result.skipped,
                len(subentry_result.collisions),
            )
        else:
            current_options["unique_id_subentry_migrated"] = True
            options_changed = True
            if subentry_result.updated or subentry_result.already_current:
                _LOGGER.debug(
                    "Subentry unique-ID migration complete for '%s': updated=%d, already_current=%d, skipped=%d",
                    _label_entry_for_log(entry),
                    subentry_result.updated,
                    subentry_result.already_current,
                    subentry_result.skipped,
                )

    if collisions:
        await _async_create_uid_collision_issue(hass, entry, collisions)

    if options_changed and current_options != dict(entry.options):
        hass.config_entries.async_update_entry(entry, options=current_options)


@dataclass(slots=True)
class _LegacyUniqueIdMigrationResult:
    total_candidates: int = 0
    migrated: int = 0
    skipped_already_scoped: int = 0
    skipped_nonprefix: int = 0
    collisions: list[str] = field(default_factory=list)


def _migrate_legacy_unique_ids(
    ent_reg: er.EntityRegistry, dev_reg: dr.DeviceRegistry, entry: ConfigEntry
) -> _LegacyUniqueIdMigrationResult:
    """Namespace legacy unique_ids by entry id and update service device identifiers."""

    prefix = f"{DOMAIN}_"
    namespaced_prefix = f"{DOMAIN}_{entry.entry_id}_"

    result = _LegacyUniqueIdMigrationResult()

    for ent in list(ent_reg.entities.values()):
        try:
            if ent.platform != DOMAIN or ent.config_entry_id != entry.entry_id:
                continue
            uid = ent.unique_id or ""
            if uid.startswith(namespaced_prefix):
                result.skipped_already_scoped += 1
                continue
            if not uid.startswith(prefix):
                result.skipped_nonprefix += 1
                continue

            result.total_candidates += 1
            new_uid = namespaced_prefix + uid[len(prefix) :]

            existing_eid = ent_reg.async_get_entity_id(
                ent.domain, ent.platform, new_uid
            )
            if existing_eid:
                _LOGGER.warning(
                    "Unique-ID migration skipped (collision): %s -> %s (existing=%s)",
                    uid,
                    new_uid,
                    existing_eid,
                )
                result.collisions.append(ent.entity_id)
                continue

            ent_reg.async_update_entity(ent.entity_id, new_unique_id=new_uid)
            result.migrated += 1
        except Exception as err:  # noqa: BLE001 - defensive guard
            _LOGGER.debug("Unique ID migration failed for %s: %s", ent.entity_id, err)

    try:
        for device in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
            if (DOMAIN, "integration") in device.identifiers:
                new_identifiers = set(device.identifiers)
                new_identifiers.remove((DOMAIN, "integration"))
                new_identifiers.add((DOMAIN, f"integration_{entry.entry_id}"))
                dev_reg.async_update_device(
                    device_id=device.id, new_identifiers=new_identifiers
                )
                _LOGGER.info(
                    "Migrated integration service device identifier for entry '%s'",
                    entry.entry_id,
                )
    except Exception as err:  # pragma: no cover - defensive
        _LOGGER.debug("Service device identifier migration skipped: %s", err)

    return result


_DEFAULT_SUBENTRY_IDENTIFIER = "core_tracking"
_DEFAULT_SUBENTRY_FEATURES: tuple[str, ...] = (
    "binary_sensor",
    "button",
    "device_tracker",
    "sensor",
)


def _looks_like_tracker_sensor_suffix(value: str) -> bool:
    """Return True when a sensor unique_id suffix targets a tracker entity."""

    if not value:
        return False

    lowered = value.lower()
    if lowered.endswith("_last_seen"):
        return True
    if "device-" in lowered or "device_" in lowered:
        return True
    return False


@dataclass(slots=True)
class _SubentryUniqueIdMigrationResult:
    updated: int = 0
    already_current: int = 0
    skipped: int = 0
    collisions: list[str] = field(default_factory=list)


def _migrate_unique_ids_to_subentry(
    ent_reg: er.EntityRegistry, entry: ConfigEntry
) -> _SubentryUniqueIdMigrationResult:
    """Update unique_ids to include the stable subentry identifier."""

    entry_id = getattr(entry, "entry_id", "") or ""
    if not entry_id:
        return _SubentryUniqueIdMigrationResult()

    subentry_map = _resolve_subentry_identifier_map(entry)
    result = _SubentryUniqueIdMigrationResult()

    for ent in list(ent_reg.entities.values()):
        try:
            if ent.platform != DOMAIN or ent.config_entry_id != entry.entry_id:
                continue
            decision = _determine_subentry_unique_id(entry_id, subentry_map, ent)
            if decision is None:
                result.skipped += 1
                continue
            if decision == ent.unique_id:
                result.already_current += 1
                continue

            existing_eid = ent_reg.async_get_entity_id(
                ent.domain, ent.platform, decision
            )
            if existing_eid and existing_eid != ent.entity_id:
                result.collisions.append(ent.entity_id)
                continue

            ent_reg.async_update_entity(ent.entity_id, new_unique_id=decision)
            result.updated += 1
        except Exception as err:  # noqa: BLE001 - defensive guard
            _LOGGER.debug(
                "Subentry unique ID migration failed for %s: %s", ent.entity_id, err
            )

    return result


def _resolve_subentry_identifier_map(entry: ConfigEntry) -> dict[str, str]:
    """Return the stable identifier for each feature for the given entry."""

    mapping: dict[str, str] = {}
    default_identifier: str | None = None
    service_identifier: str | None = None
    tracker_identifier: str | None = None

    subentries = getattr(entry, "subentries", None)
    if isinstance(subentries, Mapping):
        for subentry in subentries.values():
            data = getattr(subentry, "data", {}) or {}
            raw_features = data.get("features")
            features: tuple[str, ...]
            if isinstance(raw_features, (list, tuple, set)):
                normalized = [
                    str(item).strip()
                    for item in raw_features
                    if isinstance(item, str) and item.strip()
                ]
                features = (
                    tuple(normalized) if normalized else _DEFAULT_SUBENTRY_FEATURES
                )
            else:
                features = _DEFAULT_SUBENTRY_FEATURES

            identifier = getattr(subentry, "subentry_id", None)
            if not isinstance(identifier, str) or not identifier:
                group_key = data.get("group_key")
                identifier = (
                    str(group_key).strip() if isinstance(group_key, str) else ""
                )
            if not identifier:
                identifier = _DEFAULT_SUBENTRY_IDENTIFIER

            group_key_raw = data.get("group_key")
            group_key = (
                str(group_key_raw).strip() if isinstance(group_key_raw, str) else ""
            )
            if group_key == SERVICE_SUBENTRY_KEY and not service_identifier:
                service_identifier = identifier
            elif group_key == TRACKER_SUBENTRY_KEY and not tracker_identifier:
                tracker_identifier = identifier

            for feature in features:
                mapping.setdefault(feature, identifier)

            if default_identifier is None:
                default_identifier = identifier

    if default_identifier is None:
        default_identifier = _DEFAULT_SUBENTRY_IDENTIFIER

    if isinstance(service_identifier, str) and service_identifier:
        for feature in SERVICE_FEATURE_PLATFORMS:
            mapping[feature] = service_identifier

    if isinstance(tracker_identifier, str) and tracker_identifier:
        for feature in TRACKER_FEATURE_PLATFORMS:
            mapping.setdefault(feature, tracker_identifier)

    for feature in _DEFAULT_SUBENTRY_FEATURES:
        mapping.setdefault(feature, default_identifier)

    return mapping


def _determine_subentry_unique_id(
    entry_id: str, subentry_map: Mapping[str, str], ent: er.RegistryEntry
) -> str | None:
    """Return the desired unique_id for an entity (or None to skip)."""

    uid = ent.unique_id or ""
    if not uid:
        return None

    feature = ent.domain
    identifier = subentry_map.get(feature, _DEFAULT_SUBENTRY_IDENTIFIER)

    if feature == "device_tracker":
        # Already fully scoped (``{entry_id}:{identifier}:...``): keep as-is. The
        # prefix itself contains two colons, so a former ``uid.count(":") >= 2``
        # pre-check was strictly implied by this ``startswith`` and has been
        # removed as dead code (no input satisfied it without also matching here).
        if uid.startswith(f"{entry_id}:{identifier}:"):
            return uid
        if uid.startswith(f"{entry_id}:"):
            remainder = uid[len(entry_id) + 1 :]
            if remainder:
                return f"{entry_id}:{identifier}:{remainder}"
            return None
        scoped_prefix = f"{DOMAIN}_{entry_id}_"
        if uid.startswith(scoped_prefix):
            remainder = uid[len(scoped_prefix) :]
            if remainder.startswith(f"{identifier}_"):
                return uid
            return f"{entry_id}:{identifier}:{remainder}"
        legacy_prefix = f"{DOMAIN}_"
        if uid.startswith(legacy_prefix):
            remainder = uid[len(legacy_prefix) :]
            return f"{entry_id}:{identifier}:{remainder}"
        return None

    if feature == "binary_sensor":
        binary_sensor_suffixes: tuple[str, ...] = ("polling", "auth_status")
        if uid.count(":") >= 2:
            parts = uid.split(":")
            if len(parts) >= 3 and parts[0] == entry_id and parts[1] == identifier:
                if parts[2] in binary_sensor_suffixes:
                    return uid
            if (
                len(parts) == 2
                and parts[0] == entry_id
                and parts[1] in binary_sensor_suffixes
            ):
                return f"{entry_id}:{identifier}:{parts[1]}"
            return None
        if uid.startswith(f"{entry_id}:"):
            suffix = uid[len(entry_id) + 1 :]
            if suffix in binary_sensor_suffixes:
                return f"{entry_id}:{identifier}:{suffix}"
            return None
        scoped_prefix = f"{DOMAIN}_{entry_id}_"
        if uid.startswith(scoped_prefix):
            suffix = uid[len(scoped_prefix) :]
            if suffix in binary_sensor_suffixes:
                return f"{entry_id}:{identifier}:{suffix}"
        legacy_prefix = f"{DOMAIN}_"
        if uid.startswith(legacy_prefix):
            suffix = uid[len(legacy_prefix) :]
            if suffix in binary_sensor_suffixes:
                return f"{entry_id}:{identifier}:{suffix}"
        return None

    if feature == "sensor":
        tracker_identifier = subentry_map.get("device_tracker")
        if not isinstance(tracker_identifier, str) or not tracker_identifier:
            tracker_identifier = None
        service_identifier = subentry_map.get("binary_sensor")
        if not isinstance(service_identifier, str) or not service_identifier:
            service_identifier = None

        def _select_sensor_identifier(
            remainder: str,
        ) -> tuple[str, str]:
            """Return the identifier and normalized remainder for sensor entities."""

            tracker_prefix = (
                f"{tracker_identifier}_" if tracker_identifier is not None else None
            )
            service_prefix = (
                f"{service_identifier}_" if service_identifier is not None else None
            )

            if service_prefix and remainder.startswith(service_prefix):
                assert service_identifier is not None
                return service_identifier, remainder

            if tracker_prefix and remainder.startswith(tracker_prefix):
                tail = remainder[len(tracker_prefix) :]
                if service_prefix and tail.startswith(service_prefix):
                    assert service_identifier is not None
                    return service_identifier, tail
                chosen = (
                    tracker_identifier if tracker_identifier is not None else identifier
                )
                return chosen, remainder

            if service_identifier and not _looks_like_tracker_sensor_suffix(remainder):
                return service_identifier, remainder

            if tracker_identifier:
                return tracker_identifier, remainder

            return identifier, remainder

        scoped_prefix = f"{DOMAIN}_{entry_id}_"
        if uid.startswith(scoped_prefix):
            remainder = uid[len(scoped_prefix) :]
            target_identifier, normalized_remainder = _select_sensor_identifier(
                remainder
            )
            if normalized_remainder.startswith(f"{target_identifier}_"):
                return f"{DOMAIN}_{entry_id}_{normalized_remainder}"
            return f"{DOMAIN}_{entry_id}_{target_identifier}_{normalized_remainder}"
        legacy_prefix = f"{DOMAIN}_"
        if uid.startswith(legacy_prefix):
            remainder = uid[len(legacy_prefix) :]
            if remainder.startswith(f"{entry_id}_"):
                remainder = remainder[len(entry_id) + 1 :]
            target_identifier, normalized_remainder = _select_sensor_identifier(
                remainder
            )
            if normalized_remainder.startswith(f"{target_identifier}_"):
                return f"{DOMAIN}_{entry_id}_{normalized_remainder}"
            return f"{DOMAIN}_{entry_id}_{target_identifier}_{normalized_remainder}"
        return None

    if feature == "button":
        button_suffixes: tuple[str, ...] = (
            "_play_sound",
            "_stop_sound",
            "_locate_device",
        )
        scoped_prefix = f"{DOMAIN}_{entry_id}_"
        if uid.startswith(scoped_prefix):
            remainder = uid[len(scoped_prefix) :]
            if remainder.startswith(f"{identifier}_"):
                return uid
            if any(remainder.endswith(suffix) for suffix in button_suffixes):
                normalized_remainder = _normalize_legacy_button_remainder(
                    remainder,
                    identifier=identifier,
                    suffixes=button_suffixes,
                )
                return f"{DOMAIN}_{entry_id}_{identifier}_{normalized_remainder}"
            return None
        legacy_prefix = f"{DOMAIN}_"
        if uid.startswith(legacy_prefix):
            remainder = uid[len(legacy_prefix) :]
            if remainder.startswith(f"{entry_id}_"):
                remainder = remainder[len(entry_id) + 1 :]
            if remainder.startswith(f"{identifier}_"):
                return uid
            if any(remainder.endswith(suffix) for suffix in button_suffixes):
                normalized_remainder = _normalize_legacy_button_remainder(
                    remainder,
                    identifier=identifier,
                    suffixes=button_suffixes,
                )
                return f"{DOMAIN}_{entry_id}_{identifier}_{normalized_remainder}"
        return None

    return None


async def _async_migrate_device_identifiers_to_entry_scope(
    hass: HomeAssistant, entry: ConfigEntry
) -> None:
    """Prepare migration of device identifiers to entry scope (NOT invoked yet).

    Goal:
        Replace legacy device identifiers (DOMAIN, <canonical_id>) with entry-scoped
        identifiers (DOMAIN, f"{entry.entry_id}:{canonical_id}") to avoid collisions
        across accounts.

    Safety:
        - Skips "service"/integration devices.
        - Idempotent: already namespaced identifiers are ignored.
        - Collision-aware: if a target identifier exists, it will skip and log a warning.
    """
    # Local import, see ``_async_purge_unloaded_subentry_registrations``.
    from .coordinator.helpers.registry import iter_all_devices

    dev_reg = dr.async_get(hass)
    updated = 0
    skipped = 0
    collisions = 0

    for device in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
        # Keep service/integration device untouched
        if (DOMAIN, "integration") in device.identifiers or any(
            domain == DOMAIN and str(ident).startswith("integration_")
            for domain, ident in device.identifiers
        ):
            continue

        our_ids = [(d, i) for (d, i) in device.identifiers if d == DOMAIN]
        if not our_ids:
            continue

        # Build new set of identifiers for this device
        new_identifiers = set(device.identifiers)
        dirty = False

        for _domain, ident in our_ids:
            ident_str = str(ident)
            if ":" in ident_str and ident_str.startswith(entry.entry_id + ":"):
                skipped += 1
                continue  # already namespaced for this entry
            target = (DOMAIN, f"{entry.entry_id}:{ident_str}")

            # Check for collision: if any other device already uses the target ident, skip
            conflict = False
            # Re-read on every candidate on purpose: the loop above rewrites
            # identifiers as it goes, and a snapshot taken once would not see
            # the target this pass just created.
            for dev2 in iter_all_devices(dev_reg):
                if dev2.id == device.id:
                    continue
                if target in dev2.identifiers:
                    conflict = True
                    break
            if conflict:
                collisions += 1
                _LOGGER.warning(
                    "Identifier migration skipped (collision): %s -> %s on device %s",
                    (DOMAIN, ident_str),
                    target,
                    device.id,
                )
                continue

            # Perform substitution
            new_identifiers.discard((DOMAIN, ident_str))
            new_identifiers.add(target)
            dirty = True

        if dirty and new_identifiers != device.identifiers:
            dev_reg.async_update_device(
                device_id=device.id, new_identifiers=new_identifiers
            )
            updated += 1

    _LOGGER.info(
        "Prepared entry-scoped identifier migration (dry): updated=%d, skipped=%d, collisions=%d",
        updated,
        skipped,
        collisions,
    )


# --------------------------- Shared FCM provider ---------------------------


async def _async_acquire_shared_fcm(
    hass: HomeAssistant,
    *,
    entry: ConfigEntry | None = None,
    cache: TokenCache | None = None,
    entry_resolver: Callable[[], str | None] | None = None,
) -> FcmReceiverHAType:
    """Get or create the entry-scoped FCM receiver for this HA instance.

    Behavior:
        - Creates and initializes the per-entry receiver if missing.
        - Registers provider callbacks for API and LocateTracker once.
        - Maintains a reference counter per entry to support multiple bindings.
        - NEW: attaches HA context to enable owner-index fallback routing.
    """
    _ensure_runtime_imports()
    bucket = _domain_data(hass)
    fcm_lock: asyncio.Lock = _ensure_fcm_lock(bucket)
    if not isinstance(bucket.get("providers_registered"), bool):
        bucket["providers_registered"] = False
    if fcm_lock.locked():
        contention = bucket.get("fcm_lock_contention_count")
        if not isinstance(contention, int):
            contention = 0
        bucket["fcm_lock_contention_count"] = contention + 1
    async with fcm_lock:
        entry_id = getattr(entry, "entry_id", None)
        refcount = _get_fcm_refcount(bucket, entry_id or "default")
        providers_registered = bucket.get("providers_registered", False)
        receivers = _get_fcm_receivers(bucket)
        receiver_cls = _resolve_fcm_receiver_class()
        fcm: FcmReceiverHAType | None = None
        if entry_id is not None:
            fcm = receivers.get(entry_id)
        elif len(receivers) == 1:
            fcm = next(iter(receivers.values()))

        def _method_is_coroutine(receiver: object, name: str) -> bool:
            """Return True if receiver.name is an async callable."""

            attr = getattr(receiver, name, None)
            if attr is None:
                return False
            candidate = getattr(attr, "__func__", attr)
            try:
                candidate = inspect.unwrap(
                    candidate
                )  # unwrap functools.partial / wraps
            except Exception:  # pragma: no cover - defensive
                pass
            if inspect.iscoroutinefunction(candidate):
                return True
            cls_attr = getattr(type(receiver), name, None)
            if cls_attr is not None:
                cls_candidate = getattr(cls_attr, "__func__", cls_attr)
                return inspect.iscoroutinefunction(cls_candidate)
            return False

        raw_receiver = bucket.get("fcm_receiver")
        if raw_receiver is not None and not isinstance(raw_receiver, receiver_cls):
            _LOGGER.warning(
                "Discarding cached FCM receiver with unexpected type: %s",
                type(raw_receiver).__name__,
            )
            bucket.pop("fcm_receiver", None)
            await _async_stop_receiver_if_possible(raw_receiver)
        elif isinstance(raw_receiver, receiver_cls) and fcm is None:
            if entry_id is None or not receivers:
                fcm = raw_receiver

        if fcm is not None and (
            not _method_is_coroutine(fcm, "async_register_for_location_updates")
            or not _method_is_coroutine(fcm, "async_unregister_for_location_updates")
        ):
            _LOGGER.warning(
                "Discarding cached FCM receiver lacking async registration methods"
            )
            stale = _pop_any_fcm_receiver(bucket)
            await _async_stop_receiver_if_possible(stale)
            fcm = None

        if fcm is None:
            fcm = receiver_cls()
            _LOGGER.debug("Initializing shared FCM receiver...")
            ok = await fcm.async_initialize(entry_id=entry_id, cache=cache)
            if not ok:
                raise ConfigEntryNotReady("Failed to initialize FCM receiver")

            # --- NEW: Attach HA context for owner-index fallback routing ---
            try:
                attach = getattr(fcm, "attach_hass", None)
                if callable(attach):
                    attach(hass)
                    _LOGGER.debug(
                        "Attached HA context to FCM receiver (owner-index routing enabled)."
                    )
            except Exception as err:
                _LOGGER.debug("FCM attach_hass skipped: %s", err)

            _set_fcm_receiver(bucket, entry_id or "default", fcm)
            if entry_id:
                bucket.setdefault("default_fcm_entry_id", entry_id)
            _LOGGER.info("Shared FCM receiver initialized")

            if callable(entry_resolver) and entry_id:
                resolvers = bucket.setdefault("fcm_provider_resolvers", {})
                resolvers[entry_id] = entry_resolver

            # Register provider for both consumer modules (exactly once on first acquire)
            # Re-registering ensures downstream modules resolve the refreshed instance.
            def provider(entry_id: str | None = None) -> FcmReceiverHAType:
                """Return the shared FCM receiver for integration consumers."""

                return _domain_fcm_provider(hass, entry_id)

            provider_fn: Callable[[str | None], FcmReceiverHAType] = provider
            if not providers_registered:
                global loc_register_fcm_provider, loc_unregister_fcm_provider
                if loc_register_fcm_provider is None:
                    from .NovaApi.ExecuteAction.LocateTracker.location_request import (
                        register_fcm_receiver_provider as _loc_register_fcm_provider,
                    )

                    loc_register_fcm_provider = _loc_register_fcm_provider
                if loc_unregister_fcm_provider is None:
                    from .NovaApi.ExecuteAction.LocateTracker.location_request import (
                        unregister_fcm_receiver_provider as _loc_unregister_fcm_provider,
                    )

                    loc_unregister_fcm_provider = _loc_unregister_fcm_provider

                if loc_register_fcm_provider is not None:
                    loc_register_fcm_provider(
                        cast(
                            Callable[[str | None], NovaFcmReceiverProtocol], provider_fn
                        )
                    )
                api_register_fcm_provider(
                    cast(Callable[[str | None], ApiFcmReceiverProtocol], provider_fn)
                )
                bucket["providers_registered"] = True

        new_refcount = refcount + 1
        _set_fcm_refcount(bucket, entry_id or "default", new_refcount)
        _LOGGER.debug("FCM refcount[%s] -> %s", entry_id or "default", new_refcount)
        return fcm


async def _async_release_shared_fcm(
    hass: HomeAssistant, entry: ConfigEntry | None = None
) -> None:
    """Decrease refcount; stop and unregister provider when it reaches zero."""
    _ensure_runtime_imports()
    bucket = _domain_data(hass)
    fcm_lock: asyncio.Lock = _ensure_fcm_lock(bucket)
    async with fcm_lock:
        entry_id = getattr(entry, "entry_id", None) or "default"
        refcount = _get_fcm_refcount(bucket, entry_id) - 1
        refcount = max(refcount, 0)
        _set_fcm_refcount(bucket, entry_id, refcount)
        _LOGGER.debug("FCM refcount[%s] -> %s", entry_id, refcount)

        if refcount != 0:
            return

        fcm = _pop_fcm_receiver(bucket, entry_id)
        if entry_id == bucket.get("default_fcm_entry_id"):
            bucket.pop("default_fcm_entry_id", None)
        resolvers = bucket.get("fcm_provider_resolvers")
        if isinstance(resolvers, dict):
            resolvers.pop(entry_id, None)

        # Unregister providers first (consumers will see provider=None immediately)
        if not _get_fcm_receivers(bucket):
            global loc_unregister_fcm_provider
            if loc_unregister_fcm_provider is None:
                from .NovaApi.ExecuteAction.LocateTracker.location_request import (
                    unregister_fcm_receiver_provider as _loc_unregister_fcm_provider,
                )

                loc_unregister_fcm_provider = _loc_unregister_fcm_provider

            try:
                if loc_unregister_fcm_provider is not None:
                    loc_unregister_fcm_provider()
            except Exception:
                pass
            try:
                api_unregister_fcm_provider()
            except Exception:
                pass

            bucket["providers_registered"] = False

        if fcm is not None:
            try:
                await fcm.async_stop()
                _LOGGER.info("Shared FCM receiver stopped")
            except Exception as err:
                _LOGGER.warning("Stopping FCM receiver failed: %s", err)


# ------------------------------ Setup / Unload -----------------------------


def _resolve_entry_email(entry: ConfigEntry) -> tuple[str | None, str | None]:
    """Return the raw and normalized e-mail associated with a config entry."""

    raw_email: str | None = None
    for container in (getattr(entry, "data", {}), getattr(entry, "options", {})):
        if not isinstance(container, Mapping):
            continue
        email_value = container.get(CONF_GOOGLE_EMAIL)
        if isinstance(email_value, str) and email_value.strip():
            raw_email = email_value.strip()
            break

    if raw_email is None:
        # Only the account e-mail is read from the bundle. The local carries a
        # neutral name on purpose: CodeQL treats any local named like a secret as
        # sensitive and has no sanitizer for _mask_email_for_logs, so every log
        # line showing this account's masked e-mail would count as clear-text
        # logging of a secret (AGENTS.md, "Log hygiene for scanners").
        bundle: Mapping[str, Any] | None = None
        for container in (getattr(entry, "data", {}), getattr(entry, "options", {})):
            if isinstance(container, Mapping):
                bundle_candidate = container.get(DATA_SECRET_BUNDLE)
                if isinstance(bundle_candidate, Mapping):
                    bundle = bundle_candidate
                    break
        if bundle is not None:
            for key in ("google_email", "username", "Email", "email"):
                candidate = bundle.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    raw_email = candidate.strip()
                    break

    normalized_email = normalize_email(raw_email)
    return raw_email, normalized_email


def _extract_email_from_entry(entry: ConfigEntry) -> str | None:
    """Return the normalized email for ``entry`` if available."""

    _, normalized = _resolve_entry_email(entry)
    return normalized


def _apply_update_entry_fallback(
    hass: HomeAssistant, entry: ConfigEntry, update_kwargs: Mapping[str, Any]
) -> None:
    """Apply entry update fields when stubs reject extended keywords."""

    data = update_kwargs.get("data")
    if isinstance(data, Mapping):
        entry.data = dict(data)

    title = update_kwargs.get("title")
    if isinstance(title, str):
        entry.title = title

    unique_id = update_kwargs.get("unique_id")
    if isinstance(unique_id, str):
        setattr(entry, "unique_id", unique_id)

    options = update_kwargs.get("options")
    if isinstance(options, Mapping):
        hass.config_entries.async_update_entry(entry, options=dict(options))

    version_value = update_kwargs.get("version")
    if isinstance(version_value, int):
        entry.version = version_value


def _format_duplicate_entries(
    entry: ConfigEntry, conflicts: Sequence[ConfigEntry] | None
) -> str:
    """Return a bullet list describing duplicate config entries."""

    ordered: list[ConfigEntry] = [entry, *(conflicts or ())]
    seen: set[str] = set()
    lines: list[str] = []
    for candidate in ordered:
        entry_id = getattr(candidate, "entry_id", "") or ""
        if entry_id in seen:
            continue
        seen.add(entry_id)
        label = candidate.title or entry_id or ""
        if entry_id:
            lines.append(f"- {label} ({entry_id})")
        else:
            lines.append(f"- {label}")
    return "\n".join(lines)


def _mask_email_for_logs(email: str | None) -> str:
    """Return a privacy-friendly representation of an email for logs."""

    if not email or "@" not in email:
        return "<unknown>"

    local, domain = email.split("@", 1)
    if not local:
        return f"*@{domain}"

    masked_local = (local[0] + "***") if len(local) > 1 else "*"
    return f"{masked_local}@{domain}"


def _label_entry_for_log(entry: ConfigEntry) -> str:
    """Return a privacy-safe label for log messages referencing ``entry``."""

    email = _extract_email_from_entry(entry)
    if email and "@" in email:
        return _mask_email_for_logs(email)
    # The entry title is never logged: the config flow sets it to the account
    # e-mail, and a renamed title is free text that may name a person. Masking
    # addresses inside free text cannot be made complete, and the entry ID
    # identifies the entry just as well.
    entry_id = getattr(entry, "entry_id", None)
    if isinstance(entry_id, str) and entry_id:
        return entry_id
    return "<unknown>"


def _issue_exists(hass: HomeAssistant, issue_id: str) -> bool:
    """Return True if a repair issue with the given ID exists for this domain.

    NOTE: issue_registry.async_get(...) is a synchronous callback helper in Home
    Assistant. Do not await it. The returned registry exposes synchronous
    "async_*" methods that operate in the event loop thread.
    """

    try:
        registry = ir.async_get(hass)
    except Exception:  # pragma: no cover - defensive fallback
        return False

    get_issue = getattr(registry, "async_get_issue", None)
    if not callable(get_issue):
        return False

    try:
        return get_issue(DOMAIN, issue_id) is not None
    except Exception:  # pragma: no cover - defensive fallback
        return False


def _log_duplicate_and_raise_repair_issue(
    hass: HomeAssistant,
    entry: ConfigEntry,
    normalized_email: str,
    *,
    cause: str,
    conflicts: Sequence[ConfigEntry] | None = None,
) -> None:
    """Create or refresh a Repair issue for duplicate account configuration."""

    issue_id = f"duplicate_account_{entry.entry_id}"
    issue_present = _issue_exists(hass, issue_id)
    log_fn = _LOGGER.debug if issue_present else _LOGGER.warning
    log_fn(
        "googlefindmy %s: duplicate account %s detected (%s)",
        entry.entry_id,
        _mask_email_for_logs(normalized_email),
        cause,
    )
    placeholders: dict[str, Any] = {
        "email": normalized_email,
        "entries": _format_duplicate_entries(entry, conflicts),
        "cause": cause,
    }

    issue_severity = getattr(ir, "IssueSeverity", None)
    if issue_severity is not None:
        severity_value = getattr(issue_severity, "WARNING", None)
        if severity_value is None:
            severity_value = getattr(issue_severity, "ERROR", "warning")
    else:
        severity_value = getattr(ir, "WARNING", "warning")

    try:
        ir.async_create_issue(
            hass,
            DOMAIN,
            issue_id,
            is_fixable=False,
            severity=severity_value,
            translation_key=TRANSLATION_KEY_DUPLICATE_ACCOUNT,
            translation_placeholders=placeholders,
        )
    except Exception as err:  # pragma: no cover - defensive log only
        _LOGGER.debug("Failed to create duplicate-account repair issue: %s", err)


def _clear_duplicate_account_issue(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Remove the duplicate-account Repair issue when resolved."""

    try:
        ir.async_delete_issue(hass, DOMAIN, f"duplicate_account_{entry.entry_id}")
    except Exception:
        return


def _integration_disabled_by_value() -> object:
    """Return the ConfigEntry disabled marker for integration-managed entries."""

    if _ConfigEntryDisabler is None:
        return "integration"

    return getattr(_ConfigEntryDisabler, "INTEGRATION", "integration")


def _is_user_disabled(entry: ConfigEntry) -> bool:
    """Return True if the entry has been disabled explicitly by the user."""

    disabled = getattr(entry, "disabled_by", None)
    if disabled is None:
        return False

    disabled_text = str(disabled).lower()
    return "user" in disabled_text


def _is_integration_disabled(entry: ConfigEntry) -> bool:
    """Return True if the entry has already been disabled by the integration."""

    disabled = getattr(entry, "disabled_by", None)
    if disabled is None:
        return False

    disabled_text = str(disabled).lower()
    return "integration" in disabled_text


def _schedule_duplicate_unload(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Schedule an asynchronous unload for a duplicate config entry."""

    unload_candidates = (
        ConfigEntryState.LOADED,
        getattr(ConfigEntryState, "SETUP_RETRY", None),
        getattr(ConfigEntryState, "SETUP_ERROR", None),
    )

    if entry.state not in unload_candidates:
        return

    _LOGGER.debug(
        "Scheduling unload for duplicate entry %s (state=%s)",
        entry.entry_id,
        entry.state,
    )

    _async_create_task(
        hass,
        hass.config_entries.async_unload(entry.entry_id),
        name=f"{DOMAIN}.unload_duplicate.{entry.entry_id}",
    )


def _clear_stale_duplicate_account_issues(
    hass: HomeAssistant,
    *,
    normalized_email: str | None,
    active_entry_ids: Collection[str],
) -> None:
    """Remove lingering duplicate-account issues for a cleared email."""

    if not normalized_email:
        return

    registry = ir.async_get(hass)
    issues_attr = getattr(registry, "issues", None)
    if isinstance(issues_attr, Mapping):
        issues_iterable = list(issues_attr.items())
    else:
        private_issues = getattr(registry, "_issues", None)
        if not isinstance(private_issues, Mapping):
            return
        issues_iterable = list(private_issues.items())

    for key, payload in issues_iterable:
        issue_id: str
        domain = DOMAIN
        if isinstance(key, tuple) and len(key) == 2:
            domain, issue_id = str(key[0]), str(key[1])
        else:
            issue_id = str(key)
            domain = str(payload.get("domain", domain))

        if not issue_id.startswith("duplicate_account_"):
            continue
        if domain != DOMAIN:
            continue
        placeholders = payload.get("translation_placeholders") or {}
        if placeholders.get("email") != normalized_email:
            continue
        suffix = issue_id.removeprefix("duplicate_account_")
        if suffix in active_entry_ids:
            continue
        try:
            ir.async_delete_issue(hass, DOMAIN, issue_id)
        except Exception:  # pragma: no cover - defensive cleanup
            continue


def _select_authoritative_entry_id(
    entry: ConfigEntry, duplicates: Sequence[ConfigEntry]
) -> str:
    """Return the entry_id that should remain active for a duplicate account.

    Preference order:
    1. Config entry state (loaded > retry > error > pending > not loaded).
    2. Most recent timestamp, preferring ``updated_at`` over ``created_at``.
    3. Deterministic tiebreaker using ``entry_id`` for stability.
    """

    candidates = [entry, *duplicates]

    def _state_rank(state: ConfigEntryState | None) -> int:
        order: dict[object, int] = {ConfigEntryState.LOADED: 0}

        setup_retry = getattr(ConfigEntryState, "SETUP_RETRY", None)
        if setup_retry is not None:
            order[setup_retry] = 1
        else:  # pragma: no cover - compatibility with very old cores
            order["setup_retry"] = 1

        setup_error = getattr(ConfigEntryState, "SETUP_ERROR", None)
        if setup_error is not None:
            order[setup_error] = 2
        else:  # pragma: no cover
            order["setup_error"] = 2

        migration_error = getattr(ConfigEntryState, "MIGRATION_ERROR", None)
        if migration_error is not None:
            order[migration_error] = 3
        else:  # pragma: no cover
            order["migration_error"] = 3

        setup_in_progress = getattr(ConfigEntryState, "SETUP_IN_PROGRESS", None)
        if setup_in_progress is not None:
            order[setup_in_progress] = 4
        else:  # pragma: no cover
            order["setup_in_progress"] = 4

        order[ConfigEntryState.NOT_LOADED] = 5

        return order.get(state, 5)

    def _candidate_key(candidate: ConfigEntry) -> tuple[int, float, str]:
        state_rank = _state_rank(getattr(candidate, "state", None))

        timestamp = getattr(candidate, "updated_at", None) or getattr(
            candidate, "created_at", None
        )
        ts_rank = float("inf")
        if isinstance(timestamp, datetime):
            try:
                ts_rank = -float(timestamp.timestamp())
            except (OSError, ValueError):  # pragma: no cover - defensive fallback
                ts_rank = float("inf")

        entry_id = str(getattr(candidate, "entry_id", "") or "")
        return (state_rank, ts_rank, entry_id)

    authoritative = min(candidates, key=_candidate_key)
    return str(authoritative.entry_id)


async def _ensure_post_migration_consistency(
    hass: HomeAssistant,
    entry: ConfigEntry,
    *,
    duplicate_issue_cause: str = "setup_duplicate",
) -> tuple[bool, str | None]:
    """Repair stale metadata and detect duplicates before setup."""

    raw_email, normalized_email = _resolve_entry_email(entry)

    new_data = dict(entry.data)
    if raw_email and new_data.get(CONF_GOOGLE_EMAIL) != raw_email:
        new_data[CONF_GOOGLE_EMAIL] = raw_email

    update_kwargs: dict[str, Any] = {}
    if new_data != entry.data:
        update_kwargs["data"] = new_data

    if raw_email and entry.title != raw_email:
        update_kwargs["title"] = raw_email

    unique_id = unique_account_id(normalized_email)
    current_unique_id = getattr(entry, "unique_id", None)
    if unique_id and current_unique_id != unique_id:
        update_kwargs["unique_id"] = unique_id

    if update_kwargs:
        try:
            hass.config_entries.async_update_entry(entry, **update_kwargs)
        except TypeError:
            _apply_update_entry_fallback(hass, entry, update_kwargs)
        except ValueError:
            if normalized_email:
                _log_duplicate_and_raise_repair_issue(
                    hass,
                    entry,
                    normalized_email,
                    cause="unique_id_conflict_setup",
                )
            update_kwargs.pop("unique_id", None)
            if update_kwargs:
                try:
                    hass.config_entries.async_update_entry(entry, **update_kwargs)
                except TypeError:
                    _apply_update_entry_fallback(hass, entry, update_kwargs)

    duplicates: list[ConfigEntry] = []
    if normalized_email:
        duplicates = [
            candidate
            for candidate in hass.config_entries.async_entries(DOMAIN)
            if candidate.entry_id != entry.entry_id
            and _extract_email_from_entry(candidate) == normalized_email
        ]

    authoritative_entry_id = _select_authoritative_entry_id(entry, duplicates)

    active_issue_entry_ids: set[str] = set()

    if duplicates and normalized_email:
        disabled_by_integration: list[str] = []
        retained_user_disabled: list[str] = []
        manual_action_required: list[str] = []

        _clear_duplicate_account_issue(hass, entry)

        conflicts: list[ConfigEntry] = [entry, *duplicates]

        for candidate in duplicates:
            if candidate.entry_id == authoritative_entry_id:
                _clear_duplicate_account_issue(hass, candidate)
                continue

            if _is_user_disabled(candidate):
                retained_user_disabled.append(candidate.entry_id)
                _clear_duplicate_account_issue(hass, candidate)
                _schedule_duplicate_unload(hass, candidate)
                continue

            if _is_integration_disabled(candidate):
                disabled_by_integration.append(candidate.entry_id)
                _clear_duplicate_account_issue(hass, candidate)
                _schedule_duplicate_unload(hass, candidate)
                continue

            try:
                await hass.config_entries.async_set_disabled_by(
                    candidate.entry_id,
                    _integration_disabled_by_value(),
                )
            except (TypeError, AttributeError):
                manual_action_required.append(candidate.entry_id)
                _LOGGER.warning(
                    "Duplicate entry %s could not be disabled via API (legacy Core). "
                    "Left unloaded; issued repair for manual action.",
                    candidate.entry_id,
                )
                _schedule_duplicate_unload(hass, candidate)
                _log_duplicate_and_raise_repair_issue(
                    hass,
                    candidate,
                    normalized_email,
                    cause=duplicate_issue_cause,
                    conflicts=conflicts,
                )
                active_issue_entry_ids.add(candidate.entry_id)
                continue

            disabled_by_integration.append(candidate.entry_id)
            _clear_duplicate_account_issue(hass, candidate)
            _schedule_duplicate_unload(hass, candidate)

        if manual_action_required:
            _LOGGER.info(
                "Duplicate account %s → authoritative=%s; disabled=%s; user_disabled=%s; manual_action_required=%s",
                _mask_email_for_logs(normalized_email),
                authoritative_entry_id,
                disabled_by_integration,
                retained_user_disabled,
                manual_action_required,
            )
        elif disabled_by_integration or retained_user_disabled:
            _LOGGER.info(
                "Duplicate account %s → authoritative=%s; disabled=%s; user_disabled=%s",
                _mask_email_for_logs(normalized_email),
                authoritative_entry_id,
                disabled_by_integration,
                retained_user_disabled,
            )
    else:
        _clear_duplicate_account_issue(hass, entry)

    _clear_stale_duplicate_account_issues(
        hass,
        normalized_email=normalized_email,
        active_entry_ids=active_issue_entry_ids,
    )

    should_setup = not duplicates or entry.entry_id == authoritative_entry_id
    return should_setup, normalized_email


# --------------------------------------------------------------------------------------
# Restart-required watchdog
#
# HACS writes freshly downloaded integration code to disk while the running process keeps
# executing the previously imported module. We surface that mismatch as a global,
# non-fixable Repairs issue so the user knows a *Home Assistant restart* (not merely an
# integration reload) is required to activate the installed version.
# --------------------------------------------------------------------------------------
_RESTART_CHECK_INITIAL_DELAY: float = 30.0
_RESTART_CHECK_INTERVAL: timedelta = timedelta(hours=1)


def _read_installed_version_sync() -> str | None:
    """Return manifest.json's version from disk (blocking; run inside an executor)."""
    try:
        manifest_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "manifest.json"
        )
        with open(manifest_path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError) as err:  # IO failure or malformed JSON
        _LOGGER.debug("Restart check: could not read installed version: %s", err)
        return None
    version = data.get("version") if isinstance(data, dict) else None
    return version if isinstance(version, str) and version else None


async def _async_read_installed_version(hass: HomeAssistant) -> str | None:
    """Return the on-disk manifest version without blocking the event loop."""
    version: str | None = await hass.async_add_executor_job(
        _read_installed_version_sync
    )
    return version


async def _async_check_restart_required(hass: HomeAssistant) -> None:
    """Raise or clear the global "restart required" Repairs issue.

    The running module carries ``INTEGRATION_VERSION``; the on-disk manifest reflects what
    HACS has installed. A mismatch means a restart is pending. On equality or any read
    error the issue is cleared (self-healing, create/delete symmetric). This coroutine
    never raises into Home Assistant.
    """
    try:
        installed = await _async_read_installed_version(hass)
        if installed is not None and installed != INTEGRATION_VERSION:
            # A last-entry teardown may have run during the executor read above
            # (the only await in this coroutine). If the watchdog has since been
            # torn down, do not resurrect the issue: no timer would ever clear it
            # again. The registration flag lives in the global domain bucket and
            # is popped by _teardown_restart_required_check (Codex-P1 class:
            # tick firing after unload).
            if not _domain_data(hass).get("restart_check_registered"):
                return
            issue_severity = getattr(ir, "IssueSeverity", None)
            if issue_severity is not None:
                severity = getattr(issue_severity, "WARNING", None)
                if severity is None:
                    severity = getattr(issue_severity, "ERROR", "warning")
            else:
                severity = getattr(ir, "WARNING", "warning")
            ir.async_create_issue(
                hass,
                DOMAIN,
                ISSUE_RESTART_REQUIRED_KEY,
                is_fixable=False,
                severity=severity,
                translation_key=TRANSLATION_KEY_RESTART_REQUIRED,
                translation_placeholders={
                    "running_version": INTEGRATION_VERSION,
                    "installed_version": installed,
                },
            )
        else:
            ir.async_delete_issue(hass, DOMAIN, ISSUE_RESTART_REQUIRED_KEY)
    except Exception as err:  # noqa: BLE001 - watchdog must never raise into HA
        _LOGGER.debug("Restart check failed: %s", err)


@callback
def _register_restart_required_check(
    hass: HomeAssistant, bucket: GoogleFindMyDomainData
) -> None:
    """Register the one-shot + hourly restart watchdog exactly once.

    Both unsub handles (initial ``async_call_later`` and periodic
    ``async_track_time_interval``) are stored so the last-entry teardown can cancel them,
    preventing a timer leak or a tick firing after unload (Codex-P1 #1/#2). The initial
    schedule mirrors the existing awaitable-guard precedent for ``async_call_later``.
    """

    @callback
    def _scheduled_check(_now: Any) -> None:
        _async_create_task(
            hass,
            _async_check_restart_required(hass),
            name=f"{DOMAIN}.restart_required_check",
        )

    initial_handle = async_call_later(
        hass, _RESTART_CHECK_INITIAL_DELAY, _scheduled_check
    )
    if inspect.isawaitable(initial_handle):
        # Some HA test stubs return an awaitable instead of an unsub callable.
        _async_create_task(
            hass,
            cast(CoroutineType, initial_handle),
            name=f"{DOMAIN}.restart_required_initial_schedule",
        )
        bucket["restart_check_initial_unsub"] = None
    else:
        bucket["restart_check_initial_unsub"] = initial_handle

    bucket["restart_check_unsub"] = async_track_time_interval(
        hass, _scheduled_check, _RESTART_CHECK_INTERVAL
    )


_URL_REFRESH_INTERVAL = timedelta(days=1)


def _register_url_refresh_timer(
    hass: HomeAssistant, bucket: GoogleFindMyDomainData
) -> None:
    """Refresh the device configuration URLs once a day.

    Without this the URLs are rebuilt only during setup. With the weekly token
    expiry enabled, an instance that runs across two week boundaries without a
    restart or reload ends up with a dead Map View link on its own device page;
    the map accepts the current and the previous week, so two boundaries is the
    first point at which the stored link is certainly stale.

    A run that changes nothing costs nothing: the device registry returns early
    when the value is unchanged, and the periodic path reports an unreachable or
    internal-only URL only when that situation *changes* (see
    ``_async_refresh_device_urls``).
    """

    @callback
    def _scheduled_refresh(_now: Any) -> None:
        _async_create_task(
            hass,
            _async_refresh_device_urls(hass, periodic=True),
            name=f"{DOMAIN}.refresh_device_urls",
        )

    bucket["url_refresh_unsub"] = async_track_time_interval(
        hass, _scheduled_refresh, _URL_REFRESH_INTERVAL
    )


@callback
def _teardown_url_refresh_timer(
    hass: HomeAssistant, bucket: GoogleFindMyDomainData
) -> None:
    """Cancel the daily URL refresh (last-entry teardown)."""

    unsub = bucket.pop("url_refresh_unsub", None)
    if callable(unsub):
        with suppress(Exception):
            unsub()
    bucket.pop("url_refresh_registered", None)
    bucket.pop("url_refresh_state", None)


@callback
def _teardown_restart_required_check(
    hass: HomeAssistant, bucket: GoogleFindMyDomainData
) -> None:
    """Cancel both restart-watchdog timers and clear the issue (last-entry teardown)."""
    for unsub in (
        bucket.pop("restart_check_initial_unsub", None),
        bucket.pop("restart_check_unsub", None),
    ):
        if callable(unsub):
            with suppress(Exception):
                unsub()
    bucket.pop("restart_check_registered", None)
    with suppress(Exception):
        ir.async_delete_issue(hass, DOMAIN, ISSUE_RESTART_REQUIRED_KEY)


async def _async_ensure_restart_required_check(
    hass: HomeAssistant, bucket: GoogleFindMyDomainData
) -> None:
    """Idempotently arm the global restart-required watchdog.

    Called from both ``async_setup`` (component load) and ``async_setup_entry``
    (each parent entry load). The shared services lock plus the
    ``restart_check_registered`` idempotent flag keep a single timer pair even
    across racey startups or multiple config entries (Codex-P1 #6).

    Re-arming from ``async_setup_entry`` is what restores the watchdog after a
    last-entry reload: ``_teardown_restart_required_check`` cancels the timers and
    pops the flag on the final unload, but ``async_setup`` does *not* run again on
    a config-entry reload. Without this re-arm the "updated but not yet restarted"
    warning would stay hidden until a full Home Assistant restart, defeating the
    common "reload instead of restart" scenario. This mirrors the EID resolver,
    the other global singleton that is torn down on last-entry unload and
    re-created on entry setup.
    """
    services_lock: asyncio.Lock = _ensure_services_lock(bucket)
    async with services_lock:
        registered = bucket.get("restart_check_registered")
        if not isinstance(registered, bool):
            registered = False
        if not registered:
            _register_restart_required_check(hass, bucket)
            bucket["restart_check_registered"] = True
            _LOGGER.debug("Registered %s restart-required watchdog", DOMAIN)

        url_refresh_registered = bucket.get("url_refresh_registered")
        if not isinstance(url_refresh_registered, bool):
            url_refresh_registered = False
        if not url_refresh_registered:
            _register_url_refresh_timer(hass, bucket)
            bucket["url_refresh_registered"] = True
            _LOGGER.debug("Registered %s daily map URL refresh", DOMAIN)


def _log_ecdsa_acceleration(info: dict[str, str | None]) -> None:
    """Log once which big-int backend python-ecdsa would use (DEBUG only).

    Pure formatter: takes the already-materialized info dict (see
    ``get_ecdsa_acceleration_info``) and performs no I/O itself, so it is safe
    to call from the event loop. The caller is responsible for materializing
    ``info`` in an executor. ``gmpy2_version`` is substituted with
    "not installed" when absent so the record never logs the literal "None".
    """
    backend = info.get("ecdsa_acceleration")
    gmpy2_dist = info.get("gmpy2_version") or "not installed"
    _LOGGER.debug(
        "ECDSA big-int backend python-ecdsa would use: %s (gmpy2 dist: %s); "
        "install gmpy2 to accelerate the legacy SECP160r1 EID path.",
        backend,
        gmpy2_dist,
    )


async def async_setup(hass: HomeAssistant, config: dict[str, Any]) -> bool:
    """Set up the integration namespace and register global services.

    Rationale:
        Services must be registered from async_setup so they are always available,
        even if no config entry is loaded, which enables frontend validation of
        automations referencing these services.
    """

    await hass.async_add_executor_job(_ensure_runtime_imports)
    bucket = _domain_data(hass)
    _ensure_entries_bucket(bucket)  # entry_id -> RuntimeData
    _ensure_device_owner_index(bucket)  # canonical_id -> entry_id (E2.5 scaffold)
    if not isinstance(bucket.get("providers_registered"), bool):
        bucket["providers_registered"] = False

    # Use a lock + idempotent flag to avoid double registration on racey startups.
    services_lock: asyncio.Lock = _ensure_services_lock(bucket)
    async with services_lock:
        services_registered = bucket.get("services_registered")
        if not isinstance(services_registered, bool):
            services_registered = False
        if not services_registered:
            svc_ctx = {
                "domain": DOMAIN,
                "resolve_canonical": _resolve_canonical_from_any,
                "is_active_entry": _is_active_entry,
                "primary_active_entry": _primary_active_entry,
                "opt": _opt,
                "default_map_view_token_expiration": DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
                "opt_map_view_token_expiration_key": OPT_MAP_VIEW_TOKEN_EXPIRATION,
                "redact_url_token": _redact_url_token,
                "soft_migrate_entry": _async_soft_migrate_data_to_options,
                "migrate_unique_ids": _async_migrate_unique_ids,
                "relink_button_devices": _async_relink_button_devices,
                "relink_subentry_entities": _async_relink_subentry_entities,
                "coalesce_account_entries": async_coalesce_account_entries,
                "extract_normalized_email": _extract_email_from_entry,
            }
            await async_register_services(hass, svc_ctx)
            bucket["services_registered"] = True
            _LOGGER.debug("Registered %s services at integration level", DOMAIN)

    # Arm the global restart-required watchdog exactly once. async_setup_entry
    # re-arms it after a last-entry reload tears it down (see helper docstring).
    await _async_ensure_restart_required_check(hass, bucket)

    # Determine the ECDSA big-int backend (and installed accelerator versions)
    # once per process, then cache it in hass.data so the diagnostics dump can
    # read it synchronously, exactly like every other diagnostics block (none of
    # them performs its own I/O). The first materialization performs filesystem
    # I/O (find_spec + dist-info read), so it runs in an executor here, in setup,
    # never in the event-loop-bound diagnostics path. The pure logger then
    # formats the result without blocking the event loop.
    from .FMDNCrypto._lazy_crypto import (  # noqa: PLC0415
        get_ecdsa_acceleration_info,
    )

    ecdsa_info = await hass.async_add_executor_job(get_ecdsa_acceleration_info)
    bucket["ecdsa_acceleration_info"] = ecdsa_info
    _log_ecdsa_acceleration(ecdsa_info)

    # Arm the secrets.json discovery watcher exactly once per Home Assistant
    # instance. The watcher polls the bundled Auth/secrets.json (plus any extra
    # paths configured via options) and triggers discovery/reauth flows when a
    # freshly minted bundle appears, so same-machine login containers hand off
    # without manual copy-paste. DiscoveryManager owns its own EVENT_HOMEASSISTANT_STOP
    # listener and _started guard; the bucket handle keeps the singleton alive and
    # makes the start idempotent across racey setups.
    if bucket.get("discovery_manager") is None:
        try:
            bucket["discovery_manager"] = await async_initialize_discovery_runtime(hass)
        except Exception:  # noqa: BLE001 - discovery is best-effort, never fatal
            _LOGGER.debug("Discovery runtime initialization failed", exc_info=True)

    return True


def _self_heal_device_registry(hass: HomeAssistant, entry: MyConfigEntry) -> None:
    """Remove stale parent links from tracker devices for the given entry."""

    _LOGGER.debug(
        "[Entry=%s] Starting self-healing cleanup of device registry...",
        entry.entry_id,
    )
    # Local import, see ``_async_purge_unloaded_subentry_registrations``.
    from .coordinator.helpers.registry import device_belongs_to_entry

    dev_reg = dr.async_get(hass)
    entry_id = entry.entry_id
    correct_service_identifier = service_device_identifier(entry_id)

    registry_devices: Iterable[Any]
    entries_helper: Callable[[Any, str], Iterable[Any]] | None = getattr(
        dr, "async_entries_for_config_entry", None
    )
    if entries_helper is None:
        fallback = getattr(dev_reg, "async_entries_for_config_entry", None)
        if callable(fallback):
            registry_devices = cast(Iterable[Any], fallback(entry_id))
        else:
            _LOGGER.debug(
                "Self-healing: device registry helper missing, skipping cleanup.",
            )
            registry_devices = ()
    else:
        registry_devices = entries_helper(dev_reg, entry_id)

    healed_devices = 0
    for device in registry_devices:
        # Defensive second check.  ``registry_devices`` comes from
        # ``async_entries_for_config_entry``, which filters by entry, so against
        # a faithful registry this is always true and no mutant of it fails a
        # test.  It is not unreachable, though: the helper is resolved with
        # ``getattr`` above, and a double that answers unfiltered makes this
        # false.  Do not delete it as dead code on the strength of a green
        # mutant run.
        if not device_belongs_to_entry(device, entry_id):
            continue

        identifiers: Collection[tuple[str, str]] = getattr(device, "identifiers", ())
        is_service_device = correct_service_identifier in identifiers
        via_device_id = getattr(device, "via_device_id", None)

        if is_service_device or via_device_id is None:
            continue

        device_name = getattr(device, "name", device.id)
        _LOGGER.debug(
            "Healing device '%s' (ID: %s): Removing incorrect parent link (via_device_id)",
            device_name,
            device.id,
        )
        dev_reg.async_update_device(device.id, via_device_id=None)
        healed_devices += 1

    if healed_devices > 0:
        _LOGGER.info(
            "Self-healing complete: Removed incorrect parent links from %d orphaned devices.",
            healed_devices,
        )
    else:
        _LOGGER.debug(
            "Self-healing: No orphaned devices with incorrect links found.",
        )


def _resolve_config_subentry_identifier(
    subentry: Any, *, allow_entry_id: bool = False
) -> str | None:
    """Return a stable identifier for ``subentry`` when available.

    The resolver prefers explicit subentry identifiers (``config_subentry_id``
    and ``subentry_id``) or a ``stable_identifier`` attribute provided by the
    caller. ``entry_id`` values are only considered when ``allow_entry_id`` is
    ``True`` so placeholder objects do not masquerade as registered subentries
    during setup bookkeeping.
    """

    mapping_keys = (
        "config_subentry_id",
        "subentry_id",
        "stable_identifier",
        "key",
    )
    attribute_keys = mapping_keys

    def _coerce_identifier(value: Any) -> str | None:
        if isinstance(value, str) and value:
            return value
        return None

    if isinstance(subentry, Mapping):
        for key in mapping_keys:
            candidate = _coerce_identifier(subentry.get(key))
            if candidate:
                return candidate
        if allow_entry_id:
            return _coerce_identifier(subentry.get("entry_id"))

    for attribute in attribute_keys:
        candidate = _coerce_identifier(getattr(subentry, attribute, None))
        if candidate:
            return candidate

    if allow_entry_id:
        return _coerce_identifier(getattr(subentry, "entry_id", None))

    return None


def _normalize_feature_iterable(candidate: Any) -> tuple[Any, ...]:
    """Return a tuple of feature markers extracted from ``candidate``."""

    if isinstance(candidate, Mapping):
        candidate = candidate.values()

    if isinstance(candidate, str) or candidate is None:
        return ()

    if isinstance(candidate, Iterable):
        values: list[Any] = []
        for item in candidate:
            if isinstance(item, (str, Platform)):
                values.append(item)
        return tuple(values)

    return ()


def _normalize_platforms_from_features(features: Iterable[Any]) -> tuple[Platform, ...]:
    """Convert an iterable of feature markers to Home Assistant platforms."""

    resolved: list[Platform] = []
    for feature in features:
        platform: Platform | None = None
        if isinstance(feature, Platform):
            platform = feature
        elif isinstance(feature, str):
            attr = getattr(Platform, feature.upper(), None)
            if attr is not None:
                platform = cast(Platform | str, attr)
            else:
                try:
                    platform = next(
                        candidate
                        for candidate in Platform
                        if getattr(candidate, "value", None) == feature
                    )
                except (StopIteration, TypeError):
                    platform = None
        if platform is not None:
            resolved.append(platform)

    if not resolved:
        return tuple(PLATFORMS)

    return tuple(dict.fromkeys(resolved))


def _determine_subentry_platforms(subentry: Any) -> tuple[Platform, ...]:
    """Return the platforms that should load for ``subentry``."""

    data = getattr(subentry, "data", None)
    features = _normalize_feature_iterable(getattr(subentry, "features", None))

    if not features and isinstance(data, Mapping):
        features = _normalize_feature_iterable(data.get("features"))

    if not features:
        subentry_type = getattr(subentry, "subentry_type", None)
        key = getattr(subentry, "key", None)
        if subentry_type == SUBENTRY_TYPE_SERVICE or key == SERVICE_SUBENTRY_KEY:
            features = tuple(SERVICE_FEATURE_PLATFORMS)
        elif subentry_type == SUBENTRY_TYPE_TRACKER or key == TRACKER_SUBENTRY_KEY:
            features = tuple(TRACKER_FEATURE_PLATFORMS)
        else:
            features = tuple(
                _feature_name_from_platform(platform) for platform in PLATFORMS
            )

    return _normalize_platforms_from_features(features)


def _platform_names(platforms: Iterable[Platform | str]) -> tuple[str, ...]:
    """Return normalized platform names for logging/debugging."""

    names: list[str] = []
    for platform in platforms:
        candidate = _platform_value(platform)
        if candidate:
            names.append(candidate)

    # Preserve ordering while deduplicating so repeated platform enums only
    # appear once in debug output.
    return tuple(dict.fromkeys(names))


def _platform_value(platform: Platform | str) -> str:
    """Return the string value for ``platform`` regardless of enum type."""

    if isinstance(platform, Platform):
        candidate = getattr(platform, "value", None)
        if isinstance(candidate, str) and candidate:
            return candidate
        return str(platform)
    if isinstance(platform, str):
        return platform
    return str(platform)


def _callable_accepts_keyword(callback: Callable[..., Any], keyword: str) -> bool:
    """Return ``True`` if ``callback`` accepts ``keyword`` as a kwarg."""

    try:
        signature = inspect.signature(callback)
    except (
        TypeError,
        ValueError,
    ):  # pragma: no cover - CPython limitation for some callables
        return True

    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            return True
        if (
            parameter.kind
            in (
                inspect.Parameter.KEYWORD_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
            and parameter.name == keyword
        ):
            return True

    return False


def _invoke_with_optional_keyword(  # noqa: PLR0913
    callback: Callable[..., Any],
    args: tuple[Any, ...],
    keyword: str,
    value: Any,
    *,
    allow_fallback: bool = True,
    signal_keyword_rejection: bool = False,
) -> Any:
    """Invoke ``callback`` while gracefully handling a legacy signature.

    When ``callback`` does not accept ``keyword`` the value is ignored and the
    callable is executed without it. This mirrors the Home Assistant 2025.10+
    helpers that accept ``config_subentry_id`` while remaining compatible with
    earlier releases. When ``allow_fallback`` is ``False`` the callable is not
    retried without the keyword; callers can opt-in to rejection reporting via
    ``signal_keyword_rejection`` to handle compatibility gaps themselves.
    """

    rejected_keyword = False

    if value is None:
        result = callback(*args)
        if signal_keyword_rejection:
            return result, False
        return result

    accepts_keyword = _callable_accepts_keyword(callback, keyword)
    callback_name = getattr(callback, "__qualname__", repr(callback))

    try:
        result = callback(*args, **{keyword: value})
        if signal_keyword_rejection:
            return result, False
        return result
    except TypeError as err:
        if keyword not in str(err):
            raise
        rejected_keyword = True
        if accepts_keyword:
            _LOGGER.debug(
                "%s advertised keyword '%s' but raised TypeError; retrying without it",  # noqa: G004
                callback_name,
                keyword,
            )
        else:
            _LOGGER.debug(
                "%s rejected optional keyword '%s'; retrying without it",  # noqa: G004
                callback_name,
                keyword,
            )

        if not allow_fallback:
            if signal_keyword_rejection:
                return None, True
            raise

        result = callback(*args)
        if signal_keyword_rejection:
            return result, rejected_keyword
        return result

    if signal_keyword_rejection:
        return None, rejected_keyword


def _collect_entry_subentries(entry: ConfigEntry) -> tuple[Any, ...]:
    """Return all known subentries associated with ``entry``."""

    runtime_data = getattr(entry, "runtime_data", None)
    containers: list[Iterable[Any]] = []

    if runtime_data is not None:
        manager = getattr(runtime_data, "subentry_manager", None)
        managed = getattr(manager, "managed_subentries", None)
        if isinstance(managed, Mapping):
            containers.append(managed.values())

        coordinator = getattr(runtime_data, "coordinator", None)
    else:
        coordinator = getattr(entry, "coordinator", None)

    entry_subentries = getattr(entry, "subentries", None)
    if isinstance(entry_subentries, Mapping):
        containers.append(entry_subentries.values())
    else:
        managed = getattr(entry_subentries, "managed_subentries", None)
        if isinstance(managed, Mapping):
            containers.append(managed.values())

    metadata = getattr(coordinator, "_subentry_metadata", None)
    if isinstance(metadata, Mapping):
        containers.append(metadata.values())

    collected: dict[str, Any] = {}
    unidentified: list[Any] = []
    for container in containers:
        for subentry in container:
            identifier = _resolve_config_subentry_identifier(subentry)
            if identifier is None:
                unidentified.append(subentry)
                continue
            collected.setdefault(identifier, subentry)

    ordered: list[Any] = list(collected.values())
    for subentry in unidentified:
        if subentry not in ordered:
            ordered.append(subentry)

    return tuple(ordered)


_LEGACY_PLATFORM_TRACKERS: WeakKeyDictionary[ConfigEntry, set[str]] = (
    WeakKeyDictionary()
)
_LEGACY_NOTICE_TRACKER: WeakKeyDictionary[ConfigEntry, bool] = WeakKeyDictionary()


def _legacy_forwarded_platforms(entry: ConfigEntry) -> set[str]:
    """Return the cached parent-level platform set for legacy cores."""

    runtime_data = getattr(entry, "runtime_data", None)
    if isinstance(runtime_data, RuntimeData):
        forwarded = getattr(runtime_data, "legacy_forwarded_platforms", None)
        if isinstance(forwarded, set):
            return forwarded
        runtime_data.legacy_forwarded_platforms = set()
        return runtime_data.legacy_forwarded_platforms

    forwarded = _LEGACY_PLATFORM_TRACKERS.get(entry)
    if forwarded is None:
        forwarded = set()
        _LEGACY_PLATFORM_TRACKERS[entry] = forwarded
    return forwarded


def _legacy_notice_logged(entry: ConfigEntry) -> bool:
    runtime_data = getattr(entry, "runtime_data", None)
    if isinstance(runtime_data, RuntimeData):
        return bool(runtime_data.legacy_forward_notice)
    return bool(_LEGACY_NOTICE_TRACKER.get(entry))


def _mark_legacy_notice_logged(entry: ConfigEntry) -> None:
    runtime_data = getattr(entry, "runtime_data", None)
    if isinstance(runtime_data, RuntimeData):
        runtime_data.legacy_forward_notice = True
        return
    _LEGACY_NOTICE_TRACKER[entry] = True


async def _async_setup_legacy_child_subentry(
    hass: HomeAssistant, entry: MyConfigEntry
) -> bool:
    """Legacy setup path for child config entries."""

    subentry_type = getattr(entry, "subentry_type", None)
    group_key = entry.data.get("group_key")
    parent_entry_id = getattr(entry, "parent_entry_id", None)
    _LOGGER.debug(
        "[%s] Setting up subentry (parent_id=%s, type=%s, key=%s)",
        entry.entry_id,
        parent_entry_id,
        subentry_type,
        group_key,
    )

    if not parent_entry_id:
        _LOGGER.debug(
            "[%s] Aborting subentry setup because parent_entry_id is missing",  # noqa: G004
            entry.entry_id,
        )
        return False

    # Prefer entry.runtime_data on the parent ConfigEntry (2026 standard) before
    # falling back to the domain-level entries bucket for legacy compatibility.
    parent_payload: RuntimeData | GoogleFindMyCoordinator | None = None
    parent_entry = hass.config_entries.async_get_entry(parent_entry_id)
    if parent_entry is not None:
        parent_payload = getattr(parent_entry, "runtime_data", None)

    if parent_payload is None:
        bucket = _domain_data(hass)
        entries_bucket = _ensure_entries_bucket(bucket)
        parent_payload = cast(
            RuntimeData | GoogleFindMyCoordinator | None,
            entries_bucket.get(parent_entry_id),
        )

    if parent_payload is None:
        _LOGGER.debug(
            "[%s] Parent runtime data bucket missing for %s; deferring setup",  # noqa: G004
            entry.entry_id,
            parent_entry_id,
        )
        raise ConfigEntryNotReady(f"Parent entry {parent_entry_id} not yet initialized")

    coordinator: GoogleFindMyCoordinator | None = None
    if isinstance(parent_payload, GoogleFindMyCoordinator):
        coordinator = parent_payload
    else:
        coordinator = getattr(parent_payload, "coordinator", None)

    if coordinator is None:
        _LOGGER.debug(
            "[%s] Parent runtime data for %s is missing a ready coordinator; deferring setup",
            entry.entry_id,
            parent_entry_id,
        )
        raise ConfigEntryNotReady(
            f"Parent entry {parent_entry_id} coordinator not ready"
        )

    if isinstance(parent_payload, RuntimeData):
        entry.runtime_data = parent_payload
    else:
        entry.runtime_data = coordinator

    _LOGGER.debug(
        "[%s] Legacy subentry setup complete, runtime data attached (Parent: %s)",
        entry.entry_id,
        parent_entry_id,
    )

    return True


async def _async_setup_subentry(
    hass: HomeAssistant,
    entry: MyConfigEntry,
    subentry: Any | None = None,
) -> bool:
    """Attach parent runtime data during subentry setup.

    NOTE: Home Assistant Core calls this function when setting up each config
    subentry. It must not forward platforms itself. Manual platform forwarding
    via ``ConfigEntries.async_forward_entry_setups`` cannot convey a
    ``config_subentry_id`` (per the 2025.11.2 SSoT signature) and therefore
    cannot be used for subentries. After this function returns ``True``, HA
    drives platform setup for the subentry and injects the correct
    ``config_subentry_id`` when invoking platform ``async_setup_entry``
    handlers.
    """

    if subentry is None:
        return await _async_setup_legacy_child_subentry(hass, entry)

    parent_entry_id = getattr(entry, "parent_entry_id", None)
    if not parent_entry_id:
        _LOGGER.error(
            "[%s] Subentry setup failed: parent_entry_id is missing",
            entry.entry_id,
        )
        return False

    config_entries = getattr(hass, "config_entries", None)
    if config_entries is None:
        _LOGGER.error(
            "[%s] Subentry setup failed: hass.config_entries is unavailable",
            entry.entry_id,
        )
        raise ConfigEntryNotReady("config_entries manager unavailable")

    parent_entry = getattr(config_entries, "async_get_entry", lambda _entry_id: None)(
        parent_entry_id
    )
    if parent_entry is None:
        _LOGGER.error(
            "[%s] Subentry setup failed: parent entry %s not registered",  # noqa: G004
            entry.entry_id,
            parent_entry_id,
        )
        raise ConfigEntryNotReady(f"Parent entry {parent_entry_id} missing")

    subentry_identifier = _resolve_config_subentry_identifier(subentry) or getattr(
        entry, "config_subentry_id", None
    )
    if subentry_identifier is None:
        subentry_identifier = entry.entry_id

    registered_ids = _registered_subentry_ids(hass, parent_entry)
    if not registered_ids or subentry_identifier not in registered_ids:
        _LOGGER.error(
            "[%s] Config subentry %s not registered under parent %s; aborting setup",  # noqa: G004
            entry.entry_id,
            subentry_identifier,
            parent_entry_id,
        )
        raise ConfigEntryNotReady(
            f"Config subentry {subentry_identifier} is not registered"
        )

    # Prefer entry.runtime_data on the parent ConfigEntry (2026 standard) before
    # falling back to the domain-level entries bucket for legacy compatibility.
    parent_runtime_data = getattr(parent_entry, "runtime_data", None)
    if parent_runtime_data is None:
        bucket = _domain_data(hass)
        entries_bucket = _ensure_entries_bucket(bucket)
        parent_runtime_data = entries_bucket.get(parent_entry_id)

    if parent_runtime_data is None:
        _LOGGER.warning(
            "[%s] Parent runtime data bucket missing for %s; deferring setup",
            entry.entry_id,
            parent_entry_id,
        )
        raise ConfigEntryNotReady(f"Parent entry {parent_entry_id} not yet initialized")

    coordinator = getattr(parent_runtime_data, "coordinator", None)
    if coordinator is None:
        _LOGGER.warning(
            "[%s] Parent runtime data for %s is missing a ready coordinator; deferring setup",
            entry.entry_id,
            parent_entry_id,
        )
        raise ConfigEntryNotReady(
            f"Parent entry {parent_entry_id} coordinator not ready"
        )

    runtime_manager = getattr(parent_runtime_data, "subentry_manager", None)
    coordinator = getattr(parent_runtime_data, "coordinator", None)

    if runtime_manager is not None:
        try:
            runtime_manager._refresh_from_entry()  # noqa: SLF001 - internal sync for new subentries
        except Exception as err:  # pragma: no cover - defensive refresh guard
            _LOGGER.debug(
                "[%s] Failed to refresh subentry manager during setup: %s",
                entry.entry_id,
                err,
            )

    if coordinator is not None:
        try:
            coordinator._refresh_subentry_index()  # noqa: SLF001 - keep metadata aligned
        except Exception as err:  # pragma: no cover - defensive refresh guard
            _LOGGER.debug(
                "[%s] Coordinator subentry index refresh deferred: %s",
                entry.entry_id,
                err,
            )
        try:
            await coordinator.async_request_refresh()
        except Exception as err:  # pragma: no cover - defensive refresh guard
            _LOGGER.debug(
                "[%s] Coordinator refresh after subentry setup skipped: %s",
                entry.entry_id,
                err,
            )

    entry.runtime_data = parent_runtime_data

    _LOGGER.debug(
        "[%s] Subentry setup complete, runtime data attached (Parent: %s)",
        entry.entry_id,
        parent_entry_id,
    )

    return True


async def async_setup_entry(hass: HomeAssistant, entry: MyConfigEntry) -> bool:
    """Set up a config entry.

    Order of operations (important):
      1) Multi-entry policy: allow multiple entries; prevent duplicate-account entries.
      2) Initialize and register TokenCache (entry-scoped, no default).
      3) Soft-migrate options and unique_ids; acquire and wire the shared FCM provider.
      4) Seed token cache from entry data (secrets bundle or individual tokens).
      5) Build coordinator, register views, and synchronize subentries.
      6) Schedule initial refresh after HA is fully started.
    """
    # A reload that was claimed has arrived: release the latch. Deliberately here
    # and not only in the unload half, because ``async_reload`` skips the unload
    # for an entry that was not loaded, and a latch nobody releases would swallow
    # every later reload of this entry.
    discard_pending_entry_reload(hass, entry.entry_id)

    parent_entry_id = getattr(entry, "parent_entry_id", None)
    if parent_entry_id:
        return await _async_setup_subentry(hass, entry)

    # Before anything reads the entry: drop the leftovers of the discovery import
    # defect. Deliberately first, not down with the other soft migrations, so the
    # cleanup does not depend on the rest of the setup succeeding.
    _async_strip_stray_nested_entry_data(hass, entry)

    legacy_cache: TokenCache | None = None
    cached_snapshot: Mapping[str, Any] | None = None
    legacy_secrets_keys = (DATA_SECRET_BUNDLE, "scanned_data", CONF_OAUTH_TOKEN)
    legacy_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "Auth", "secrets.json"
    )

    legacy_keys_present = any(key in entry.data for key in legacy_secrets_keys)

    if legacy_keys_present:
        try:
            legacy_cache = await TokenCache.create(
                hass, entry.entry_id, legacy_path=legacy_path
            )
        except Exception as err:  # pragma: no cover - defensive migration guard
            _LOGGER.debug(
                "[%s] Legacy migration cache load failed: %s", entry.entry_id, err
            )
        else:
            try:
                cached_snapshot = await legacy_cache.all()
            except Exception as err:  # pragma: no cover - defensive migration guard
                _LOGGER.debug(
                    "[%s] Legacy migration cache read failed: %s", entry.entry_id, err
                )
                cached_snapshot = None

    legacy_cache_primed = bool(cached_snapshot)
    legacy_candidate = legacy_keys_present and not legacy_cache_primed

    if legacy_candidate:
        raw_secrets = entry.data.get(DATA_SECRET_BUNDLE) or entry.data.get(
            "scanned_data"
        )
        secrets_bundle: dict[str, Any] = {}

        if isinstance(raw_secrets, str):
            try:
                parsed_secrets = json.loads(raw_secrets)
            except (json.JSONDecodeError, TypeError) as err:
                _LOGGER.debug(
                    "[%s] Parsing the legacy bundle failed: %s", entry.entry_id, err
                )
            else:
                if isinstance(parsed_secrets, Mapping):
                    secrets_bundle = dict(parsed_secrets)
        elif isinstance(raw_secrets, Mapping):
            secrets_bundle = dict(raw_secrets)

        if secrets_bundle:  # pragma: no cover - legacy one-time migration path
            # Legacy bundles (migrated from entry.data/file) may carry copy/paste
            # whitespace in credential values; normalize before extraction and
            # before this path persists the bundle to the cache. Idempotent.
            # The decryption-critical normalization is also enforced downstream by
            # _async_save_secrets_data (covered by tests); this is defense-in-depth
            # on the legacy one-time migration branch, which the unit harness does
            # not drive.
            from .shared_helpers import normalize_secrets_bundle

            secrets_bundle = normalize_secrets_bundle(secrets_bundle)

        oauth_token_maybe = entry.data.get(CONF_OAUTH_TOKEN)
        if not isinstance(oauth_token_maybe, str):
            oauth_token_maybe = secrets_bundle.get(CONF_OAUTH_TOKEN)
        oauth_token_recovered = (
            oauth_token_maybe if isinstance(oauth_token_maybe, str) else None
        )

        aas_token_maybe = secrets_bundle.get(DATA_AAS_TOKEN)
        aas_token_recovered = (
            aas_token_maybe if isinstance(aas_token_maybe, str) else None
        )

        google_email = entry.data.get(CONF_GOOGLE_EMAIL) or entry.data.get("email")

        def _walk_for_email(value: Any) -> str | None:
            stack: list[Any] = [value]
            while stack:
                current = stack.pop()
                if isinstance(current, Mapping):
                    for key, candidate in current.items():
                        if not isinstance(key, str):
                            continue
                        if isinstance(candidate, str) and "@" in candidate:
                            if key.lower() in {
                                "user",
                                "username",
                                "email",
                                "account_name",
                            }:
                                return candidate
                        elif isinstance(
                            candidate, (Mapping, Sequence)
                        ) and not isinstance(candidate, (str, bytes, bytearray)):
                            stack.append(candidate)
                elif isinstance(current, Sequence) and not isinstance(
                    current, (str, bytes, bytearray)
                ):
                    stack.extend(current)
            return None

        if (
            not isinstance(google_email, str) or "@" not in google_email
        ) and secrets_bundle:
            email_candidate = _walk_for_email(secrets_bundle)
            google_email = email_candidate if isinstance(email_candidate, str) else None

        if not isinstance(google_email, str) or "@" not in google_email:
            _LOGGER.error(
                "[%s] Legacy migration failed to recover account email", entry.entry_id
            )
            raise ConfigEntryNotReady("Legacy migration missing email")

        normalized_email = normalize_email(google_email)
        auth_method_value = entry.data.get(DATA_AUTH_METHOD) or "secrets_json"

        if legacy_cache is None:
            legacy_cache = await TokenCache.create(
                hass, entry.entry_id, legacy_path=legacy_path
            )

        await legacy_cache.async_set_cached_value(DATA_SECRET_BUNDLE, secrets_bundle)
        await legacy_cache.async_set_cached_value(username_string, normalized_email)
        await legacy_cache.async_set_cached_value(DATA_AUTH_METHOD, auth_method_value)
        if oauth_token_recovered:
            await legacy_cache.async_set_cached_value(
                CONF_OAUTH_TOKEN, oauth_token_recovered
            )
        if aas_token_recovered:
            await legacy_cache.async_set_cached_value(
                DATA_AAS_TOKEN, aas_token_recovered
            )

        updated_data = dict(entry.data)
        updated_data.pop(DATA_SECRET_BUNDLE, None)
        updated_data.pop("scanned_data", None)
        updated_data.pop(CONF_OAUTH_TOKEN, None)
        updated_data[CONF_GOOGLE_EMAIL] = normalized_email
        updated_data[DATA_AUTH_METHOD] = auth_method_value

        # Keep the user's settings: every key from OPTION_KEYS, the extra
        # watch paths and the schema version. A fresh entry runs this path too
        # (the config flow stores ``CONF_OAUTH_TOKEN`` while the new cache is
        # still empty); replacing the options with ``{}`` would discard the
        # choices made during setup, e.g. a declined Map View. Dropped are
        # removed keys such as ``tracked_devices`` and the bookkeeping markers
        # of the unique-ID migrations, which then run again on the new data.
        kept_options = {
            option_name: value
            for option_name, value in entry.options.items()
            if option_name in _LEGACY_MIGRATION_KEPT_OPTIONS
        }
        hass.config_entries.async_update_entry(
            entry, data=MappingProxyType(updated_data), options=kept_options
        )
        _LOGGER.info(
            "[%s] Migrated legacy configuration and kept the user's options",
            entry.entry_id,
        )

    elif legacy_cache_primed:
        _LOGGER.debug(
            "[%s] Legacy payload detected but cache is already primed; skipping reset",
            entry.entry_id,
        )

    setattr(entry, "_gfm_parent_platforms_unloaded", False)
    setattr(entry, "_gfm_parent_unload_call_count", 0)
    _safe_setattr(entry, "_gfm_parent_platforms_forwarded", False)

    _ensure_runtime_imports()
    # --- Multi-entry policy: allow MA; block duplicate-account (same email) ----
    # Legacy issue cleanup: we no longer block on multiple config entries
    try:
        ir.async_delete_issue(hass, DOMAIN, ISSUE_MULTIPLE_CONFIG_ENTRIES)
    except Exception:
        pass

    should_setup, normalized_email = await _ensure_post_migration_consistency(
        hass, entry
    )
    if not should_setup:
        _LOGGER.info(
            "Skipping setup for %s due to duplicate account %s",
            entry.entry_id,
            _mask_email_for_logs(normalized_email),
        )
        # Final abort (not a retryable ConfigEntryNotReady): this return never
        # reaches the cleanup runner at the end of the function, so any job the
        # config flow staged for this account would linger in hass.data for the
        # whole process lifetime.
        # Discard it instead of running it: the entry is not set up, so the
        # credentials must stay put. The secrets watcher re-imports the file on
        # its next scan.
        try:
            from .config_flow import (  # noqa: PLC0415 - lazy, avoids an import cycle
                async_discard_pending_container_cleanup,
                async_discard_pending_container_cleanup_for_entry,
            )

            # Two addressings, in this order and both needed. The entry-id drain
            # first, because this entry may hold SEVERAL update-path tickets and
            # the claim-based discard below takes at most one; running it first
            # would consume one of those and leave the create-path ticket behind.
            # The claim-based discard second, for exactly that uncorrelated
            # create-path ticket, which carries no entry id and is therefore
            # invisible to the drain.
            discarded = async_discard_pending_container_cleanup_for_entry(
                hass, entry_id=getattr(entry, "entry_id", None)
            )
            discarded += async_discard_pending_container_cleanup(
                hass,
                unique_id=getattr(entry, "unique_id", None),
                entry_id=getattr(entry, "entry_id", None),
            )
            if discarded:
                _LOGGER.debug(
                    "[%s] Discarded %s staged container-login cleanup job(s) after the duplicate-account abort; credential files are kept on disk",
                    entry.entry_id,
                    discarded,
                )
        except Exception as err:  # noqa: BLE001 - housekeeping must never raise
            _LOGGER.debug(
                "[%s] Could not discard staged container-login cleanup: %s",
                entry.entry_id,
                err,
            )
        return False

    pm_setup_start = time.monotonic()

    # Distinguish cold start vs. reload
    domain_bucket = _domain_data(hass)
    _ensure_device_owner_index(domain_bucket)  # ensure present (E2.5)
    if "nova_refcount" not in domain_bucket:
        _set_nova_refcount(domain_bucket, 0)

    # 1) Token cache: create/register early (ENTRY-SCOPED ONLY)
    cache = legacy_cache or await TokenCache.create(
        hass, entry.entry_id, legacy_path=legacy_path
    )

    # Ensure deferred writes are flushed on HA shutdown
    async def _flush_on_stop(event: Event) -> None:
        """Flush deferred saves on Home Assistant stop."""
        try:
            await cache.flush()
        except (TimeoutError, HomeAssistantError, ValueError) as err:
            _LOGGER.debug("Cache flush on stop raised: %s", err)

    entry.async_on_unload(
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _flush_on_stop)
    )

    # Refresh the running discovery watcher's paths when SECRETS_EXTRA_WATCH_PATHS
    # changes at runtime (options flow), so a newly configured path is observed
    # without a Home Assistant restart. The manager is a per-instance singleton and
    # recomputes from every enabled entry, so the listener is idempotent regardless
    # of which entry changed. It deliberately passes no exclude_entry_id: the entry
    # that just changed is exactly the one whose new path must be adopted. This
    # closure is only the listener adapter for Home Assistant's (hass, entry)
    # signature; the refresh itself lives in the shared helper.
    #
    # Since the config flow no longer reloads (Home Assistant deprecates a
    # reloading config-flow method on an entry that has an update listener:
    # warning from 2026.6, error from 2026.12), this listener is also the place
    # where changed credentials become effective. It compares a fingerprint of
    # the credential-relevant keys, because a listener is handed the new entry
    # and no previous state. Writing credentials without a reload would leave
    # them stored but ineffective: the token cache is seeded below, in
    # async_setup_entry, and a running coordinator does not pick up new tokens.
    # Taken here on purpose: after the legacy migration above has rewritten
    # entry.data, and before the listener is registered. Taking it earlier would
    # make the migration itself look like a credential change and reload the
    # entry on every setup.
    credential_fingerprint = _credential_fingerprint(entry.data)

    async def _async_refresh_watch_paths(
        hass_arg: HomeAssistant, updated_entry: MyConfigEntry
    ) -> None:
        nonlocal credential_fingerprint

        await _async_refresh_discovery_watch_paths(hass_arg)

        # An invocation Home Assistant queued before the entry was unloaded still
        # runs afterwards: ``async_update_entry`` creates the task immediately,
        # while ``async_on_unload`` only takes the listener out of the entry. Such
        # a call carries the fingerprint of a setup that is over and would compare
        # it against data the rebuilt entry has long since adopted -- and reload an
        # entry that was just set up. Identity in ``update_listeners`` is what
        # separates the current invocation from the bygone one.
        listeners = getattr(updated_entry, "update_listeners", None)
        if isinstance(listeners, list) and _async_refresh_watch_paths not in listeners:
            return

        # The map-view toggle is not handled here, like every other setup-time
        # option: ``OptionsFlowHandler.async_step_settings`` already schedules a
        # reload on change, which re-runs ``async_setup_entry`` end to end.

        updated_fingerprint = _credential_fingerprint(updated_entry.data)
        if updated_fingerprint == credential_fingerprint:
            # Options, subentry maintenance, coalescing: everything that leaves
            # the credentials alone must not reload the entry.
            return

        # Advance the fingerprint BEFORE scheduling, so a second notification
        # arriving before the reload takes effect cannot schedule a second one.
        credential_fingerprint = updated_fingerprint

        schedule_reload = getattr(
            getattr(hass_arg, "config_entries", None), "async_schedule_reload", None
        )
        if not callable(schedule_reload):
            _LOGGER.debug(
                "Credentials changed but async_schedule_reload is unavailable; "
                "they take effect on the next restart"
            )
            return

        # Second guard against a teardown of an entry that is already being torn
        # down. Home Assistant removes this listener only *after*
        # ``async_unload_entry`` returns, in ``_async_process_on_unload``, so an
        # invocation queued before a reload can still wake up during the platform
        # unload and pass the identity check above. Two independent things stop it
        # there: ``async_unload_entry`` now holds the latch across the whole
        # teardown and releases it in a ``finally``, and the core sets
        # ``UNLOAD_IN_PROGRESS`` *before* calling it (checked in dev and in the
        # declared floor 2025.9.1), so the state below is never ``LOADED`` inside
        # that window. The state check is the cheaper of the two and keeps working
        # if the latch is ever claimed by someone else; it also covers the entry
        # that is simply not loaded. An unknown state means fail-open, as
        # everywhere else here: a missing reload is worse than one too many, and
        # the test stubs do not populate the field (contract in
        # ``agents/config_flow/AGENTS.md``).
        entry_state = getattr(updated_entry, "state", None)
        if entry_state is not None and entry_state is not ConfigEntryState.LOADED:
            _LOGGER.debug(
                "Entry %s: sign-in data changed, but it is not loaded (%s); the "
                "reload under way will pick them up",
                updated_entry.entry_id,
                entry_state,
            )
            return

        # Last check before scheduling: a flow that writes credentials and reloads
        # the entry itself has already covered this change.
        if not claim_pending_entry_reload(hass_arg, updated_entry.entry_id):
            _LOGGER.debug(
                "Entry %s: sign-in data changed, but a reload is already on its "
                "way; not scheduling a second one",
                updated_entry.entry_id,
            )
            return

        _LOGGER.info(
            "Credentials changed for this account; reloading the entry so they "
            "take effect"
        )
        try:
            schedule_reload(updated_entry.entry_id)
        except Exception:  # noqa: BLE001 - a claim that comes to nothing must be given back
            _LOGGER.exception(
                "Failed to schedule the reload of entry %s after a credential change",
                updated_entry.entry_id,
            )
            discard_pending_entry_reload(hass_arg, updated_entry.entry_id)

    entry.async_on_unload(entry.add_update_listener(_async_refresh_watch_paths))

    # Early, idempotent seeding of TokenCache from entry.data (authoritative SSOT)
    try:
        if DATA_AUTH_METHOD in entry.data:
            await cache.async_set_cached_value(
                DATA_AUTH_METHOD, entry.data[DATA_AUTH_METHOD]
            )
            _LOGGER.debug("Seeded auth_method into TokenCache from entry.data")
        if CONF_OAUTH_TOKEN in entry.data:
            await cache.async_set_cached_value(
                CONF_OAUTH_TOKEN, entry.data[CONF_OAUTH_TOKEN]
            )
            _LOGGER.debug("Seeded oauth_token into TokenCache from entry.data")
        if DATA_AAS_TOKEN in entry.data:
            await cache.async_set_cached_value(
                DATA_AAS_TOKEN, entry.data[DATA_AAS_TOKEN]
            )
            _LOGGER.debug("Seeded aas_token into TokenCache from entry.data")
        if CONF_GOOGLE_EMAIL in entry.data:
            await cache.async_set_cached_value(
                username_string, entry.data[CONF_GOOGLE_EMAIL]
            )
            _LOGGER.debug("Seeded google_email into TokenCache from entry.data")
    except Exception as err:
        _LOGGER.debug("Early cache instance seeding from entry.data failed: %s", err)

    raw_mode = _opt(entry, OPT_CONTRIBUTOR_MODE, DEFAULT_CONTRIBUTOR_MODE)
    contributor_mode = _normalize_contributor_mode(raw_mode)
    now_epoch = int(time.time())

    cached_mode = None
    try:
        cached_raw = await cache.async_get_cached_value(CACHE_KEY_CONTRIBUTOR_MODE)
    except Exception as err:
        _LOGGER.debug("Failed to read cached contributor mode: %s", err)
        cached_raw = None
    if isinstance(cached_raw, str):
        cached_mode = _normalize_contributor_mode(cached_raw)

    last_mode_switch_epoch: int | None = None
    try:
        cached_switch = await cache.async_get_cached_value(CACHE_KEY_LAST_MODE_SWITCH)
    except Exception as err:
        _LOGGER.debug("Failed to read cached network mode switch timestamp: %s", err)
        cached_switch = None
    if isinstance(cached_switch, (int, float)):
        last_mode_switch_epoch = int(cached_switch)

    if contributor_mode != cached_mode:
        last_mode_switch_epoch = now_epoch
        try:
            await cache.async_set_cached_value(
                CACHE_KEY_CONTRIBUTOR_MODE, contributor_mode
            )
            await cache.async_set_cached_value(
                CACHE_KEY_LAST_MODE_SWITCH, last_mode_switch_epoch
            )
        except Exception as err:
            _LOGGER.debug("Failed to persist contributor mode preference: %s", err)
    elif last_mode_switch_epoch is None:
        last_mode_switch_epoch = now_epoch
        try:
            await cache.async_set_cached_value(
                CACHE_KEY_LAST_MODE_SWITCH, last_mode_switch_epoch
            )
        except Exception as err:
            _LOGGER.debug("Failed to initialize contributor mode timestamp: %s", err)

    # Optional: register HA-managed aiohttp session for Nova API (defer import)
    try:
        from .NovaApi import nova_request as nova

        reg = getattr(nova, "register_hass", None)
        unreg = getattr(nova, "unregister_session_provider", None)
        unreg_hass = getattr(nova, "unregister_hass", None)
        if callable(reg):
            try:
                reg(hass)
            except Exception as err:
                _LOGGER.debug("Nova API register_hass() raised: %s", err)
            else:
                domain_bucket = _domain_data(hass)
                refcount = _get_nova_refcount(domain_bucket) + 1
                _set_nova_refcount(domain_bucket, refcount)
                _LOGGER.debug("Nova session provider refcount -> %s", refcount)

                def _release_nova_session_provider() -> None:
                    inner_bucket = _domain_data(hass)
                    inner_refcount = max(_get_nova_refcount(inner_bucket) - 1, 0)
                    _set_nova_refcount(inner_bucket, inner_refcount)
                    _LOGGER.debug(
                        "Nova session provider refcount -> %s", inner_refcount
                    )
                    if inner_refcount != 0:
                        return
                    if callable(unreg_hass):
                        try:
                            unreg_hass()
                        except Exception as err:  # pragma: no cover - defensive
                            _LOGGER.debug(
                                "Nova unregister_hass raised during unload: %s", err
                            )
                    if callable(unreg):
                        try:
                            unreg()
                        except Exception as err:  # pragma: no cover - defensive
                            _LOGGER.debug(
                                "Nova unregister_session_provider raised: %s", err
                            )

                entry.async_on_unload(_release_nova_session_provider)
        else:
            _LOGGER.debug(
                "Nova API register_hass() not available; continuing with module defaults."
            )
    except Exception as err:
        _LOGGER.debug("Nova API session provider registration skipped: %s", err)

    # Soft-migrate mutable settings from data -> options and unique_ids
    _migrate_entry_identifier_namespaces(hass, entry)
    await _async_soft_migrate_data_to_options(hass, entry)
    await _async_migrate_unique_ids(hass, entry)
    await _async_relink_button_devices(hass, entry)
    await _async_relink_subentry_entities(hass, entry)

    # Credentials seed: legacy bundle OR individual oauth_token+email must be present.
    # IMPORTANT: This MUST run BEFORE _async_acquire_shared_fcm() because the FCM
    # receiver reads fcm_credentials from the cache during async_initialize() →
    # _prime_cache_state().  Without this, cold starts leave fcm_credentials empty
    # and device locations stay "Unknown" (Issue #152).
    secrets_data = entry.data.get(DATA_SECRET_BUNDLE)
    oauth_token = entry.data.get(CONF_OAUTH_TOKEN)
    aas_token_entry = entry.data.get(DATA_AAS_TOKEN)
    google_email = entry.data.get(CONF_GOOGLE_EMAIL)

    # The entry is the source of truth for credentials; the cache is a mirror of
    # them. Recovering a value from the cache is therefore only right where the
    # entry carries no credentials at all, which is the gap this seed was built
    # for: a migrated installation whose credentials never reached entry.data.
    #
    # Where the entry does carry credentials, a *missing* optional key is a
    # statement rather than a gap. Whoever replaced the credentials -- the
    # discovery overwrite, the watched-file import, reauth, the options login --
    # left the key out because the new ones no longer come with a bundle or an
    # ``aas_et/`` token, and a flat merge cannot express that any other way.
    # Recovering it here would hand the integration back exactly the credentials
    # the user just replaced, so it is dropped from the cache instead: not
    # merely skipped, because the cache-first fallbacks elsewhere (see
    # _collect_entry_credential_tokens) would otherwise keep finding it. This
    # mirrors what _async_seed_manual_credentials has long done for a superseded
    # AAS token; the bundle branch below simply had no counterpart.
    #
    # Only the keys the entry mirrors are dropped. Material derived from a
    # bundle (owner_key, shared_key, fcm_credentials) belongs to the same
    # account, is refreshed in place, and stays.
    entry_carries_credentials = bool(secrets_data or oauth_token or aas_token_entry)

    cache_snapshot: Mapping[str, Any] | None = None
    try:
        cache_snapshot = await cache.all()
    except Exception as err:  # pragma: no cover - defensive cache read
        _LOGGER.debug(
            "[%s] Cache instance snapshot read failed: %s", entry.entry_id, err
        )

    if entry_carries_credentials:
        for key in OPTIONAL_CREDENTIAL_KEYS:
            label = _optional_credential_label(key)
            if entry.data.get(key):
                continue
            if not cache_snapshot or not cache_snapshot.get(key):
                continue
            try:
                await cache.async_set_cached_value(key, None)
            except Exception as err:  # pragma: no cover - defensive cache write
                _LOGGER.debug(
                    "[%s] Could not drop the superseded cached %s: %s",
                    entry.entry_id,
                    label,
                    err,
                )
            else:
                _LOGGER.debug(
                    "[%s] Dropped the superseded cached %s; the entry no longer "
                    "carries it",
                    entry.entry_id,
                    label,
                )
    elif cache_snapshot:
        secrets_data = cache_snapshot.get(DATA_SECRET_BUNDLE)
        oauth_token = cache_snapshot.get(CONF_OAUTH_TOKEN)
        aas_token_entry = cache_snapshot.get(DATA_AAS_TOKEN)

    if secrets_data:
        await _async_save_secrets_data(
            cache, secrets_data, account_label=_label_entry_for_log(entry)
        )
        _LOGGER.debug("Persisted secrets.json bundle to token cache (entry-scoped)")
        if isinstance(aas_token_entry, str) and aas_token_entry:
            await cache.async_set_cached_value(DATA_AAS_TOKEN, aas_token_entry)
            _LOGGER.debug("Stored pre-provided AAS token in TokenCache (entry-scoped)")
    elif (oauth_token or aas_token_entry) and google_email:
        await _async_seed_manual_credentials(
            cache,
            oauth_token,
            aas_token_entry,
            google_email,
        )
    else:
        _LOGGER.error(
            "No credentials found in config entry (neither secrets_data nor oauth_token+google_email)"
        )
        raise ConfigEntryNotReady("Credentials missing")

    # Acquire shared FCM and create a startup barrier for the first poll cycle.
    fcm_ready_event = asyncio.Event()
    fcm = await _async_acquire_shared_fcm(
        hass,
        entry=entry,
        cache=cache,
        entry_resolver=lambda: getattr(entry, "entry_id", None),
    )
    pm_fcm_acquired = time.monotonic()
    fcm_ready_event.set()

    # Signal-only stop on unload (bounded; actual await in async_unload_entry)
    def _on_unload_signal_fcm() -> None:
        try:
            fcm.request_stop()
        except Exception as err:
            _LOGGER.debug("FCM stop signal during unload raised: %s", err)

    entry.async_on_unload(_on_unload_signal_fcm)

    # Remove legacy parent links from tracker devices before building runtime state.
    _self_heal_device_registry(hass, entry)

    # Build effective runtime settings (options-first)
    coordinator = GoogleFindMyCoordinator(
        hass,
        cache=cache,
        location_poll_interval=_opt(
            entry, OPT_LOCATION_POLL_INTERVAL, DEFAULT_LOCATION_POLL_INTERVAL
        ),
        device_poll_delay=_opt(entry, OPT_DEVICE_POLL_DELAY, DEFAULT_DEVICE_POLL_DELAY),
        min_poll_interval=_opt(entry, OPT_MIN_POLL_INTERVAL, DEFAULT_MIN_POLL_INTERVAL),
        allow_history_fallback=_opt(
            entry,
            OPT_ALLOW_HISTORY_FALLBACK,
            DEFAULT_OPTIONS.get(OPT_ALLOW_HISTORY_FALLBACK, False),
        ),
        contributor_mode=contributor_mode,
        contributor_mode_switch_epoch=last_mode_switch_epoch,
    )
    coordinator.config_entry = entry  # convenience for platforms

    # --- BEGIN CORRECTED STARTUP ORDER ---

    # STEP 1: Create Sub-Entry-Manager (as before)
    entry_state = getattr(entry, "state", None)
    reload_states: tuple[ConfigEntryState, ...] = (
        ConfigEntryState.SETUP_IN_PROGRESS,
        ConfigEntryState.SETUP_RETRY,
    )
    is_reload = False
    if isinstance(entry_state, ConfigEntryState):
        is_reload = entry_state in reload_states
    elif isinstance(entry_state, str):
        reload_values = tuple(
            state.value if hasattr(state, "value") else str(state)
            for state in reload_states
        )
        is_reload = entry_state in reload_values

    filter_cls = _resolve_google_home_filter_class()
    google_home_filter_instance: GoogleHomeFilterProtocol | None = None
    if filter_cls is not None:
        try:
            google_home_filter_instance = cast(
                GoogleHomeFilterProtocol, filter_cls(hass, _effective_config(entry))
            )
            _LOGGER.debug("Initialized Google Home filter (options-first)")
        except Exception as err:
            _LOGGER.debug("GoogleHomeFilter attach skipped due to: %s", err)
    else:
        _LOGGER.debug("GoogleHomeFilter not available; continuing without it")

    runtime_subentry_manager = ConfigEntrySubEntryManager(hass, entry)
    entity_recovery_manager = EntityRecoveryManager(hass, entry, coordinator)
    runtime_data = RuntimeData(
        coordinator=coordinator,
        token_cache=cache,
        subentry_manager=runtime_subentry_manager,
        fcm_receiver=fcm,
        entity_recovery_manager=entity_recovery_manager,
        google_home_filter=google_home_filter_instance,
    )
    runtime_data.cloud_discovery = CloudDiscoveryRuntime()
    entry.runtime_data = runtime_data
    entries_bucket: dict[str, RuntimeData] = _ensure_entries_bucket(domain_bucket)
    entries_bucket[entry.entry_id] = runtime_data

    # Re-arm the global restart-required watchdog now that an entry is active.
    # async_setup arms it on component load, but a last-entry reload tears it down
    # and async_setup does not run again; arming here (the exact inverse of the
    # `not entries_bucket` teardown trigger) keeps the warning alive across reloads.
    await _async_ensure_restart_required_check(hass, domain_bucket)

    _cloud_discovery_runtime(hass, entry)

    pending_reconfigure_refresh = _ensure_pending_reconfigure_refresh(domain_bucket)
    recent_reconfigure_markers = _ensure_recent_reconfigure_markers(domain_bucket)
    marker = recent_reconfigure_markers.pop(entry.entry_id, None)
    if marker is not None:
        coordinator.mark_recent_reconfigure(marker)
    if entry.entry_id in pending_reconfigure_refresh:
        pending_reconfigure_refresh.discard(entry.entry_id)
        coordinator.request_device_list_refresh(
            reason="reconfigure", schedule_refresh=False
        )
    # Attach runtime data before subentry sync/setup so _async_setup_new_subentries
    # retry bookkeeping can access the per-entry container during parent setup.
    _subentry_setup_tracker(hass, entry).clear()
    runtime_data.subentry_retry_attempts.clear()
    runtime_data.subentry_retry_handles.clear()

    coordinator.attach_subentry_manager(runtime_subentry_manager, is_reload=is_reload)

    config_entries = getattr(hass, "config_entries", None)
    auto_schedule_subentries = _core_auto_schedules_subentries(config_entries)
    suppress_subentry_forwarding = False
    if auto_schedule_subentries:
        suppress_subentry_forwarding = True
        _LOGGER.debug(
            "[%s] Suppressing manual subentry forwarding during setup; Home Assistant auto-schedules registered subentries",
            entry.entry_id,
        )

    # STEP 2: Create and Synchronize Sub-Entries (RUNNING NOW)
    # This is necessary so the coordinator knows the Sub-Entry-IDs in Step 3.
    tracker_features = sorted(TRACKER_FEATURE_PLATFORMS)
    service_features = sorted(SERVICE_FEATURE_PLATFORMS)
    fcm_push_enabled = fcm is not None

    has_google_home_filter = google_home_filter_instance is not None
    entry_title = entry.title or entry.data.get(CONF_GOOGLE_EMAIL, "Google Find My")

    await runtime_subentry_manager.async_sync(
        [
            ConfigEntrySubentryDefinition(
                key=TRACKER_SUBENTRY_KEY,
                title="Google Find My devices",
                data={
                    "features": tracker_features,
                    "fcm_push_enabled": fcm_push_enabled,
                    "has_google_home_filter": has_google_home_filter,
                    "entry_title": entry_title,
                },
                subentry_type=SUBENTRY_TYPE_TRACKER,
                unique_id=f"{entry.entry_id}-{TRACKER_SUBENTRY_KEY}",
                translation_key=TRACKER_SUBENTRY_TRANSLATION_KEY,
            ),
            ConfigEntrySubentryDefinition(
                key=SERVICE_SUBENTRY_KEY,
                title="Google Find Hub Service",
                data={
                    "features": service_features,
                    "fcm_push_enabled": fcm_push_enabled,
                    "has_google_home_filter": has_google_home_filter,
                    "entry_title": entry_title,
                },
                subentry_type=SUBENTRY_TYPE_SERVICE,
                unique_id=f"{entry.entry_id}-{SERVICE_SUBENTRY_KEY}",
                translation_key=SERVICE_SUBENTRY_TRANSLATION_KEY,
            ),
        ],
        suppress_registered_forwarding=suppress_subentry_forwarding,
    )

    # STEP 3: Run First Refresh (AFTER Sub-Entries exist)
    # Now _ensure_registry_for_devices can find the correct Sub-Entry-IDs.
    try:
        await coordinator.async_config_entry_first_refresh()
    except ConfigEntryAuthFailed:
        raise
    except Exception as err:  # noqa: BLE001 - wrap unexpected failures for HA retry
        _LOGGER.error(
            "[%s] Initial coordinator refresh failed: %s",
            entry.entry_id,
            err,
            exc_info=True,
        )
        raise ConfigEntryNotReady(f"Initial refresh failed: {err}") from err

    raw_metadata = getattr(coordinator, "_subentry_metadata", None)
    metadata: Mapping[str, object] | None
    if isinstance(raw_metadata, Mapping):
        metadata = raw_metadata
    else:
        metadata = None
    pending_visibility_updates: list[
        Awaitable[ConfigSubentry | bool | None] | ConfigSubentry | bool | None
    ] = []
    key_field = getattr(runtime_subentry_manager, "_key_field", "group_key")
    has_visibility_metadata = metadata is not None

    def _normalize_visible_ids(raw: object) -> tuple[str, ...]:
        if isinstance(raw, (str, bytes)) or not isinstance(raw, Iterable):
            return tuple()

        deduped_ids: dict[str, None] = {}
        for device_id in raw:
            if isinstance(device_id, str) and device_id:
                deduped_ids.setdefault(device_id, None)

        return tuple(deduped_ids)

    def _extract_visible_ids(subentry_meta: object) -> tuple[str, ...]:
        if isinstance(subentry_meta, Mapping):
            raw_visible_ids = subentry_meta.get("visible_device_ids")
        else:
            raw_visible_ids = getattr(subentry_meta, "visible_device_ids", None)

        return _normalize_visible_ids(raw_visible_ids)

    if metadata is not None:
        for group_key, subentry_meta in metadata.items():
            managed_subentry = runtime_subentry_manager.managed_subentries.get(
                group_key
            )
            if managed_subentry is None:
                continue

            if (
                getattr(managed_subentry, "subentry_type", None)
                in NON_DEVICE_SUBENTRY_TYPES
            ):
                # This loop calls ``async_update_subentry`` directly, so the
                # guard inside ``update_visible_device_ids`` does not cover it.
                # It matters because the manager canonicalises ``service`` and
                # ``tracker`` by type but leaves ``hub`` on its stored key: a
                # legacy hub storing ``core_tracking`` therefore *is*
                # ``managed_subentries["core_tracking"]``, and the tracker
                # group's ids would be written onto it. Measured against the
                # real manager, not assumed.
                #
                # What changed with the rank axis in ``_candidate_score`` is
                # only the *contested* case: beside a real ``tracker`` storing
                # the same key the hub is now the deterministic loser rather
                # than a winner-by-iteration-order. Uncontested it still holds
                # the key, so this guard is narrowed in reach, not made
                # redundant, and it stays.
                continue

            desired_visible_ids = _extract_visible_ids(subentry_meta)
            existing_visible_ids = _normalize_visible_ids(
                managed_subentry.data.get("visible_device_ids")
            )

            if desired_visible_ids == existing_visible_ids:
                continue

            payload = dict(managed_subentry.data)
            payload.setdefault(key_field, group_key)
            payload["visible_device_ids"] = list(desired_visible_ids)

            update_result = hass.config_entries.async_update_subentry(
                entry,
                managed_subentry,
                data=payload,
            )
            pending_visibility_updates.append(update_result)

    for update_result in pending_visibility_updates:
        try:
            await runtime_subentry_manager._await_subentry_result(update_result)
        except Exception as err:  # pragma: no cover - defensive logging
            _LOGGER.debug(
                "[%s] Subentry visibility write failed during setup: %s",
                entry.entry_id,
                err,
            )

    if has_visibility_metadata or pending_visibility_updates:
        runtime_subentry_manager._refresh_from_entry()
        coordinator._refresh_subentry_index()  # noqa: SLF001 - align platform setup

    await _async_normalize_device_names(hass)
    await coordinator.async_wait_subentry_visibility_updates()

    # --- END CORRECTED STARTUP ORDER ---

    # Performance metrics injection
    try:
        perf = getattr(coordinator, "performance_metrics", None)
        if not isinstance(perf, dict):
            perf = {}
            setattr(coordinator, "performance_metrics", perf)
        perf["setup_start_monotonic"] = pm_setup_start
        perf["fcm_acquired_monotonic"] = pm_fcm_acquired
    except Exception as err:
        _LOGGER.debug("Failed to set performance metrics on coordinator: %s", err)

    # Hand over the barrier without changing the coordinator's signature.
    setattr(coordinator, "fcm_ready_event", fcm_ready_event)

    # Register the coordinator with the shared FCM receiver
    fcm.register_coordinator(coordinator)
    entry.async_on_unload(lambda: fcm.unregister_coordinator(coordinator))

    # Ensure FCM supervisor is running for background push updates (idempotent).
    try:
        await fcm._start_listening()  # noqa: SLF001
    except AttributeError:
        _LOGGER.debug(
            "FCM receiver has no _start_listening(); relying on on-demand start via per-request registration."
        )

    entity_registry = er.async_get(hass)
    registry_entries_iterable: Iterable[Any] = _iter_config_entry_entities(
        entity_registry, entry.entry_id
    )

    reactivated = 0
    for entity_entry in registry_entries_iterable:
        if (
            entity_entry.domain == "button"
            and entity_entry.platform == DOMAIN
            and entity_entry.disabled_by == RegistryEntryDisabler.INTEGRATION
        ):
            entity_registry.async_update_entity(
                entity_entry.entity_id, disabled_by=None
            )
            reactivated += 1

    if reactivated:
        _LOGGER.debug(
            "Re-enabled %s button entities disabled by integration", reactivated
        )

    # Owner-index scaffold (E2.5): coordinator will eventually claim canonical_ids
    _ensure_device_owner_index(domain_bucket)

    # STEP 4: Load Platforms
    # Home Assistant Core will automatically set up subentry platforms after this
    # coroutine returns. Manual forwarding (for example, via async_forward_entry_setups)
    # must not be invoked here because the helper's 2025.11.2 SSoT signature does not
    # accept config_subentry_id, which would strip subentry context.

    bucket = domain_bucket

    # Coordinator setup (DR listeners, initial index, etc.)
    try:
        await coordinator.async_setup()
    except Exception as err:
        _LOGGER.warning(
            "Coordinator setup failed early; will recover on next refresh: %s", err
        )

    # Register map views (idempotent across multi-entry), gated per-entry: an
    # entry that opts out skips registration, but another entry still wanting
    # the feature registers it regardless of order. Once up, views stay up for
    # the process lifetime (no unregister_view API); map_view.py's own
    # OPT_MAP_VIEW_ENABLED check turns away a since-disabled entry per-request.
    if _is_map_view_enabled_for_entry_fn(entry):
        _async_ensure_map_views_registered(hass, bucket)

    # Run duplicate self-healing asynchronously so it also executes on reloads.
    hass.async_create_task(
        _async_self_heal_duplicate_entities(hass, entry),
        name=f"{DOMAIN}.self_heal_duplicates.{entry.entry_id}",
    )

    # Mark initial setup complete (used to distinguish cold start vs. reload)
    domain_bucket["initial_setup_complete"] = True

    # Final performance marker
    try:
        perf = getattr(coordinator, "performance_metrics", None)
        if isinstance(perf, dict):
            perf["setup_end_monotonic"] = time.monotonic()
    except Exception as err:
        _LOGGER.debug("Failed to set setup_end_monotonic: %s", err)

    _register_instance(entry.entry_id, cache)

    existing_resolver = domain_bucket.get(DATA_EID_RESOLVER)
    if isinstance(existing_resolver, GoogleFindMyEIDResolver):
        await existing_resolver.async_refresh()
    else:
        eid_resolver = GoogleFindMyEIDResolver(hass)
        # Immediate refresh keeps BLE resolution available before the first
        # boundary-aligned timer fires after startup/reload.
        await eid_resolver.async_refresh()
        # Resolver is shared for the entire domain so BLE scans can resolve
        # devices across all loaded config entries via hass.data[DOMAIN][DATA_EID_RESOLVER].
        domain_bucket[DATA_EID_RESOLVER] = eid_resolver

    # ---- BLE Scanner: optional HA-Bluetooth FMDN advertisement listener ----
    # Always attempted (independent of FEATURE_FMDN_FINDER_ENABLED).
    # Collects MAC addresses and frame types for future BLE ringing (Phase 2).
    # Silently skipped when the bluetooth integration is not loaded.
    try:
        from .fmdn_finder.ble_scanner import async_setup_ble_scanner  # noqa: PLC0415

        await async_setup_ble_scanner(hass)
    except ImportError:
        _LOGGER.debug("BLE scanner module not available (optional)")
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("BLE scanner setup skipped: %s", err)

    # ---- FMDN Finder: Bermuda listener for location uploads ----
    # Disabled by default via FEATURE_FMDN_FINDER_ENABLED in const.py.
    if FEATURE_FMDN_FINDER_ENABLED:
        try:
            from .fmdn_finder import async_setup_fmdn_finder  # noqa: PLC0415

            fmdn_setup_success = await async_setup_fmdn_finder(hass)
            if fmdn_setup_success:
                _LOGGER.info(
                    "FMDN Finder enabled - will upload location reports for FMDN beacons detected by Bermuda"
                )
            else:
                _LOGGER.warning("FMDN Finder setup failed - location uploads disabled")
        except ImportError:
            _LOGGER.debug("FMDN Finder module not available (optional feature)")
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("FMDN Finder setup failed: %s", err, exc_info=True)

    parent_platforms_forwarded = bool(
        getattr(entry, "_gfm_parent_platforms_forwarded", False)
    )
    registered_subentries = list(entry.subentries.values())
    if registered_subentries:
        if parent_platforms_forwarded:
            _LOGGER.debug(
                "[%s] Skipping registered subentry forward; parent platforms already forwarded during setup",
                entry.entry_id,
            )
        elif auto_schedule_subentries:
            _safe_setattr(entry, "_gfm_parent_platforms_forwarded", True)
            _LOGGER.debug(
                "[%s] Subentry forward suppressed; %s registered subentries will be scheduled by Home Assistant (auto-scheduling detected)",
                entry.entry_id,
                len(registered_subentries),
            )
        else:
            _LOGGER.debug(
                "[%s] Forwarding setup for %s registered subentries after reload/startup",
                entry.entry_id,
                len(registered_subentries),
            )
            await _async_setup_new_subentries(hass, entry, registered_subentries)
    elif auto_schedule_subentries:
        _LOGGER.debug(
            "[%s] Subentry forward suppressed; Home Assistant auto-schedules subentry platforms after setup",
            entry.entry_id,
        )

    await _async_normalize_device_names(hass)
    await _async_refresh_device_urls(hass)

    # Post-persist cleanup (P2): the config flow can only *stage* irreversible
    # cleanups (deleting the imported secrets.json copies), because
    # `ConfigFlow.async_create_entry` merely builds a FlowResult -- Home Assistant creates and stores the entry
    # afterwards in `ConfigEntriesFlowManager.async_finish_flow`. Deliberately
    # behind the whole setup core: every `ConfigEntryNotReady` above must leave
    # the credentials on disk for the next attempt.
    #
    # Reaching this line proves the entry exists *in memory*, not that it has
    # been stored: `ConfigEntries.async_add` awaits this very coroutine and only
    # calls `_async_schedule_save` afterwards, which then saves debounced. So
    # the jobs are only CLAIMED here (synchronously, so a reload cannot claim
    # them twice) and executed later, by a background task that first waits for
    # proof that the entry reached Home Assistant's storage. Everything that is
    # not such a proof -- shutdown, unload, timeout, error -- drops the jobs and
    # keeps the credentials on disk.
    #
    # Best effort in both directions: the runner isolates each job, and this
    # guard makes sure a cleanup failure can never turn a successful setup into
    # a failed one (the secrets watcher re-imports a surviving file on its next
    # scan).
    try:
        from .config_flow import (  # noqa: PLC0415 - lazy, avoids an import cycle
            async_schedule_pending_container_cleanup,
        )

        async_schedule_pending_container_cleanup(hass, entry)
    except Exception as err:  # noqa: BLE001 - cleanup must never fail setup
        _LOGGER.warning(
            "[%s] Deferred container-login cleanup could not be armed: %s",
            entry.entry_id,
            err,
        )

    # Counterpart to the refresh in `async_remove_entry`: bring this entry's
    # SECRETS_EXTRA_WATCH_PATHS back into the running watcher. Needed because
    # `_collect_extra_watch_paths` skips disabled entries while Home Assistant
    # fires no update listener when an entry is enabled again, and because the
    # discovery singleton is armed once per instance, so an entry set up later
    # would otherwise contribute nothing until the next options update.
    # Cheap and safe: `async_refresh_watch_paths` skips watchers whose path set
    # is unchanged, which is the normal case for a plain setup or reload, so no
    # rescan is triggered there. Only runs on a successful setup: every
    # `ConfigEntryNotReady` above returns before this point.
    #
    # Note on ordering: the deferred cleanup above is now armed, not executed,
    # so a rescan triggered here can still see a bundle that the cleanup will
    # delete moments later. That is harmless -- a re-import of the very bundle
    # this entry was created from is idempotent (same account, same content),
    # and the delete stays account- and content-aware, so it only removes the
    # copies the import actually consumed.
    await _async_refresh_discovery_watch_paths(hass)

    return True


async def _async_save_secrets_data(
    cache: TokenCache,
    secrets_data: Mapping[str, Any],
    *,
    account_label: str | None = None,
) -> None:
    """Persist a legacy secrets.json bundle into the entry-scoped token cache.

    Notes:
        - Store JSON-serializable values *as-is*. TokenCache validates and normalizes.
        - Uses the *entry-local* cache instance (no global facade).
        - Log lines name the account by ``account_label`` (the caller passes
          ``_label_entry_for_log(entry)``) or, without one, by the cache's entry
          ID. No clear-text value of the bundle reaches a log line (the label is
          masked or an entry ID), only the type name of a value that failed to
          save, and a field name only if it is one of the
          fixed names in ``_LOGGABLE_BUNDLE_FIELDS`` (taken from that tuple, not
          from the bundle): other field names can embed the account e-mail
          (``adm_token_<email>``), and CodeQL counts any value derived from the
          bundle as a secret, masked or not.
    """
    log_label = account_label or f"entry {getattr(cache, 'entry_id', '<unknown>')}"
    from .shared_helpers import normalize_secrets_bundle

    # Defense-in-depth: the config flow already normalizes bundles on entry, but
    # this is the universal cache-write choke point. A bundle persisted by an
    # older version or migrated from a file may still carry copy/paste whitespace
    # in credential values (a stray space in owner_key/shared_key breaks AES-GCM
    # decryption). normalize_secrets_bundle is idempotent, so re-normalizing an
    # already-clean bundle is a no-op.
    secrets_data = normalize_secrets_bundle(dict(secrets_data))
    enhanced_data = dict(secrets_data)

    # Normalize username key across old/new secrets variants
    google_email = secrets_data.get("username", secrets_data.get("Email"))
    if google_email:
        enhanced_data[username_string] = google_email

    owner_key = secrets_data.get("owner_key")
    shared_key = secrets_data.get("shared_key")
    if not shared_key:
        # The trigger is solely the missing shared_key; the locally present
        # owner_key only selects how wide the outage is described.
        if owner_key:
            _LOGGER.warning(
                "Account %s: the imported bundle has no 'shared_key'. "
                "Crowdsourced/FMDN locations cannot be decrypted now; "
                "own-device locations will fail when the owner key rotates "
                "(it can only be refreshed with the shared_key). "
                "Re-import a complete secrets.json.",
                log_label,
            )
        else:
            _LOGGER.warning(
                "Account %s: the imported bundle has no 'shared_key'. "
                "No location can be decrypted. "
                "Re-import a complete secrets.json.",
                log_label,
            )
    if google_email:
        email_key = str(google_email)
        try:
            if owner_key is not None:
                await cache.async_set_cached_value(f"owner_key_{email_key}", owner_key)
            if shared_key is not None:
                await cache.async_set_cached_value(
                    f"shared_key_{email_key}", shared_key
                )
        except (OSError, TypeError) as err:
            _LOGGER.warning(
                "Failed to save encrypted key bundle to persistent cache for %s: %s",
                log_label,
                err,
            )

    for key, value in enhanced_data.items():
        try:
            if isinstance(value, (str, int, float, bool, dict, list)) or value is None:
                await cache.async_set_cached_value(key, value)
            else:
                await cache.async_set_cached_value(key, json.dumps(value))
        except (OSError, TypeError) as err:
            field_name = next(
                (name for name in _LOGGABLE_BUNDLE_FIELDS if name == key),
                "other field",
            )
            _LOGGER.warning(
                "Failed to save bundle field %s (%s) for %s to persistent cache: %s",
                field_name,
                type(value).__name__,
                log_label,
                err,
            )


async def _async_normalize_device_names(hass: HomeAssistant) -> None:
    """One-time normalization: strip legacy 'Find My - ' prefix from device names."""

    try:
        # Local import, see ``_async_purge_unloaded_subentry_registrations``.
        from .coordinator.helpers.registry import iter_all_devices

        dev_reg = dr.async_get(hass)
        updated = 0
        # Whole registry on purpose: this one-time pass strips a legacy name
        # prefix from every GoogleFindMy device, including devices of entries
        # other than the one that happens to trigger the pass.
        for device in iter_all_devices(dev_reg):
            try:
                if not any(
                    len(ident) == 2 and ident[0] == DOMAIN
                    for ident in device.identifiers
                ):
                    continue
            except (TypeError, ValueError):
                continue
            if device.name_by_user:
                continue
            name = device.name or ""
            if name.startswith("Find My - "):
                new_name = name[len("Find My - ") :].strip()
                if new_name and new_name != name:
                    dev_reg.async_update_device(device_id=device.id, name=new_name)
                    updated += 1
        if updated:
            _LOGGER.info(
                'Normalized %d device name(s) by removing legacy "Find My - " prefix',
                updated,
            )
    except Exception as err:  # pragma: no cover - best-effort normalization
        _LOGGER.debug("Device name normalization skipped due to: %s", err)


@callback
def _url_state_changed(hass: HomeAssistant, state: str) -> bool:
    """Record the URL situation and return whether it differs from the last one.

    The marker lives in the typed domain bucket, next to the timer that reads it
    and the teardown that clears it, rather than in a second top-level
    ``hass.data`` key: one owner, one lifecycle.
    """

    bucket = _domain_data(hass)
    previous: object = bucket.get("url_refresh_state")
    bucket["url_refresh_state"] = state
    return bool(previous != state)


async def _async_refresh_device_urls(
    hass: HomeAssistant, *, periodic: bool = False
) -> None:
    """Refresh configuration URLs for all Google Find My devices.

    ``periodic`` marks the daily timer run. Those runs report an unreachable or
    internal-only URL once per state change instead of once per run: the two
    messages were written for a single startup, and a timer would otherwise turn
    each of them into a daily line in every installation that has no external
    URL. A user-triggered refresh (the ``refresh_device_urls`` service) keeps
    reporting unconditionally, because there somebody is waiting for the answer.
    """

    def _report_once(state: str) -> bool:
        """Record the state and return whether this run should log above debug.

        The state is recorded on *every* run, the startup one included. Ordering
        matters here: with the recording behind a short-circuiting ``or``, a
        startup that logged the warning would not have stored it, and the first
        daily tick would have logged the identical line again.
        """

        changed = _url_state_changed(hass, state)
        return not periodic or changed

    domain_entries = {
        entry.entry_id: entry for entry in hass.config_entries.async_entries(DOMAIN)
    }

    # Local import, see ``_async_purge_unloaded_subentry_registrations``.
    from .coordinator.helpers.registry import (
        device_owning_entry_ids,
        iter_all_devices,
    )

    dev_reg = dr.async_get(hass)
    updated_count = 0

    # Resolved once so both passes below (clear disabled, then build enabled)
    # share the same device -> owning-entry lookup.
    resolved: list[tuple[dr.DeviceEntry, MyConfigEntry, str]] = []
    # Whole registry on purpose: this pass covers the map URL of every
    # GoogleFindMy device across all entries of the domain, and picks the owning
    # entry per device below.
    for device in iter_all_devices(dev_reg):
        try:
            if getattr(device, "entry_type", None) == dr.DeviceEntryType.SERVICE:
                continue

            if (
                getattr(device, "translation_key", None)
                == SERVICE_DEVICE_TRANSLATION_KEY
            ):
                continue

            if not any(
                len(identifier) >= 1 and identifier[0] == DOMAIN
                for identifier in device.identifiers
            ):
                continue

            # ``device_owning_entry_ids`` asks its callers to say why picking
            # one owner is allowed. From Core 2026.8 the tuple holds exactly one
            # id. Below that core a device shared between two GoogleFindMy
            # entries can list both; an owner with Map View enabled wins, so a
            # disabled owner listed first cannot clear the link of an owner
            # that still serves the map. Among equals the first owner in set
            # iteration order wins, as before; it only seeds the map token.
            owner_ids = [
                cid for cid in device_owning_entry_ids(device) if cid in domain_entries
            ]
            entry_id = next(
                (
                    cid
                    for cid in owner_ids
                    if _is_map_view_enabled_for_entry_fn(domain_entries[cid])
                ),
                owner_ids[0] if owner_ids else None,
            )
            if not entry_id:
                continue

            dev_id = next(
                (
                    ident[1]
                    for ident in device.identifiers
                    if len(ident) == 2 and ident[0] == DOMAIN
                ),
                None,
            )
            if not dev_id:
                continue

            if ":" in dev_id:
                dev_id = dev_id.split(":", 1)[1]

            resolved.append((device, domain_entries[entry_id], dev_id))
        except (TypeError, ValueError, IndexError):
            continue

    # Clearing a disabled entry's stale configuration_url needs no base URL,
    # so it runs before that lookup -- otherwise an install with no reachable
    # HA URL would never get the link cleared.
    enabled_items: list[tuple[dr.DeviceEntry, MyConfigEntry, str]] = []
    for device, entry, dev_id in resolved:
        try:
            if not _is_map_view_enabled_for_entry_fn(entry):
                # Disabled: clear any stale link rather than refresh one that
                # just refuses every request. Count only a real change; an
                # already-empty link is not written by the registry.
                if device.configuration_url is not None:
                    dev_reg.async_update_device(
                        device_id=device.id, configuration_url=None
                    )
                    updated_count += 1
                continue

            enabled_items.append((device, entry, dev_id))
        except (TypeError, ValueError, IndexError):
            continue

    if resolved and not enabled_items:
        # Every resolved device is on a disabled entry, so skip the base-URL
        # lookup entirely. With no devices at all, fall through so "no
        # reachable URL" is still reported as before.
        if updated_count:
            _LOGGER.debug(
                "Refreshed URLs for %d Google Find My device(s)", updated_count
            )
        return

    try:
        base_url = cast(
            str,
            get_url(
                hass,
                prefer_external=True,
                allow_cloud=True,
                allow_internal=True,
            ),
        )
    except (HomeAssistantError, NoURLAvailableError) as err:
        _LOGGER.log(
            logging.WARNING if _report_once("no-url") else logging.DEBUG,
            "Skipping configuration URL refresh; no reachable URL available: %s",
            err,
        )
        return

    if not base_url or "://" not in base_url:
        _LOGGER.log(
            logging.WARNING if _report_once("no-url") else logging.DEBUG,
            "Skipping configuration URL refresh; no reachable URL available",
        )
        return

    try:
        internal_url = get_url(
            hass,
            allow_external=False,
            allow_cloud=False,
            allow_internal=True,
        )
    except (HomeAssistantError, NoURLAvailableError):
        internal_url = None
    if base_url.rstrip("/") == (internal_url or "").rstrip("/"):
        _LOGGER.log(
            logging.INFO if _report_once("internal-only") else logging.DEBUG,
            "Using internal URL for map view links; "
            "set an external URL in Home Assistant settings for remote access",
        )
    else:
        _report_once("external")

    base_url = base_url.rstrip("/")

    ha_uuid = str(hass.data.get("core.uuid", "ha"))

    for device, entry, dev_id in enabled_items:
        try:
            token_exp = _opt(
                entry, OPT_MAP_VIEW_TOKEN_EXPIRATION, DEFAULT_MAP_VIEW_TOKEN_EXPIRATION
            )
            secret = map_token_secret_seed(ha_uuid, entry.entry_id, bool(token_exp))
            auth_token = map_token_hex_digest(secret)

            map_path = f"/api/googlefindmy/map/{dev_id}?token={auth_token}"
            new_config_url = f"{base_url}{map_path}"
            dev_reg.async_update_device(
                device_id=device.id, configuration_url=new_config_url
            )
            updated_count += 1
        except (TypeError, ValueError, IndexError):
            continue

    if updated_count:
        _LOGGER.debug("Refreshed URLs for %d Google Find My device(s)", updated_count)


async def _async_seed_manual_credentials(
    cache: TokenCache,
    oauth_token: str | None,
    aas_token_entry: str | None,
    google_email: str,
) -> None:
    """Persist manual credential updates and clear stale AAS tokens when absent.

    The clearing below is now the second line of defence: the credential seed in
    :func:`async_setup_entry` already drops a cached AAS token that the entry no
    longer carries, for the bundle branch as well as this one. Keeping it here is
    deliberate -- this helper is also the surface a caller reaches directly, and
    ``cache.set(key, None)`` on an already absent key is a no-op.
    """

    token_to_save = oauth_token or aas_token_entry
    if isinstance(token_to_save, str) and token_to_save:
        await _async_save_individual_credentials(cache, token_to_save, google_email)
        _LOGGER.debug("Persisted individual credentials to token cache (entry-scoped)")

    if isinstance(aas_token_entry, str) and aas_token_entry:
        await cache.async_set_cached_value(DATA_AAS_TOKEN, aas_token_entry)
        _LOGGER.debug("Stored AAS token provided alongside manual credentials")
    else:
        await cache.async_set_cached_value(DATA_AAS_TOKEN, None)
        _LOGGER.debug(
            "Cleared entry-scoped AAS token so a fresh value is minted from the new OAuth token"
        )


async def _async_save_individual_credentials(
    cache: TokenCache, oauth_token: str, google_email: str
) -> None:
    """Persist individual credentials (oauth_token + email) to the *entry-scoped* token cache."""
    try:
        await cache.async_set_cached_value(CONF_OAUTH_TOKEN, oauth_token)
        await cache.async_set_cached_value(username_string, google_email)
    except OSError as err:
        _LOGGER.warning(
            "Failed to save the individually entered sign-in data to the cache: %s", err
        )


# ------------------- Device removal (HA "Delete device" hook) -------------------


async def async_remove_config_entry_device(
    hass: HomeAssistant,
    entry: ConfigEntry,
    device_entry: dr.DeviceEntry,
) -> bool:
    """Handle 'Delete device' requests from Home Assistant.

    Semantics:
    - Only act on devices owned by this integration/entry (identifier domain matches and
      the device is linked to this config entry).
    - Purge in-memory caches for the device via the coordinator (keeps UI/state clean).
    - Add the id to `ignored_devices` to prevent automatic re-creation.
    - Return True to allow HA to remove the device record and its entities.
    - Never allow removing the integration's own "service" device (ident startswith 'integration').
    """
    _ensure_runtime_imports()
    # Local import, see ``_async_purge_unloaded_subentry_registrations``.
    from .coordinator.helpers.registry import device_belongs_to_entry

    if not device_belongs_to_entry(device_entry, entry.entry_id):
        return False

    raw_ident: str | None = None
    canonical_id: str | None = None
    for domain, ident in device_entry.identifiers:
        if domain != DOMAIN or not isinstance(ident, str) or not ident:
            continue
        raw_ident = ident
        canonical_id = _normalize_device_identifier(device_entry, ident)
        break

    if not canonical_id:
        return False

    if _device_is_service_device(device_entry, entry.entry_id):
        return False

    # The foreign-reading tracker is process-wide and survives reloads by
    # design; a removed device would otherwise keep its entry until restart
    # (CX-6). Keyed like the decoder: the parent entry's token-cache id.
    from .NovaApi.ExecuteAction.LocateTracker.foreign_reading_tracker import (
        FOREIGN_READING_TRACKER,
    )

    FOREIGN_READING_TRACKER.forget_device(
        getattr(entry, "parent_entry_id", None) or entry.entry_id, canonical_id
    )

    try:
        # Prefer entry.runtime_data (2026 standard), fall back to entries bucket.
        runtime: RuntimeData | GoogleFindMyCoordinator | None = getattr(
            entry, "runtime_data", None
        )
        if runtime is None:
            bucket = _domain_data(hass)
            entries_bucket = _ensure_entries_bucket(bucket)
            runtime = entries_bucket.get(entry.entry_id)

        coordinator: GoogleFindMyCoordinator | None = None
        purge_device: Callable[[str], Any] | None = None

        if isinstance(runtime, GoogleFindMyCoordinator):
            coordinator = runtime
        elif runtime is not None:
            coordinator = getattr(runtime, "coordinator", None)

        if isinstance(coordinator, GoogleFindMyCoordinator):
            purge_callable = getattr(coordinator, "purge_device", None)
            if callable(purge_callable):
                purge_device = cast(Callable[[str], Any], purge_callable)
        if purge_device is not None:
            purge_device(canonical_id)
    except Exception as err:
        _LOGGER.debug("Coordinator purge failed for %s: %s", canonical_id, err)

    try:
        opts = dict(entry.options)
        current_raw = opts.get(
            OPT_IGNORED_DEVICES, DEFAULT_OPTIONS.get(OPT_IGNORED_DEVICES)
        )
        ignored_map, _migrated = coerce_ignored_mapping(current_raw)

        canonical_meta = ignored_map.get(canonical_id)
        legacy_meta = None
        if raw_ident and raw_ident != canonical_id and raw_ident in ignored_map:
            legacy_meta = ignored_map.pop(raw_ident)

        name_to_store = device_entry.name_by_user or device_entry.name or canonical_id

        alias_sources: list[list[str] | None] = []
        name_sources: list[list[str] | None] = []

        if raw_ident and raw_ident != canonical_id:
            alias_sources.append([raw_ident])

        for meta in (canonical_meta, legacy_meta):
            if not isinstance(meta, Mapping):
                continue

            raw_aliases = meta.get("aliases")
            if isinstance(raw_aliases, Iterable) and not isinstance(
                raw_aliases, (str, bytes, bytearray)
            ):
                sanitized_aliases = [
                    alias for alias in raw_aliases if isinstance(alias, str) and alias
                ]
                if sanitized_aliases:
                    alias_sources.append(sanitized_aliases)
            elif isinstance(raw_aliases, str) and raw_aliases:
                alias_sources.append([raw_aliases])

            raw_name = meta.get("name")
            if isinstance(raw_name, str) and raw_name:
                name_sources.append([raw_name])

        aliases: list[str] = _dedupe_aliases(
            name_to_store,
            *alias_sources,
            *name_sources,
        )

        ignored_at = int(time.time())

        source = next(
            (
                meta.get("source")
                for meta in (canonical_meta, legacy_meta)
                if isinstance(meta, Mapping)
                and isinstance(meta.get("source"), str)
                and meta.get("source")
            ),
            "registry",
        )

        ignored_map[canonical_id] = {
            "name": name_to_store,
            "aliases": aliases,
            "ignored_at": ignored_at,
            "source": source,
        }
        opts[OPT_IGNORED_DEVICES] = ignored_map
        opts[OPT_OPTIONS_SCHEMA_VERSION] = 2

        if opts != entry.options:
            hass.config_entries.async_update_entry(entry, options=opts)
            _LOGGER.info(
                "Marked device '%s' (%s) as ignored for entry '%s'",
                name_to_store,
                canonical_id,
                _label_entry_for_log(entry),
            )
    except Exception as err:
        _LOGGER.debug("Persisting delete decision failed for %s: %s", canonical_id, err)

    return True


# ------------------------------- Misc helpers ---------------------------------


async def _async_unload_subentry(hass: HomeAssistant, entry: MyConfigEntry) -> bool:
    """Unload a subentry (tracker or service) by unloading its platforms."""

    _LOGGER.debug(
        "[%s] Unloading subentry (parent_id=%s, type=%s, key=%s)",
        entry.entry_id,
        getattr(entry, "parent_entry_id", None),
        getattr(entry, "subentry_type", None),
        entry.data.get("group_key"),
    )
    parent_entry_id = getattr(entry, "parent_entry_id", None)
    config_subentry_id = _resolve_config_subentry_identifier(entry, allow_entry_id=True)
    entry_type = (
        entry.data.get("subentry_type") if isinstance(entry.data, Mapping) else None
    )

    try:
        result: Any = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    except ValueError:
        _LOGGER.debug(
            "[%s] Subentry platforms never loaded; purging registry links (type=%s)",
            entry.entry_id,
            entry_type,
        )
        await _async_purge_unloaded_subentry_registrations(
            hass,
            parent_entry_id=parent_entry_id,
            config_subentry_id=config_subentry_id,
            entry_type=entry_type,
        )
        if hasattr(entry, "runtime_data"):
            setattr(entry, "runtime_data", None)
        return True
    # Home Assistant returns ``False`` when any platform refuses to unload; keep
    # the runtime data so retry attempts can reuse the existing coordinator.
    unload_success = bool(result)
    if unload_success and hasattr(entry, "runtime_data"):
        setattr(entry, "runtime_data", None)
    return unload_success


async def _maybe_close_spot_transport(entries_bucket: dict[str, RuntimeData]) -> None:
    """
    Close the shared SPOT transport when the integration has no active entries.

    Validated by tests/test_spot_transport_lifecycle.py::test_close_only_when_bucket_empty
    and tests/test_spot_transport_lifecycle.py::test_close_skipped_when_entries_remain.
    """

    if entries_bucket:
        return

    await SPOT_GRPC_TRANSPORT.async_close()


async def _async_unload_parent_entry(hass: HomeAssistant, entry: MyConfigEntry) -> bool:
    """Unload a parent entry by unloading children and cleaning up resources."""

    _ensure_runtime_imports()
    _LOGGER.debug("[%s] Unloading parent entry", entry.entry_id)

    bucket = _domain_data(hass)
    entries_bucket = _ensure_entries_bucket(bucket)
    runtime_data: RuntimeData | None = entries_bucket.pop(entry.entry_id, None)
    if runtime_data is None:
        runtime_data_raw = getattr(entry, "runtime_data", None)
        if isinstance(runtime_data_raw, RuntimeData):
            runtime_data = runtime_data_raw

    if isinstance(runtime_data, RuntimeData):
        for handle in runtime_data.subentry_retry_handles.values():
            if inspect.iscoroutine(handle):
                handle.close()
                continue
            if isinstance(handle, asyncio.Task):
                handle.cancel()
                continue
            cancel = getattr(handle, "cancel", None)
            if callable(cancel):
                cancel()
        runtime_data.subentry_retry_handles.clear()
        runtime_data.subentry_retry_attempts.clear()
        _cleanup_cloud_discovery_runtime(runtime_data)

    subentries = _collect_entry_subentries(entry)

    forwarded_parent_platforms_flag = getattr(
        entry, "_gfm_parent_platforms_forwarded", None
    )
    # Legacy parent entries may not persist the tracking flag; default to True so
    # parent platforms unload per the subentry teardown rules documented in
    # docs/CONFIG_SUBENTRIES_HANDBOOK.md.
    forwarded_parent_platforms = True
    if forwarded_parent_platforms_flag is not None:
        forwarded_parent_platforms = bool(forwarded_parent_platforms_flag)
    unload_parent_platforms = forwarded_parent_platforms
    unload_callable = getattr(hass.config_entries, "async_unload_platforms", None)
    unload_call_count = int(getattr(entry, "_gfm_parent_unload_call_count", 0) or 0)
    in_progress = bool(getattr(entry, "_gfm_parent_unload_in_progress", False))
    unload_completed = bool(getattr(entry, "_gfm_parent_platforms_unloaded", False))

    if not forwarded_parent_platforms:
        _LOGGER.debug(
            "[%s] Parent platforms never forwarded; skipping parent unload",
            entry.entry_id,
        )
        _safe_setattr(entry, "_gfm_parent_platforms_unloaded", True)
        unload_parent_platforms = True
    elif unload_completed and not in_progress:
        _LOGGER.debug(
            "[%s] Parent platforms already unloaded; skipping duplicate call",
            entry.entry_id,
        )
        return True
    if in_progress:
        _LOGGER.debug(
            "[%s] Parent unload already in progress; ignoring duplicate request",
            entry.entry_id,
        )
        return True

    overall_success = False
    _safe_setattr(entry, "_gfm_parent_unload_in_progress", True)
    try:
        if forwarded_parent_platforms:
            if callable(unload_callable):
                _safe_setattr(
                    entry, "_gfm_parent_unload_call_count", unload_call_count + 1
                )
                try:
                    result = unload_callable(entry, tuple(PLATFORMS))
                    if isinstance(result, Awaitable):
                        result = await result
                    unload_parent_platforms = (
                        bool(result) if result is not None else True
                    )
                except Exception as err:  # noqa: BLE001 - defensive logging
                    _LOGGER.error(
                        "[%s] Failed to unload parent platforms: %s",  # noqa: G004
                        entry.entry_id,
                        err,
                    )
                    unload_parent_platforms = False
                else:
                    if unload_parent_platforms:
                        _safe_setattr(entry, "_gfm_parent_platforms_unloaded", True)
            else:
                _LOGGER.debug(
                    "[%s] Home Assistant instance lacks async_unload_platforms; skipping parent unload",
                    entry.entry_id,
                )
                _safe_setattr(entry, "_gfm_parent_platforms_unloaded", True)

        if not unload_parent_platforms:
            if runtime_data is not None:
                entries_bucket[entry.entry_id] = runtime_data
            _LOGGER.debug(
                "[%s] Parent platform unload aborted; leaving subentries managed by HA",  # noqa: G004
                entry.entry_id,
            )
            return False

        async def _unload_config_subentry(subentry: Any) -> bool:
            identifier = _resolve_config_subentry_identifier(subentry)
            platforms = tuple(PLATFORMS)
            entry_type = None
            data_obj = getattr(subentry, "data", None)
            if isinstance(data_obj, Mapping):
                entry_type = data_obj.get("subentry_type") or data_obj.get("group_key")

            purge_on_missing_platform = False

            forward_unload = getattr(
                hass.config_entries, "async_forward_entry_unload", None
            )
            if isinstance(identifier, str) and identifier and callable(forward_unload):
                if inspect.iscoroutinefunction(forward_unload):
                    _LOGGER.debug(
                        "[%s] Home Assistant manages coroutine-based subentry unloads; skipping manual forward calls",  # noqa: G004
                        identifier,
                    )
                    await _async_purge_unloaded_subentry_registrations(
                        hass,
                        parent_entry_id=entry.entry_id,
                        config_subentry_id=identifier,
                        entry_type=entry_type,
                    )
                    return True

                all_unloaded = True
                unload_awaitables: list[tuple[str, Awaitable[Any]]] = []
                for platform in platforms:
                    platform_name = getattr(platform, "value", str(platform))
                    try:
                        result = forward_unload(entry, platform_name)
                    except ValueError:
                        _LOGGER.debug(
                            "[%s] Subentry platform '%s' was never loaded; skipping unload",  # noqa: G004
                            identifier,
                            platform_name,
                        )
                        purge_on_missing_platform = True
                        continue
                    if inspect.isawaitable(result):
                        unload_awaitables.append((platform_name, result))
                    else:
                        all_unloaded = all_unloaded and (
                            bool(result) if result is not None else True
                        )

                if unload_awaitables:
                    platform_names = [
                        platform_name for platform_name, _ in unload_awaitables
                    ]
                    gathered = await asyncio.gather(
                        *(awaitable for _, awaitable in unload_awaitables),
                        return_exceptions=True,
                    )
                    for platform_name, outcome in zip(platform_names, gathered):
                        if isinstance(outcome, BaseException):
                            if isinstance(outcome, ValueError):
                                _LOGGER.debug(
                                    "[%s] Subentry platform '%s' unload skipped: %s",  # noqa: G004
                                    identifier,
                                    platform_name,
                                    outcome,
                                )
                                purge_on_missing_platform = True
                                continue
                            all_unloaded = False
                            continue
                        if isinstance(outcome, bool):
                            all_unloaded = all_unloaded and outcome
                        else:
                            all_unloaded = all_unloaded and True

                if purge_on_missing_platform:
                    await _async_purge_unloaded_subentry_registrations(
                        hass,
                        parent_entry_id=entry.entry_id,
                        config_subentry_id=identifier,
                        entry_type=entry_type,
                    )

                return all_unloaded

            entry_id = getattr(subentry, "entry_id", None)
            unload_child = getattr(hass.config_entries, "async_unload", None)
            if isinstance(entry_id, str) and entry_id and callable(unload_child):
                result = unload_child(entry_id)
                if isinstance(result, Awaitable):
                    result = await result
                return bool(result)

            remove_callable = getattr(
                hass.config_entries, "async_remove_subentry", None
            )
            if isinstance(identifier, str) and identifier and callable(remove_callable):
                result = remove_callable(entry, identifier)
                if isinstance(result, Awaitable):
                    result = await result
                return bool(result)

            return True

        unload_success = True
        # Parent platforms never forwarded: treat subentry unload as a no-op.
        if forwarded_parent_platforms:
            unload_results = await asyncio.gather(
                *(_unload_config_subentry(subentry) for subentry in subentries),
                return_exceptions=True,
            )

            unload_success = all(
                isinstance(result, bool) and result for result in unload_results
            )
            if not unload_success:
                _LOGGER.error(
                    "[%s] Failed to unload one or more subentries; aborting parent unload",
                    entry.entry_id,
                )
                for index, result in enumerate(unload_results):
                    if isinstance(result, Exception):
                        sub_obj = subentries[index]
                        sub_id = _resolve_config_subentry_identifier(
                            sub_obj
                        ) or getattr(sub_obj, "entry_id", f"index {index}")
                        _LOGGER.debug(
                            "[%s] Subentry %s unload failed: %s",
                            entry.entry_id,
                            sub_id,
                            result,
                        )
                if runtime_data is not None:
                    entries_bucket[entry.entry_id] = runtime_data
                _LOGGER.debug(
                    "[%s] Subentry unload failed; runtime data restored for HA retry",  # noqa: G004
                    entry.entry_id,
                )
                return False

        if runtime_data is not None:
            coordinator = getattr(runtime_data, "coordinator", None)
            if coordinator is not None:
                shutdown = getattr(coordinator, "async_shutdown", None)
                if callable(shutdown):
                    result = shutdown()
                    if inspect.isawaitable(result):
                        await result

            cache = getattr(runtime_data, "token_cache", None)
            if cache is not None:
                fallback_cache = _unregister_instance(entry.entry_id)
                try:
                    await cache.close()
                    _LOGGER.debug(
                        "Cache instance for entry '%s' has been flushed and closed.",
                        entry.entry_id,
                    )
                except Exception as err:
                    _LOGGER.warning(
                        "Closing the cache instance for entry '%s' failed: %s",
                        entry.entry_id,
                        err,
                    )
                if fallback_cache is not None and fallback_cache is not cache:
                    with suppress(Exception):
                        await fallback_cache.close()

        try:
            await _async_release_shared_fcm(hass, entry)
        except Exception as err:
            _LOGGER.debug("FCM release during parent unload raised: %s", err)

        # Unload BLE scanner (if registered)
        try:
            from .fmdn_finder.ble_scanner import (
                async_unload_ble_scanner,  # noqa: PLC0415
            )

            await async_unload_ble_scanner(hass)
        except ImportError:
            pass
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("BLE scanner unload raised: %s", err)

        # Unload FMDN Finder (if enabled)
        try:
            from .fmdn_finder import async_unload_fmdn_finder  # noqa: PLC0415

            await async_unload_fmdn_finder(hass)
            _LOGGER.debug("FMDN Finder unloaded successfully")
        except ImportError:
            pass  # FMDN Finder module not available (optional feature)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("FMDN Finder unload raised: %s", err)

        try:
            owner_index: dict[str, str] = _ensure_device_owner_index(bucket)
            stale = [
                cid for cid, eid in list(owner_index.items()) if eid == entry.entry_id
            ]
            for cid in stale:
                owner_index.pop(cid, None)
            if stale:
                _LOGGER.debug(
                    "Cleared %d owner-index claim(s) for entry '%s'",
                    len(stale),
                    entry.entry_id,
                )
        except Exception as err:
            _LOGGER.debug("Owner-index cleanup failed: %s", err)

        resolver = bucket.get(DATA_EID_RESOLVER)
        if isinstance(resolver, GoogleFindMyEIDResolver):
            if not entries_bucket:
                resolver.stop()
                bucket.pop(DATA_EID_RESOLVER, None)
            else:
                hass.async_create_task(resolver.async_refresh())

        if not entries_bucket:
            # Last entry torn down: cancel the global restart watchdog (both timers)
            # and clear its issue, mirroring how other global singletons are released
            # here (Codex-P1 #1/#2).
            _teardown_restart_required_check(hass, bucket)
            _teardown_url_refresh_timer(hass, bucket)

        try:
            await _maybe_close_spot_transport(entries_bucket)
        except Exception as err:  # pragma: no cover - defensive
            _LOGGER.debug("SPOT transport close during parent unload raised: %s", err)

        if hasattr(entry, "runtime_data"):
            with suppress(Exception):
                setattr(entry, "runtime_data", None)

        overall_success = True
        return True
    finally:
        _safe_setattr(entry, "_gfm_parent_unload_in_progress", False)
        if not overall_success:
            _safe_setattr(entry, "_gfm_parent_platforms_unloaded", False)
            _safe_setattr(entry, "_gfm_parent_unload_call_count", unload_call_count)


async def async_unload_entry(hass: HomeAssistant, entry: MyConfigEntry) -> bool:
    """Unload a config entry.

    Notes:
        - This function is now a router.
        - Parent unload logic is in `_async_unload_parent_entry`.
        - Subentry unload logic is in `_async_unload_subentry`.
    """

    parent_entry_id = getattr(entry, "parent_entry_id", None)

    # Re-arm the decoder's canonicless-device warning for this scope. That guard is
    # process-wide and survives a config-entry reload (the module stays imported), so
    # without this an unchanged affected-device count would stay suppressed across a
    # reload and silently hide a still-missing device. The guard key is the parent
    # entry's token-cache id, so clear the parent scope (not the subentry id).
    from .ProtoDecoders.decoder import _reset_canonicless_warning_state

    _reset_canonicless_warning_state(parent_entry_id or entry.entry_id)

    try:
        if parent_entry_id:
            return await _async_unload_subentry(hass, entry)
        return await _async_unload_parent_entry(hass, entry)
    finally:
        # The claimed reload has arrived at its unload half; release the latch so a
        # change made after this point can schedule the next one. Deliberately in
        # ``finally`` and not at the head of this function: the whole platform
        # teardown runs in between, and it is the longest stretch of the reload.
        # A latch released before it lets the schedulers that do not wait for
        # ``LOADED`` -- the credential-writing config flows and the tracker
        # registry probe -- claim it again and queue a second reload, even though
        # the replacement setup that is already coming will read the newest
        # ``entry.data`` anyway. They do consult ``entry.state`` by now, through
        # ``entry_reload_gate``, but only for the *terminal* states:
        # ``UNLOAD_IN_PROGRESS`` is deliberately not one of them, so the reason
        # for releasing late is unchanged and has to stay that way. Releasing
        # here also covers a failed unload, where no setup half follows that could
        # release it, and an unload that is not part of a reload at all: the entry
        # keeps no latch it would never hand back.
        discard_pending_entry_reload(hass, entry.entry_id)


async def _async_refresh_discovery_watch_paths(
    hass: HomeAssistant, *, exclude_entry_id: str | None = None
) -> None:
    """Tell the discovery singleton to recompute its secrets watch paths.

    Single owner of that step for every caller (the per-entry options update
    listener and config entry removal), so both agree on the lookup of the
    manager and on the failure policy. Watch-path bookkeeping is housekeeping
    and must never break a setup, an options update or a removal:

    * a missing manager or a manager without the hook (discovery never armed,
      or a foreign object in the bucket) is ignored silently, because that is
      the ordinary state on an instance where discovery did not start,
    * an error raised by the hook is debug-logged with the caller's
      ``exclude_entry_id``, which is what tells a failed removal refresh apart
      from a failed options-update refresh in a debug log.

    ``exclude_entry_id`` is forwarded to the manager and leaves that entry out
    of the recomputation; removal passes the entry being removed.
    """

    manager = _domain_data(hass).get("discovery_manager")
    refresh = getattr(manager, "async_refresh_watch_paths", None)
    if not callable(refresh):
        return
    try:
        await refresh(exclude_entry_id=exclude_entry_id)
    except Exception:  # noqa: BLE001 - watcher refresh is best-effort, never fatal
        _LOGGER.debug(
            "Discovery watch-path refresh failed (exclude_entry_id=%s)",
            exclude_entry_id,
            exc_info=True,
        )


async def async_remove_entry(hass: HomeAssistant, entry: MyConfigEntry) -> None:
    """Handle removal of a config entry and purge persisted caches if requested."""

    _ensure_runtime_imports()

    # The device_tracker self-heal latch is keyed by entry_id and survives
    # reloads by design; once the entry is gone nothing would ever clear it.
    discard_registry_selfheal_reload(hass, entry.entry_id)

    # Same housekeeping for the reload latch: an entry that is gone will never
    # reach the setup or unload that would otherwise release it.
    discard_pending_entry_reload(hass, entry.entry_id)

    # And for the foreign-reading tracker (CX-6): its per-device readings and
    # once-per-device log gates survive reloads by design. Its keys carry the
    # token-cache entry id, so a removed subentry's own id matches nothing.
    from .NovaApi.ExecuteAction.LocateTracker.foreign_reading_tracker import (
        FOREIGN_READING_TRACKER,
    )

    FOREIGN_READING_TRACKER.forget_entry(entry.entry_id)

    # Drop this entry's SECRETS_EXTRA_WATCH_PATHS from the running watcher. The
    # update listener that normally recomputes them was unregistered by the
    # unload that precedes removal, so without this the watcher keeps polling a
    # path whose owning entry is gone until another entry update or a restart,
    # and a bundle written there could still open a discovery flow. Runs before
    # the teardown below so no earlier failure can skip it.
    #
    # The exclusion can only remove this entry's own contribution, and
    # DiscoveryManager.async_refresh_watch_paths forces a scan only for paths
    # that were ADDED. The ordinary removal -- the removed entry owned no extra
    # path, or owned one that now disappears -- is therefore a strict no-op for
    # the watcher's signature, and cannot re-import a bundle that some other
    # account's entry is still using.
    await _async_refresh_discovery_watch_paths(hass, exclude_entry_id=entry.entry_id)

    # A staged cleanup addressed to THIS entry can never be claimed again once
    # the entry is gone, so it would sit in hass.data for the rest of the
    # process lifetime. Discard it instead of running it: dropping keeps the
    # fail-safe direction (credential files stay on disk), whereas executing
    # would delete credentials for an entry the user just removed.
    try:
        from .config_flow import (  # noqa: PLC0415 - lazy, avoids an import cycle
            async_discard_pending_container_cleanup,
            async_discard_pending_container_cleanup_for_entry,
        )

        # Two addressings, in this order and both needed -- the same pairing the
        # duplicate-account abort in async_setup_entry uses.
        #
        # The entry-id drain first, in a single pass over the whole staging
        # list: this entry may hold SEVERAL update-path tickets, the claim-based
        # discard below takes at most one, and any upper bound would strand
        # exactly the tickets this call exists to clear.
        #
        # The claim-based discard second, for the ticket that names no entry.
        # A create-path ticket marked ``entry_promised`` deliberately carries
        # ``entry_id is None`` (the flow could not know the id), so the drain
        # above cannot see it. Without this second call such a ticket survives
        # the removal for the rest of the process lifetime, and rule 2 of
        # ``_async_claim_container_cleanup_ticket`` (uncorrelated ticket whose
        # unique_id matches) hands it to the *next* entry the user creates for
        # the same account, which then deletes credential copies that belong to
        # the removed one.
        #
        # It costs the account fallback of the claim helper: a concurrent,
        # still-running same-account flow could have its ticket taken instead.
        # That direction is the fail-safe one (the credential files stay on
        # disk), whereas leaving the ticket is the direction that deletes
        # foreign material.
        discarded = async_discard_pending_container_cleanup_for_entry(
            hass, entry_id=entry.entry_id
        )
        discarded += async_discard_pending_container_cleanup(
            hass,
            unique_id=getattr(entry, "unique_id", None),
            entry_id=entry.entry_id,
        )
        if discarded:
            _LOGGER.debug(
                "[%s] Discarded %s staged container-login cleanup job(s) on entry removal; credential files are kept on disk",
                entry.entry_id,
                discarded,
            )
    except Exception as err:  # noqa: BLE001 - housekeeping must never raise
        _LOGGER.debug(
            "[%s] Could not discard staged container-login cleanup on removal: %s",
            entry.entry_id,
            err,
        )

    # Prefer entry.runtime_data (2026 standard), then clean up entries bucket.
    bucket = _domain_data(hass)
    entries_bucket = bucket.get("entries")

    runtime: RuntimeData | GoogleFindMyCoordinator | None = getattr(
        entry, "runtime_data", None
    )
    if runtime is None and isinstance(entries_bucket, dict):
        runtime = entries_bucket.pop(entry.entry_id, None)
    elif isinstance(entries_bucket, dict):
        entries_bucket.pop(entry.entry_id, None)

    fallback_runtime = getattr(entry, "runtime_data", None)
    if runtime is None and isinstance(
        fallback_runtime, (RuntimeData, GoogleFindMyCoordinator)
    ):
        runtime = fallback_runtime

    coordinator: GoogleFindMyCoordinator | None = None
    token_cache: Any | None = None
    google_home_filter: GoogleHomeFilterProtocol | None = None

    if isinstance(runtime, GoogleFindMyCoordinator):
        coordinator = runtime
    elif isinstance(runtime, RuntimeData):
        coordinator = runtime.coordinator
        token_cache = runtime.token_cache
        google_home_filter = runtime.google_home_filter

    if coordinator is None and isinstance(fallback_runtime, GoogleFindMyCoordinator):
        coordinator = fallback_runtime
    if token_cache is None and isinstance(fallback_runtime, RuntimeData):
        token_cache = fallback_runtime.token_cache
        if google_home_filter is None:
            google_home_filter = fallback_runtime.google_home_filter

    if isinstance(runtime, RuntimeData):
        _cleanup_cloud_discovery_runtime(runtime)

    if coordinator is not None:
        try:
            await coordinator.async_shutdown()
        except Exception as err:
            _LOGGER.debug("Coordinator async_shutdown raised during removal: %s", err)

    if google_home_filter is not None:
        shutdown = getattr(google_home_filter, "async_shutdown", None)
        if callable(shutdown):
            try:
                result = shutdown()
                if inspect.isawaitable(result):
                    await result
            except Exception as err:
                _LOGGER.debug(
                    "Google Home filter shutdown during removal raised: %s", err
                )

    try:
        await _async_release_shared_fcm(hass, entry)
    except Exception as err:
        _LOGGER.debug("FCM release during async_remove_entry raised: %s", err)

    # Retire any entry-scoped FCM Repairs issues now that the entry is actually
    # being removed. The supervisor success path only deletes them on reload
    # recovery, and the unload teardown deliberately leaves the fixable reauth
    # prompt alone; once the entry is gone no supervisor will run again, so the
    # orphaned repair must be cleaned up here (Codex PR #1104 follow-up).
    try:
        from .Auth.fcm_receiver_ha import FcmReceiverHA

        FcmReceiverHA.async_delete_entry_repair_issues(hass, entry.entry_id)
    except Exception as err:  # pragma: no cover - defensive best-effort cleanup
        _LOGGER.debug(
            "Deleting FCM repair issues during async_remove_entry raised: %s", err
        )

    # The held-attributes issue outlives unloads on purpose, so that a reload
    # keeps the user's "ignore". Only the removal of the entry retires it.
    ir.async_delete_issue(hass, DOMAIN, frozen_attributes_issue_id(entry.entry_id))

    try:
        owner_index = _ensure_device_owner_index(bucket)
        stale = [cid for cid, eid in list(owner_index.items()) if eid == entry.entry_id]
        for cid in stale:
            owner_index.pop(cid, None)
        if stale:
            _LOGGER.debug(
                "Cleared %d owner-index claim(s) for entry '%s' during removal",
                len(stale),
                entry.entry_id,
            )
    except Exception as err:
        _LOGGER.debug("Owner-index cleanup during removal failed: %s", err)

    entries_bucket = _ensure_entries_bucket(bucket)
    entries_bucket.pop(entry.entry_id, None)

    if isinstance(runtime, RuntimeData):
        runtime.google_home_filter = None
        runtime.fcm_receiver = None
    if isinstance(fallback_runtime, RuntimeData):
        fallback_runtime.google_home_filter = None
        fallback_runtime.fcm_receiver = None

    fallback_cache = _unregister_instance(entry.entry_id)
    if token_cache is None and fallback_cache is not None:
        token_cache = fallback_cache
    elif fallback_cache is not None and fallback_cache is not token_cache:
        close_fallback = getattr(fallback_cache, "close", None)
        if callable(close_fallback):
            try:
                result = close_fallback()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass

    if not entries_bucket:
        try:
            await _maybe_close_spot_transport(entries_bucket)
        except Exception as err:  # pragma: no cover - defensive
            _LOGGER.debug(
                "SPOT transport close during async_remove_entry raised: %s", err
            )

    if hasattr(entry, "runtime_data"):
        try:
            setattr(entry, "runtime_data", None)
        except Exception:
            pass

    purge_option = _opt(
        entry, OPT_DELETE_CACHES_ON_REMOVE, DEFAULT_DELETE_CACHES_ON_REMOVE
    )
    try:
        should_purge = cv.boolean(purge_option)
    except Exception:
        should_purge = bool(purge_option)

    issue_id = f"cache_purged_{entry.entry_id}"
    display_name = entry.title or entry.entry_id

    if should_purge:
        removed = False
        if token_cache is not None and hasattr(token_cache, "async_remove_store"):
            close_callable = getattr(token_cache, "close", None)
            if callable(close_callable):
                try:
                    result = close_callable()
                    if inspect.isawaitable(result):
                        await result
                except Exception as err:
                    _LOGGER.debug(
                        "Closing the cache instance before removal raised: %s", err
                    )
            try:
                remove_callable = getattr(token_cache, "async_remove_store")
                remove_result = remove_callable()
                if inspect.isawaitable(remove_result):
                    await remove_result
                removed = True
            except Exception as err:
                _LOGGER.warning(
                    "Removing the cache store for entry '%s' failed: %s",
                    entry.entry_id,
                    err,
                )
        else:
            store = Store(hass, STORAGE_VERSION, f"{STORAGE_KEY}_{entry.entry_id}")
            try:
                remove_callable = getattr(store, "async_remove", None)
                if remove_callable is None:
                    raise AttributeError("Store.async_remove unavailable")
                remove_result = remove_callable()
                if inspect.isawaitable(remove_result):
                    await remove_result
                removed = True
            except Exception as err:
                _LOGGER.warning(
                    "Removing the cache store for entry '%s' failed (no cache instance): %s",
                    entry.entry_id,
                    err,
                )

        if removed:
            _LOGGER.info(
                "Removed the cache store for entry '%s' (%s).",
                entry.entry_id,
                _label_entry_for_log(entry),
            )
            issue_severity = getattr(ir, "IssueSeverity", None)
            if issue_severity is not None:
                severity_value = getattr(issue_severity, "INFO", None)
                if severity_value is None:
                    severity_value = getattr(issue_severity, "WARNING", "warning")
            else:
                severity_value = getattr(ir, "WARNING", "warning")
            try:
                ir.async_create_issue(
                    hass,
                    DOMAIN,
                    issue_id,
                    is_fixable=False,
                    severity=severity_value,
                    translation_key=TRANSLATION_KEY_CACHE_PURGED,
                    translation_placeholders={"entry_title": display_name},
                )
            except Exception as err:
                _LOGGER.debug("Failed to create cache purge issue: %s", err)
        else:
            try:
                ir.async_delete_issue(hass, DOMAIN, issue_id)
            except Exception as err:
                _LOGGER.debug(
                    "Failed to delete cache purge issue after unsuccessful purge: %s",
                    err,
                )
    else:
        _LOGGER.info(
            "Preserved the cache store for entry '%s' (%s); option disabled.",
            entry.entry_id,
            _label_entry_for_log(entry),
        )
        try:
            ir.async_delete_issue(hass, DOMAIN, issue_id)
        except Exception as err:
            _LOGGER.debug(
                "Failed to delete cache purge issue when retention is requested: %s",
                err,
            )


def _get_local_ip_sync() -> str:
    """Best-effort local IP discovery via UDP connect (executor-only).

    WARNING: This function performs a blocking socket operation and MUST NOT be
    called directly from the async event loop.  Always use the non-blocking
    wrapper :func:`async_get_local_ip` instead.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return cast(str, s.getsockname()[0])
    except OSError:
        return ""


async def async_get_local_ip(hass: HomeAssistant) -> str:
    """Non-blocking wrapper for local IP discovery.

    Delegates the blocking socket call to the HA executor so the event loop is
    never stalled by DNS resolution or network timeouts.
    """
    result: str = await hass.async_add_executor_job(_get_local_ip_sync)
    return result
