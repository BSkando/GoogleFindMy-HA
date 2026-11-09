"""Typing-only base class for coordinator mixin modules.

This module defines ``_MixinBase`` — a class whose **sole purpose** is to provide
attribute and method type declarations so that mypy has visibility into the full
``GoogleFindMyCoordinator`` interface when type-checking mixin classes
(``RegistryOperations``, ``SubentryOperations``, etc.).

At **runtime** the class is essentially empty:
- Attribute annotations (without assignment) are stored in ``__annotations__``
  but create no instance state.
- Method stubs raise ``NotImplementedError``; they are overridden by the real
  implementations provided by the mixin classes or the main coordinator,
  which precede ``_MixinBase`` in the MRO.
- Methods from ``DataUpdateCoordinator`` are guarded by ``TYPE_CHECKING``
  because ``_MixinBase`` precedes ``DataUpdateCoordinator`` in the MRO and
  concrete stubs would shadow the real implementations at runtime.

Why this is needed:
- The mixin pattern relies on each Operations class being composed into the
  final ``GoogleFindMyCoordinator`` via multiple inheritance.
- Without explicit ``self: GoogleFindMyCoordinator`` annotations (which mypy
  rejects because the coordinator is a *sub*type, not a *super*type of each
  mixin), mypy cannot see attributes and methods defined on sibling mixins
  or on the ``DataUpdateCoordinator`` base.
- This class bridges the gap by declaring the union of all relevant attributes
  so that ``self.hass``, ``self.config_entry``, cross-mixin method calls, etc.
  resolve correctly during static analysis.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers import device_registry as dr

    from .._reauth_reason import ReauthReason, ReauthReasonCode
    from ..api import GoogleFindMyAPI
    from .subentry import SubentryMetadata


class _MixinBase:
    """Type-declaration-only base for coordinator mixin classes.

    All mixins (``RegistryOperations``, ``SubentryOperations``, …) inherit
    from this class to gain visibility into the full coordinator interface
    during mypy analysis.  At runtime the stubs below are immediately
    shadowed by the real implementations in the composed class hierarchy.
    """

    # ------------------------------------------------------------------
    # Attributes from DataUpdateCoordinator / HomeAssistant
    # ------------------------------------------------------------------
    hass: HomeAssistant
    config_entry: ConfigEntry | None
    data: list[dict[str, Any]]

    # ------------------------------------------------------------------
    # Attributes from GoogleFindMyCoordinator.__init__
    # ------------------------------------------------------------------
    api: GoogleFindMyAPI
    location_poll_interval: int
    device_poll_delay: int
    min_poll_interval: int
    allow_history_fallback: bool

    # Internal caches
    _device_location_data: dict[str, dict[str, Any]]
    _device_last_good_location: dict[str, dict[str, Any]]
    _round_trip_anchors: dict[str, dict[str, Any]]
    _device_caps: dict[str, dict[str, Any]]
    _present_last_seen: dict[str, float]
    # Subentry ids already reported as holding an allow-list that selects
    # nothing. Unlike its neighbours here this one is created lazily on first
    # use rather than in ``__init__``, because it is a diagnostic ledger with
    # no meaningful empty-state behaviour; declared all the same, so the union
    # this module exists to provide stays complete.
    _allow_list_reinterpretations: set[str | None]
    _poll_lock: asyncio.Lock
    _push_cooldown_until: float
    _locate_inflight: set[str]
    _locate_cooldown_until: dict[str, float]
    _locate_requested_mono: dict[str, float]
    _device_action_locks: dict[str, asyncio.Lock]
    _sound_request_uuids: dict[str, str]
    _sound_play_accepted_at: dict[str, tuple[str, float]]
    _device_poll_cooldown_until: dict[str, float]
    _enabled_poll_device_ids: set[str]
    _devices_with_entry: set[str]
    _identity_key_to_devices: dict[bytes, set[str]]
    _subentry_metadata: dict[str, SubentryMetadata]
    _default_subentry_key_value: str

    # AP-B2: pending debounce timer handles for EID-resolver refresh triggers.
    # One per trigger site (central helper in identity.py, inline path in
    # cache.py) so each site coalesces its own burst independently.
    _eid_refresh_debounce_handle: asyncio.TimerHandle | None
    _eid_inline_refresh_debounce_handle: asyncio.TimerHandle | None

    # Polling state
    _consecutive_timeouts: int
    _last_poll_result: str | None
    _empty_location_streak: dict[str, int]
    _last_device_list: list[dict[str, Any]]
    _empty_list_streak: int
    _stale_device_misses: dict[str, int]
    _stale_removed_google_ids: set[str]
    _stale_name_baseline: dict[str, Mapping[tuple[str, str], int]]
    _stale_removal_attempts: dict[str, int]
    _stale_removal_given_up: set[str]
    _last_list_poll_mono: float
    _last_nonempty_wall: float
    _force_device_list_refresh: bool
    _fcm_defer_started_mono: float
    _consecutive_transient_auth_failures: int
    _last_transient_auth_error: str | None
    # Structured, redaction-safe record of the most recent reauth trigger (FIX 3).
    _reauth_reason: ReauthReason | None
    _consecutive_decrypt_failures: int
    _last_decrypt_reauth_monotonic: float | None
    _last_decrypt_error: str | None
    _monotonic: Callable[[], float]

    # Diagnostics / statistics
    stats: dict[str, int]
    performance_metrics: dict[str, float]
    _propagating_location: bool
    # Stats persistence. The load runs as a task created in the constructor, and the
    # unload waits for it before flushing, so the handle has to be visible here too.
    # The three flags around it travel with the same record: the load window, the
    # deferral it hands to the load's `finally`, and the generation a reset starts.
    _stats_load_task: asyncio.Task[None] | None
    _stats_save_task: asyncio.Task[None] | None
    _stats_loaded: bool
    _save_after_stats_load: bool
    _stats_epoch: int

    # Service device tracking
    _service_device_ready: bool
    _service_device_id: str | None

    # ------------------------------------------------------------------
    # Methods from DataUpdateCoordinator
    # ------------------------------------------------------------------
    # NOTE: async_set_updated_data, async_request_refresh, and
    # async_set_update_error must NOT be defined here at runtime.
    # _MixinBase precedes DataUpdateCoordinator in the MRO, so any
    # concrete stub here would shadow the real implementations and
    # raise NotImplementedError at runtime.  We guard them with
    # TYPE_CHECKING so mypy can still see the signatures.
    if TYPE_CHECKING:

        def async_set_updated_data(self, data: list[dict[str, Any]]) -> None: ...

        async def async_request_refresh(self) -> None: ...

        def async_set_update_error(self, error: Exception) -> None: ...

    # ------------------------------------------------------------------
    # Methods from GoogleFindMyCoordinator (main.py)
    # ------------------------------------------------------------------
    def increment_stat(self, stat_name: str) -> None:
        raise NotImplementedError

    def _refresh_canonicless_drop_stats(self, entry_id: str | None) -> None:
        raise NotImplementedError

    def push_updated(
        self,
        device_ids: list[str] | None = None,
        *,
        reset_baseline: bool = True,
    ) -> None:
        raise NotImplementedError

    def get_device_display_name(self, device_id: str) -> str | None:
        raise NotImplementedError

    def note_error(
        self, exc: Exception, *, where: str = "", device: str | None = None
    ) -> None:
        raise NotImplementedError

    def record_reauth_reason(
        self,
        code: ReauthReasonCode,
        origin: str,
        *,
        counters: Mapping[str, int] | None = None,
    ) -> None:
        raise NotImplementedError

    def safe_update_metric(self, key: str, value: float) -> None:
        raise NotImplementedError

    def get_metric(self, key: str) -> float | None:
        raise NotImplementedError

    def is_ignored(self, device_id: str) -> bool:
        raise NotImplementedError

    def _get_ignored_set(self) -> set[str]:
        raise NotImplementedError

    def _get_google_home_filter(self) -> Any:
        raise NotImplementedError

    def _set_auth_state(self, *, failed: bool, reason: str | None = None) -> None:
        raise NotImplementedError

    def note_decrypt_failure(
        self, *, stale: bool, error: Exception, device: str | None = None
    ) -> bool:
        raise NotImplementedError

    def note_decrypt_success(self) -> None:
        raise NotImplementedError

    def note_background_decrypt_success(self) -> None:
        raise NotImplementedError

    def _short_error_message(self, exc: Exception | str) -> str:
        raise NotImplementedError

    def _get_duration(self, start_key: str, end_key: str) -> float | None:
        raise NotImplementedError

    def _record_semantic_label(
        self, payload: Mapping[str, Any], *, device_id: str | None = None
    ) -> None:
        raise NotImplementedError

    def _apply_semantic_mapping(self, payload: dict[str, Any]) -> bool:
        raise NotImplementedError

    def _should_preserve_precise_home_coordinates(
        self,
        prev_location: Mapping[str, Any] | None,
        replacement_attrs: Mapping[str, Any],
    ) -> bool:
        raise NotImplementedError

    async def _async_save_sound_uuids(self) -> None:
        raise NotImplementedError

    async def _async_build_device_snapshot_with_fallbacks(
        self, devices: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _build_snapshot_from_cache(
        self, devices: list[dict[str, Any]], wall_now: float
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def is_device_present(self, device_id: str) -> bool:
        raise NotImplementedError

    def get_device_last_seen(self, device_id: str) -> datetime | None:
        raise NotImplementedError

    def _api_push_ready(self) -> bool:
        raise NotImplementedError

    # Static methods exposed as instance methods in mixins
    @staticmethod
    def _normalize_identity_key(raw: object) -> bytes | None:
        raise NotImplementedError

    @staticmethod
    def _normalize_identity_key_candidates(raw: object) -> list[bytes]:
        raise NotImplementedError

    @staticmethod
    def _normalize_optional_string(raw: object) -> str | None:
        raise NotImplementedError

    @staticmethod
    def _normalize_encrypted_blob(raw: object) -> bytes | None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Cross-mixin methods: RegistryOperations
    # ------------------------------------------------------------------
    def _entry_id(self) -> str | None:
        raise NotImplementedError

    def _config_entry_exists(self, entry_id: str | None = None) -> bool:
        raise NotImplementedError

    def _reindex_poll_targets_from_device_registry(self) -> None:
        raise NotImplementedError

    def _extract_our_identifier(self, device: dr.DeviceEntry) -> str | None:
        raise NotImplementedError

    def _ensure_service_device_exists(self, entry: ConfigEntry | None = None) -> None:
        raise NotImplementedError

    def _ensure_device_name_cache(self) -> dict[str, str]:
        raise NotImplementedError

    def _ensure_registry_for_devices(
        self,
        devices: list[dict[str, Any]],
        ignored: set[str],
    ) -> int:
        raise NotImplementedError

    def _sync_owner_index(self, devices: list[dict[str, Any]] | None) -> None:
        raise NotImplementedError

    def _remove_stale_devices(
        self,
        fresh_list_ids: set[str],
        *,
        dropped_candidates: int,
        canonicless_warn: int,
        canonicless_names: Mapping[tuple[str, str], int] | None = None,
    ) -> int:
        raise NotImplementedError

    def purge_device(self, device_id: str) -> None:
        raise NotImplementedError

    def _find_tracker_entity_entry(self, device_id: str) -> Any:
        raise NotImplementedError

    def _redact_text(self, text: str | None) -> str:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Cross-mixin methods: SubentryOperations
    # ------------------------------------------------------------------
    def _refresh_subentry_index(
        self,
        visible_devices: Sequence[Mapping[str, Any]] | None = None,
        *,
        skip_manager_update: bool = False,
        skip_repair: bool = False,
    ) -> None:
        raise NotImplementedError

    def _store_subentry_snapshots(self, snapshot: Sequence[Mapping[str, Any]]) -> None:
        raise NotImplementedError

    def get_subentry_snapshot(
        self,
        key: str | None = None,
        *,
        feature: str | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_device_label_in_subentry(
        self, subentry_key: str | None, device_id: str
    ) -> str | None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Cross-mixin methods: PollingOperations
    # ------------------------------------------------------------------
    def _is_on_hass_loop(self) -> bool:
        raise NotImplementedError

    def _run_on_hass_loop(
        self, func: Callable[..., None], *args: Any, **kwargs: Any
    ) -> None:
        raise NotImplementedError

    def _apply_report_type_cooldown(
        self, device_id: str, report_hint: str | None
    ) -> None:
        raise NotImplementedError

    def _note_push_transport_problem(self, cooldown_s: int = 90) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Cross-mixin methods: IdentityOperations
    # ------------------------------------------------------------------
    def _schedule_eid_resolver_refresh(self) -> None:
        raise NotImplementedError

    def _register_identity_key(self, device_id: str, identity_key: bytes) -> None:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Cross-mixin methods: LocateOperations
    # ------------------------------------------------------------------
    def _normalize_coords(
        self,
        payload: dict[str, Any],
        *,
        device_label: str | None = None,
        warn_on_invalid: bool = True,
    ) -> bool:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Cross-mixin methods: CacheOperations
    # ------------------------------------------------------------------
    def get_device_location_data(self, device_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def get_display_location_data(self, device_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def update_device_cache(
        self,
        device_id: str,
        payload: dict[str, Any],
        *,
        source: str = "poll",
    ) -> None:
        raise NotImplementedError

    def _apply_weighted_location_fusion(
        self,
        device_id: str,
        new_data: dict[str, Any],
    ) -> bool:
        raise NotImplementedError

    def count_accuracy_class(self, row: Mapping[str, Any]) -> None:
        """Tally the reported accuracy class of an incoming fix (#216).

        Declared here because the poll loop and the manual locate call it
        alongside ``_apply_weighted_location_fusion`` above; the implementation
        lives in ``CacheOperations``.
        """
        raise NotImplementedError

    def _merge_with_existing_cache_row(
        self,
        device_id: str,
        new_row: dict[str, Any],
    ) -> dict[str, Any]:
        raise NotImplementedError

    def _expire_coarse_fix(
        self,
        device_id: str,
        committed: Mapping[str, Any],
    ) -> None:
        raise NotImplementedError

    def is_replayed_report(
        self,
        device_id: str,
        row: Mapping[str, Any],
    ) -> bool:
        raise NotImplementedError

    def claim_report_for_tally(
        self,
        device_id: str,
        row: Mapping[str, Any],
    ) -> bool:
        raise NotImplementedError

    def _persist_anchor_metadata(
        self,
        device_id: str,
        payload: dict[str, Any],
        *,
        clear_metadata_only: bool = False,
    ) -> None:
        raise NotImplementedError

    def seed_device_last_seen(self, device_id: str, ts: float) -> None:
        raise NotImplementedError

    def prime_device_location_cache(self, device_id: str, data: dict[str, Any]) -> None:
        raise NotImplementedError
