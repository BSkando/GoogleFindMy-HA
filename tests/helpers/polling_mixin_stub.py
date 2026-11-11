# tests/helpers/polling_mixin_stub.py
"""Test stub for :class:`PollingOperations` mixin standalone unit tests.

PR 1.A.3 (Phase 2, AP-B): exercise the simple polling mixin methods
(``_set_api_status``, ``_set_fcm_status``, ``api_status``, ``fcm_status``,
``is_fcm_connected``, ``consecutive_timeouts``, ``last_poll_result``,
``_is_on_hass_loop``, ``_run_on_hass_loop``, ``_compute_type_cooldown_seconds``,
``_apply_report_type_cooldown``, ``is_polling``,
``get_fcm_acquire_duration_seconds``, ``get_last_poll_duration_seconds``,
``_clear_fcm_deferral``, ``_nudge_fcm_supervisor``,
``_get_predicted_poll_time``, ``_note_push_transport_problem``,
``force_poll_due``) without the full coordinator. ``_MixinBase`` is
runtime-empty, so a minimal subclass seeds every attribute the mixin reads
and pre-mocks cross-mixin methods (``_entry_id``, ``get_metric``,
``_get_duration``, ``async_set_updated_data``,
``_reindex_poll_targets_from_device_registry``, and the two sets the
prediction skips, ``_get_ignored_set`` and
``_get_automatic_polling_disabled_set``) to dodge the
``NotImplementedError`` traps that come from ``_MixinBase``.

The complex async methods (``_async_update_data``,
``_async_start_poll_cycle``, ``_handle_dr_event``,
``_dispatch_async_request_refresh``, ``_schedule_short_retry``,
``_is_fcm_ready_soft``, ``_note_fcm_deferral``) are intentionally out of
scope here; they land in a later AP.
"""

from __future__ import annotations

from collections import deque
from typing import Any
from unittest.mock import MagicMock

from custom_components.googlefindmy.coordinator.helpers.stats import (
    ApiStatus,
    CryptoStatus,
    FcmStatus,
)
from custom_components.googlefindmy.coordinator.polling import PollingOperations


def make_hass_stub(*, loop: Any | None = None) -> MagicMock:
    """Return a Home Assistant stub with ``data``, ``loop`` and task helpers.

    ``loop`` defaults to a fresh :class:`MagicMock` so equality checks against
    the running loop default to ``False``. Tests that want the "on-loop"
    branch can pass ``asyncio.get_event_loop()`` (or the running loop).
    """

    hass = MagicMock(spec_set=["data", "loop", "async_create_task"])
    hass.data = {}
    hass.loop = loop if loop is not None else MagicMock()
    hass.async_create_task = MagicMock(return_value=None)
    return hass


class PollingStub(PollingOperations):
    """Minimal :class:`PollingOperations` subclass for isolated mixin tests.

    Cross-mixin ``_MixinBase`` stubs (``_entry_id``, ``get_metric``,
    ``_get_duration``, ``async_set_updated_data``,
    ``_reindex_poll_targets_from_device_registry``) are seeded as
    ``MagicMock`` so methods under test can call them transparently; tests
    rebind per-case via :func:`setattr`. No ``super().__init__`` chain runs
    because ``DataUpdateCoordinator.__init__`` would require a full HA
    runtime.
    """

    def __init__(
        self,
        hass: Any | None = None,
        config_entry: Any | None = None,
        *,
        api: Any | None = None,
        location_poll_interval: int = 120,
        min_poll_interval: int = 60,
    ) -> None:
        self.hass = hass if hass is not None else make_hass_stub()
        self.config_entry = config_entry
        self.data: list[dict[str, Any]] = []

        # Polling-state attributes (declared on _MixinBase, populated here).
        self._api_status_state: str = ApiStatus.UNKNOWN
        self._api_status_reason: str | None = None
        self._api_status_changed_at: float | None = None
        self._fcm_status_state: str = FcmStatus.UNKNOWN
        self._fcm_status_reason: str | None = None
        self._fcm_status_changed_at: float | None = None
        self._crypto_status_state: str = CryptoStatus.UNKNOWN
        self._crypto_status_reason: str | None = None
        self._crypto_status_changed_at: float | None = None

        self._consecutive_timeouts: int = 0
        self._last_poll_result: str | None = None
        self._is_polling: bool = False

        self._fcm_defer_started_mono: float = 0.0
        self._fcm_last_stage: int = 0
        self._degraded_mode_warned: bool = False

        self._device_poll_cooldown_until: dict[str, float] = {}
        self._short_retry_cancel = None
        self._push_cooldown_until: float = 0.0
        self._push_ready_memo: bool | None = None

        self._last_poll_mono: float = 0.0
        self._device_update_history: dict[str, deque[float]] = {}

        # Cross-mixin scalars used by helpers.
        self.location_poll_interval = location_poll_interval
        self.min_poll_interval = min_poll_interval
        self.api = api if api is not None else MagicMock()

        # Cross-mixin methods owned by other mixins or DataUpdateCoordinator:
        # mock them so default behavior is a no-op. Tests override per-case.
        entry_id = getattr(config_entry, "entry_id", None) if config_entry else None
        self._entry_id = MagicMock(return_value=entry_id)
        self.get_metric = MagicMock(return_value=None)
        self._get_duration = MagicMock(return_value=None)
        self.async_set_updated_data = MagicMock(return_value=None)
        self._reindex_poll_targets_from_device_registry = MagicMock(return_value=None)
        # Owned by the main coordinator; read by ``_get_predicted_poll_time``.
        self._get_ignored_set = MagicMock(return_value=set())
        self._get_automatic_polling_disabled_set = MagicMock(return_value=set())


__all__ = ["PollingStub", "make_hass_stub"]
