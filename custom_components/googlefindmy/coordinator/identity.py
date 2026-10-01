"""Identity operations for GoogleFindMyCoordinator.

This module contains identity-related methods extracted from main.py.

Methods moved here:
- _get_account_email: Get configured Google account email
- _create_auth_issue: Create Repairs issue for auth problems
- _dismiss_auth_issue: Dismiss auth Repairs issue
- _schedule_eid_resolver_refresh: Refresh the global EID resolver
- _register_identity_key: Register device identity key for shared tracker detection
- _reset_resolver_offset: Clear resolver offsets when identity keys rotate
- get_active_device_identities: Return identity keys for enabled, non-ignored devices
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import issue_registry as ir

from ..const import (
    _EID_REFRESH_DEBOUNCE_S,
    CONF_GOOGLE_EMAIL,
    DATA_EID_RESOLVER,
    DOMAIN,
    ISSUE_AUTH_EXPIRED_KEY,
    issue_id_for,
)
from ..device_registry_helpers import async_get_device_by_identifiers_compat
from ..KeyBackup.cloud_key_decryptor import decrypt_eik
from ._mixin_typing import _MixinBase
from .helpers.identity import (
    extract_pair_date as _extract_pair_date_impl,
)
from .helpers.identity import (
    extract_secrets_creation_date as _extract_secrets_creation_date_impl,
)
from .helpers.identity import (
    extract_time_anchors_debug as _extract_time_anchors_debug_impl,
)
from .helpers.identity import (
    lookup_prio as _lookup_prio_impl,
)
from .helpers.identity import (
    lookup_prio_with_source as _lookup_prio_with_source_impl,
)
from .helpers.identity import (
    normalize_device_type as _normalize_device_type_impl,
)
from .helpers.identity import (
    normalize_fast_pair_model_id as _normalize_fast_pair_model_id_impl,
)
from .helpers.identity import (
    store_if_value as _store_if_value_impl,
)
from .helpers.subentry import normalize_epoch_seconds

if TYPE_CHECKING:
    from .main import DeviceIdentity

_LOGGER = logging.getLogger(__name__)


class IdentityOperations(_MixinBase):
    """Identity operations mixin for GoogleFindMyCoordinator.

    This class contains methods that manage device identities,
    including identity key registration and account information.
    """

    def _get_account_email(self) -> str:
        """Return the configured Google account email for this entry (empty if unknown)."""
        entry = self.config_entry
        if entry is not None:
            email_value = entry.data.get(CONF_GOOGLE_EMAIL)
            if isinstance(email_value, str):
                return email_value
        return ""

    def _create_auth_issue(self) -> None:
        """Create (idempotent) a Repairs issue for an authentication problem.

        Uses:
            - domain: `googlefindmy`
            - issue_id: stable per-entry (via `issue_id_for(entry_id)`)
            - translation_key: `ISSUE_AUTH_EXPIRED_KEY` (localizable title/description)
            - placeholders: `email` (shown in repairs UI)
        """
        entry = self.config_entry
        if not entry:
            return
        issue_id = issue_id_for(entry.entry_id)
        email = self._get_account_email() or "unknown"
        try:
            ir.async_create_issue(
                self.hass,
                DOMAIN,
                issue_id,
                is_fixable=False,
                severity=ir.IssueSeverity.ERROR,
                translation_key=ISSUE_AUTH_EXPIRED_KEY,
                translation_placeholders={
                    "email": email,
                    "entry_title": entry.title or entry.entry_id,
                },
            )
        except Exception as err:
            _LOGGER.debug("Failed to create Repairs issue: %s", err)

    def _dismiss_auth_issue(self) -> bool:
        """Dismiss (idempotently) the Repairs issue if present.

        Returns True when an issue existed and was removed, False otherwise.
        """
        entry = self.config_entry
        if not entry:
            return False

        issue_id = issue_id_for(entry.entry_id)

        issue_present = False
        try:
            registry = ir.async_get(self.hass)
        except Exception:  # pragma: no cover - defensive fallback
            registry = None

        if registry and hasattr(registry, "async_get_issue"):
            try:
                issue_present = registry.async_get_issue(DOMAIN, issue_id) is not None
            except Exception:  # pragma: no cover - defensive fallback
                issue_present = False

        try:
            ir.async_delete_issue(self.hass, DOMAIN, issue_id)
        except Exception:
            # Deleting a non-existent issue is fine; keep silent.
            return False

        return issue_present

    def _schedule_eid_resolver_refresh(self) -> None:
        """Refresh the global EID resolver when active device sets change.

        AP-B2 trigger-task debounce: the four read-only call sites that drive
        this helper can fire back-to-back during an FCM burst. Without
        coalescing, each call spawns its own ``async_create_task`` even though
        the resolver only needs one rebuild. We hold a single pending timer
        handle per coordinator instance: the first trigger arms a
        ``loop.call_later`` timer, and any further trigger within
        ``_EID_REFRESH_DEBOUNCE_S`` is dropped (no second task). When the timer
        fires the handle is cleared *before* the task is created, so a trigger
        arriving after the window still arms a fresh timer and produces a new
        task (no swallowed window-change). The rebuild-vs-skip decision stays in
        the AP-B1 skip guard, so debouncing the trigger never drops a real
        change.
        """

        hass = getattr(self, "hass", None)
        hass_data = getattr(hass, "data", None)
        if not isinstance(hass_data, dict):
            return

        bucket = hass_data.get(DOMAIN)
        if not isinstance(bucket, dict):
            return

        resolver = bucket.get(DATA_EID_RESOLVER)
        refresh = getattr(resolver, "async_refresh", None)
        if not callable(refresh):
            return

        create_task = getattr(hass, "async_create_task", None)
        if not callable(create_task):
            return

        def _spawn_refresh_task() -> None:
            """Clear the debounce handle, then create the single refresh task."""
            # Clear first so a trigger after the window arms a fresh timer.
            self._eid_refresh_debounce_handle = None
            create_task(refresh())

        loop = getattr(hass, "loop", None)
        call_later = getattr(loop, "call_later", None)
        if not callable(call_later):
            # No event loop available (e.g. minimal stubs): fall back to the
            # legacy immediate behaviour so a refresh is never lost.
            create_task(refresh())
            return

        # A timer is already pending for this window: coalesce this trigger.
        if getattr(self, "_eid_refresh_debounce_handle", None) is not None:
            return

        self._eid_refresh_debounce_handle = call_later(
            _EID_REFRESH_DEBOUNCE_S, _spawn_refresh_task
        )

    def _register_identity_key(self, device_id: str, identity_key: bytes) -> None:
        """Register a device's identity_key for shared tracker detection.

        Maintains a mapping from identity_key to all device_ids that share the
        same physical tracker. This enables location propagation across accounts.

        Args:
            device_id: Canonical device identifier.
            identity_key: Normalized 32-byte identity key.
        """
        if not isinstance(identity_key, bytes) or len(identity_key) != 32:
            return

        device_set = self._identity_key_to_devices.setdefault(identity_key, set())
        if device_id not in device_set:
            device_set.add(device_id)
            if len(device_set) > 1:
                _LOGGER.info(
                    "Shared tracker detected: identity_key=%s... shared by %d devices: %s",
                    identity_key[:8].hex(),
                    len(device_set),
                    sorted(device_set),
                )

    def _reset_resolver_offset(self, device_id: str) -> None:
        """Clear resolver offsets using registry IDs when identity keys rotate."""

        hass = getattr(self, "hass", None)
        if hass is None:
            return

        registry_id: str | None = None
        entry_id = self._entry_id()

        dev_reg = dr.async_get(hass)
        if entry_id and dev_reg:
            identifiers = {
                (DOMAIN, f"{entry_id}:{device_id}"),
                (DOMAIN, device_id),
            }
            device = async_get_device_by_identifiers_compat(
                dev_reg,
                tuple(identifiers),
                config_entry_id=entry_id,
            )
            if device:
                registry_id = device.id

        if not registry_id:
            _LOGGER.debug(
                "Could not resolve Registry ID for canonical %s; skipping offset reset.",
                device_id,
            )
            return

        hass_data = getattr(hass, "data", None)
        if not isinstance(hass_data, dict):
            return

        bucket = hass_data.get(DOMAIN)
        if not isinstance(bucket, dict):
            return

        resolver = bucket.get(DATA_EID_RESOLVER)
        if resolver is None:
            return

        reset = getattr(resolver, "reset_device_offset", None)
        if callable(reset):
            _LOGGER.debug(
                "Triggering resolver offset reset for %s (Registry ID: %s)",
                device_id,
                registry_id,
            )
            reset(registry_id)

    def get_active_device_identities(
        self,
    ) -> list[DeviceIdentity]:
        """Return identity keys for enabled, non-ignored devices.

        Devices disabled in the device registry or ignored via options are
        excluded from the returned list. Only devices currently eligible for
        polling (tracked in ``_enabled_poll_device_ids``) are considered for
        EID resolution. The returned ``registry_id`` refers to the Home
        Assistant Device Registry identifier for each tracker. Pairing and
        secrets-creation timestamps are forwarded when available to help the
        resolver reason about EID rotation windows, but the integration treats
        them as hypotheses because Google has not documented the server-side
        semantics. Any debug time anchor hints present in cached payloads are
        also forwarded best-effort for diagnostics.
        """
        # Lazy imports to avoid circular dependencies
        from ..Auth.token_cache import TokenCache
        from .main import DeviceIdentity, _update_preserve_metadata

        # Use imported helpers from coordinator_identity.py instead of inline functions
        _normalize_device_type = _normalize_device_type_impl
        _normalize_fast_pair_model_id = _normalize_fast_pair_model_id_impl
        _lookup_prio = _lookup_prio_impl
        _lookup_prio_with_source = _lookup_prio_with_source_impl
        _store_if_value = _store_if_value_impl
        _extract_pair_date = _extract_pair_date_impl
        _extract_secrets_creation_date = _extract_secrets_creation_date_impl
        _extract_time_anchors_debug = _extract_time_anchors_debug_impl

        _expected_identity_key_length = 32

        cached_owner_key: bytes | None = None
        cached_owner_key_version: int | None = None
        owner_key_checked = False

        def _get_cached_owner_key() -> tuple[bytes | None, int | None]:
            """Return the cached owner key when available without I/O."""

            nonlocal cached_owner_key, cached_owner_key_version, owner_key_checked

            if owner_key_checked:
                return cached_owner_key, cached_owner_key_version

            owner_key_checked = True

            cache_obj = getattr(self, "_cache", None)
            if not isinstance(cache_obj, TokenCache):
                return None, None

            cache_snapshot = getattr(cache_obj, "_data", None)
            if not isinstance(cache_snapshot, Mapping):
                return None, None

            username = cache_snapshot.get("username")
            if not isinstance(username, str) or not username:
                return None, None

            owner_entry = cache_snapshot.get(
                f"owner_key_{username}"
            ) or cache_snapshot.get("owner_key")

            raw_key: Any | None
            version: int | None = None
            if isinstance(owner_entry, Mapping):
                raw_key = owner_entry.get("key")
                version_value = owner_entry.get("version")
                version = version_value if isinstance(version_value, int) else None
            else:
                raw_key = owner_entry

            candidate: bytes | None = None
            if isinstance(raw_key, str):
                try:
                    candidate = bytes.fromhex(raw_key.strip())
                except ValueError:
                    candidate = None
            elif isinstance(raw_key, (bytes, bytearray)):
                candidate = bytes(raw_key)

            if candidate is None or len(candidate) != _expected_identity_key_length:
                return None, None

            cached_owner_key = candidate
            cached_owner_key_version = version
            return cached_owner_key, cached_owner_key_version

        def _decrypt_identity_key(
            ciphertext: bytes, canonical_id: str
        ) -> tuple[bytes | None, int | None]:
            """Attempt to decrypt the encrypted identity key using cached material.

            FIX: Differentiate between expected and unexpected decrypt errors (#132).
            Expected errors (key rotation, padding issues) are logged at DEBUG level.
            Unexpected errors are logged at WARNING and recorded in diagnostics.
            """

            owner_key, owner_version = _get_cached_owner_key()
            if owner_key is None:
                return None, None

            try:
                decrypted = decrypt_eik(owner_key, ciphertext)
            except Exception as err:  # noqa: BLE001 - defensive
                err_type = type(err).__name__

                # FIX: Use exception type matching instead of keyword search (#132)
                # Expected exceptions from decrypt_eik and underlying crypto:
                # - ValueError: invalid length, IV problems, key size issues
                # - InvalidTag: AES-GCM authentication failed (wrong key, corrupted data)
                # - InvalidKey: cryptography library key validation failure
                expected_exception_types = ("ValueError", "InvalidTag", "InvalidKey")
                is_expected = err_type in expected_exception_types

                if is_expected:
                    _LOGGER.debug(
                        "Identity key decryption failed for %s (%s): %s",
                        canonical_id,
                        err_type,
                        err,
                    )
                else:
                    # Unexpected error - log at WARNING and record in diagnostics
                    _LOGGER.warning(
                        "Unexpected decryption error for %s: %s (%s)",
                        canonical_id,
                        err_type,
                        err,
                    )
                    # Record in diagnostics buffer for troubleshooting
                    diag = getattr(self, "_diag", None)
                    if diag is not None:
                        diag.add_warning(
                            "decrypt_error",
                            {
                                "device_id": canonical_id,
                                "error_type": err_type,
                                "error_msg": str(err)[:100] if str(err) else "unknown",
                            },
                        )
                return None, None

            if not isinstance(decrypted, (bytes, bytearray)):
                return None, None

            key_bytes = bytes(decrypted)
            if len(key_bytes) != _expected_identity_key_length:
                _LOGGER.warning(
                    "Decrypted identity key for %s has invalid length: %s (expected %s)",
                    canonical_id,
                    len(key_bytes),
                    _expected_identity_key_length,
                )
                return None, None

            _LOGGER.debug("Successfully decrypted identity key for %s", canonical_id)
            return key_bytes, owner_version

        enabled_ids = set(self._enabled_poll_device_ids)
        ignored = self._get_ignored_set()
        entry = self.config_entry
        entry_id = entry.entry_id if entry is not None else None

        # =====================================================================
        # EID DATA SOURCES - Documentation for maintainers
        # =====================================================================
        # The EID resolver requires identity_key, pair_date, and secrets_creation_date
        # to generate Ephemeral Identifiers. These are sourced from (in priority order):
        #
        # 1. cache_* (from _device_location_data):
        #    - Populated by decrypt_locations via FCM push or manual locate
        #    - Contains decrypted identity_key from encryptedUserSecrets
        #    - Most authoritative source for recently-updated devices
        #
        # 2. data_* (from self.data - current API response):
        #    - Populated by _async_update_data polling cycle
        #    - Contains raw API payloads with encrypted_identity_key
        #
        # 3. last_* (from _last_device_list - nbe_list_devices):
        #    - Populated by async_get_basic_device_list()
        #    - Contains device metadata including encrypted_identity_key
        #    - Useful for devices that haven't received FCM updates yet
        #
        # 4. registry_* (DEPRECATED - was never functional):
        #    - Previously attempted to use DeviceRegistry custom_fields
        #    - custom_fields does NOT exist in Home Assistant's DeviceRegistry API
        #    - These dicts remain for API compatibility but are always empty
        #
        # The identity_key is typically obtained via:
        # - decrypt_locations (from FCM/LocateTracker responses)
        # - EID resolver's own decryption of encrypted_identity_key using owner_key
        # =====================================================================

        registry_map: dict[str, tuple[str, bytes | None]] = {}
        # These dicts were intended for DeviceRegistry persistence but custom_fields
        # does not exist in HA's API. They remain empty for backward compatibility.
        registry_pair_dates: dict[str, int] = {}
        registry_secrets_creation_dates: dict[str, int] = {}
        registry_time_anchors_debug: dict[str, Any] = {}

        if entry is not None:
            device_reg = dr.async_get(self.hass)
            extract_identifier = getattr(self, "_extract_our_identifier", None)
            for device in dr.async_entries_for_config_entry(device_reg, entry.entry_id):
                if device.disabled_by is not None:
                    continue

                canonical_id: str | None = None
                if callable(extract_identifier):
                    canonical_id = extract_identifier(device)

                if not canonical_id:
                    _LOGGER.debug(
                        "Device registry entry %s skipped: No canonical identifier",
                        device.id,
                    )
                    continue

                if canonical_id in ignored:
                    continue

                # Map canonical_id -> (device.id, identity_key)
                # Note: identity_key from DeviceRegistry is always None because
                # custom_fields does not exist in HA's DeviceRegistry API.
                # The actual identity_key comes from cache_identities, data_identities,
                # or last_identities (populated from API responses and FCM).
                registry_map[canonical_id] = (device.id, None)

        device_ids_set = {dev_id for dev_id in enabled_ids if dev_id not in ignored}
        device_ids_set.update(registry_map)
        device_ids = sorted(device_ids_set)
        if not device_ids:
            return []

        _LOGGER.debug(
            "Collecting EID identities for %d eligible devices (polling=%d, registry=%d)",
            len(device_ids),
            len(enabled_ids),
            len(registry_map),
        )

        allowed_raw_ids = {did.split(":")[-1] for did in device_ids}
        registry_ids: set[str] = {registry for registry, _ in registry_map.values()}
        allowed_payload_ids = device_ids_set | allowed_raw_ids | registry_ids

        last_device_list = getattr(self, "_last_device_list", None)
        last_identities: dict[str, bytes] = {}
        last_encrypted: dict[str, tuple[bytes | None, int | None]] = {}
        last_device_types: dict[str, int | None] = {}
        last_fast_pair_model_ids: dict[str, str | None] = {}
        last_raw_keys: dict[str, list[str]] = {}
        last_identity_candidates: dict[str, list[bytes]] = {}
        last_pair_dates: dict[str, int] = {}
        last_secrets_creation_dates: dict[str, int] = {}
        last_time_anchors_debug: dict[str, Any] = {}
        last_payloads: dict[str, Mapping[str, Any]] = {}
        if isinstance(last_device_list, Iterable):
            for raw in last_device_list:
                if not isinstance(raw, dict):
                    continue
                dev_id = raw.get("id")
                if not isinstance(dev_id, str):
                    continue
                if dev_id not in allowed_payload_ids:
                    continue

                last_payloads[dev_id] = raw
                last_raw_keys[dev_id] = list(raw.keys())
                parsed = self._normalize_identity_key(
                    raw.get("identity_key") or raw.get("identityKey") or raw.get("eik")
                )
                _store_if_value(last_identities, dev_id, parsed)

                candidate_list = self._normalize_identity_key_candidates(
                    raw.get("identity_key_candidates")
                    or raw.get("identityKeyCandidates")
                )
                if candidate_list:
                    last_identity_candidates[dev_id] = candidate_list

                encrypted_identity = self._normalize_identity_key(
                    raw.get("encrypted_identity_key") or raw.get("encryptedIdentityKey")
                )
                owner_key_version = raw.get("owner_key_version")
                if isinstance(owner_key_version, str) and owner_key_version.isdigit():
                    owner_key_version = int(owner_key_version)
                elif not isinstance(owner_key_version, int):
                    owner_key_version = None

                if encrypted_identity is not None or owner_key_version is not None:
                    last_encrypted[dev_id] = (encrypted_identity, owner_key_version)

                device_type = _normalize_device_type(raw.get("device_type"))
                if device_type is not None:
                    last_device_types[dev_id] = device_type

                fast_pair_model_id = _normalize_fast_pair_model_id(
                    raw.get("fast_pair_model_id") or raw.get("fastPairModelId")
                )
                if fast_pair_model_id is not None:
                    last_fast_pair_model_ids[dev_id] = fast_pair_model_id

                pair_date = _extract_pair_date(raw)
                _store_if_value(last_pair_dates, dev_id, pair_date)

                secrets_creation_date = _extract_secrets_creation_date(raw)
                _store_if_value(
                    last_secrets_creation_dates, dev_id, secrets_creation_date
                )

                anchors_debug = _extract_time_anchors_debug(raw)
                if anchors_debug is not None:
                    last_time_anchors_debug[dev_id] = anchors_debug

        data_identities: dict[str, bytes] = {}
        data_encrypted: dict[str, tuple[bytes | None, int | None]] = {}
        data_device_types: dict[str, int | None] = {}
        data_fast_pair_model_ids: dict[str, str | None] = {}
        raw_data_keys: dict[str, list[str]] = {}
        data_identity_candidates: dict[str, list[bytes]] = {}
        data_pair_dates: dict[str, int] = {}
        data_secrets_creation_dates: dict[str, int] = {}
        data_time_anchors_debug: dict[str, Any] = {}
        data_payloads: dict[str, Mapping[str, Any]] = {}
        device_data = getattr(self, "data", None)
        if isinstance(device_data, Iterable):
            for raw in device_data:
                if not isinstance(raw, dict):
                    continue
                dev_id = raw.get("id")
                if not isinstance(dev_id, str):
                    continue
                if dev_id not in allowed_payload_ids:
                    continue
                data_payloads[dev_id] = raw
                raw_data_keys[dev_id] = list(raw.keys())
                parsed = self._normalize_identity_key(
                    raw.get("identity_key") or raw.get("identityKey") or raw.get("eik")
                )
                _store_if_value(data_identities, dev_id, parsed)

                candidate_list = self._normalize_identity_key_candidates(
                    raw.get("identity_key_candidates")
                    or raw.get("identityKeyCandidates")
                )
                if candidate_list:
                    data_identity_candidates[dev_id] = candidate_list

                encrypted_identity = self._normalize_identity_key(
                    raw.get("encrypted_identity_key") or raw.get("encryptedIdentityKey")
                )
                owner_key_version = raw.get("owner_key_version")
                if isinstance(owner_key_version, str) and owner_key_version.isdigit():
                    owner_key_version = int(owner_key_version)
                elif not isinstance(owner_key_version, int):
                    owner_key_version = None

                if encrypted_identity is not None or owner_key_version is not None:
                    data_encrypted[dev_id] = (encrypted_identity, owner_key_version)

                device_type = _normalize_device_type(raw.get("device_type"))
                if device_type is not None:
                    data_device_types[dev_id] = device_type

                fast_pair_model_id = _normalize_fast_pair_model_id(
                    raw.get("fast_pair_model_id") or raw.get("fastPairModelId")
                )
                if fast_pair_model_id is not None:
                    data_fast_pair_model_ids[dev_id] = fast_pair_model_id

                pair_date = _extract_pair_date(raw)
                _store_if_value(data_pair_dates, dev_id, pair_date)

                secrets_creation_date = _extract_secrets_creation_date(raw)
                _store_if_value(
                    data_secrets_creation_dates, dev_id, secrets_creation_date
                )

                anchors_debug = _extract_time_anchors_debug(raw)
                if anchors_debug is not None:
                    data_time_anchors_debug[dev_id] = anchors_debug

        cache = getattr(self, "_device_location_data", None)
        internal_cache: dict[str, Any] = cache if isinstance(cache, dict) else {}
        cache_identities: dict[str, bytes] = {}
        cache_encrypted: dict[str, tuple[bytes | None, int | None]] = {}
        cache_device_types: dict[str, int | None] = {}
        cache_fast_pair_model_ids: dict[str, str | None] = {}
        cache_data_keys: dict[str, list[str]] = {}
        cache_identity_candidates: dict[str, list[bytes]] = {}
        cache_pair_dates: dict[str, int] = {}
        cache_secrets_creation_dates: dict[str, int] = {}
        cache_time_anchors_debug: dict[str, Any] = {}
        if internal_cache:
            allowed_cache_keys = set(device_ids) | allowed_raw_ids | registry_ids
            for dev_id, payload in internal_cache.items():
                if dev_id not in allowed_cache_keys:
                    continue
                if not isinstance(payload, dict):
                    continue
                cache_data_keys[dev_id] = list(payload.keys())
                parsed = self._normalize_identity_key(
                    payload.get("identity_key")
                    or payload.get("identityKey")
                    or payload.get("eik")
                )
                _store_if_value(cache_identities, dev_id, parsed)

                candidate_list = self._normalize_identity_key_candidates(
                    payload.get("identity_key_candidates")
                    or payload.get("identityKeyCandidates")
                )
                if candidate_list:
                    cache_identity_candidates[dev_id] = candidate_list

                encrypted_identity = self._normalize_identity_key(
                    payload.get("encrypted_identity_key")
                    or payload.get("encryptedIdentityKey")
                )
                owner_key_version = payload.get("owner_key_version")
                if isinstance(owner_key_version, str) and owner_key_version.isdigit():
                    owner_key_version = int(owner_key_version)
                elif not isinstance(owner_key_version, int):
                    owner_key_version = None

                if encrypted_identity is not None or owner_key_version is not None:
                    cache_encrypted[dev_id] = (encrypted_identity, owner_key_version)

                device_type = _normalize_device_type(payload.get("device_type"))
                if device_type is not None:
                    cache_device_types[dev_id] = device_type

                fast_pair_model_id = _normalize_fast_pair_model_id(
                    payload.get("fast_pair_model_id") or payload.get("fastPairModelId")
                )
                if fast_pair_model_id is not None:
                    cache_fast_pair_model_ids[dev_id] = fast_pair_model_id

                pair_date = _extract_pair_date(payload)
                _store_if_value(cache_pair_dates, dev_id, pair_date)

                secrets_creation_date = _extract_secrets_creation_date(payload)
                _store_if_value(
                    cache_secrets_creation_dates, dev_id, secrets_creation_date
                )

                anchors_debug = _extract_time_anchors_debug(payload)
                if anchors_debug is not None:
                    cache_time_anchors_debug[dev_id] = anchors_debug

        identities: list[DeviceIdentity] = []
        for canonical_id in device_ids:
            lookup_id = canonical_id.split(":")[-1]
            registry_entry = registry_map.get(canonical_id)
            if registry_entry is None:
                continue

            registry_id, registry_key = registry_entry

            cache_lookup: Mapping[str, Any] | None = (
                cache if isinstance(cache, Mapping) else None
            )
            cache_keys = (canonical_id, lookup_id, registry_id)
            merged_device_data: dict[str, Any] = {}

            for source in (
                cache_lookup,
                internal_cache,
                last_payloads,
                data_payloads,
            ):
                if not isinstance(source, Mapping):
                    continue
                for key in cache_keys:
                    if key is None or key not in source:
                        continue
                    payload = source.get(key)
                    if isinstance(payload, Mapping):
                        _update_preserve_metadata(merged_device_data, payload)

            _LOGGER.debug(
                "Building Identity for %s: cached_data=%s",
                canonical_id,
                merged_device_data,
            )

            direct_pair_date = _extract_pair_date(merged_device_data)
            direct_secrets_date = _extract_secrets_creation_date(merged_device_data)

            lookup_keys = (canonical_id, lookup_id, registry_id)

            identity_key = _lookup_prio(
                lookup_keys, cache_identities, data_identities, last_identities
            )
            if identity_key is None:
                identity_key = registry_key

            identity_candidates = _lookup_prio(
                lookup_keys,
                cache_identity_candidates,
                data_identity_candidates,
                last_identity_candidates,
            )
            if identity_candidates is None:
                identity_candidates = []

            encrypted_identity_tuple = _lookup_prio(
                lookup_keys, cache_encrypted, data_encrypted, last_encrypted
            )
            encrypted_identity_key, owner_key_version = encrypted_identity_tuple or (
                None,
                None,
            )
            device_type = _lookup_prio(
                lookup_keys, cache_device_types, data_device_types, last_device_types
            )

            fast_pair_model_id = _lookup_prio(
                lookup_keys,
                cache_fast_pair_model_ids,
                data_fast_pair_model_ids,
                last_fast_pair_model_ids,
            )

            pair_date_source: str | None = None
            pair_date_raw, pair_date_source = _lookup_prio_with_source(
                lookup_keys,
                (cache_pair_dates, "cache"),
                (data_pair_dates, "live"),
                (last_pair_dates, "last"),
            )
            if pair_date_raw is None:
                registry_pair_date, registry_pair_source = _lookup_prio_with_source(
                    lookup_keys, (registry_pair_dates, "registry")
                )
                if registry_pair_date is not None:
                    pair_date_raw = registry_pair_date
                    pair_date_source = registry_pair_source
            if pair_date_raw is None:
                pair_date_raw = direct_pair_date
                if pair_date_raw is not None:
                    pair_date_source = "merged"
            pair_date = normalize_epoch_seconds(pair_date_raw)
            if pair_date is None:
                pair_date_source = None

            secrets_creation_source: str | None = None
            secrets_creation_raw, secrets_creation_source = _lookup_prio_with_source(
                lookup_keys,
                (cache_secrets_creation_dates, "cache"),
                (data_secrets_creation_dates, "live"),
                (last_secrets_creation_dates, "last"),
            )
            if secrets_creation_raw is None:
                registry_secrets_date, registry_secrets_source = (
                    _lookup_prio_with_source(
                        lookup_keys, (registry_secrets_creation_dates, "registry")
                    )
                )
                if registry_secrets_date is not None:
                    secrets_creation_raw = registry_secrets_date
                    secrets_creation_source = registry_secrets_source
            if secrets_creation_raw is None:
                secrets_creation_raw = direct_secrets_date
                if secrets_creation_raw is not None:
                    secrets_creation_source = "merged"
            secrets_creation_date = normalize_epoch_seconds(secrets_creation_raw)
            if secrets_creation_date is None:
                secrets_creation_source = None

            # Anchor fallback: use secrets_creation_date as pair_date when pair_date
            # is missing or invalid (0). This is common for Android phones that lack
            # deviceRegistration data but have valid encrypted_user_secrets bundles.
            if (pair_date is None or pair_date <= 0) and (
                secrets_creation_date is not None and secrets_creation_date > 0
            ):
                _LOGGER.debug(
                    "Anchor fallback for %s: pair_date=%s invalid, "
                    "using secrets_creation_date=%s as pair_date",
                    canonical_id,
                    pair_date,
                    secrets_creation_date,
                )
                pair_date = secrets_creation_date
                pair_date_source = f"fallback:{secrets_creation_source or 'secrets'}"

            anchors_debug = _lookup_prio(
                lookup_keys,
                registry_time_anchors_debug,
                cache_time_anchors_debug,
                data_time_anchors_debug,
                last_time_anchors_debug,
            )

            manufacturer = self._normalize_optional_string(
                merged_device_data.get("manufacturer")
            )
            model = self._normalize_optional_string(merged_device_data.get("model"))
            encrypted_account_key = self._normalize_encrypted_blob(
                merged_device_data.get("encrypted_account_key")
                or merged_device_data.get("encryptedAccountKey")
            )
            public_key_address = self._normalize_encrypted_blob(
                merged_device_data.get("public_key_address")
                or merged_device_data.get("encryptedSha256AccountKeyPublicAddress")
            )

            if not identity_candidates and identity_key is not None:
                identity_candidates = [identity_key]
            elif not identity_candidates and registry_key is not None:
                identity_candidates = [registry_key]

            normalized_candidates: list[bytes] = []
            for candidate in identity_candidates:
                normalized_key = self._normalize_identity_key(candidate)
                if (
                    normalized_key is not None
                    and normalized_key not in normalized_candidates
                ):
                    normalized_candidates.append(normalized_key)

            # Normalize identity_key before length validation to handle hex strings
            if identity_key is not None:
                normalized_identity_key = self._normalize_identity_key(identity_key)
                if normalized_identity_key is None:
                    _LOGGER.debug(
                        "Could not normalize identity key for %s",
                        canonical_id,
                    )
                    identity_key = None
                elif len(normalized_identity_key) != _expected_identity_key_length:
                    _LOGGER.debug(
                        "Discarding identity key with invalid length %s for %s",
                        len(normalized_identity_key),
                        canonical_id,
                    )
                    identity_key = None
                else:
                    # Use the normalized bytes version
                    identity_key = normalized_identity_key

            decrypted_owner_version: int | None = None
            if identity_key is None and isinstance(
                encrypted_identity_key, (bytes, bytearray)
            ):
                decrypted_identity_key, decrypted_owner_version = _decrypt_identity_key(
                    encrypted_identity_key, canonical_id
                )
                if decrypted_identity_key is not None:
                    identity_key = decrypted_identity_key
                    if owner_key_version is None:
                        owner_key_version = decrypted_owner_version
                    if not identity_candidates:
                        identity_candidates = [identity_key]

            effective_identity_for_log = identity_key
            if effective_identity_for_log is None and normalized_candidates:
                effective_identity_for_log = normalized_candidates[0]
            has_key = bool(normalized_candidates) or identity_key is not None
            has_key = has_key or encrypted_identity_key is not None
            if not has_key:
                _LOGGER.debug(
                    "Missing crypto material for %s: key=%s (pair=%s). Skipping resolution.",
                    canonical_id,
                    effective_identity_for_log,
                    pair_date,
                )
                continue

            if not normalized_candidates and encrypted_identity_key is None:
                _LOGGER.debug(
                    "Device %s skipped: No identity key found (Data: %s)",
                    canonical_id,
                    last_raw_keys.get(lookup_id)
                    or raw_data_keys.get(lookup_id)
                    or cache_data_keys.get(lookup_id)
                    or [],
                )
                continue

            _LOGGER.debug(
                "Resolving Identity for %s: Anchor=%s (source=%s), PairDate=%s (source=%s)",
                canonical_id,
                secrets_creation_date,
                (secrets_creation_source or "unknown").upper(),
                pair_date,
                (pair_date_source or "unknown").upper(),
            )

            if normalized_candidates:
                for candidate in normalized_candidates:
                    identities.append(
                        DeviceIdentity(
                            registry_id=registry_id,
                            canonical_id=canonical_id,
                            identity_key=candidate,
                            encrypted_identity_key=encrypted_identity_key,
                            owner_key_version=owner_key_version,
                            device_type=device_type,
                            config_entry_id=entry_id,
                            fast_pair_model_id=fast_pair_model_id,
                            manufacturer=manufacturer,
                            model=model,
                            pair_date=pair_date,
                            secrets_creation_date=secrets_creation_date,
                            encrypted_account_key=encrypted_account_key,
                            public_key_address=public_key_address,
                            time_anchors_debug=anchors_debug,
                        )
                    )
            else:
                identities.append(
                    DeviceIdentity(
                        registry_id=registry_id,
                        canonical_id=canonical_id,
                        identity_key=identity_key,
                        encrypted_identity_key=encrypted_identity_key,
                        owner_key_version=owner_key_version,
                        device_type=device_type,
                        config_entry_id=entry_id,
                        fast_pair_model_id=fast_pair_model_id,
                        manufacturer=manufacturer,
                        model=model,
                        pair_date=pair_date,
                        secrets_creation_date=secrets_creation_date,
                        encrypted_account_key=encrypted_account_key,
                        public_key_address=public_key_address,
                        time_anchors_debug=anchors_debug,
                    )
                )

        _LOGGER.debug("Returning %d identities to resolver", len(identities))
        return identities
