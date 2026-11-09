# custom_components/googlefindmy/fmdn_finder/ble_scanner.py
"""Optional HA-Bluetooth FMDN advertisement listener.

Registers a callback on Home Assistant's built-in Bluetooth scanner to capture
FMDN advertisements directly, without requiring Bermuda.  This provides:

- **BLE MAC address collection** for future GATT ring connections (Phase 2)
- **RSSI capture** for proximity estimation
- **Frame-type detection** (0x40 normal / 0x41 UTP separated state)

The callback piggybacks on HA's existing scanner — no additional BLE scanning
overhead is introduced.  If the ``bluetooth`` integration is not loaded, setup
is silently skipped (``after_dependencies`` ensures correct load order).

All data is fed into the existing EID Resolver via ``resolve_eid()`` with the
``ble_address`` kwarg, populating ``BLEScanInfo`` for each resolved device.

This module is independent of the FMDN Finder (location upload) feature and
can be enabled even when ``FEATURE_FMDN_FINDER_ENABLED`` is False.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from ..const import DATA_EID_RESOLVER, DOMAIN
from ..eid_resolver import FMDN_FRAME_TYPE, MODERN_FRAME_TYPE
from .location_uploader import _mask_address_for_logs

if TYPE_CHECKING:
    from homeassistant.core import CALLBACK_TYPE, HomeAssistant

_LOGGER = logging.getLogger(__name__)

# Eddystone service UUID used by FMDN advertisements.
# Standard 16-bit UUID 0xFEAA expanded to 128-bit form as used by HA Bluetooth.
FEAA_SERVICE_UUID = "0000feaa-0000-1000-8000-00805f9b34fb"

# Google Fast Pair service UUID (some FMDN trackers advertise under this).
FE2C_SERVICE_UUID = "0000fe2c-0000-1000-8000-00805f9b34fb"

# Minimum payload length: 1 byte frame type + 20 bytes legacy EID.
MIN_FMDN_PAYLOAD_LENGTH = 21

# Rate-limit DEBUG logs for unresolved EIDs (seconds).
_UNRESOLVED_LOG_INTERVAL = 300.0

# Storage key in hass.data[DOMAIN] for the unsubscribe callback.
DATA_BLE_SCANNER_UNSUB = "ble_scanner_unsub"


def _is_fmdn_service_data(
    service_data: dict[str, bytes],
) -> tuple[bytes | None, str | None]:
    """Extract FMDN payload from BLE service data, if present.

    Returns (payload, service_uuid) or (None, None).
    """
    for uuid in (FEAA_SERVICE_UUID, FE2C_SERVICE_UUID):
        data = service_data.get(uuid)
        if data is not None and len(data) >= MIN_FMDN_PAYLOAD_LENGTH:
            return bytes(data), uuid
    return None, None


async def async_setup_ble_scanner(
    hass: HomeAssistant, *, replace_existing: bool = True
) -> bool:
    """Register HA-Bluetooth callback for FMDN advertisements.

    One callback serves every config entry of the domain: it reads the shared
    resolver on each advertisement. When a callback is already registered,
    the default replaces it (register the new one, then unregister the old
    one, with no await in between), so exactly one stays active and Home
    Assistant replays the advertisement history for the entry that just
    loaded; HA replays it only inside ``async_register_callback``. If that
    registration raises, the old callback stays registered. With
    ``replace_existing=False`` an existing registration is kept as it is.

    Returns True if the scanner is registered, False if the bluetooth
    integration is not available (non-fatal).
    """
    try:
        from homeassistant.components.bluetooth import (  # noqa: PLC0415
            BluetoothChange,
            BluetoothScanningMode,
            BluetoothServiceInfoBleak,
            async_register_callback,
        )
    except ImportError:
        _LOGGER.debug(
            "HA Bluetooth integration not available — "
            "FMDN BLE scanner disabled (install bluetooth integration for "
            "BLE MAC collection and future BLE ringing support)"
        )
        return False

    domain_bucket: dict[str, Any] | None = hass.data.get(DOMAIN)
    if not isinstance(domain_bucket, dict):
        _LOGGER.debug("Domain bucket not ready — skipping BLE scanner setup")
        return False

    existing_unsub = domain_bucket.get(DATA_BLE_SCANNER_UNSUB)
    if callable(existing_unsub) and not replace_existing:
        _LOGGER.debug("FMDN BLE scanner already registered; keeping it")
        return True

    # Track last log time for unresolved EIDs (keyed by 4-byte prefix).
    unresolved_log_at: dict[str, float] = {}

    def _fmdn_advertisement_callback(
        service_info: BluetoothServiceInfoBleak,
        change: BluetoothChange,
    ) -> None:
        """Process a single FMDN BLE advertisement from HA's scanner."""
        payload, service_uuid = _is_fmdn_service_data(service_info.service_data)
        if payload is None:
            return

        # Determine frame type from the payload.
        frame_type: int | None = None
        if len(payload) >= 1 and payload[0] in (FMDN_FRAME_TYPE, MODERN_FRAME_TYPE):
            frame_type = payload[0]

        # Resolve via the shared EID Resolver.
        resolver = domain_bucket.get(DATA_EID_RESOLVER)
        if resolver is None:
            return

        ble_address = service_info.address
        rssi = service_info.rssi

        # ``service_info.time`` is the advertisement time (monotonic clock).
        # It matters because HA does not only deliver live advertisements:
        # on registration it replays the last advertisement of every known
        # address (up to 15 minutes old for non-connectable sources), and it
        # restores that history across restarts. Without the timestamp the
        # resolver would date every replayed sighting "now".
        match = resolver.resolve_eid(
            payload, ble_address=ble_address, observed_at=service_info.time
        )

        if match is not None:
            _LOGGER.debug(
                "BLE scan: resolved %s → device=%s (canonical=%s) "
                "mac=%s rssi=%d frame=0x%02x svc=%s",
                payload[:4].hex(),
                match.device_id[:8],
                (match.canonical_id or "?")[:8],
                ble_address,
                rssi,
                frame_type if frame_type is not None else 0,
                "FEAA" if service_uuid == FEAA_SERVICE_UUID else "FE2C",
            )
        else:
            # Rate-limited debug log for unresolved advertisements.
            prefix = payload[:4].hex()
            now = time.monotonic()
            # The first sighting of a prefix always logs. A default of 0.0
            # would suppress it whenever the monotonic clock is still below
            # the interval (a freshly booted host or CI runner).
            last = unresolved_log_at.get(prefix)
            if last is None or now - last >= _UNRESOLVED_LOG_INTERVAL:
                unresolved_log_at[prefix] = now
                # An advertisement that did not resolve belongs to somebody
                # else's tracker until proven otherwise: its MAC is class (c)
                # (AGENTS.md section 5) and is logged masked. The resolved
                # branch above logs the MAC of the user's own tracker in full,
                # class (a), because it rotates with the EID.
                _LOGGER.debug(
                    "BLE scan: unresolved FMDN adv prefix=%s "
                    "mac=%s rssi=%d frame=0x%02x len=%d",
                    prefix,
                    _mask_address_for_logs(ble_address),
                    rssi,
                    frame_type if frame_type is not None else 0,
                    len(payload),
                )

    # Register the callback.  HA Bluetooth will call us for EVERY BLE
    # advertisement — we filter inside _fmdn_advertisement_callback by
    # checking service_data for FEAA/FE2C.  Using BluetoothScanningMode.PASSIVE
    # avoids requesting active scans (no extra power draw).
    #
    # Note: HA's matcher accepts a single ``service_data_uuid`` per
    # registration. FMDN advertises under FEAA or FE2C, so instead of two
    # registrations we accept every advertisement and filter in the callback.
    #
    # A falsy matcher (e.g. None) is NOT "match everything" — HA's
    # BluetoothManager treats it as {"connectable": True}
    # (homeassistant/components/bluetooth/manager.py), which silently drops
    # every advertisement relayed through a non-connectable source. Shelly
    # proxies always register as non-connectable (aioshelly creates the
    # scanner with connectable=False). ESPHome proxies are connectable when
    # built with ``active: true`` (the ESP32/RP2 default) and non-connectable
    # with ``active: false`` or on advertisement-only hubs (bleak_esphome
    # derives the flag from BluetoothProxyFeature.ACTIVE_CONNECTIONS).
    # habluetooth only surfaces a non-connectable advertisement as connectable
    # (``_as_connectable``) while a still-registered connectable scanner holds
    # an unexpired history entry for the same address, so trackers heard
    # solely by non-connectable proxies never reached this callback.
    # Explicitly passing connectable=False disables that filter, so
    # local-radio and every proxy-relayed advertisement reach us; the history
    # replay HA performs on registration now covers all sources as well.
    #
    # Every entry setup calls this function. Registering a second callback
    # would process each advertisement twice and orphan the first handle;
    # skipping the registration would deny the new entry the history replay.
    # So an existing callback is replaced: register the new one first, then
    # unregister the old one, so a failing registration leaves the old one in
    # place. No await lies between reading the handle above and storing the
    # new one below, so concurrent setups cannot leave two callbacks
    # registered, and no advertisement reaches both callbacks in between.
    unsub: CALLBACK_TYPE = async_register_callback(
        hass,
        _fmdn_advertisement_callback,
        {"connectable": False},
        BluetoothScanningMode.PASSIVE,
    )

    domain_bucket[DATA_BLE_SCANNER_UNSUB] = unsub
    if callable(existing_unsub):
        try:
            existing_unsub()
        except ValueError:
            # HA removes the matcher from a list; a handle that was already
            # removed raises ValueError there (bluetooth/match.py).
            _LOGGER.debug("Previous FMDN BLE scanner handle was already removed")
        _LOGGER.debug(
            "FMDN BLE scanner re-registered to replay the advertisement history"
        )
        return True
    _LOGGER.info(
        "FMDN BLE scanner registered — collecting MAC addresses "
        "and frame types from FMDN advertisements"
    )
    return True


async def async_unload_ble_scanner(hass: HomeAssistant) -> bool:
    """Unregister the HA-Bluetooth FMDN callback.

    Returns True if cleanup succeeded or was unnecessary (scanner not loaded).
    """
    domain_bucket: dict[str, Any] | None = hass.data.get(DOMAIN)
    if not isinstance(domain_bucket, dict):
        return True

    unsub = domain_bucket.pop(DATA_BLE_SCANNER_UNSUB, None)
    if callable(unsub):
        unsub()
        _LOGGER.info("FMDN BLE scanner unregistered")

    return True
