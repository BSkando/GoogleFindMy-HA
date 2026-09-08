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


async def async_setup_ble_scanner(hass: HomeAssistant) -> bool:
    """Register HA-Bluetooth callback for FMDN advertisements.

    Returns True if the scanner was successfully registered, False if the
    bluetooth integration is not available (non-fatal).
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

        match = resolver.resolve_eid(payload, ble_address=ble_address)

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
            last = unresolved_log_at.get(prefix, 0.0)
            if now - last >= _UNRESOLVED_LOG_INTERVAL:
                unresolved_log_at[prefix] = now
                _LOGGER.debug(
                    "BLE scan: unresolved FMDN adv prefix=%s "
                    "mac=%s rssi=%d frame=0x%02x len=%d",
                    prefix,
                    ble_address,
                    rssi,
                    frame_type if frame_type is not None else 0,
                    len(payload),
                )

    # Register the callback.  HA Bluetooth will call us for EVERY BLE
    # advertisement — we filter inside _fmdn_advertisement_callback by
    # checking service_data for FEAA/FE2C.  Using BluetoothScanningMode.PASSIVE
    # avoids requesting active scans (no extra power draw).
    #
    # Note: HA's async_register_callback does not support service_data UUID
    # filtering natively, so we filter ourselves inside the callback.
    #
    # A falsy matcher (e.g. None) is NOT "match everything" — HA's
    # BluetoothManager treats it as {"connectable": True}
    # (homeassistant/components/bluetooth/manager.py), which silently drops
    # every advertisement relayed through a non-connectable source. ESPHome
    # and Shelly Bluetooth proxies register as non-connectable, so this
    # callback never fired for any proxy-heard tracker — only advertisements
    # picked up by a connectable local adapter got through. Explicitly
    # passing connectable=False disables that filter, so both local-radio
    # and proxy-relayed advertisements reach us.
    unsub: CALLBACK_TYPE = async_register_callback(
        hass,
        _fmdn_advertisement_callback,
        {"connectable": False},
        BluetoothScanningMode.PASSIVE,
    )

    domain_bucket[DATA_BLE_SCANNER_UNSUB] = unsub
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
