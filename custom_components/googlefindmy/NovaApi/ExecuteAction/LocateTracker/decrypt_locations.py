#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#

import datetime
import hashlib
import logging
from typing import Optional

from google.protobuf.message import DecodeError

from custom_components.googlefindmy.FMDNCrypto.foreign_tracker_cryptor import decrypt
from custom_components.googlefindmy.KeyBackup.cloud_key_decryptor import decrypt_eik, decrypt_aes_gcm
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypted_location import WrappedLocation
from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2
from custom_components.googlefindmy.ProtoDecoders import Common_pb2
from custom_components.googlefindmy.ProtoDecoders.DeviceUpdate_pb2 import DeviceRegistration
from custom_components.googlefindmy.ProtoDecoders.decoder import parse_device_update_protobuf
from custom_components.googlefindmy.SpotApi.CreateBleDevice.config import mcu_fast_pair_model_id
from custom_components.googlefindmy.SpotApi.CreateBleDevice.util import flip_bits
from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices.get_eid_info_request import (
    get_eid_info,
    SpotApiEmptyResponseError,
)
from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices.get_owner_key import get_owner_key

_LOGGER = logging.getLogger(__name__)


# ---- Exceptions (specific, but compatible by inheriting from RuntimeError) ----
class DecryptionError(RuntimeError):
    """Raised when decryption fails for reasons other than stale owner key."""


class StaleOwnerKeyError(DecryptionError):
    """Raised when the tracker was encrypted with an older owner key version."""


def create_google_maps_link(latitude: float, longitude: float) -> Optional[str]:
    """
    Create a standard Google Maps link for given coordinates.

    Contract:
    - Returns a valid URL string, or None if coordinates are invalid.
    - Using None avoids mixing error strings with URLs.

    This preserves call sites that only log the link; they can handle None safely.
    """
    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except (TypeError, ValueError):
        _LOGGER.warning("Invalid coordinate types for Maps link: lat=%r, lon=%r", latitude, longitude)
        return None

    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        _LOGGER.warning("Invalid coordinate values for Maps link: lat=%s, lon=%s", latitude, longitude)
        return None

    return f"https://www.google.com/maps/search/?api=1&query={latitude},{longitude}"


# Indicates if the device is a custom microcontroller
def is_mcu_tracker(device_registration: DeviceRegistration) -> bool:
    return device_registration.fastPairModelId == mcu_fast_pair_model_id


def retrieve_identity_key(device_registration: DeviceRegistration) -> bytes:
    """
    Derive the device identity key:
    - Flip bits (MCU quirk) and decrypt the Encrypted Identity Key (EIK) with the owner key.
    - On failure, raise a specific exception (no prints/exits).
    """
    is_mcu = is_mcu_tracker(device_registration)
    encrypted_user_secrets = device_registration.encryptedUserSecrets

    encrypted_identity_key = flip_bits(
        encrypted_user_secrets.encryptedIdentityKey,
        is_mcu
    )

    owner_key = get_owner_key()

    try:
        identity_key = decrypt_eik(owner_key, encrypted_identity_key)
        return identity_key
    except Exception as e:
        # Try to fetch E2EE metadata to explain likely cause (key-version mismatch vs. auth issues)
        current_owner_key_version = None
        try:
            e2eeData = get_eid_info()
            current_owner_key_version = e2eeData.encryptedOwnerKeyAndMetadata.ownerKeyVersion
        except SpotApiEmptyResponseError:
            # Auth/session issue upstream (trailers-only). Propagate with context.
            _LOGGER.error(
                "Failed to decrypt identity key due to empty trailers-only EID info response "
                "(authentication/session). Please re-authenticate and retry."
            )
            raise
        except Exception as meta_exc:
            _LOGGER.warning("Failed to retrieve E2EE metadata for diagnostics: %s", meta_exc)

        old_ver = getattr(encrypted_user_secrets, "ownerKeyVersion", None)
        if current_owner_key_version is not None and old_ver is not None and old_ver < current_owner_key_version:
            # Version mismatch → tracker cannot be decrypted anymore
            _LOGGER.error(
                "Owner key version mismatch: tracker=%s, current=%s. "
                "This typically occurs after resetting E2EE data. "
                "The tracker cannot be decrypted anymore; remove it in the Find My Device app.",
                old_ver, current_owner_key_version,
            )
            raise StaleOwnerKeyError("Tracker was encrypted with a stale owner key version.") from e

        _LOGGER.error(
            "Failed to decrypt identity key (owner key version %s vs. current %s). "
            "If you recently reset E2EE data, re-authenticate or recreate keys. "
            "If the issue persists, clear the integration secrets to force a fresh key derivation.",
            old_ver, current_owner_key_version,
        )
        raise DecryptionError("Identity key decryption failed.") from e


def decrypt_location_response_locations(device_update_protobuf):
    """
    Decrypt and normalize location reports into structured dicts for Home Assistant.
    - Robust against partial/invalid reports: logs and continues.
    - No prints and no process termination; all errors bubble up or are logged.
    """
    # Defensive guards on presence of required metadata
    try:
        device_registration = device_update_protobuf.deviceMetadata.information.deviceRegistration
    except Exception as exc:
        _LOGGER.error("Device registration metadata missing or invalid: %s", exc)
        raise

    identity_key = retrieve_identity_key(device_registration)

    try:
        locations_proto = device_update_protobuf.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations
    except Exception as exc:
        _LOGGER.error("Location information missing or invalid: %s", exc)
        raise

    is_mcu = is_mcu_tracker(device_registration)

    # At All Areas Reports or Own Reports
    recent_location = locations_proto.recentLocation
    recent_location_time = locations_proto.recentLocationTimestamp

    # High Traffic Reports
    network_locations = list(locations_proto.networkLocations)
    network_locations_time = list(locations_proto.networkLocationTimestamps)

    if locations_proto.HasField("recentLocation"):
        network_locations.append(recent_location)
        network_locations_time.append(recent_location_time)

    location_time_array = []
    for loc, time_ts in zip(network_locations, network_locations_time):
        try:
            if loc.status == Common_pb2.Status.SEMANTIC:
                _LOGGER.debug("Semantic Location Report")

                wrapped_location = WrappedLocation(
                    decrypted_location=b'',
                    time=int(time_ts.seconds),
                    accuracy=0,
                    status=loc.status,
                    is_own_report=True,
                    name=loc.semanticLocation.locationName
                )
                location_time_array.append(wrapped_location)
            else:
                encrypted_location = loc.geoLocation.encryptedReport.encryptedLocation
                public_key_random = loc.geoLocation.encryptedReport.publicKeyRandom

                if public_key_random == b"":  # Own Report
                    identity_key_hash = hashlib.sha256(identity_key).digest()
                    decrypted_location = decrypt_aes_gcm(identity_key_hash, encrypted_location)
                else:
                    time_offset = 0 if is_mcu else loc.geoLocation.deviceTimeOffset
                    decrypted_location = decrypt(identity_key, encrypted_location, public_key_random, time_offset)

                wrapped_location = WrappedLocation(
                    decrypted_location=decrypted_location,
                    time=int(time_ts.seconds),
                    accuracy=loc.geoLocation.accuracy,
                    status=loc.status,
                    is_own_report=loc.geoLocation.encryptedReport.isOwnReport,
                    name=""
                )
                location_time_array.append(wrapped_location)
        except Exception as one_exc:
            # Keep processing other reports if a single report fails (resilience)
            _LOGGER.warning("Failed to process one location report: %s", one_exc)

    _LOGGER.debug("-" * 40)
    _LOGGER.debug("[DecryptLocations] Decrypted Locations:")

    if not location_time_array:
        _LOGGER.debug("No locations found.")
        return []

    # Convert to structured data for Home Assistant
    structured_locations = []

    for loc in location_time_array:
        try:
            if loc.status == Common_pb2.Status.SEMANTIC:
                _LOGGER.debug("Semantic Location: %s", loc.name)

                location_data = {
                    "latitude": None,
                    "longitude": None,
                    "altitude": None,
                    "accuracy": loc.accuracy,
                    "last_seen": loc.time,
                    "status": str(loc.status),
                    "is_own_report": loc.is_own_report,
                    "semantic_name": loc.name
                }

            else:
                proto_loc = DeviceUpdate_pb2.Location()
                try:
                    proto_loc.ParseFromString(loc.decrypted_location)
                except DecodeError as de:
                    _LOGGER.warning("Failed to parse Location protobuf: %s", de)
                    continue

                latitude = proto_loc.latitude / 1e7
                longitude = proto_loc.longitude / 1e7
                altitude = proto_loc.altitude

                _LOGGER.debug("Latitude: %s", latitude)
                _LOGGER.debug("Longitude: %s", longitude)
                _LOGGER.debug("Altitude: %s", altitude)
                maps_link = create_google_maps_link(latitude, longitude)
                if maps_link:
                    _LOGGER.debug("Google Maps Link: %s", maps_link)

                location_data = {
                    "latitude": latitude,
                    "longitude": longitude,
                    "altitude": altitude,
                    "accuracy": loc.accuracy,
                    "last_seen": loc.time,
                    "status": str(loc.status),
                    "is_own_report": loc.is_own_report,
                    "semantic_name": None
                }

            _LOGGER.debug(
                "Time: %s | Status: %s | Is Own Report: %s",
                datetime.datetime.fromtimestamp(loc.time).strftime('%Y-%m-%d %H:%M:%S'),
                loc.status,
                loc.is_own_report,
            )
            _LOGGER.debug("-" * 40)

            structured_locations.append(location_data)
        except Exception as one_exc:
            _LOGGER.warning("Failed to convert one WrappedLocation to structured payload: %s", one_exc)

    return structured_locations


if __name__ == '__main__':
    res = parse_device_update_protobuf("")
    decrypt_location_response_locations(res)
