#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#
from custom_components.googlefindmy.ProtoDecoders import Common_pb2
from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2
from custom_components.googlefindmy.SpotApi.spot_request import spot_request
import logging
from google.protobuf.message import DecodeError  # parse-time error type for protobufs

_LOGGER = logging.getLogger(__name__)


class SpotApiEmptyResponseError(RuntimeError):
    """Raised when a SPOT API call returns an empty body where one was expected."""
    pass


def get_eid_info():
    # Build request: -1 means "latest available owner key" (API convention)
    get_eid_info_for_e2ee_devices_request = Common_pb2.GetEidInfoForE2eeDevicesRequest()
    get_eid_info_for_e2ee_devices_request.ownerKeyVersion = -1
    get_eid_info_for_e2ee_devices_request.hasOwnerKeyVersion = True

    serialized_request = get_eid_info_for_e2ee_devices_request.SerializeToString()
    response_bytes = spot_request("GetEidInfoForE2eeDevices", serialized_request)

    # Defensive checks + diagnostics for trailers-only / non-gRPC / empty payloads
    if not response_bytes:
        # Actionable guidance: most often caused by expired/invalid auth; forces re-auth in higher layers
        _LOGGER.warning(
            "GetEidInfoForE2eeDevices: empty/none response (len=0, pre=). "
            "This often indicates an authentication issue (trailers-only with grpc-status!=0). "
            "If this persists after a token refresh, please re-authenticate your Google account."
        )
        raise SpotApiEmptyResponseError(
            "Empty gRPC body (possibly trailers-only) for GetEidInfoForE2eeDevices"
        )

    eid_info = DeviceUpdate_pb2.GetEidInfoForE2eeDevicesResponse()
    try:
        eid_info.ParseFromString(response_bytes)
    except DecodeError:
        # Provide minimal, high-signal context to help diagnose corrupted/incompatible payloads
        _LOGGER.warning(
            "GetEidInfoForE2eeDevices: protobuf DecodeError (len=%s, pre=%s). "
            "This may indicate a truncated/corrupted gRPC response or a server-side format change. "
            "If this persists, try re-authenticating.",
            len(response_bytes),
            response_bytes[:16].hex(),
        )
        raise

    return eid_info


if __name__ == '__main__':
    print(get_eid_info().encryptedOwnerKeyAndMetadata)
