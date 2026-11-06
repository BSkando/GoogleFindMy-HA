# tests/test_decoder_spot_schema.py
"""The Spot schema additions must not endanger the device-list parse path.

A declared ``string`` or message field raises ``DecodeError`` on foreign content,
and the parser turns that into a failure of the whole device list. New
length-delimited fields on this path are therefore ``bytes``.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from google.protobuf import descriptor as _descriptor
from google.protobuf.descriptor import Descriptor
from google.protobuf.message import DecodeError

from custom_components.googlefindmy.ProtoDecoders import (
    Common_pb2,
    DeviceUpdate_pb2,
)

_TYPE_MESSAGE = _descriptor.FieldDescriptor.TYPE_MESSAGE
_TYPE_STRING = _descriptor.FieldDescriptor.TYPE_STRING
_TYPE_BYTES = _descriptor.FieldDescriptor.TYPE_BYTES

# Length-delimited fields on the parse path that predate the Spot additions and
# keep their declared type. Anything else on this path must be `bytes`.
_PARSE_PATH_DECLARED_BEFORE = frozenset(
    {
        "AccessInformation.email",
        "CanonicId.id",
        "CanonicIds.canonicId",
        "DeviceInformation.accessInformation",
        "DeviceInformation.deviceRegistration",
        "DeviceInformation.locationInformation",
        "DeviceMetadata.identifierInformation",
        "DeviceMetadata.imageInformation",
        "DeviceMetadata.information",
        "DeviceMetadata.userDefinedDeviceName",
        "DeviceRegistration.deviceTypeInformation",
        "DeviceRegistration.encryptedUserSecrets",
        "DeviceRegistration.fastPairModelId",
        "DeviceRegistration.manufacturer",
        "DeviceRegistration.model",
        "DeviceUpdate.deviceMetadata",
        "DeviceUpdate.fcmMetadata",
        "DeviceUpdate.requestMetadata",
        "DevicesList.deviceMetadata",
        "EncryptedUserSecrets.creationDate",
        "ExecuteActionRequestMetadata.fmdClientUuid",
        "ExecuteActionRequestMetadata.gcmRegistrationId",
        "ExecuteActionRequestMetadata.requestUuid",
        "GcmCloudMessagingIdProtobuf.id",
        "GeoLocation.encryptedReport",
        "IdentitfierInformation.canonicIds",
        "IdentitfierInformation.phoneInformation",
        "ImageInformation.imageUrl",
        "LocationInformation.reports",
        "LocationReport.geoLocation",
        "LocationReport.semanticLocation",
        "LocationsAndTimestampsWrapper.recentLocationAndNetworkLocations",
        "PhoneInformation.canonicIds",
        "RecentLocationAndNetworkLocations.networkLocationTimestamps",
        "RecentLocationAndNetworkLocations.networkLocations",
        "RecentLocationAndNetworkLocations.recentLocation",
        "RecentLocationAndNetworkLocations.recentLocationTimestamp",
        "RequestMetadata.responseTime",
        "SemanticLocation.locationName",
    }
)

_NEW_BYTES_FIELDS: dict[str, tuple[str, ...]] = {
    "DeviceMetadata": ("supportedActions", "phoneInformation"),
    "ImageInformation": ("componentImages",),
    "AccessInformation": ("sharingKeys",),
    "DeviceRegistration": (
        "canonicId",
        "capabilities",
        "ringKey",
        "recoveryKey",
        "keyRotationInformation",
        "unwantedTrackingKey",
        "lostModeInformation",
        "firstSecretsCreationDate",
    ),
    "DeviceTypeInformation": (
        "userDefinedName",
        "deviceComponentsInformation",
        "contactInfo",
        "componentDetails",
    ),
    "EncryptedUserSecrets": ("sharingKeys", "previousEikDate"),
    "SemanticLocation": ("semanticLocationIdentifier",),
}

# Foreign content: invalid UTF-8, then a truncated length-delimited field.
_GARBAGE = (b"\xff\xfe\xfd", b"\x0a\x7f\x01")


def _walk_parse_path() -> Iterator[_descriptor.FieldDescriptor]:
    seen: set[str] = set()
    pending: list[Descriptor] = [
        DeviceUpdate_pb2.DevicesList.DESCRIPTOR,
        DeviceUpdate_pb2.DeviceUpdate.DESCRIPTOR,
    ]
    while pending:
        message = pending.pop()
        if message.full_name in seen:
            continue
        seen.add(message.full_name)
        for field in message.fields:
            yield field
            if field.type == _TYPE_MESSAGE:
                pending.append(field.message_type)


def _field_tag(number: int) -> bytes:
    value = number << 3 | 2
    out = bytearray()
    while value > 0x7F:
        out.append(value & 0x7F | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _length_delimited(number: int, payload: bytes) -> bytes:
    length = len(payload)
    assert length < 0x80
    return _field_tag(number) + bytes((length,)) + payload


def _message_with_field(message_cls: type, field_name: str, payload: bytes) -> bytes:
    number: int = message_cls.DESCRIPTOR.fields_by_name[field_name].number
    return _length_delimited(number, payload)


def test_new_length_delimited_fields_on_parse_path_are_bytes() -> None:
    """A new string or message field here could fail the whole device list."""
    offenders = sorted(
        field.full_name
        for field in _walk_parse_path()
        if field.type in (_TYPE_STRING, _TYPE_MESSAGE)
        and field.full_name not in _PARSE_PATH_DECLARED_BEFORE
    )
    assert offenders == []


@pytest.mark.parametrize(
    ("message_name", "field_name"),
    [(m, f) for m, fields in _NEW_BYTES_FIELDS.items() for f in fields],
)
@pytest.mark.parametrize("garbage", _GARBAGE)
def test_garbage_in_new_fields_does_not_fail_the_parse(
    message_name: str, field_name: str, garbage: bytes
) -> None:
    module = Common_pb2 if message_name == "SemanticLocation" else DeviceUpdate_pb2
    message_cls = getattr(module, message_name)
    parsed = message_cls.FromString(
        _message_with_field(message_cls, field_name, garbage)
    )
    value = getattr(parsed, field_name)
    assert garbage == (value if isinstance(value, bytes) else value[0])


def test_garbage_capabilities_survive_the_whole_device_list() -> None:
    """The whole list parses, and a guarded reader gets the raw bytes."""
    registration = _length_delimited(11, _GARBAGE[0]) + _length_delimited(
        25, _GARBAGE[1]
    )
    information = _length_delimited(1, registration)
    metadata = _length_delimited(4, information)
    parsed = DeviceUpdate_pb2.DevicesList.FromString(_length_delimited(2, metadata))
    device = parsed.deviceMetadata[0].information.deviceRegistration
    assert device.capabilities == _GARBAGE[0]
    assert device.ringKey == _GARBAGE[1]
    with pytest.raises(DecodeError):
        DeviceUpdate_pb2.DeviceCapabilities.FromString(device.capabilities)


def test_declared_message_would_fail_on_the_same_bytes() -> None:
    """Rot-Probe of the rule: this is what `bytes` protects the list from."""
    wire = _length_delimited(
        DeviceUpdate_pb2.DeviceRegistration.DESCRIPTOR.fields_by_name[
            "deviceTypeInformation"
        ].number,
        _GARBAGE[1],
    )
    with pytest.raises(DecodeError):
        DeviceUpdate_pb2.DeviceRegistration.FromString(wire)


def test_measured_component_detail_values_are_readable() -> None:
    """`#2.#15` carried 3, 1, 2 on the measured device."""
    details = [
        DeviceUpdate_pb2.DeviceComponentDetail(component=value, detail=b"\xa5" * 4)
        for value in (3, 1, 2)
    ]
    wire = DeviceUpdate_pb2.DeviceRegistration(
        deviceTypeInformation=DeviceUpdate_pb2.DeviceTypeInformation(
            componentDetails=[item.SerializeToString() for item in details]
        ),
        ringKey=b"\xa5" * 8,
    ).SerializeToString()
    parsed = DeviceUpdate_pb2.DeviceRegistration.FromString(wire)
    assert [
        DeviceUpdate_pb2.DeviceComponentDetail.FromString(item).component
        for item in parsed.deviceTypeInformation.componentDetails
    ] == [3, 1, 2]
    assert parsed.ringKey == b"\xa5" * 8


def test_unused_spot_messages_round_trip() -> None:
    """The kept wire documentation is usable, for example the EID list."""
    message = DeviceUpdate_pb2.ListEidsForBleDevicesResponse(
        deviceEids=[
            DeviceUpdate_pb2.ListEidsForBleDevicesResponse.DevicePublicKeyIds(
                rotationPeriod=1024,
                serverList=DeviceUpdate_pb2.PublicKeyIdList(
                    publicKeyIdInfo=[
                        DeviceUpdate_pb2.PublicKeyIdList.PublicKeyIdInfo(
                            trackableComponent=255
                        )
                    ]
                ),
            )
        ]
    )
    parsed = DeviceUpdate_pb2.ListEidsForBleDevicesResponse.FromString(
        message.SerializeToString()
    )
    entry = parsed.deviceEids[0]
    assert entry.rotationPeriod == 1024
    assert entry.serverList.publicKeyIdInfo[0].trackableComponent == 255
