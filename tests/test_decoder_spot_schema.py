# tests/test_decoder_spot_schema.py
"""The Spot schema additions must not endanger the device-list parse path.

A declared ``string`` or message field raises ``DecodeError`` on foreign content,
and the parser turns that into a failure of the whole device list. New
length-delimited fields on this path are therefore ``bytes``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from types import SimpleNamespace

import pytest

from custom_components.googlefindmy.ProtoDecoders import (
    Common_pb2,
    DeviceUpdate_pb2,
    LocationReportsUpload_pb2,
    decoder,
)
from google.protobuf import descriptor as _descriptor
from google.protobuf.descriptor import Descriptor
from google.protobuf.message import DecodeError, Message

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
    """Red probe of the rule: this is what `bytes` protects the list from."""
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


# --- Redaction of key material (AP4) ------------------------------------------

_SENTINEL = bytes.fromhex("5a5a5a5a5a5a5a5a")
_SENTINEL_HEX = _SENTINEL.hex()

# Field names that read like secrets but are not raw key material.
_NOT_SECRET_NAMES = frozenset(
    {
        # Blobs already encrypted for the owner, shown by this dev tool before.
        "encryptedAccountKey",
        "encryptedIdentityKey",
        "encryptedOwnerKey",
        "encryptedSha256AccountKeyPublicAddress",
        "publicKeyRandom",
        # Public keys.
        "ownerPublicKey",
        "publicKey",
        # Counters, flags, names and identifiers.
        "cookieName",
        "domainKeyVersion",
        "encryptedWithSharedSecret",
        "firstSecretsCreationDate",
        "hasOwnerKeyVersion",
        "keyIdentifier",
        "keyRotationNeeded",
        "ownerKeyVersion",
        "publicKeyId",
        # No personal data of their own.
        "thisAccount",
        # Containers: their leaves are checked one by one.
        "accessInformation",
        "phoneIdentifierInformation",
        "cookies",
        "e2eePublicKeyRegistration",
        "ecdsaPublicKeys",
        "eciesAeadHkdfPublicKeys",
        "encryptedOwnerKeyAndMetadata",
        "encryptedUserSecrets",
        "keyInformation",
        "keys",
        "publicKeyIdInfo",
        "publicKeyIdList",
        "rotatedUserSecrets",
        "webloginCookies",
    }
)


_SUSPECT_NAME_PARTS = (
    "account",
    "contact",
    "cookie",
    "email",
    "imei",
    "key",
    "phone",
    "secret",
    "token",
)


def _all_field_names() -> set[str]:
    names: set[str] = set()

    def walk(message: Descriptor) -> None:
        names.update(field.name for field in message.fields)
        for nested in message.nested_types:
            walk(nested)

    for module in (Common_pb2, DeviceUpdate_pb2, LocationReportsUpload_pb2):
        for message in module.DESCRIPTOR.message_types_by_name.values():
            walk(message)
    return names


def test_every_secret_looking_field_is_redacted_or_explained() -> None:
    suspects = {
        name
        for name in _all_field_names()
        if any(part in name.lower() for part in _SUSPECT_NAME_PARTS)
    }
    assert suspects - decoder._SECRET_PROTO_FIELDS - _NOT_SECRET_NAMES == set()


def test_secret_names_on_message_fields_are_explained() -> None:
    """A set entry does not hide a message, so each such pair needs a reason."""
    found: set[str] = set()

    def walk(message: Descriptor) -> None:
        for field in message.fields:
            if (
                field.type == _TYPE_MESSAGE
                and field.name in decoder._SECRET_PROTO_FIELDS
            ):
                found.add(field.full_name)
        for nested in message.nested_types:
            walk(nested)

    for module in (Common_pb2, DeviceUpdate_pb2, LocationReportsUpload_pb2):
        for message in module.DESCRIPTOR.message_types_by_name.values():
            walk(message)
    assert found == {
        # Holds the canonical ids, which developer output has always shown.
        "IdentitfierInformation.phoneInformation",
        # Its fields are listed one by one, see the next test.
        "DeviceDescription.contactInfo",
    }


def test_contact_info_message_shows_no_personal_data() -> None:
    description = DeviceUpdate_pb2.DeviceDescription(
        contactInfo=DeviceUpdate_pb2.DeviceDescription.ContactInfo(
            phoneNumber="+49 1", infoMessage="call me", email="a@example.com"
        )
    )
    text = decoder.custom_message_formatter(description, "", False)
    for value in ("+49 1", "call me", "a@example.com"):
        assert value not in text
    redacted = decoder._redacted_copy(description)
    assert redacted.contactInfo.ByteSize() == 0


def test_push_token_is_hidden() -> None:
    update = DeviceUpdate_pb2.DeviceUpdate()
    update.fcmMetadata.gcmRegistrationId.id = "push-token"
    update.fcmMetadata.requestUuid = "uuid-1"
    text = decoder.custom_message_formatter(update.fcmMetadata, "", False)
    assert "push-token" not in text
    assert "gcmRegistrationId: <redacted>" in text
    assert "uuid-1" in text
    redacted = decoder._redacted_copy(update)
    assert not redacted.fcmMetadata.HasField("gcmRegistrationId")
    assert redacted.fcmMetadata.requestUuid == "uuid-1"


def test_secret_set_names_only_existing_fields() -> None:
    assert decoder._SECRET_PROTO_FIELDS - _all_field_names() == set()


def _registration_with_secrets() -> DeviceUpdate_pb2.DeviceRegistration:
    return DeviceUpdate_pb2.DeviceRegistration(
        canonicId=b"\x11\x22",
        ringKey=_SENTINEL,
        recoveryKey=_SENTINEL,
        unwantedTrackingKey=_SENTINEL,
        keyRotationInformation=_SENTINEL,
        encryptedUserSecrets=DeviceUpdate_pb2.EncryptedUserSecrets(
            sharingKeys=_SENTINEL, encryptedIdentityKey=b"\x33\x44"
        ),
    )


def _device_list_with_secrets() -> DeviceUpdate_pb2.DevicesList:
    device_list = DeviceUpdate_pb2.DevicesList()
    device = device_list.deviceMetadata.add()
    device.information.deviceRegistration.CopyFrom(_registration_with_secrets())
    device.information.accessInformation.add(sharingKeys=_SENTINEL)
    device.phoneInformation = _SENTINEL
    return device_list


def test_formatter_redacts_secrets_and_keeps_the_rest() -> None:
    text = decoder.custom_message_formatter(_registration_with_secrets(), "", False)
    assert _SENTINEL_HEX not in text
    assert "ringKey: <redacted len=8>" in text
    assert 'canonicId: "1122"' in text
    assert 'encryptedIdentityKey: "3344"' in text


def test_formatter_redacts_a_secret_string() -> None:
    cookie = DeviceUpdate_pb2.WebloginAuthResponse.WebloginCookies.Cookie(
        cookieName="SID", cookieValue="s3cr3t-cookie"
    )
    text = decoder.custom_message_formatter(cookie, "", False)
    assert "s3cr3t-cookie" not in text
    assert "cookieValue: <redacted len=13>" in text
    assert "cookieName: SID" in text


@pytest.mark.parametrize("force_type_error", [False, True])
@pytest.mark.parametrize(
    ("printer", "parser", "build"),
    [
        (
            decoder.print_device_list_protobuf,
            "parse_device_list_protobuf",
            _device_list_with_secrets,
        ),
        (
            decoder.print_device_update_protobuf,
            "parse_device_update_protobuf",
            lambda: DeviceUpdate_pb2.DeviceUpdate(
                deviceMetadata=_device_list_with_secrets().deviceMetadata[0]
            ),
        ),
    ],
)
def test_print_helpers_never_show_secrets(  # noqa: PLR0913
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    force_type_error: bool,
    printer: Callable[[str], None],
    parser: str,
    build: Callable[[], Message],
) -> None:
    monkeypatch.setattr(decoder, parser, lambda _hex: build())
    if force_type_error:
        real = decoder._get_text_format()

        class _OldTextFormat:
            @staticmethod
            def MessageToString(message: Message, **kwargs: object) -> str:  # noqa: N802
                if kwargs:
                    raise TypeError("message_formatter not supported")
                return str(real.MessageToString(message))

        monkeypatch.setattr(decoder, "_get_text_format", lambda: _OldTextFormat)
    printer("00")
    out = capsys.readouterr().out
    assert "5a5a" not in out
    assert "ZZZZ" not in out  # 0x5A as text format would print it
    assert "1122" in out or "\\021" in out  # the non-secret neighbour stays


def test_location_report_printer_keeps_printing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """No field of this message is secret; it only has to keep printing."""
    message = LocationReportsUpload_pb2.LocationReportsUpload(random1=7)
    monkeypatch.setattr(
        decoder, "parse_location_report_upload_protobuf", lambda _hex: message
    )
    decoder.print_location_report_upload_protobuf("00")
    assert "random1: 7" in capsys.readouterr().out


def test_formatter_prints_every_value_of_a_repeated_bytes_field() -> None:
    metadata = DeviceUpdate_pb2.DeviceMetadata(supportedActions=[b"\x08\x01", b"\x10"])
    text = decoder.custom_message_formatter(metadata, "", False)
    assert 'supportedActions: "0801"' in text
    assert 'supportedActions: "10"' in text


def test_formatter_redacts_personal_data_in_type_information() -> None:
    information = DeviceUpdate_pb2.DeviceTypeInformation(contactInfo=_SENTINEL)
    text = decoder.custom_message_formatter(information, "", False)
    assert _SENTINEL_HEX not in text
    assert "contactInfo: <redacted len=8>" in text


def test_identifier_phone_information_stays_visible() -> None:
    """Same field name as the secret bytes field, but a message with the ids."""
    metadata = DeviceUpdate_pb2.DeviceMetadata()
    metadata.identifierInformation.phoneInformation.canonicIds.canonicId.add(id="cid-1")
    assert "cid-1" in decoder.custom_message_formatter(metadata, "", False)
    redacted = decoder._redacted_copy(metadata)
    identifier = redacted.identifierInformation
    assert identifier.phoneInformation.canonicIds.canonicId[0].id == "cid-1"


def test_redacted_copy_leaves_the_original_alone() -> None:
    original = _device_list_with_secrets()
    redacted = decoder._redacted_copy(original)
    assert (
        original.deviceMetadata[0].information.deviceRegistration.ringKey == _SENTINEL
    )
    assert not redacted.deviceMetadata[0].information.deviceRegistration.ringKey
    assert not redacted.deviceMetadata[0].information.accessInformation[0].sharingKeys
    assert not redacted.deviceMetadata[0].phoneInformation


def test_device_rows_and_debug_log_never_carry_secrets(
    caplog: pytest.LogCaptureFixture,
) -> None:
    device_list = _device_list_with_secrets()
    device = device_list.deviceMetadata[0]
    device.identifierInformation.canonicIds.canonicId.add(id="cid-1")
    with caplog.at_level(logging.DEBUG):
        rows = decoder.get_devices_with_location(device_list)
    assert len(rows) == 1
    assert _SENTINEL_HEX not in repr(rows)
    assert repr(_SENTINEL)[2:-1] not in repr(rows)
    assert _SENTINEL_HEX not in caplog.text
    assert "ZZZZ" not in caplog.text


# --- Guarded reader for the component count (AP5) -----------------------------


def _capabilities(trackable: int) -> bytes:
    return DeviceUpdate_pb2.DeviceCapabilities(
        trackableComponents=trackable
    ).SerializeToString()


def test_trackable_components_reads_the_value() -> None:
    registration = DeviceUpdate_pb2.DeviceRegistration(capabilities=_capabilities(3))
    assert decoder.trackable_components_of(registration) == 3


@pytest.mark.parametrize(
    "raw",
    [b"", *_GARBAGE, _capabilities(0), _capabilities(-1)],
    ids=["empty", "utf8", "truncated", "zero", "negative"],
)
def test_trackable_components_rejects_bad_values_without_logging(
    raw: bytes, caplog: pytest.LogCaptureFixture
) -> None:
    registration = DeviceUpdate_pb2.DeviceRegistration(capabilities=raw)
    with caplog.at_level(logging.DEBUG):
        assert decoder.trackable_components_of(registration) is None
    assert caplog.text == ""


@pytest.mark.parametrize(
    "stand_in",
    [SimpleNamespace(), SimpleNamespace(capabilities="text"), None],
    ids=["no-field", "not-bytes", "none"],
)
def test_trackable_components_accepts_test_doubles(stand_in: object) -> None:
    assert decoder.trackable_components_of(stand_in) is None


def test_device_row_carries_trackable_components() -> None:
    device_list = DeviceUpdate_pb2.DevicesList()
    device = device_list.deviceMetadata.add()
    device.identifierInformation.canonicIds.canonicId.add(id="cid-1")
    device.information.deviceRegistration.capabilities = _capabilities(3)
    device.information.deviceRegistration.pairDate = 1_700_000_000
    rows = decoder.get_devices_with_location(device_list)
    assert rows[0]["trackable_components"] == 3


def test_device_row_without_capabilities_has_none() -> None:
    device_list = DeviceUpdate_pb2.DevicesList()
    device = device_list.deviceMetadata.add()
    device.identifierInformation.canonicIds.canonicId.add(id="cid-1")
    rows = decoder.get_devices_with_location(device_list)
    assert rows[0]["trackable_components"] is None


def test_device_row_survives_garbage_capabilities() -> None:
    device_list = DeviceUpdate_pb2.DevicesList()
    device = device_list.deviceMetadata.add()
    device.identifierInformation.canonicIds.canonicId.add(id="cid-1")
    device.information.deviceRegistration.capabilities = _GARBAGE[1]
    rows = decoder.get_devices_with_location(device_list)
    assert rows[0]["trackable_components"] is None
