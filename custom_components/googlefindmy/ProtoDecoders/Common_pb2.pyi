# custom_components/googlefindmy/ProtoDecoders/Common_pb2.pyi
from __future__ import annotations

from collections.abc import Mapping as _Mapping
from typing import (
    Any as _Any,
)
from typing import (
    ClassVar as _ClassVar,
)

from custom_components.googlefindmy.protobuf_typing import (
    EnumTypeWrapperMeta as _EnumTypeWrapperMeta,
)
from custom_components.googlefindmy.protobuf_typing import (
    MessageProto as _MessageProto,
)
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message

EnumTypeWrapper = _EnumTypeWrapperMeta[int]
Message = _message.Message
MessageProto = _MessageProto

DESCRIPTOR: _descriptor.FileDescriptor

class Status(int, metaclass=EnumTypeWrapper):
    __slots__ = ()
    SEMANTIC: _ClassVar[Status]
    LAST_KNOWN: _ClassVar[Status]
    CROWDSOURCED: _ClassVar[Status]
    AGGREGATED: _ClassVar[Status]

SEMANTIC: Status
LAST_KNOWN: Status
CROWDSOURCED: Status
AGGREGATED: Status

class Time(Message, _MessageProto):
    __slots__ = ("seconds", "nanos")
    SECONDS_FIELD_NUMBER: _ClassVar[int]
    NANOS_FIELD_NUMBER: _ClassVar[int]
    seconds: int
    nanos: int
    def __init__(self, seconds: int | None = ..., nanos: int | None = ...) -> None: ...

class LocationReport(Message, _MessageProto):
    __slots__ = ("semanticLocation", "geoLocation", "status")
    SEMANTICLOCATION_FIELD_NUMBER: _ClassVar[int]
    GEOLOCATION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    semanticLocation: SemanticLocation
    geoLocation: GeoLocation
    status: Status
    def __init__(
        self,
        semanticLocation: SemanticLocation | _Mapping[str, _Any] | None = ...,
        geoLocation: GeoLocation | _Mapping[str, _Any] | None = ...,
        status: Status | str | None = ...,
    ) -> None: ...

class SemanticLocation(Message, _MessageProto):
    __slots__ = ("locationName", "status", "semanticLocationIdentifier")
    class SemanticLocationIdentifier(Message, _MessageProto):
        __slots__ = ("uuid",)
        UUID_FIELD_NUMBER: _ClassVar[int]
        uuid: str
        def __init__(self, uuid: str | None = ...) -> None: ...

    LOCATIONNAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SEMANTICLOCATIONIDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    locationName: str
    status: int
    semanticLocationIdentifier: bytes
    def __init__(
        self,
        locationName: str | None = ...,
        status: int | None = ...,
        semanticLocationIdentifier: bytes | None = ...,
    ) -> None: ...

class GeoLocation(Message, _MessageProto):
    __slots__ = ("encryptedReport", "deviceTimeOffset", "accuracy")
    ENCRYPTEDREPORT_FIELD_NUMBER: _ClassVar[int]
    DEVICETIMEOFFSET_FIELD_NUMBER: _ClassVar[int]
    ACCURACY_FIELD_NUMBER: _ClassVar[int]
    encryptedReport: EncryptedReport
    deviceTimeOffset: int
    accuracy: float
    def __init__(
        self,
        encryptedReport: EncryptedReport | _Mapping[str, _Any] | None = ...,
        deviceTimeOffset: int | None = ...,
        accuracy: float | None = ...,
    ) -> None: ...

class EncryptedReport(Message, _MessageProto):
    __slots__ = ("publicKeyRandom", "encryptedLocation", "isOwnReport")
    PUBLICKEYRANDOM_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTEDLOCATION_FIELD_NUMBER: _ClassVar[int]
    ISOWNREPORT_FIELD_NUMBER: _ClassVar[int]
    publicKeyRandom: bytes
    encryptedLocation: bytes
    isOwnReport: bool
    def __init__(
        self,
        publicKeyRandom: bytes | None = ...,
        encryptedLocation: bytes | None = ...,
        isOwnReport: bool = ...,
    ) -> None: ...

class GetEidInfoForE2eeDevicesRequest(Message, _MessageProto):
    __slots__ = ("ownerKeyVersion", "hasOwnerKeyVersion")
    OWNERKEYVERSION_FIELD_NUMBER: _ClassVar[int]
    HASOWNERKEYVERSION_FIELD_NUMBER: _ClassVar[int]
    ownerKeyVersion: int
    hasOwnerKeyVersion: bool
    def __init__(
        self, ownerKeyVersion: int | None = ..., hasOwnerKeyVersion: bool = ...
    ) -> None: ...
