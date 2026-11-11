# custom_components/googlefindmy/ProtoDecoders/DeviceUpdate_pb2.pyi
from __future__ import annotations

from collections.abc import Iterable as _Iterable
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
from custom_components.googlefindmy.ProtoDecoders import Common_pb2 as _Common_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

EnumTypeWrapper = _EnumTypeWrapperMeta[int]
Message = _message.Message
MessageProto = _MessageProto

DESCRIPTOR: _descriptor.FileDescriptor

class DeviceType(int, metaclass=EnumTypeWrapper):
    __slots__ = ()
    UNKNOWN_DEVICE_TYPE: _ClassVar[DeviceType]
    ANDROID_DEVICE: _ClassVar[DeviceType]
    SPOT_DEVICE: _ClassVar[DeviceType]
    TEST_DEVICE_TYPE: _ClassVar[DeviceType]
    AUTO_DEVICE: _ClassVar[DeviceType]
    FASTPAIR_DEVICE: _ClassVar[DeviceType]
    SUPERVISED_ANDROID_DEVICE: _ClassVar[DeviceType]

class SpotContributorType(int, metaclass=EnumTypeWrapper):
    __slots__ = ()
    FMDN_DISABLED_DEFAULT: _ClassVar[SpotContributorType]
    FMDN_CONTRIBUTOR_HIGH_TRAFFIC: _ClassVar[SpotContributorType]
    FMDN_CONTRIBUTOR_ALL_LOCATIONS: _ClassVar[SpotContributorType]
    FMDN_HIGH_TRAFFIC: _ClassVar[SpotContributorType]
    FMDN_ALL_LOCATIONS: _ClassVar[SpotContributorType]

class DeviceComponent(int, metaclass=EnumTypeWrapper):
    __slots__ = ()
    DEVICE_COMPONENT_UNSPECIFIED: _ClassVar[DeviceComponent]
    DEVICE_COMPONENT_RIGHT: _ClassVar[DeviceComponent]
    DEVICE_COMPONENT_LEFT: _ClassVar[DeviceComponent]
    DEVICE_COMPONENT_CASE: _ClassVar[DeviceComponent]

class IdentifierInformationType(int, metaclass=EnumTypeWrapper):
    __slots__ = ()
    IDENTIFIER_UNKNOWN: _ClassVar[IdentifierInformationType]
    IDENTIFIER_ANDROID: _ClassVar[IdentifierInformationType]
    IDENTIFIER_SPOT: _ClassVar[IdentifierInformationType]

class SpotDeviceType(int, metaclass=EnumTypeWrapper):
    __slots__ = ()
    DEVICE_TYPE_UNKNOWN: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_BEACON: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_HEADPHONES: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_KEYS: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_WATCH: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_WALLET: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_BAG: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_LAPTOP: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_CAR: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_REMOTE_CONTROL: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_BADGE: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_BIKE: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_CAMERA: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_CAT: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_CHARGER: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_CLOTHING: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_DOG: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_NOTEBOOK: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_PASSPORT: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_PHONE: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_SPEAKER: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_TABLET: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_TOY: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_UMBRELLA: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_STYLUS: _ClassVar[SpotDeviceType]
    DEVICE_TYPE_EARBUDS: _ClassVar[SpotDeviceType]

class DeviceEncryptionScheme(int, metaclass=EnumTypeWrapper):
    __slots__ = ()
    EID_20_BYTES: _ClassVar[DeviceEncryptionScheme]
    EID_32_BYTES: _ClassVar[DeviceEncryptionScheme]

class SpotBeaconType(int, metaclass=EnumTypeWrapper):
    __slots__ = ()
    BEACON_TYPE_DEFAULT_TAG: _ClassVar[SpotBeaconType]
    BEACON_TYPE_EMBEDDED: _ClassVar[SpotBeaconType]
    BEACON_TYPE_SELF_REPORT_ONLY: _ClassVar[SpotBeaconType]
    BEACON_TYPE_SELF_REPORT_OR_ADVERTISING: _ClassVar[SpotBeaconType]
    BEACON_TYPE_NON_ADVERTISING_ACCESSORY: _ClassVar[SpotBeaconType]

UNKNOWN_DEVICE_TYPE: DeviceType
ANDROID_DEVICE: DeviceType
SPOT_DEVICE: DeviceType
TEST_DEVICE_TYPE: DeviceType
AUTO_DEVICE: DeviceType
FASTPAIR_DEVICE: DeviceType
SUPERVISED_ANDROID_DEVICE: DeviceType
FMDN_DISABLED_DEFAULT: SpotContributorType
FMDN_CONTRIBUTOR_HIGH_TRAFFIC: SpotContributorType
FMDN_CONTRIBUTOR_ALL_LOCATIONS: SpotContributorType
FMDN_HIGH_TRAFFIC: SpotContributorType
FMDN_ALL_LOCATIONS: SpotContributorType
DEVICE_COMPONENT_UNSPECIFIED: DeviceComponent
DEVICE_COMPONENT_RIGHT: DeviceComponent
DEVICE_COMPONENT_LEFT: DeviceComponent
DEVICE_COMPONENT_CASE: DeviceComponent
IDENTIFIER_UNKNOWN: IdentifierInformationType
IDENTIFIER_ANDROID: IdentifierInformationType
IDENTIFIER_SPOT: IdentifierInformationType
DEVICE_TYPE_UNKNOWN: SpotDeviceType
DEVICE_TYPE_BEACON: SpotDeviceType
DEVICE_TYPE_HEADPHONES: SpotDeviceType
DEVICE_TYPE_KEYS: SpotDeviceType
DEVICE_TYPE_WATCH: SpotDeviceType
DEVICE_TYPE_WALLET: SpotDeviceType
DEVICE_TYPE_BAG: SpotDeviceType
DEVICE_TYPE_LAPTOP: SpotDeviceType
DEVICE_TYPE_CAR: SpotDeviceType
DEVICE_TYPE_REMOTE_CONTROL: SpotDeviceType
DEVICE_TYPE_BADGE: SpotDeviceType
DEVICE_TYPE_BIKE: SpotDeviceType
DEVICE_TYPE_CAMERA: SpotDeviceType
DEVICE_TYPE_CAT: SpotDeviceType
DEVICE_TYPE_CHARGER: SpotDeviceType
DEVICE_TYPE_CLOTHING: SpotDeviceType
DEVICE_TYPE_DOG: SpotDeviceType
DEVICE_TYPE_NOTEBOOK: SpotDeviceType
DEVICE_TYPE_PASSPORT: SpotDeviceType
DEVICE_TYPE_PHONE: SpotDeviceType
DEVICE_TYPE_SPEAKER: SpotDeviceType
DEVICE_TYPE_TABLET: SpotDeviceType
DEVICE_TYPE_TOY: SpotDeviceType
DEVICE_TYPE_UMBRELLA: SpotDeviceType
DEVICE_TYPE_STYLUS: SpotDeviceType
DEVICE_TYPE_EARBUDS: SpotDeviceType
EID_20_BYTES: DeviceEncryptionScheme
EID_32_BYTES: DeviceEncryptionScheme
BEACON_TYPE_DEFAULT_TAG: SpotBeaconType
BEACON_TYPE_EMBEDDED: SpotBeaconType
BEACON_TYPE_SELF_REPORT_ONLY: SpotBeaconType
BEACON_TYPE_SELF_REPORT_OR_ADVERTISING: SpotBeaconType
BEACON_TYPE_NON_ADVERTISING_ACCESSORY: SpotBeaconType

class GetEidInfoForE2eeDevicesResponse(Message, _MessageProto):
    __slots__ = (
        "encryptedOwnerKeyAndMetadata",
        "eidResponseField3",
        "eidResponseField5",
    )
    ENCRYPTEDOWNERKEYANDMETADATA_FIELD_NUMBER: _ClassVar[int]
    EIDRESPONSEFIELD3_FIELD_NUMBER: _ClassVar[int]
    EIDRESPONSEFIELD5_FIELD_NUMBER: _ClassVar[int]
    encryptedOwnerKeyAndMetadata: EncryptedOwnerKeyAndMetadata
    eidResponseField3: bytes
    eidResponseField5: bytes
    def __init__(
        self,
        encryptedOwnerKeyAndMetadata: EncryptedOwnerKeyAndMetadata
        | _Mapping[str, _Any]
        | None = ...,
        eidResponseField3: bytes | None = ...,
        eidResponseField5: bytes | None = ...,
    ) -> None: ...

class EncryptedOwnerKeyAndMetadata(Message, _MessageProto):
    __slots__ = (
        "encryptedOwnerKey",
        "ownerKeyVersion",
        "securityDomain",
        "domainKeyVersion",
        "domainType",
    )
    ENCRYPTEDOWNERKEY_FIELD_NUMBER: _ClassVar[int]
    OWNERKEYVERSION_FIELD_NUMBER: _ClassVar[int]
    SECURITYDOMAIN_FIELD_NUMBER: _ClassVar[int]
    DOMAINKEYVERSION_FIELD_NUMBER: _ClassVar[int]
    DOMAINTYPE_FIELD_NUMBER: _ClassVar[int]
    encryptedOwnerKey: bytes
    ownerKeyVersion: int
    securityDomain: str
    domainKeyVersion: int
    domainType: int
    def __init__(
        self,
        encryptedOwnerKey: bytes | None = ...,
        ownerKeyVersion: int | None = ...,
        securityDomain: str | None = ...,
        domainKeyVersion: int | None = ...,
        domainType: int | None = ...,
    ) -> None: ...

class DevicesList(Message, _MessageProto):
    __slots__ = ("deviceMetadata",)
    DEVICEMETADATA_FIELD_NUMBER: _ClassVar[int]
    deviceMetadata: _containers.RepeatedCompositeFieldContainer[DeviceMetadata]
    def __init__(
        self,
        deviceMetadata: _Iterable[DeviceMetadata | _Mapping[str, _Any]] | None = ...,
    ) -> None: ...

class DevicesListRequest(Message, _MessageProto):
    __slots__ = ("deviceListRequestPayload",)
    DEVICELISTREQUESTPAYLOAD_FIELD_NUMBER: _ClassVar[int]
    deviceListRequestPayload: DevicesListRequestPayload
    def __init__(
        self,
        deviceListRequestPayload: DevicesListRequestPayload
        | _Mapping[str, _Any]
        | None = ...,
    ) -> None: ...

class DevicesListRequestPayload(Message, _MessageProto):
    __slots__ = ("type", "id")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    type: DeviceType
    id: str
    def __init__(
        self, type: DeviceType | str | None = ..., id: str | None = ...
    ) -> None: ...

class ExecuteActionRequest(Message, _MessageProto):
    __slots__ = ("scope", "action", "requestMetadata")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    REQUESTMETADATA_FIELD_NUMBER: _ClassVar[int]
    scope: ExecuteActionScope
    action: ExecuteActionType
    requestMetadata: ExecuteActionRequestMetadata
    def __init__(
        self,
        scope: ExecuteActionScope | _Mapping[str, _Any] | None = ...,
        action: ExecuteActionType | _Mapping[str, _Any] | None = ...,
        requestMetadata: ExecuteActionRequestMetadata
        | _Mapping[str, _Any]
        | None = ...,
    ) -> None: ...

class ExecuteActionRequestMetadata(Message, _MessageProto):
    __slots__ = ("type", "requestUuid", "fmdClientUuid", "gcmRegistrationId", "unknown")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    REQUESTUUID_FIELD_NUMBER: _ClassVar[int]
    FMDCLIENTUUID_FIELD_NUMBER: _ClassVar[int]
    GCMREGISTRATIONID_FIELD_NUMBER: _ClassVar[int]
    UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    type: DeviceType
    requestUuid: str
    fmdClientUuid: str
    gcmRegistrationId: GcmCloudMessagingIdProtobuf
    unknown: bool
    def __init__(
        self,
        type: DeviceType | str | None = ...,
        requestUuid: str | None = ...,
        fmdClientUuid: str | None = ...,
        gcmRegistrationId: GcmCloudMessagingIdProtobuf
        | _Mapping[str, _Any]
        | None = ...,
        unknown: bool = ...,
    ) -> None: ...

class GcmCloudMessagingIdProtobuf(Message, _MessageProto):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: str | None = ...) -> None: ...

class ExecuteActionType(Message, _MessageProto):
    __slots__ = (
        "locateTracker",
        "startSound",
        "stopSound",
        "phoneActionField1",
        "phoneActionField2",
        "phoneActionField4",
        "phoneActionField5",
        "phoneActionField6",
        "phoneActionField7",
        "phoneActionField13",
        "phoneActionField14",
    )
    LOCATETRACKER_FIELD_NUMBER: _ClassVar[int]
    STARTSOUND_FIELD_NUMBER: _ClassVar[int]
    STOPSOUND_FIELD_NUMBER: _ClassVar[int]
    PHONEACTIONFIELD1_FIELD_NUMBER: _ClassVar[int]
    PHONEACTIONFIELD2_FIELD_NUMBER: _ClassVar[int]
    PHONEACTIONFIELD4_FIELD_NUMBER: _ClassVar[int]
    PHONEACTIONFIELD5_FIELD_NUMBER: _ClassVar[int]
    PHONEACTIONFIELD6_FIELD_NUMBER: _ClassVar[int]
    PHONEACTIONFIELD7_FIELD_NUMBER: _ClassVar[int]
    PHONEACTIONFIELD13_FIELD_NUMBER: _ClassVar[int]
    PHONEACTIONFIELD14_FIELD_NUMBER: _ClassVar[int]
    locateTracker: ExecuteActionLocateTrackerType
    startSound: ExecuteActionSoundType
    stopSound: ExecuteActionSoundType
    phoneActionField1: ExecuteActionPhoneAction
    phoneActionField2: ExecuteActionPhoneAction
    phoneActionField4: ExecuteActionPhoneAction
    phoneActionField5: ExecuteActionPhoneAction
    phoneActionField6: ExecuteActionPhoneAction
    phoneActionField7: ExecuteActionPhoneAction
    phoneActionField13: ExecuteActionPhoneAction
    phoneActionField14: ExecuteActionPhoneAction
    def __init__(
        self,
        locateTracker: ExecuteActionLocateTrackerType
        | _Mapping[str, _Any]
        | None = ...,
        startSound: ExecuteActionSoundType | _Mapping[str, _Any] | None = ...,
        stopSound: ExecuteActionSoundType | _Mapping[str, _Any] | None = ...,
        phoneActionField1: ExecuteActionPhoneAction | _Mapping[str, _Any] | None = ...,
        phoneActionField2: ExecuteActionPhoneAction | _Mapping[str, _Any] | None = ...,
        phoneActionField4: ExecuteActionPhoneAction | _Mapping[str, _Any] | None = ...,
        phoneActionField5: ExecuteActionPhoneAction | _Mapping[str, _Any] | None = ...,
        phoneActionField6: ExecuteActionPhoneAction | _Mapping[str, _Any] | None = ...,
        phoneActionField7: ExecuteActionPhoneAction | _Mapping[str, _Any] | None = ...,
        phoneActionField13: ExecuteActionPhoneAction | _Mapping[str, _Any] | None = ...,
        phoneActionField14: ExecuteActionPhoneAction | _Mapping[str, _Any] | None = ...,
    ) -> None: ...

class ExecuteActionPhoneAction(Message, _MessageProto):
    __slots__ = ()
    def __init__(self) -> None: ...

class ExecuteActionLocateTrackerType(Message, _MessageProto):
    __slots__ = ("lastHighTrafficEnablingTime", "contributorType")
    LASTHIGHTRAFFICENABLINGTIME_FIELD_NUMBER: _ClassVar[int]
    CONTRIBUTORTYPE_FIELD_NUMBER: _ClassVar[int]
    lastHighTrafficEnablingTime: _Common_pb2.Time
    contributorType: SpotContributorType
    def __init__(
        self,
        lastHighTrafficEnablingTime: _Common_pb2.Time
        | _Mapping[str, _Any]
        | None = ...,
        contributorType: SpotContributorType | str | None = ...,
    ) -> None: ...

class ExecuteActionSoundType(Message, _MessageProto):
    __slots__ = ("component",)
    COMPONENT_FIELD_NUMBER: _ClassVar[int]
    component: DeviceComponent
    def __init__(self, component: DeviceComponent | str | None = ...) -> None: ...

class ExecuteActionScope(Message, _MessageProto):
    __slots__ = ("type", "device")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DEVICE_FIELD_NUMBER: _ClassVar[int]
    type: DeviceType
    device: ExecuteActionDeviceIdentifier
    def __init__(
        self,
        type: DeviceType | str | None = ...,
        device: ExecuteActionDeviceIdentifier | _Mapping[str, _Any] | None = ...,
    ) -> None: ...

class ExecuteActionDeviceIdentifier(Message, _MessageProto):
    __slots__ = ("canonicId",)
    CANONICID_FIELD_NUMBER: _ClassVar[int]
    canonicId: CanonicId
    def __init__(
        self, canonicId: CanonicId | _Mapping[str, _Any] | None = ...
    ) -> None: ...

class DeviceUpdate(Message, _MessageProto):
    __slots__ = ("fcmMetadata", "deviceMetadata", "requestMetadata")
    FCMMETADATA_FIELD_NUMBER: _ClassVar[int]
    DEVICEMETADATA_FIELD_NUMBER: _ClassVar[int]
    REQUESTMETADATA_FIELD_NUMBER: _ClassVar[int]
    fcmMetadata: ExecuteActionRequestMetadata
    deviceMetadata: DeviceMetadata
    requestMetadata: RequestMetadata
    def __init__(
        self,
        fcmMetadata: ExecuteActionRequestMetadata | _Mapping[str, _Any] | None = ...,
        deviceMetadata: DeviceMetadata | _Mapping[str, _Any] | None = ...,
        requestMetadata: RequestMetadata | _Mapping[str, _Any] | None = ...,
    ) -> None: ...

class DeviceMetadata(Message, _MessageProto):
    __slots__ = (
        "identifierInformation",
        "supportedActions",
        "phoneInformation",
        "information",
        "userDefinedDeviceName",
        "imageInformation",
        "metadataField8",
        "metadataField10",
        "metadataField11",
        "metadataField12",
        "metadataField13",
    )
    class SupportedActions(Message, _MessageProto):
        __slots__ = ("type", "supportedActionsField2")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        SUPPORTEDACTIONSFIELD2_FIELD_NUMBER: _ClassVar[int]
        type: ExecuteActionType
        supportedActionsField2: int
        def __init__(
            self,
            type: ExecuteActionType | _Mapping[str, _Any] | None = ...,
            supportedActionsField2: int | None = ...,
        ) -> None: ...

    class PhoneInformation(Message, _MessageProto):
        __slots__ = (
            "modelNumber",
            "manufacturer",
            "codename",
            "carrier",
            "imei",
            "information",
        )
        MODELNUMBER_FIELD_NUMBER: _ClassVar[int]
        MANUFACTURER_FIELD_NUMBER: _ClassVar[int]
        CODENAME_FIELD_NUMBER: _ClassVar[int]
        CARRIER_FIELD_NUMBER: _ClassVar[int]
        IMEI_FIELD_NUMBER: _ClassVar[int]
        INFORMATION_FIELD_NUMBER: _ClassVar[int]
        modelNumber: str
        manufacturer: str
        codename: str
        carrier: str
        imei: int
        information: DeviceInformation
        def __init__(
            self,
            modelNumber: str | None = ...,
            manufacturer: str | None = ...,
            codename: str | None = ...,
            carrier: str | None = ...,
            imei: int | None = ...,
            information: DeviceInformation | _Mapping[str, _Any] | None = ...,
        ) -> None: ...

    IDENTIFIERINFORMATION_FIELD_NUMBER: _ClassVar[int]
    SUPPORTEDACTIONS_FIELD_NUMBER: _ClassVar[int]
    PHONEINFORMATION_FIELD_NUMBER: _ClassVar[int]
    INFORMATION_FIELD_NUMBER: _ClassVar[int]
    USERDEFINEDDEVICENAME_FIELD_NUMBER: _ClassVar[int]
    IMAGEINFORMATION_FIELD_NUMBER: _ClassVar[int]
    METADATAFIELD8_FIELD_NUMBER: _ClassVar[int]
    METADATAFIELD10_FIELD_NUMBER: _ClassVar[int]
    METADATAFIELD11_FIELD_NUMBER: _ClassVar[int]
    METADATAFIELD12_FIELD_NUMBER: _ClassVar[int]
    METADATAFIELD13_FIELD_NUMBER: _ClassVar[int]
    identifierInformation: IdentitfierInformation
    supportedActions: _containers.RepeatedScalarFieldContainer[bytes]
    phoneInformation: bytes
    information: DeviceInformation
    userDefinedDeviceName: str
    imageInformation: ImageInformation
    metadataField8: int
    metadataField10: bytes
    metadataField11: bytes
    metadataField12: bytes
    metadataField13: bytes
    def __init__(
        self,
        identifierInformation: IdentitfierInformation
        | _Mapping[str, _Any]
        | None = ...,
        supportedActions: _Iterable[bytes] | None = ...,
        phoneInformation: bytes | None = ...,
        information: DeviceInformation | _Mapping[str, _Any] | None = ...,
        userDefinedDeviceName: str | None = ...,
        imageInformation: ImageInformation | _Mapping[str, _Any] | None = ...,
        metadataField8: int | None = ...,
        metadataField10: bytes | None = ...,
        metadataField11: bytes | None = ...,
        metadataField12: bytes | None = ...,
        metadataField13: bytes | None = ...,
    ) -> None: ...

class ImageInformation(Message, _MessageProto):
    __slots__ = ("imageUrl", "components", "componentImages")
    class ComponentImage(Message, _MessageProto):
        __slots__ = ("imageUrl",)
        IMAGEURL_FIELD_NUMBER: _ClassVar[int]
        imageUrl: str
        def __init__(self, imageUrl: str | None = ...) -> None: ...

    IMAGEURL_FIELD_NUMBER: _ClassVar[int]
    COMPONENTS_FIELD_NUMBER: _ClassVar[int]
    COMPONENTIMAGES_FIELD_NUMBER: _ClassVar[int]
    imageUrl: str
    components: int
    componentImages: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(
        self,
        imageUrl: str | None = ...,
        components: int | None = ...,
        componentImages: _Iterable[bytes] | None = ...,
    ) -> None: ...

class IdentitfierInformation(Message, _MessageProto):
    __slots__ = (
        "phoneInformation",
        "type",
        "canonicIds",
        "identifierField5",
    )
    PHONEINFORMATION_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    CANONICIDS_FIELD_NUMBER: _ClassVar[int]
    IDENTIFIERFIELD5_FIELD_NUMBER: _ClassVar[int]
    phoneInformation: PhoneInformation
    type: IdentifierInformationType
    canonicIds: CanonicIds
    identifierField5: bytes
    def __init__(
        self,
        phoneInformation: PhoneInformation | _Mapping[str, _Any] | None = ...,
        type: IdentifierInformationType | str | None = ...,
        canonicIds: CanonicIds | _Mapping[str, _Any] | None = ...,
        identifierField5: bytes | None = ...,
    ) -> None: ...

class PhoneInformation(Message, _MessageProto):
    __slots__ = ("canonicIds", "phoneInformationField1")
    CANONICIDS_FIELD_NUMBER: _ClassVar[int]
    PHONEINFORMATIONFIELD1_FIELD_NUMBER: _ClassVar[int]
    canonicIds: CanonicIds
    phoneInformationField1: int
    def __init__(
        self,
        canonicIds: CanonicIds | _Mapping[str, _Any] | None = ...,
        phoneInformationField1: int | None = ...,
    ) -> None: ...

class CanonicIds(Message, _MessageProto):
    __slots__ = ("canonicId",)
    CANONICID_FIELD_NUMBER: _ClassVar[int]
    canonicId: _containers.RepeatedCompositeFieldContainer[CanonicId]
    def __init__(
        self, canonicId: _Iterable[CanonicId | _Mapping[str, _Any]] | None = ...
    ) -> None: ...

class CanonicId(Message, _MessageProto):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: str | None = ...) -> None: ...

class DeviceInformation(Message, _MessageProto):
    __slots__ = ("deviceRegistration", "locationInformation", "accessInformation")
    DEVICEREGISTRATION_FIELD_NUMBER: _ClassVar[int]
    LOCATIONINFORMATION_FIELD_NUMBER: _ClassVar[int]
    ACCESSINFORMATION_FIELD_NUMBER: _ClassVar[int]
    deviceRegistration: DeviceRegistration
    locationInformation: LocationInformation
    accessInformation: _containers.RepeatedCompositeFieldContainer[AccessInformation]
    def __init__(
        self,
        deviceRegistration: DeviceRegistration | _Mapping[str, _Any] | None = ...,
        locationInformation: LocationInformation | _Mapping[str, _Any] | None = ...,
        accessInformation: _Iterable[AccessInformation | _Mapping[str, _Any]]
        | None = ...,
    ) -> None: ...

class DeviceTypeInformation(Message, _MessageProto):
    __slots__ = (
        "userDefinedName",
        "deviceType",
        "deviceComponentsInformation",
        "beaconType",
        "contactInfo",
        "ringingVolume",
        "componentDetails",
        "typeInformationField14",
    )
    USERDEFINEDNAME_FIELD_NUMBER: _ClassVar[int]
    DEVICETYPE_FIELD_NUMBER: _ClassVar[int]
    DEVICECOMPONENTSINFORMATION_FIELD_NUMBER: _ClassVar[int]
    BEACONTYPE_FIELD_NUMBER: _ClassVar[int]
    CONTACTINFO_FIELD_NUMBER: _ClassVar[int]
    RINGINGVOLUME_FIELD_NUMBER: _ClassVar[int]
    COMPONENTDETAILS_FIELD_NUMBER: _ClassVar[int]
    TYPEINFORMATIONFIELD14_FIELD_NUMBER: _ClassVar[int]
    userDefinedName: bytes
    deviceType: SpotDeviceType
    deviceComponentsInformation: _containers.RepeatedScalarFieldContainer[bytes]
    beaconType: SpotBeaconType
    contactInfo: bytes
    ringingVolume: DeviceDescription.RingingVolume
    componentDetails: _containers.RepeatedScalarFieldContainer[bytes]
    typeInformationField14: int
    def __init__(
        self,
        userDefinedName: bytes | None = ...,
        deviceType: SpotDeviceType | str | None = ...,
        deviceComponentsInformation: _Iterable[bytes] | None = ...,
        beaconType: SpotBeaconType | str | None = ...,
        contactInfo: bytes | None = ...,
        ringingVolume: DeviceDescription.RingingVolume | str | None = ...,
        componentDetails: _Iterable[bytes] | None = ...,
        typeInformationField14: int | None = ...,
    ) -> None: ...

class DeviceComponentDetail(Message, _MessageProto):
    __slots__ = ("component", "detail")
    COMPONENT_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    component: DeviceComponent
    detail: bytes
    def __init__(
        self,
        component: DeviceComponent | str | None = ...,
        detail: bytes | None = ...,
    ) -> None: ...

class DeviceRegistration(Message, _MessageProto):
    __slots__ = (
        "canonicId",
        "deviceTypeInformation",
        "capabilities",
        "isShared",
        "encryptedUserSecrets",
        "manufacturer",
        "fastPairModelId",
        "pairDate",
        "rotationExponent",
        "ringKey",
        "recoveryKey",
        "keyRotationInformation",
        "deviceEncryptionScheme",
        "unwantedTrackingKey",
        "lostModeInformation",
        "firstSecretsCreationDate",
        "model",
        "registrationField15",
        "registrationField22",
        "registrationField38",
        "registrationField40",
    )
    class KeyRotationInformation(Message, _MessageProto):
        __slots__ = ("rotatedEIKs", "keyRotationNeeded")
        class RotatedEIK(Message, _MessageProto):
            __slots__ = ("encryptedEIK", "creationDate", "rotationRequestDate")
            ENCRYPTEDEIK_FIELD_NUMBER: _ClassVar[int]
            CREATIONDATE_FIELD_NUMBER: _ClassVar[int]
            ROTATIONREQUESTDATE_FIELD_NUMBER: _ClassVar[int]
            encryptedEIK: bytes
            creationDate: _Common_pb2.Time
            rotationRequestDate: _Common_pb2.Time
            def __init__(
                self,
                encryptedEIK: bytes | None = ...,
                creationDate: _Common_pb2.Time | _Mapping[str, _Any] | None = ...,
                rotationRequestDate: _Common_pb2.Time
                | _Mapping[str, _Any]
                | None = ...,
            ) -> None: ...

        ROTATEDEIKS_FIELD_NUMBER: _ClassVar[int]
        KEYROTATIONNEEDED_FIELD_NUMBER: _ClassVar[int]
        rotatedEIKs: _containers.RepeatedCompositeFieldContainer[
            DeviceRegistration.KeyRotationInformation.RotatedEIK
        ]
        keyRotationNeeded: bool
        def __init__(
            self,
            rotatedEIKs: _Iterable[
                DeviceRegistration.KeyRotationInformation.RotatedEIK
                | _Mapping[str, _Any]
            ]
            | None = ...,
            keyRotationNeeded: bool = ...,
        ) -> None: ...

    class LostModeInformation(Message, _MessageProto):
        __slots__ = ("lostModeEnabled",)
        LOSTMODEENABLED_FIELD_NUMBER: _ClassVar[int]
        lostModeEnabled: bool
        def __init__(self, lostModeEnabled: bool = ...) -> None: ...

    CANONICID_FIELD_NUMBER: _ClassVar[int]
    DEVICETYPEINFORMATION_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    ISSHARED_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTEDUSERSECRETS_FIELD_NUMBER: _ClassVar[int]
    MANUFACTURER_FIELD_NUMBER: _ClassVar[int]
    FASTPAIRMODELID_FIELD_NUMBER: _ClassVar[int]
    PAIRDATE_FIELD_NUMBER: _ClassVar[int]
    ROTATIONEXPONENT_FIELD_NUMBER: _ClassVar[int]
    RINGKEY_FIELD_NUMBER: _ClassVar[int]
    RECOVERYKEY_FIELD_NUMBER: _ClassVar[int]
    KEYROTATIONINFORMATION_FIELD_NUMBER: _ClassVar[int]
    DEVICEENCRYPTIONSCHEME_FIELD_NUMBER: _ClassVar[int]
    UNWANTEDTRACKINGKEY_FIELD_NUMBER: _ClassVar[int]
    LOSTMODEINFORMATION_FIELD_NUMBER: _ClassVar[int]
    FIRSTSECRETSCREATIONDATE_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    REGISTRATIONFIELD15_FIELD_NUMBER: _ClassVar[int]
    REGISTRATIONFIELD22_FIELD_NUMBER: _ClassVar[int]
    REGISTRATIONFIELD38_FIELD_NUMBER: _ClassVar[int]
    REGISTRATIONFIELD40_FIELD_NUMBER: _ClassVar[int]
    canonicId: bytes
    deviceTypeInformation: DeviceTypeInformation
    capabilities: bytes
    isShared: bool
    encryptedUserSecrets: EncryptedUserSecrets
    manufacturer: str
    fastPairModelId: str
    pairDate: int
    rotationExponent: int
    ringKey: bytes
    recoveryKey: bytes
    keyRotationInformation: bytes
    deviceEncryptionScheme: DeviceEncryptionScheme
    unwantedTrackingKey: bytes
    lostModeInformation: bytes
    firstSecretsCreationDate: bytes
    model: str
    registrationField15: bytes
    registrationField22: int
    registrationField38: bytes
    registrationField40: bytes
    def __init__(
        self,
        canonicId: bytes | None = ...,
        deviceTypeInformation: DeviceTypeInformation | _Mapping[str, _Any] | None = ...,
        capabilities: bytes | None = ...,
        isShared: bool = ...,
        encryptedUserSecrets: EncryptedUserSecrets | _Mapping[str, _Any] | None = ...,
        manufacturer: str | None = ...,
        fastPairModelId: str | None = ...,
        pairDate: int | None = ...,
        rotationExponent: int | None = ...,
        ringKey: bytes | None = ...,
        recoveryKey: bytes | None = ...,
        keyRotationInformation: bytes | None = ...,
        deviceEncryptionScheme: DeviceEncryptionScheme | str | None = ...,
        unwantedTrackingKey: bytes | None = ...,
        lostModeInformation: bytes | None = ...,
        firstSecretsCreationDate: bytes | None = ...,
        model: str | None = ...,
        registrationField15: bytes | None = ...,
        registrationField22: int | None = ...,
        registrationField38: bytes | None = ...,
        registrationField40: bytes | None = ...,
    ) -> None: ...

class EncryptedUserSecrets(Message, _MessageProto):
    __slots__ = (
        "encryptedIdentityKey",
        "ownerKeyVersion",
        "encryptedAccountKey",
        "sharingKeys",
        "creationDate",
        "previousEikDate",
        "encryptedWithSharedSecret",
        "encryptedSha256AccountKeyPublicAddress",
    )
    ENCRYPTEDIDENTITYKEY_FIELD_NUMBER: _ClassVar[int]
    OWNERKEYVERSION_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTEDACCOUNTKEY_FIELD_NUMBER: _ClassVar[int]
    SHARINGKEYS_FIELD_NUMBER: _ClassVar[int]
    CREATIONDATE_FIELD_NUMBER: _ClassVar[int]
    PREVIOUSEIKDATE_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTEDWITHSHAREDSECRET_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTEDSHA256ACCOUNTKEYPUBLICADDRESS_FIELD_NUMBER: _ClassVar[int]
    encryptedIdentityKey: bytes
    ownerKeyVersion: int
    encryptedAccountKey: bytes
    sharingKeys: bytes
    creationDate: _Common_pb2.Time
    previousEikDate: bytes
    encryptedWithSharedSecret: bool
    encryptedSha256AccountKeyPublicAddress: bytes
    def __init__(
        self,
        encryptedIdentityKey: bytes | None = ...,
        ownerKeyVersion: int | None = ...,
        encryptedAccountKey: bytes | None = ...,
        sharingKeys: bytes | None = ...,
        creationDate: _Common_pb2.Time | _Mapping[str, _Any] | None = ...,
        previousEikDate: bytes | None = ...,
        encryptedWithSharedSecret: bool = ...,
        encryptedSha256AccountKeyPublicAddress: bytes | None = ...,
    ) -> None: ...

class LocationInformation(Message, _MessageProto):
    __slots__ = ("reports",)
    REPORTS_FIELD_NUMBER: _ClassVar[int]
    reports: LocationsAndTimestampsWrapper
    def __init__(
        self,
        reports: LocationsAndTimestampsWrapper | _Mapping[str, _Any] | None = ...,
    ) -> None: ...

class LocationsAndTimestampsWrapper(Message, _MessageProto):
    __slots__ = (
        "recentLocationAndNetworkLocations",
        "reportsField5",
    )
    RECENTLOCATIONANDNETWORKLOCATIONS_FIELD_NUMBER: _ClassVar[int]
    REPORTSFIELD5_FIELD_NUMBER: _ClassVar[int]
    recentLocationAndNetworkLocations: RecentLocationAndNetworkLocations
    reportsField5: int
    def __init__(
        self,
        recentLocationAndNetworkLocations: RecentLocationAndNetworkLocations
        | _Mapping[str, _Any]
        | None = ...,
        reportsField5: int | None = ...,
    ) -> None: ...

class RecentLocationAndNetworkLocations(Message, _MessageProto):
    __slots__ = (
        "recentLocation",
        "recentLocationTimestamp",
        "networkLocations",
        "networkLocationTimestamps",
        "minLocationsNeededForAggregation",
        "recentField8",
        "recentField11",
        "recentField7",
    )
    RECENTLOCATION_FIELD_NUMBER: _ClassVar[int]
    RECENTLOCATIONTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    NETWORKLOCATIONS_FIELD_NUMBER: _ClassVar[int]
    NETWORKLOCATIONTIMESTAMPS_FIELD_NUMBER: _ClassVar[int]
    MINLOCATIONSNEEDEDFORAGGREGATION_FIELD_NUMBER: _ClassVar[int]
    RECENTFIELD8_FIELD_NUMBER: _ClassVar[int]
    RECENTFIELD11_FIELD_NUMBER: _ClassVar[int]
    RECENTFIELD7_FIELD_NUMBER: _ClassVar[int]
    recentLocation: _Common_pb2.LocationReport
    recentLocationTimestamp: _Common_pb2.Time
    networkLocations: _containers.RepeatedCompositeFieldContainer[
        _Common_pb2.LocationReport
    ]
    networkLocationTimestamps: _containers.RepeatedCompositeFieldContainer[
        _Common_pb2.Time
    ]
    minLocationsNeededForAggregation: int
    recentField8: _containers.RepeatedScalarFieldContainer[int]
    recentField11: _containers.RepeatedScalarFieldContainer[int]
    recentField7: int
    def __init__(
        self,
        recentLocation: _Common_pb2.LocationReport | _Mapping[str, _Any] | None = ...,
        recentLocationTimestamp: _Common_pb2.Time | _Mapping[str, _Any] | None = ...,
        networkLocations: _Iterable[_Common_pb2.LocationReport | _Mapping[str, _Any]]
        | None = ...,
        networkLocationTimestamps: _Iterable[_Common_pb2.Time | _Mapping[str, _Any]]
        | None = ...,
        minLocationsNeededForAggregation: int | None = ...,
        recentField8: _Iterable[int] | None = ...,
        recentField11: _Iterable[int] | None = ...,
        recentField7: int | None = ...,
    ) -> None: ...

class AccessInformation(Message, _MessageProto):
    __slots__ = ("email", "hasAccess", "isOwner", "thisAccount", "sharingKeys")
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    HASACCESS_FIELD_NUMBER: _ClassVar[int]
    ISOWNER_FIELD_NUMBER: _ClassVar[int]
    THISACCOUNT_FIELD_NUMBER: _ClassVar[int]
    SHARINGKEYS_FIELD_NUMBER: _ClassVar[int]
    email: str
    hasAccess: bool
    isOwner: bool
    thisAccount: bool
    sharingKeys: bytes
    def __init__(
        self,
        email: str | None = ...,
        hasAccess: bool = ...,
        isOwner: bool = ...,
        thisAccount: bool = ...,
        sharingKeys: bytes | None = ...,
    ) -> None: ...

class SharingKeys(Message, _MessageProto):
    __slots__ = ("inviterKey", "inviteeKey")
    INVITERKEY_FIELD_NUMBER: _ClassVar[int]
    INVITEEKEY_FIELD_NUMBER: _ClassVar[int]
    inviterKey: bytes
    inviteeKey: bytes
    def __init__(
        self, inviterKey: bytes | None = ..., inviteeKey: bytes | None = ...
    ) -> None: ...

class RequestMetadata(Message, _MessageProto):
    __slots__ = ("responseTime",)
    RESPONSETIME_FIELD_NUMBER: _ClassVar[int]
    responseTime: _Common_pb2.Time
    def __init__(
        self, responseTime: _Common_pb2.Time | _Mapping[str, _Any] | None = ...
    ) -> None: ...

class EncryptionUnlockRequestExtras(Message, _MessageProto):
    __slots__ = ("operation", "securityDomain", "sessionId")
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    SECURITYDOMAIN_FIELD_NUMBER: _ClassVar[int]
    SESSIONID_FIELD_NUMBER: _ClassVar[int]
    operation: int
    securityDomain: SecurityDomain
    sessionId: str
    def __init__(
        self,
        operation: int | None = ...,
        securityDomain: SecurityDomain | _Mapping[str, _Any] | None = ...,
        sessionId: str | None = ...,
    ) -> None: ...

class SecurityDomain(Message, _MessageProto):
    __slots__ = ("name", "unknown")
    NAME_FIELD_NUMBER: _ClassVar[int]
    UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    name: str
    unknown: int
    def __init__(self, name: str | None = ..., unknown: int | None = ...) -> None: ...

class Location(Message, _MessageProto):
    __slots__ = ("latitude", "longitude", "altitude")
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    ALTITUDE_FIELD_NUMBER: _ClassVar[int]
    latitude: int
    longitude: int
    altitude: int
    def __init__(
        self,
        latitude: int | None = ...,
        longitude: int | None = ...,
        altitude: int | None = ...,
    ) -> None: ...

class RegisterBleDeviceRequest(Message, _MessageProto):
    __slots__ = (
        "fastPairModelId",
        "description",
        "capabilities",
        "e2eePublicKeyRegistration",
        "manufacturerName",
        "ringKey",
        "recoveryKey",
        "unwantedTrackingKey",
        "modelName",
    )
    FASTPAIRMODELID_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    E2EEPUBLICKEYREGISTRATION_FIELD_NUMBER: _ClassVar[int]
    MANUFACTURERNAME_FIELD_NUMBER: _ClassVar[int]
    RINGKEY_FIELD_NUMBER: _ClassVar[int]
    RECOVERYKEY_FIELD_NUMBER: _ClassVar[int]
    UNWANTEDTRACKINGKEY_FIELD_NUMBER: _ClassVar[int]
    MODELNAME_FIELD_NUMBER: _ClassVar[int]
    fastPairModelId: str
    description: DeviceDescription
    capabilities: DeviceCapabilities
    e2eePublicKeyRegistration: E2EEPublicKeyRegistration
    manufacturerName: str
    ringKey: bytes
    recoveryKey: bytes
    unwantedTrackingKey: bytes
    modelName: str
    def __init__(
        self,
        fastPairModelId: str | None = ...,
        description: DeviceDescription | _Mapping[str, _Any] | None = ...,
        capabilities: DeviceCapabilities | _Mapping[str, _Any] | None = ...,
        e2eePublicKeyRegistration: E2EEPublicKeyRegistration
        | _Mapping[str, _Any]
        | None = ...,
        manufacturerName: str | None = ...,
        ringKey: bytes | None = ...,
        recoveryKey: bytes | None = ...,
        unwantedTrackingKey: bytes | None = ...,
        modelName: str | None = ...,
    ) -> None: ...

class E2EEPublicKeyRegistration(Message, _MessageProto):
    __slots__ = (
        "rotationExponent",
        "encryptedUserSecrets",
        "publicKeyIdList",
        "pairingDate",
        "deviceEncryptionScheme",
    )
    ROTATIONEXPONENT_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTEDUSERSECRETS_FIELD_NUMBER: _ClassVar[int]
    PUBLICKEYIDLIST_FIELD_NUMBER: _ClassVar[int]
    PAIRINGDATE_FIELD_NUMBER: _ClassVar[int]
    DEVICEENCRYPTIONSCHEME_FIELD_NUMBER: _ClassVar[int]
    rotationExponent: int
    encryptedUserSecrets: EncryptedUserSecrets
    publicKeyIdList: PublicKeyIdList
    pairingDate: int
    deviceEncryptionScheme: DeviceEncryptionScheme
    def __init__(
        self,
        rotationExponent: int | None = ...,
        encryptedUserSecrets: EncryptedUserSecrets | _Mapping[str, _Any] | None = ...,
        publicKeyIdList: PublicKeyIdList | _Mapping[str, _Any] | None = ...,
        pairingDate: int | None = ...,
        deviceEncryptionScheme: DeviceEncryptionScheme | str | None = ...,
    ) -> None: ...

class PublicKeyIdList(Message, _MessageProto):
    __slots__ = ("publicKeyIdInfo",)
    class PublicKeyIdInfo(Message, _MessageProto):
        __slots__ = ("timestamp", "publicKeyId", "trackableComponent")
        TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
        PUBLICKEYID_FIELD_NUMBER: _ClassVar[int]
        TRACKABLECOMPONENT_FIELD_NUMBER: _ClassVar[int]
        timestamp: _Common_pb2.Time
        publicKeyId: TruncatedEID
        trackableComponent: int
        def __init__(
            self,
            timestamp: _Common_pb2.Time | _Mapping[str, _Any] | None = ...,
            publicKeyId: TruncatedEID | _Mapping[str, _Any] | None = ...,
            trackableComponent: int | None = ...,
        ) -> None: ...

    PUBLICKEYIDINFO_FIELD_NUMBER: _ClassVar[int]
    publicKeyIdInfo: _containers.RepeatedCompositeFieldContainer[
        PublicKeyIdList.PublicKeyIdInfo
    ]
    def __init__(
        self,
        publicKeyIdInfo: _Iterable[
            PublicKeyIdList.PublicKeyIdInfo | _Mapping[str, _Any]
        ]
        | None = ...,
    ) -> None: ...

class TruncatedEID(Message, _MessageProto):
    __slots__ = ("truncatedEid",)
    TRUNCATEDEID_FIELD_NUMBER: _ClassVar[int]
    truncatedEid: bytes
    def __init__(self, truncatedEid: bytes | None = ...) -> None: ...

class UploadPrecomputedPublicKeyIdsRequest(Message, _MessageProto):
    __slots__ = ("deviceEids",)
    class DevicePublicKeyIds(Message, _MessageProto):
        __slots__ = ("canonicId", "clientList", "pairDate")
        CANONICID_FIELD_NUMBER: _ClassVar[int]
        CLIENTLIST_FIELD_NUMBER: _ClassVar[int]
        PAIRDATE_FIELD_NUMBER: _ClassVar[int]
        canonicId: CanonicId
        clientList: PublicKeyIdList
        pairDate: int
        def __init__(
            self,
            canonicId: CanonicId | _Mapping[str, _Any] | None = ...,
            clientList: PublicKeyIdList | _Mapping[str, _Any] | None = ...,
            pairDate: int | None = ...,
        ) -> None: ...

    DEVICEEIDS_FIELD_NUMBER: _ClassVar[int]
    deviceEids: _containers.RepeatedCompositeFieldContainer[
        UploadPrecomputedPublicKeyIdsRequest.DevicePublicKeyIds
    ]
    def __init__(
        self,
        deviceEids: _Iterable[
            UploadPrecomputedPublicKeyIdsRequest.DevicePublicKeyIds
            | _Mapping[str, _Any]
        ]
        | None = ...,
    ) -> None: ...

class DeviceCapabilities(Message, _MessageProto):
    __slots__ = (
        "isAdvertising",
        "ringingVolumeSupported",
        "capableComponents",
        "trackableComponents",
    )
    ISADVERTISING_FIELD_NUMBER: _ClassVar[int]
    RINGINGVOLUMESUPPORTED_FIELD_NUMBER: _ClassVar[int]
    CAPABLECOMPONENTS_FIELD_NUMBER: _ClassVar[int]
    TRACKABLECOMPONENTS_FIELD_NUMBER: _ClassVar[int]
    isAdvertising: bool
    ringingVolumeSupported: bool
    capableComponents: int
    trackableComponents: int
    def __init__(
        self,
        isAdvertising: bool = ...,
        ringingVolumeSupported: bool = ...,
        capableComponents: int | None = ...,
        trackableComponents: int | None = ...,
    ) -> None: ...

class DeviceDescription(Message, _MessageProto):
    __slots__ = (
        "userDefinedName",
        "deviceType",
        "deviceComponentsInformation",
        "beaconType",
        "contactInfo",
        "ringingVolume",
    )
    class RingingVolume(int, metaclass=EnumTypeWrapper):
        __slots__ = ()
        FACTORY_DEFAULT: _ClassVar[DeviceDescription.RingingVolume]
        LOW: _ClassVar[DeviceDescription.RingingVolume]
        MEDIUM: _ClassVar[DeviceDescription.RingingVolume]
        HIGH: _ClassVar[DeviceDescription.RingingVolume]

    FACTORY_DEFAULT: DeviceDescription.RingingVolume
    LOW: DeviceDescription.RingingVolume
    MEDIUM: DeviceDescription.RingingVolume
    HIGH: DeviceDescription.RingingVolume
    class ContactInfo(Message, _MessageProto):
        __slots__ = ("phoneNumber", "infoMessage", "email")
        PHONENUMBER_FIELD_NUMBER: _ClassVar[int]
        INFOMESSAGE_FIELD_NUMBER: _ClassVar[int]
        EMAIL_FIELD_NUMBER: _ClassVar[int]
        phoneNumber: str
        infoMessage: str
        email: str
        def __init__(
            self,
            phoneNumber: str | None = ...,
            infoMessage: str | None = ...,
            email: str | None = ...,
        ) -> None: ...

    USERDEFINEDNAME_FIELD_NUMBER: _ClassVar[int]
    DEVICETYPE_FIELD_NUMBER: _ClassVar[int]
    DEVICECOMPONENTSINFORMATION_FIELD_NUMBER: _ClassVar[int]
    BEACONTYPE_FIELD_NUMBER: _ClassVar[int]
    CONTACTINFO_FIELD_NUMBER: _ClassVar[int]
    RINGINGVOLUME_FIELD_NUMBER: _ClassVar[int]
    userDefinedName: str
    deviceType: SpotDeviceType
    deviceComponentsInformation: _containers.RepeatedCompositeFieldContainer[
        DeviceComponentInformation
    ]
    beaconType: SpotBeaconType
    contactInfo: DeviceDescription.ContactInfo
    ringingVolume: DeviceDescription.RingingVolume
    def __init__(
        self,
        userDefinedName: str | None = ...,
        deviceType: SpotDeviceType | str | None = ...,
        deviceComponentsInformation: _Iterable[
            DeviceComponentInformation | _Mapping[str, _Any]
        ]
        | None = ...,
        beaconType: SpotBeaconType | str | None = ...,
        contactInfo: DeviceDescription.ContactInfo | _Mapping[str, _Any] | None = ...,
        ringingVolume: DeviceDescription.RingingVolume | str | None = ...,
    ) -> None: ...

class DeviceComponentInformation(Message, _MessageProto):
    __slots__ = ("imageUrl", "deviceComponent")
    IMAGEURL_FIELD_NUMBER: _ClassVar[int]
    DEVICECOMPONENT_FIELD_NUMBER: _ClassVar[int]
    imageUrl: str
    deviceComponent: DeviceComponent
    def __init__(
        self,
        imageUrl: str | None = ...,
        deviceComponent: DeviceComponent | str | None = ...,
    ) -> None: ...

class ListEidsForBleDevicesResponse(Message, _MessageProto):
    __slots__ = ("deviceEids",)
    class DevicePublicKeyIds(Message, _MessageProto):
        __slots__ = ("canonicId", "rotationPeriod", "serverList")
        CANONICID_FIELD_NUMBER: _ClassVar[int]
        ROTATIONPERIOD_FIELD_NUMBER: _ClassVar[int]
        SERVERLIST_FIELD_NUMBER: _ClassVar[int]
        canonicId: CanonicId
        rotationPeriod: int
        serverList: PublicKeyIdList
        def __init__(
            self,
            canonicId: CanonicId | _Mapping[str, _Any] | None = ...,
            rotationPeriod: int | None = ...,
            serverList: PublicKeyIdList | _Mapping[str, _Any] | None = ...,
        ) -> None: ...

    DEVICEEIDS_FIELD_NUMBER: _ClassVar[int]
    deviceEids: _containers.RepeatedCompositeFieldContainer[
        ListEidsForBleDevicesResponse.DevicePublicKeyIds
    ]
    def __init__(
        self,
        deviceEids: _Iterable[
            ListEidsForBleDevicesResponse.DevicePublicKeyIds | _Mapping[str, _Any]
        ]
        | None = ...,
    ) -> None: ...

class GetBleDeviceRequest(Message, _MessageProto):
    __slots__ = ("device",)
    DEVICE_FIELD_NUMBER: _ClassVar[int]
    device: CanonicId
    def __init__(
        self, device: CanonicId | _Mapping[str, _Any] | None = ...
    ) -> None: ...

class GetBleDeviceResponse(Message, _MessageProto):
    __slots__ = ("deviceRegistration",)
    DEVICEREGISTRATION_FIELD_NUMBER: _ClassVar[int]
    deviceRegistration: DeviceRegistration
    def __init__(
        self,
        deviceRegistration: DeviceRegistration | _Mapping[str, _Any] | None = ...,
    ) -> None: ...

class DeleteBleDeviceRequest(Message, _MessageProto):
    __slots__ = ("deviceToBeDeleted",)
    DEVICETOBEDELETED_FIELD_NUMBER: _ClassVar[int]
    deviceToBeDeleted: CanonicId
    def __init__(
        self, deviceToBeDeleted: CanonicId | _Mapping[str, _Any] | None = ...
    ) -> None: ...

class WebloginAuthResponse(Message, _MessageProto):
    __slots__ = ("webloginCookies",)
    class WebloginCookies(Message, _MessageProto):
        __slots__ = ("resultCode", "cookies")
        class Cookie(Message, _MessageProto):
            __slots__ = (
                "cookieName",
                "cookieValue",
                "domain",
                "subDomain",
                "path",
                "secure",
                "httpOnly",
                "maxAge",
            )
            COOKIENAME_FIELD_NUMBER: _ClassVar[int]
            COOKIEVALUE_FIELD_NUMBER: _ClassVar[int]
            DOMAIN_FIELD_NUMBER: _ClassVar[int]
            SUBDOMAIN_FIELD_NUMBER: _ClassVar[int]
            PATH_FIELD_NUMBER: _ClassVar[int]
            SECURE_FIELD_NUMBER: _ClassVar[int]
            HTTPONLY_FIELD_NUMBER: _ClassVar[int]
            MAXAGE_FIELD_NUMBER: _ClassVar[int]
            cookieName: str
            cookieValue: str
            domain: str
            subDomain: str
            path: str
            secure: bool
            httpOnly: bool
            maxAge: int
            def __init__(
                self,
                cookieName: str | None = ...,
                cookieValue: str | None = ...,
                domain: str | None = ...,
                subDomain: str | None = ...,
                path: str | None = ...,
                secure: bool = ...,
                httpOnly: bool = ...,
                maxAge: int | None = ...,
            ) -> None: ...

        RESULTCODE_FIELD_NUMBER: _ClassVar[int]
        COOKIES_FIELD_NUMBER: _ClassVar[int]
        resultCode: int
        cookies: _containers.RepeatedCompositeFieldContainer[
            WebloginAuthResponse.WebloginCookies.Cookie
        ]
        def __init__(
            self,
            resultCode: int | None = ...,
            cookies: _Iterable[
                WebloginAuthResponse.WebloginCookies.Cookie | _Mapping[str, _Any]
            ]
            | None = ...,
        ) -> None: ...

    WEBLOGINCOOKIES_FIELD_NUMBER: _ClassVar[int]
    webloginCookies: WebloginAuthResponse.WebloginCookies
    def __init__(
        self,
        webloginCookies: WebloginAuthResponse.WebloginCookies
        | _Mapping[str, _Any]
        | None = ...,
    ) -> None: ...

class UpdateBleDeviceSharingSettingsResponse(Message, _MessageProto):
    __slots__ = ("accessInformation", "deviceRegistration")
    ACCESSINFORMATION_FIELD_NUMBER: _ClassVar[int]
    DEVICEREGISTRATION_FIELD_NUMBER: _ClassVar[int]
    accessInformation: _containers.RepeatedCompositeFieldContainer[AccessInformation]
    deviceRegistration: DeviceRegistration
    def __init__(
        self,
        accessInformation: _Iterable[AccessInformation | _Mapping[str, _Any]]
        | None = ...,
        deviceRegistration: DeviceRegistration | _Mapping[str, _Any] | None = ...,
    ) -> None: ...

class InvitationToken(Message, _MessageProto):
    __slots__ = ("canonicId", "sharingID", "ownerPublicKey")
    CANONICID_FIELD_NUMBER: _ClassVar[int]
    SHARINGID_FIELD_NUMBER: _ClassVar[int]
    OWNERPUBLICKEY_FIELD_NUMBER: _ClassVar[int]
    canonicId: bytes
    sharingID: int
    ownerPublicKey: bytes
    def __init__(
        self,
        canonicId: bytes | None = ...,
        sharingID: int | None = ...,
        ownerPublicKey: bytes | None = ...,
    ) -> None: ...

class UpdateBleDeviceSharingSettingsRequest(Message, _MessageProto):
    __slots__ = (
        "canonicId",
        "operation",
        "updateForAccount",
        "encryptedUserSecrets",
        "sharingID",
        "encryptedPrivateKeyWrapper",
    )
    class SharingOperation(int, metaclass=EnumTypeWrapper):
        __slots__ = ()
        RETRIEVE_INVITATION: _ClassVar[
            UpdateBleDeviceSharingSettingsRequest.SharingOperation
        ]
        STOP_SHARING: _ClassVar[UpdateBleDeviceSharingSettingsRequest.SharingOperation]
        CREATE_INVITE: _ClassVar[UpdateBleDeviceSharingSettingsRequest.SharingOperation]
        ACCEPT_INVITE: _ClassVar[UpdateBleDeviceSharingSettingsRequest.SharingOperation]
        UPLOAD_KEYS: _ClassVar[UpdateBleDeviceSharingSettingsRequest.SharingOperation]
        UPLOAD_REENCRYPTED_KEYS: _ClassVar[
            UpdateBleDeviceSharingSettingsRequest.SharingOperation
        ]

    RETRIEVE_INVITATION: UpdateBleDeviceSharingSettingsRequest.SharingOperation
    STOP_SHARING: UpdateBleDeviceSharingSettingsRequest.SharingOperation
    CREATE_INVITE: UpdateBleDeviceSharingSettingsRequest.SharingOperation
    ACCEPT_INVITE: UpdateBleDeviceSharingSettingsRequest.SharingOperation
    UPLOAD_KEYS: UpdateBleDeviceSharingSettingsRequest.SharingOperation
    UPLOAD_REENCRYPTED_KEYS: UpdateBleDeviceSharingSettingsRequest.SharingOperation
    CANONICID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    UPDATEFORACCOUNT_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTEDUSERSECRETS_FIELD_NUMBER: _ClassVar[int]
    SHARINGID_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTEDPRIVATEKEYWRAPPER_FIELD_NUMBER: _ClassVar[int]
    canonicId: CanonicId
    operation: UpdateBleDeviceSharingSettingsRequest.SharingOperation
    updateForAccount: str
    encryptedUserSecrets: EncryptedUserSecrets
    sharingID: int
    encryptedPrivateKeyWrapper: bytes
    def __init__(
        self,
        canonicId: CanonicId | _Mapping[str, _Any] | None = ...,
        operation: UpdateBleDeviceSharingSettingsRequest.SharingOperation
        | str
        | None = ...,
        updateForAccount: str | None = ...,
        encryptedUserSecrets: EncryptedUserSecrets | _Mapping[str, _Any] | None = ...,
        sharingID: int | None = ...,
        encryptedPrivateKeyWrapper: bytes | None = ...,
    ) -> None: ...

class PrivateKeyWrapper(Message, _MessageProto):
    __slots__ = ("privateKey", "publicKey")
    PRIVATEKEY_FIELD_NUMBER: _ClassVar[int]
    PUBLICKEY_FIELD_NUMBER: _ClassVar[int]
    privateKey: bytes
    publicKey: bytes
    def __init__(
        self, privateKey: bytes | None = ..., publicKey: bytes | None = ...
    ) -> None: ...

class UploadScansResponse(Message, _MessageProto):
    __slots__ = ("sightedDeviceInteractions",)
    class SightedDeviceInteraction(Message, _MessageProto):
        __slots__ = ("truncatedEid", "unwantedTrackingKey", "ringKey", "isLost")
        TRUNCATEDEID_FIELD_NUMBER: _ClassVar[int]
        UNWANTEDTRACKINGKEY_FIELD_NUMBER: _ClassVar[int]
        RINGKEY_FIELD_NUMBER: _ClassVar[int]
        ISLOST_FIELD_NUMBER: _ClassVar[int]
        truncatedEid: _containers.RepeatedCompositeFieldContainer[TruncatedEID]
        unwantedTrackingKey: bytes
        ringKey: bytes
        isLost: bool
        def __init__(
            self,
            truncatedEid: _Iterable[TruncatedEID | _Mapping[str, _Any]] | None = ...,
            unwantedTrackingKey: bytes | None = ...,
            ringKey: bytes | None = ...,
            isLost: bool = ...,
        ) -> None: ...

    SIGHTEDDEVICEINTERACTIONS_FIELD_NUMBER: _ClassVar[int]
    sightedDeviceInteractions: _containers.RepeatedCompositeFieldContainer[
        UploadScansResponse.SightedDeviceInteraction
    ]
    def __init__(
        self,
        sightedDeviceInteractions: _Iterable[
            UploadScansResponse.SightedDeviceInteraction | _Mapping[str, _Any]
        ]
        | None = ...,
    ) -> None: ...

class UploadScansRequest(Message, _MessageProto):
    __slots__ = ("reportsEncryptedWithServerKey", "droidGuardResults")
    REPORTSENCRYPTEDWITHSERVERKEY_FIELD_NUMBER: _ClassVar[int]
    DROIDGUARDRESULTS_FIELD_NUMBER: _ClassVar[int]
    reportsEncryptedWithServerKey: bytes
    droidGuardResults: str
    def __init__(
        self,
        reportsEncryptedWithServerKey: bytes | None = ...,
        droidGuardResults: str | None = ...,
    ) -> None: ...

class RotateBleDeviceKeysRequest(Message, _MessageProto):
    __slots__ = ("canonicId", "rotatedUserSecrets")
    class RotatedUserSecrets(Message, _MessageProto):
        __slots__ = (
            "encryptedUserSecrets",
            "publicKeyIdList",
            "pairDate",
            "ringKey",
            "recoveryKey",
            "unwantedTrackingKey",
        )
        ENCRYPTEDUSERSECRETS_FIELD_NUMBER: _ClassVar[int]
        PUBLICKEYIDLIST_FIELD_NUMBER: _ClassVar[int]
        PAIRDATE_FIELD_NUMBER: _ClassVar[int]
        RINGKEY_FIELD_NUMBER: _ClassVar[int]
        RECOVERYKEY_FIELD_NUMBER: _ClassVar[int]
        UNWANTEDTRACKINGKEY_FIELD_NUMBER: _ClassVar[int]
        encryptedUserSecrets: EncryptedUserSecrets
        publicKeyIdList: PublicKeyIdList
        pairDate: int
        ringKey: bytes
        recoveryKey: bytes
        unwantedTrackingKey: bytes
        def __init__(
            self,
            encryptedUserSecrets: EncryptedUserSecrets
            | _Mapping[str, _Any]
            | None = ...,
            publicKeyIdList: PublicKeyIdList | _Mapping[str, _Any] | None = ...,
            pairDate: int | None = ...,
            ringKey: bytes | None = ...,
            recoveryKey: bytes | None = ...,
            unwantedTrackingKey: bytes | None = ...,
        ) -> None: ...

    CANONICID_FIELD_NUMBER: _ClassVar[int]
    ROTATEDUSERSECRETS_FIELD_NUMBER: _ClassVar[int]
    canonicId: CanonicId
    rotatedUserSecrets: RotateBleDeviceKeysRequest.RotatedUserSecrets
    def __init__(
        self,
        canonicId: CanonicId | _Mapping[str, _Any] | None = ...,
        rotatedUserSecrets: RotateBleDeviceKeysRequest.RotatedUserSecrets
        | _Mapping[str, _Any]
        | None = ...,
    ) -> None: ...

class UpdateBleDeviceRequest(Message, _MessageProto):
    __slots__ = ("value", "scope")
    class UpdateBleDeviceValue(Message, _MessageProto):
        __slots__ = ("canonicId", "description", "encryptedUserSecrets", "lostMode")
        class LostModeValue(Message, _MessageProto):
            __slots__ = ("enableLostMode",)
            ENABLELOSTMODE_FIELD_NUMBER: _ClassVar[int]
            enableLostMode: bool
            def __init__(self, enableLostMode: bool = ...) -> None: ...

        CANONICID_FIELD_NUMBER: _ClassVar[int]
        DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
        ENCRYPTEDUSERSECRETS_FIELD_NUMBER: _ClassVar[int]
        LOSTMODE_FIELD_NUMBER: _ClassVar[int]
        canonicId: CanonicId
        description: DeviceDescription
        encryptedUserSecrets: EncryptedUserSecrets
        lostMode: UpdateBleDeviceRequest.UpdateBleDeviceValue.LostModeValue
        def __init__(
            self,
            canonicId: CanonicId | _Mapping[str, _Any] | None = ...,
            description: DeviceDescription | _Mapping[str, _Any] | None = ...,
            encryptedUserSecrets: EncryptedUserSecrets
            | _Mapping[str, _Any]
            | None = ...,
            lostMode: UpdateBleDeviceRequest.UpdateBleDeviceValue.LostModeValue
            | _Mapping[str, _Any]
            | None = ...,
        ) -> None: ...

    class UpdateBleDeviceScope(Message, _MessageProto):
        __slots__ = ("scope",)
        SCOPE_FIELD_NUMBER: _ClassVar[int]
        scope: str
        def __init__(self, scope: str | None = ...) -> None: ...

    VALUE_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    value: UpdateBleDeviceRequest.UpdateBleDeviceValue
    scope: UpdateBleDeviceRequest.UpdateBleDeviceScope
    def __init__(
        self,
        value: UpdateBleDeviceRequest.UpdateBleDeviceValue
        | _Mapping[str, _Any]
        | None = ...,
        scope: UpdateBleDeviceRequest.UpdateBleDeviceScope
        | _Mapping[str, _Any]
        | None = ...,
    ) -> None: ...

class TrackerIdentifier(Message, _MessageProto):
    __slots__ = ("truncatedEid", "canonicDeviceId")
    TRUNCATEDEID_FIELD_NUMBER: _ClassVar[int]
    CANONICDEVICEID_FIELD_NUMBER: _ClassVar[int]
    truncatedEid: bytes
    canonicDeviceId: bytes
    def __init__(
        self,
        truncatedEid: bytes | None = ...,
        canonicDeviceId: bytes | None = ...,
    ) -> None: ...

class GetScannedDeviceInfoRequest(Message, _MessageProto):
    __slots__ = ("identifier", "languageTag")
    IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    LANGUAGETAG_FIELD_NUMBER: _ClassVar[int]
    identifier: TrackerIdentifier
    languageTag: str
    def __init__(
        self,
        identifier: TrackerIdentifier | _Mapping[str, _Any] | None = ...,
        languageTag: str | None = ...,
    ) -> None: ...

class GetScannedDeviceInfoResponse(Message, _MessageProto):
    __slots__ = (
        "trackableComponent",
        "manufacturerName",
        "deviceComponentsInformation",
        "modelName",
        "disableTrackerInstructionsUrl",
        "enableOwnerInformationModeInstructions",
    )
    TRACKABLECOMPONENT_FIELD_NUMBER: _ClassVar[int]
    MANUFACTURERNAME_FIELD_NUMBER: _ClassVar[int]
    DEVICECOMPONENTSINFORMATION_FIELD_NUMBER: _ClassVar[int]
    MODELNAME_FIELD_NUMBER: _ClassVar[int]
    DISABLETRACKERINSTRUCTIONSURL_FIELD_NUMBER: _ClassVar[int]
    ENABLEOWNERINFORMATIONMODEINSTRUCTIONS_FIELD_NUMBER: _ClassVar[int]
    trackableComponent: int
    manufacturerName: str
    deviceComponentsInformation: _containers.RepeatedCompositeFieldContainer[
        DeviceComponentInformation
    ]
    modelName: str
    disableTrackerInstructionsUrl: str
    enableOwnerInformationModeInstructions: _containers.RepeatedScalarFieldContainer[
        str
    ]
    def __init__(
        self,
        trackableComponent: int | None = ...,
        manufacturerName: str | None = ...,
        deviceComponentsInformation: _Iterable[
            DeviceComponentInformation | _Mapping[str, _Any]
        ]
        | None = ...,
        modelName: str | None = ...,
        disableTrackerInstructionsUrl: str | None = ...,
        enableOwnerInformationModeInstructions: _Iterable[str] | None = ...,
    ) -> None: ...

class GooglePublicKeys(Message, _MessageProto):
    __slots__ = ("eciesAeadHkdfPublicKeys", "ecdsaPublicKeys")
    class EciesAeadHkdfPublicKeys(Message, _MessageProto):
        __slots__ = ("keys",)
        class EciesAeadHkdfPublicKey(Message, _MessageProto):
            __slots__ = ("keyInformation", "keyIdentifier")
            class EciesAeadHkdfPublicKeyInformation(Message, _MessageProto):
                __slots__ = ("name", "material")
                class EciesAeadHkdfPublicKeyMaterial(Message, _MessageProto):
                    __slots__ = ("x", "y")
                    X_FIELD_NUMBER: _ClassVar[int]
                    Y_FIELD_NUMBER: _ClassVar[int]
                    x: bytes
                    y: bytes
                    def __init__(
                        self, x: bytes | None = ..., y: bytes | None = ...
                    ) -> None: ...

                NAME_FIELD_NUMBER: _ClassVar[int]
                MATERIAL_FIELD_NUMBER: _ClassVar[int]
                name: str
                material: GooglePublicKeys.EciesAeadHkdfPublicKeys.EciesAeadHkdfPublicKey.EciesAeadHkdfPublicKeyInformation.EciesAeadHkdfPublicKeyMaterial
                def __init__(
                    self,
                    name: str | None = ...,
                    material: GooglePublicKeys.EciesAeadHkdfPublicKeys.EciesAeadHkdfPublicKey.EciesAeadHkdfPublicKeyInformation.EciesAeadHkdfPublicKeyMaterial
                    | _Mapping[str, _Any]
                    | None = ...,
                ) -> None: ...

            KEYINFORMATION_FIELD_NUMBER: _ClassVar[int]
            KEYIDENTIFIER_FIELD_NUMBER: _ClassVar[int]
            keyInformation: GooglePublicKeys.EciesAeadHkdfPublicKeys.EciesAeadHkdfPublicKey.EciesAeadHkdfPublicKeyInformation
            keyIdentifier: int
            def __init__(
                self,
                keyInformation: GooglePublicKeys.EciesAeadHkdfPublicKeys.EciesAeadHkdfPublicKey.EciesAeadHkdfPublicKeyInformation
                | _Mapping[str, _Any]
                | None = ...,
                keyIdentifier: int | None = ...,
            ) -> None: ...

        KEYS_FIELD_NUMBER: _ClassVar[int]
        keys: _containers.RepeatedCompositeFieldContainer[
            GooglePublicKeys.EciesAeadHkdfPublicKeys.EciesAeadHkdfPublicKey
        ]
        def __init__(
            self,
            keys: _Iterable[
                GooglePublicKeys.EciesAeadHkdfPublicKeys.EciesAeadHkdfPublicKey
                | _Mapping[str, _Any]
            ]
            | None = ...,
        ) -> None: ...

    class EcdsaPublicKeys(Message, _MessageProto):
        __slots__ = ()
        def __init__(self) -> None: ...

    ECIESAEADHKDFPUBLICKEYS_FIELD_NUMBER: _ClassVar[int]
    ECDSAPUBLICKEYS_FIELD_NUMBER: _ClassVar[int]
    eciesAeadHkdfPublicKeys: GooglePublicKeys.EciesAeadHkdfPublicKeys
    ecdsaPublicKeys: GooglePublicKeys.EcdsaPublicKeys
    def __init__(
        self,
        eciesAeadHkdfPublicKeys: GooglePublicKeys.EciesAeadHkdfPublicKeys
        | _Mapping[str, _Any]
        | None = ...,
        ecdsaPublicKeys: GooglePublicKeys.EcdsaPublicKeys
        | _Mapping[str, _Any]
        | None = ...,
    ) -> None: ...

class FastPairGetObservedDeviceRequest(Message, _MessageProto):
    __slots__ = ("fastPairModelId", "language", "metadata")
    class FastPairGetObservedDeviceRequestMetadata(Message, _MessageProto):
        __slots__ = ("playServices",)
        class FastPairGetObservedDeviceRequestMetadataPlayServices(
            Message, _MessageProto
        ):
            __slots__ = ("version",)
            VERSION_FIELD_NUMBER: _ClassVar[int]
            version: int
            def __init__(self, version: int | None = ...) -> None: ...

        PLAYSERVICES_FIELD_NUMBER: _ClassVar[int]
        playServices: FastPairGetObservedDeviceRequest.FastPairGetObservedDeviceRequestMetadata.FastPairGetObservedDeviceRequestMetadataPlayServices
        def __init__(
            self,
            playServices: FastPairGetObservedDeviceRequest.FastPairGetObservedDeviceRequestMetadata.FastPairGetObservedDeviceRequestMetadataPlayServices
            | _Mapping[str, _Any]
            | None = ...,
        ) -> None: ...

    FASTPAIRMODELID_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    fastPairModelId: int
    language: str
    metadata: FastPairGetObservedDeviceRequest.FastPairGetObservedDeviceRequestMetadata
    def __init__(
        self,
        fastPairModelId: int | None = ...,
        language: str | None = ...,
        metadata: FastPairGetObservedDeviceRequest.FastPairGetObservedDeviceRequestMetadata
        | _Mapping[str, _Any]
        | None = ...,
    ) -> None: ...

class FastPairGetObservedDeviceResponse(Message, _MessageProto):
    __slots__ = ("deviceInformation",)
    class FastPairGetObservedDeviceResponseDeviceInformation(Message, _MessageProto):
        __slots__ = (
            "fastPairModelId",
            "deviceImage",
            "modelName",
            "keys",
            "manufacturerName",
        )
        class FastPairAntiSpoofingKeys(Message, _MessageProto):
            __slots__ = ("publicKey",)
            PUBLICKEY_FIELD_NUMBER: _ClassVar[int]
            publicKey: bytes
            def __init__(self, publicKey: bytes | None = ...) -> None: ...

        FASTPAIRMODELID_FIELD_NUMBER: _ClassVar[int]
        DEVICEIMAGE_FIELD_NUMBER: _ClassVar[int]
        MODELNAME_FIELD_NUMBER: _ClassVar[int]
        KEYS_FIELD_NUMBER: _ClassVar[int]
        MANUFACTURERNAME_FIELD_NUMBER: _ClassVar[int]
        fastPairModelId: int
        deviceImage: str
        modelName: str
        keys: FastPairGetObservedDeviceResponse.FastPairGetObservedDeviceResponseDeviceInformation.FastPairAntiSpoofingKeys
        manufacturerName: str
        def __init__(
            self,
            fastPairModelId: int | None = ...,
            deviceImage: str | None = ...,
            modelName: str | None = ...,
            keys: FastPairGetObservedDeviceResponse.FastPairGetObservedDeviceResponseDeviceInformation.FastPairAntiSpoofingKeys
            | _Mapping[str, _Any]
            | None = ...,
            manufacturerName: str | None = ...,
        ) -> None: ...

    DEVICEINFORMATION_FIELD_NUMBER: _ClassVar[int]
    deviceInformation: FastPairGetObservedDeviceResponse.FastPairGetObservedDeviceResponseDeviceInformation
    def __init__(
        self,
        deviceInformation: FastPairGetObservedDeviceResponse.FastPairGetObservedDeviceResponseDeviceInformation
        | _Mapping[str, _Any]
        | None = ...,
    ) -> None: ...
