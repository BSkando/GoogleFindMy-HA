# tests/test_unknown_field_tree.py
"""Limits of the unknown-field tree diagnostic (``AGENTS.md`` 5, Schema-drift).

``custom_components/googlefindmy/unknown_field_tree.py`` reports protobuf
fields the vendored schema does not know, at DEBUG only, on the logger
``custom_components.googlefindmy.unknown_fields``. These tests pin what one
line may show; ``AGENTS.md`` section 5 points here, so a limit changes only
together with this file. Expected strings are derived from
``ProtoDecoders/DeviceUpdate.proto`` and ``Common.proto``, never from the
helper. Every field number used as "unknown" passes through ``_free``, which
fails when the schema declares that number (a later schema addition then
breaks the fixture loudly instead of silently turning it into a known field).

An autouse fixture empties the signature store before every test, so each
case starts from a first sighting.

API pinned here:

* ``describe_unknown_fields(message) -> list[tuple[str, str, str]]``: pure;
  one ``(path, wire, display)`` per unknown field, in walk order (the
  message's own unknown fields in wire order, then its set known fields in
  ``ListFields`` order). It is not capped; the cap belongs to the line.
* ``log_unknown_fields(kind, device_ref, label, messages) -> None``:
  ``messages`` is a sequence of messages of one response for one device
  (merged, CA-F5). First statement ``isEnabledFor(DEBUG)``. It calls
  ``describe_unknown_fields`` through the module global for every message, so
  that a patched walker is the one that runs. Signature key is
  ``(kind, device_ref)``; ``device_ref`` is never logged. Success line:
  ``_LOGGER.debug("Unknown protobuf fields (%s) for %s: %s", kind, label,
  line)``, exactly three arguments. Any ``Exception`` is swallowed and
  reported as one DEBUG record on the same logger that names the exception
  class (recommended ``"Unknown protobuf fields (%s) skipped: %s"`` with
  ``kind`` and ``type(err).__name__``), never its text.
* ``_reset_signatures_for_tests() -> None`` empties the LRU.

Format decisions (where the plan left a choice, decided once for all cases):

* Path: ``DESCRIPTOR.name`` of the message handed in, then field names joined
  by ``.``; a repeated field adds ``[]`` without index; an unknown field is
  ``#<number>``; a known ``bytes`` field opened through ``_FORM_BY_FIELD``
  keeps its field name (``supportedActions[]``, ``firstSecretsCreationDate``).
* Entry: ``<path>=<display>``; entries joined by ``"; "``; no entries gives
  ``(none)``; more than ``_MAX_ENTRIES`` gives the first 64 and a last item
  ``+<rest>``.
* ``wire`` is one of ``varint``, ``fixed32``, ``fixed64``, ``len``, ``group``.
* Display: ``varint:<v>`` for ``v <= 255``, else ``varint:>255``;
  ``fixed32`` and ``fixed64`` alone; ``len=<n>``, plus `` text`` for printable
  UTF-8 (non-empty, strict decode, ``str.isprintable``), else plus
  `` msg{...}`` when the bytes parse cleanly as one schemaless level, else
  nothing more. Text wins over ``msg``. ``len=0`` stays ``len=0``. A group is
  ``group`` plus `` msg{...}`` like a length-delimited field. A known bytes
  field whose declared form fails to parse is ``form-mismatch len=<n>``
  (wire ``len``).
* Schemaless subtree (V-5, ``_MAX_UNKNOWN_DEPTH = 1``): items joined by
  ``", "``, each ``#<nr>=varint``, ``#<nr>=fixed32``, ``#<nr>=fixed64``,
  ``#<nr>=len=<m> depth-limit`` or ``#<nr>=group depth-limit``: no value, no
  text marker, no further level.
* Closed messages (``_CLOSED_MESSAGES``, inherited by every message below
  them): unknown varints as above, fixed as above, a length-delimited field or
  group only ``len=<n>`` / ``group`` (no text marker, no subtree).
* Signature: multiset of ``(path, wire, value)``; the value is the varint for
  ``v <= 255`` and otherwise absent; lengths (also inside a subtree), ``>255``
  values and order do not count.
* Kinds used by the four call sites: ``device_list`` (per device, label
  ``userDefinedDeviceName``), ``device_update`` (label ditto), ``location``
  (label the device name of the ``DeviceUpdate``), ``eid_response`` (label
  ``account``, key only the kind).
"""

from __future__ import annotations

import logging
import re
import secrets
import struct
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, cast

import pytest

from custom_components.googlefindmy import unknown_field_tree as uft
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker import (
    decrypt_locations,
)
from custom_components.googlefindmy.ProtoDecoders import (
    Common_pb2,
    DeviceUpdate_pb2,
    decoder,
)
from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices import (
    get_eid_info_request,
)
from google.protobuf.message import Message

_LOGGER_NAME = "custom_components.googlefindmy.unknown_fields"
_FORMAT = "Unknown protobuf fields (%s) for %s: %s"
_PROTO = (
    Path(__file__).resolve().parents[1]
    / "custom_components"
    / "googlefindmy"
    / "ProtoDecoders"
    / "DeviceUpdate.proto"
)

_RECENT_PATH = (
    "DeviceUpdate.deviceMetadata.information.locationInformation.reports."
    "recentLocationAndNetworkLocations"
)

# --------------------------------------------------------------------------
# Wire builders and fixtures
# --------------------------------------------------------------------------


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        low = value & 0x7F
        value >>= 7
        if value:
            out.append(low | 0x80)
        else:
            out.append(low)
            return bytes(out)


def _tag(number: int, wire_type: int) -> bytes:
    return _varint((number << 3) | wire_type)


def _vf(number: int, value: int) -> bytes:
    return _tag(number, 0) + _varint(value)


def _ld(number: int, payload: bytes) -> bytes:
    return _tag(number, 2) + _varint(len(payload)) + payload


def _fixed32(number: int, payload: bytes) -> bytes:
    assert len(payload) == 4
    return _tag(number, 5) + payload


def _fixed64(number: int, payload: bytes) -> bytes:
    assert len(payload) == 8
    return _tag(number, 1) + payload


def _group(number: int, payload: bytes) -> bytes:
    return _tag(number, 3) + payload + _tag(number, 4)


def _free(message_type: type[Message], number: int) -> int:
    """Return ``number`` after checking that the schema does not declare it."""
    declared = message_type.DESCRIPTOR.fields_by_number
    assert number not in declared, (
        f"{message_type.DESCRIPTOR.full_name} declares #{number} now; "
        "pick another free number for this fixture"
    )
    return number


def _reparse[M: Message](message: M, extra: bytes) -> M:
    """Copy ``message`` with ``extra`` wire bytes appended (unknown fields)."""
    result = type(message)()
    result.ParseFromString(message.SerializeToString() + extra)
    return result


def _fake_cipher() -> tuple[bytes, int]:
    """Random bytes that parse cleanly as protobuf wire data.

    Ciphertext parses as protobuf in up to 1.33 % of cases (EE-10); this one
    always does. Returns the bytes and the large varint they carry.
    """
    big = (1 << 40) + secrets.randbelow(1 << 20)
    return _vf(1, big) + _ld(2, secrets.token_bytes(16)), big


@pytest.fixture(autouse=True)
def _reset_signatures() -> None:
    uft._reset_signatures_for_tests()


def _render(entries: Iterable[tuple[str, str, str]]) -> list[str]:
    return [f"{path}={display}" for path, _wire, display in entries]


def _records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == _LOGGER_NAME and r.msg == _FORMAT]


def _lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in _records(caplog)]


def _payloads(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [str(cast(tuple[object, ...], r.args)[2]) for r in _records(caplog)]


def _debug(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)


def _windows(value: str, size: int = 8) -> list[str]:
    return [value[i : i + size] for i in range(len(value) - size + 1)]


def _assert_absent(
    text: str,
    *,
    raw: Sequence[bytes] = (),
    words: Sequence[str] = (),
    numbers: Sequence[int] = (),
) -> None:
    """No 8-character window of any secret, no decimal of any large number."""
    for blob in raw:
        for window in _windows(blob.hex()):
            assert window not in text, "a hex window of payload bytes is in the log"
        for window in _windows(repr(blob)[2:-1]):
            assert window not in text, "a repr window of payload bytes is in the log"
    for word in words:
        for window in _windows(word):
            assert window not in text, "a window of a text payload is in the log"
    for number in numbers:
        assert str(number) not in text, "a value above 255 is in the log"


def _device(name: str, canonic_id: str, extra: bytes = b"") -> Any:
    device = DeviceUpdate_pb2.DeviceMetadata()
    device.userDefinedDeviceName = name
    device.identifierInformation.canonicIds.canonicId.add().id = canonic_id
    return _reparse(device, extra)


def _supported_action(action_number: int, flag: int = 1) -> bytes:
    return _ld(1, _ld(action_number, b"")) + _vf(2, flag)


def _locate_action() -> bytes:
    action = DeviceUpdate_pb2.DeviceMetadata.SupportedActions()
    action.type.locateTracker.SetInParent()
    action.supportedActionsField2 = 1
    return action.SerializeToString()


def _time_bytes(seconds: int, nanos: int) -> bytes:
    return Common_pb2.Time(seconds=seconds, nanos=nanos).SerializeToString()


def build_device_list(devices: int) -> Any:
    """A ``DevicesList`` shaped like the live list of 2026-11-10 (EE-15).

    Every field that was unknown on 2026-11-10 is present per device with its
    live wire type and length (``wiretypen.txt``); since commit 6c3e2697 the
    schema declares them, so they are walked as known fields. Two fields stay
    unknown per device so that the whole log path runs:
    ``DeviceMetadata.#99=varint:1`` and
    ``DeviceMetadata.information.deviceRegistration.#98=len=2 msg{#1=varint}``.
    Used by the cost measurement in AP12 (K7); attrappes are random per call.
    """
    device_list = DeviceUpdate_pb2.DevicesList()
    for index in range(devices):
        device = DeviceUpdate_pb2.DeviceMetadata()
        device.userDefinedDeviceName = f"Test device {index:02d}"
        device.identifierInformation.canonicIds.canonicId.add().id = secrets.token_hex(
            16
        )
        device.identifierInformation.identifierField5 = _ld(
            1, secrets.token_bytes(20)
        ) + _ld(2, secrets.token_bytes(18))
        device.metadataField8 = 6 + index % 6
        if index % 6 == 0:
            device.metadataField10 = _ld(1, secrets.token_bytes(400)) + _ld(
                2, secrets.token_bytes(390)
            )
        device.metadataField11 = secrets.token_urlsafe(33).encode()
        device.metadataField12 = _time_bytes(1_700_000_000 + index, 123_456_789)
        device.metadataField13 = secrets.token_hex(16).encode()
        device.supportedActions.append(_locate_action())
        device.supportedActions.append(_supported_action(31))
        device.supportedActions.append(_supported_action(13))
        image = DeviceUpdate_pb2.ImageInformation.ComponentImage(
            imageUrl=f"https://example.invalid/images/component-{index:02d}.png"
        )
        device.imageInformation.componentImages.append(image.SerializeToString())

        registration = device.information.deviceRegistration
        registration.registrationField15 = secrets.token_urlsafe(27).encode()
        registration.registrationField22 = 1
        registration.registrationField38 = secrets.token_urlsafe(5).encode()
        registration.registrationField40 = _time_bytes(
            1_700_000_000 + index, 300_000_000
        )
        registration.firstSecretsCreationDate = _time_bytes(
            1_690_000_000 + index, 300_000_000
        )
        registration.capabilities = DeviceUpdate_pb2.DeviceCapabilities(
            isAdvertising=True,
            ringingVolumeSupported=True,
            capableComponents=1,
            trackableComponents=1,
        ).SerializeToString()
        type_info = registration.deviceTypeInformation
        type_info.typeInformationField14 = 2
        component = DeviceUpdate_pb2.DeviceComponentInformation(
            imageUrl=f"https://example.invalid/images/part-{index:02d}.png",
            deviceComponent=DeviceUpdate_pb2.DEVICE_COMPONENT_RIGHT,
        ).SerializeToString()
        type_info.deviceComponentsInformation.append(component)
        type_info.componentDetails.append(
            DeviceUpdate_pb2.DeviceComponentDetail(
                component=DeviceUpdate_pb2.DEVICE_COMPONENT_RIGHT, detail=component
            ).SerializeToString()
        )

        reports = device.information.locationInformation.reports
        reports.reportsField5 = 1
        recent = reports.recentLocationAndNetworkLocations
        recent.recentField8.extend([1, 2, 3])
        recent.recentField11.extend([2, 3, 4])

        registration_extra = _ld(
            _free(DeviceUpdate_pb2.DeviceRegistration, 98), _vf(1, 7)
        )
        registration.CopyFrom(_reparse(registration, registration_extra))
        device = _reparse(device, _vf(_free(DeviceUpdate_pb2.DeviceMetadata, 99), 1))
        device_list.deviceMetadata.add().CopyFrom(device)
    return device_list


# --------------------------------------------------------------------------
# K1: path of field names and #number, no depth limit for known fields
# --------------------------------------------------------------------------


def test_k1_depth5_path_names_and_number() -> None:
    number = _free(DeviceUpdate_pb2.RecentLocationAndNetworkLocations, 12)
    update = DeviceUpdate_pb2.DeviceUpdate()
    recent = update.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations
    recent.CopyFrom(_reparse(recent, _vf(number, 3)))
    update = _reparse(update, b"")

    entries = uft.describe_unknown_fields(update)

    assert entries == [(f"{_RECENT_PATH}.#12", "varint", "varint:3")]
    assert _render(entries) == [f"{_RECENT_PATH}.#12=varint:3"]


def test_k1_known_descent_has_no_depth_limit() -> None:
    number = _free(Common_pb2.SemanticLocation, 9)
    update = DeviceUpdate_pb2.DeviceUpdate()
    semantic = update.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations.recentLocation.semanticLocation
    semantic.locationName = "Home"
    semantic.CopyFrom(_reparse(semantic, _vf(number, 1)))

    assert _render(uft.describe_unknown_fields(update)) == [
        f"{_RECENT_PATH}.recentLocation.semanticLocation.#9=varint:1"
    ]


def test_k1_repeated_field_brackets_without_index() -> None:
    number = _free(Common_pb2.LocationReport, 12)
    update = DeviceUpdate_pb2.DeviceUpdate()
    recent = update.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations
    for _ in range(2):
        report = Common_pb2.LocationReport(status=Common_pb2.Status.CROWDSOURCED)
        recent.networkLocations.add().CopyFrom(_reparse(report, _vf(number, 1)))

    assert _render(uft.describe_unknown_fields(update)) == [
        f"{_RECENT_PATH}.networkLocations[].#12=varint:1",
        f"{_RECENT_PATH}.networkLocations[].#12=varint:1",
    ]


def test_k1_line_carries_full_path(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    number = _free(DeviceUpdate_pb2.RecentLocationAndNetworkLocations, 12)
    update = DeviceUpdate_pb2.DeviceUpdate()
    recent = update.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations
    recent.CopyFrom(_reparse(recent, _vf(number, 3)))

    uft.log_unknown_fields("device_update", secrets.token_hex(16), "Tag", [update])

    assert _lines(caplog) == [
        f"Unknown protobuf fields (device_update) for Tag: {_RECENT_PATH}.#12=varint:3"
    ]


# --------------------------------------------------------------------------
# K2: values at an unknown field of a known message
# --------------------------------------------------------------------------


@pytest.mark.parametrize("value", [0, 7, 255])
def test_k2_varint_up_to_255_shows_value(value: int) -> None:
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), _vf(number, value))

    assert uft.describe_unknown_fields(device) == [
        ("DeviceMetadata.#99", "varint", f"varint:{value}")
    ]


@pytest.mark.parametrize("value", [256, (1 << 64) - 1])
def test_k2_varint_above_255_shows_marker(value: int) -> None:
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), _vf(number, value))

    entries = uft.describe_unknown_fields(device)

    assert entries == [("DeviceMetadata.#99", "varint", "varint:>255")]
    _assert_absent(repr(entries), numbers=[value])


def test_k2_fixed32_shows_no_value() -> None:
    payload = secrets.token_bytes(4)
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), _fixed32(number, payload))

    entries = uft.describe_unknown_fields(device)

    assert entries == [("DeviceMetadata.#99", "fixed32", "fixed32")]
    unsigned = int.from_bytes(payload, "little")
    signed = int.from_bytes(payload, "little", signed=True)
    as_float = repr(struct.unpack("<f", payload)[0])
    text = repr(entries)
    assert payload.hex() not in text
    assert str(unsigned) not in text
    assert str(signed) not in text
    assert as_float not in text


def test_k2_fixed64_shows_no_value() -> None:
    payload = secrets.token_bytes(8)
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), _fixed64(number, payload))

    entries = uft.describe_unknown_fields(device)

    assert entries == [("DeviceMetadata.#99", "fixed64", "fixed64")]
    unsigned = int.from_bytes(payload, "little")
    signed = int.from_bytes(payload, "little", signed=True)
    as_float = repr(struct.unpack("<d", payload)[0])
    text = repr(entries)
    _assert_absent(text, raw=[payload], numbers=[unsigned])
    assert str(signed) not in text
    assert as_float not in text


def test_k2_text_shows_length_and_marker_only() -> None:
    word = secrets.token_urlsafe(24)
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), _ld(number, word.encode()))

    entries = uft.describe_unknown_fields(device)

    assert entries == [("DeviceMetadata.#99", "len", f"len={len(word)} text")]
    _assert_absent(repr(entries), words=[word])


def test_k2_unparseable_bytes_show_length_only() -> None:
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), _ld(number, b"\xff" * 5))

    assert uft.describe_unknown_fields(device) == [
        ("DeviceMetadata.#99", "len", "len=5")
    ]


def test_k2_group_like_length_delimited() -> None:
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), _group(number, _vf(1, 7)))

    assert uft.describe_unknown_fields(device) == [
        ("DeviceMetadata.#99", "group", "group msg{#1=varint}")
    ]


def test_k2_65_fields_give_64_entries_and_rest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _debug(caplog)
    assert uft._MAX_ENTRIES == 64
    extra = b"".join(
        _vf(_free(DeviceUpdate_pb2.DeviceMetadata, number), 1)
        for number in range(100, 165)
    )
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), extra)
    assert len(uft.describe_unknown_fields(device)) == 65

    uft.log_unknown_fields("device_list", secrets.token_hex(16), "Tag", [device])

    payloads = _payloads(caplog)
    assert len(payloads) == 1
    items = payloads[0].split("; ")
    assert len(items) == 65
    assert items[-1] == "+1"
    assert all(re.fullmatch(r"DeviceMetadata\.#\d+=varint:1", i) for i in items[:-1])


def test_k2_constant_varint_threshold() -> None:
    assert uft._VARINT_VALUE_MAX == 255


# --------------------------------------------------------------------------
# K3: known bytes fields opened only through _FORM_BY_FIELD
# --------------------------------------------------------------------------

_EXPECTED_FORM_FIELDS = frozenset(
    {
        "DeviceMetadata.supportedActions",
        "ImageInformation.componentImages",
        "DeviceTypeInformation.deviceComponentsInformation",
        "DeviceTypeInformation.componentDetails",
        "DeviceComponentDetail.detail",
        "DeviceRegistration.capabilities",
        "DeviceRegistration.firstSecretsCreationDate",
        "DeviceRegistration.lostModeInformation",
        "DeviceMetadata.metadataField12",
        "DeviceRegistration.registrationField40",
    }
)


def _proto_forms() -> dict[str, str]:
    """``Message.field`` -> first word after ``form:`` in the field's comment."""
    forms: dict[str, str] = {}
    stack: list[str | None] = []
    opener = re.compile(r"^\s*(message|enum)\s+(\w+)\s*\{")
    field = re.compile(
        r"^\s*(?:repeated\s+|optional\s+)?[\w.]+\s+(\w+)\s*=\s*\d+\s*;.*\bform:\s*([\w.]+)"
    )
    for line in _PROTO.read_text(encoding="utf-8").splitlines():
        code = line.split("//", 1)[0]
        match = opener.match(line)
        if match:
            stack.append(match.group(2) if match.group(1) == "message" else None)
            continue
        found = field.match(line)
        if found and stack and all(part is not None for part in stack):
            owner = ".".join(cast(list[str], stack))
            forms[f"{owner}.{found.group(1)}"] = found.group(2)
        if code.strip() == "}" and stack:
            stack.pop()
    return forms


def test_k3_form_by_field_lists_exactly_the_structural_fields() -> None:
    assert set(uft._FORM_BY_FIELD) == _EXPECTED_FORM_FIELDS


def test_k3_form_by_field_matches_proto_comments() -> None:
    forms = _proto_forms()
    table = uft._FORM_BY_FIELD
    for name, form in table.items():
        assert name in forms, f"{name} has no form: comment in DeviceUpdate.proto"
        assert forms[name] == form, f"{name}: table {form}, proto {forms[name]}"


def test_k3_known_forms_produce_no_entries() -> None:
    device = DeviceUpdate_pb2.DeviceMetadata()
    device.information.deviceRegistration.capabilities = (
        DeviceUpdate_pb2.DeviceCapabilities(
            isAdvertising=True,
            ringingVolumeSupported=True,
            capableComponents=1,
            trackableComponents=1,
        ).SerializeToString()
    )
    device.supportedActions.append(_locate_action())

    assert uft.describe_unknown_fields(device) == []


def test_k3_unknown_action_shows_path() -> None:
    action = _free(DeviceUpdate_pb2.ExecuteActionType, 99)
    device = DeviceUpdate_pb2.DeviceMetadata()
    device.supportedActions.append(_locate_action())
    device.supportedActions.append(_supported_action(action))

    assert uft.describe_unknown_fields(device) == [
        ("DeviceMetadata.supportedActions[].type.#99", "len", "len=0")
    ]


def test_k3_form_mismatch_shows_length_only() -> None:
    device = DeviceUpdate_pb2.DeviceMetadata()
    device.supportedActions.append(b"\xff\xff\xff")

    entries = uft.describe_unknown_fields(device)

    assert entries == [
        ("DeviceMetadata.supportedActions[]", "len", "form-mismatch len=3")
    ]
    assert "ff" not in repr(entries)


def test_k3_nested_forms_report_unknown_inside() -> None:
    number = _free(DeviceUpdate_pb2.DeviceComponentInformation, 7)
    detail = DeviceUpdate_pb2.DeviceComponentInformation(
        imageUrl="https://example.invalid/images/part.png"
    ).SerializeToString() + _vf(number, 1)
    device = DeviceUpdate_pb2.DeviceMetadata()
    device.information.deviceRegistration.deviceTypeInformation.componentDetails.append(
        DeviceUpdate_pb2.DeviceComponentDetail(
            component=DeviceUpdate_pb2.DEVICE_COMPONENT_LEFT, detail=detail
        ).SerializeToString()
    )

    assert _render(uft.describe_unknown_fields(device)) == [
        "DeviceMetadata.information.deviceRegistration.deviceTypeInformation."
        "componentDetails[].detail.#7=varint:1"
    ]


def test_k3_metadata_field12_opened_with_time() -> None:
    number = _free(Common_pb2.Time, 3)
    device = DeviceUpdate_pb2.DeviceMetadata()
    device.metadataField12 = _time_bytes(1_700_000_000, 5) + _vf(number, 1)

    assert _render(uft.describe_unknown_fields(device)) == [
        "DeviceMetadata.metadataField12.#3=varint:1"
    ]


# --------------------------------------------------------------------------
# K4: key material and identifiers never opened, closed messages inherit
# --------------------------------------------------------------------------


def test_k4_key_field_with_parsing_cipher_not_opened() -> None:
    cipher, big = _fake_cipher()
    update = DeviceUpdate_pb2.DeviceUpdate()
    secrets_block = (
        update.deviceMetadata.information.deviceRegistration.encryptedUserSecrets
    )
    secrets_block.encryptedAccountKey = cipher
    secrets_block.encryptedIdentityKey = _fake_cipher()[0]

    entries = uft.describe_unknown_fields(update)

    assert entries == []
    _assert_absent(repr(entries), raw=[cipher], numbers=[big])


def test_k4_unlisted_bytes_field_not_opened() -> None:
    cipher, _big = _fake_cipher()
    device = DeviceUpdate_pb2.DeviceMetadata()
    device.metadataField10 = cipher + _vf(99, 1)
    device.information.deviceRegistration.canonicId = _ld(1, cipher)

    assert uft.describe_unknown_fields(device) == []


def _eid_with_closed_unknowns(cipher: bytes) -> Any:
    inner = DeviceUpdate_pb2.EncryptedOwnerKeyAndMetadata(ownerKeyVersion=1)
    inner_bytes = (
        inner.SerializeToString()
        + _vf(_free(DeviceUpdate_pb2.EncryptedOwnerKeyAndMetadata, 9), 4)
        + _ld(_free(DeviceUpdate_pb2.EncryptedOwnerKeyAndMetadata, 10), cipher)
    )
    return DeviceUpdate_pb2.GetEidInfoForE2eeDevicesResponse.FromString(
        _ld(4, inner_bytes)
    )


def test_k4_closed_message_shows_varint() -> None:
    cipher, _big = _fake_cipher()

    rendered = _render(uft.describe_unknown_fields(_eid_with_closed_unknowns(cipher)))

    assert (
        "GetEidInfoForE2eeDevicesResponse.encryptedOwnerKeyAndMetadata.#9=varint:4"
        in rendered
    )


def test_k4_closed_message_length_only() -> None:
    cipher, big = _fake_cipher()

    entries = uft.describe_unknown_fields(_eid_with_closed_unknowns(cipher))

    assert (
        "GetEidInfoForE2eeDevicesResponse.encryptedOwnerKeyAndMetadata.#10",
        "len",
        f"len={len(cipher)}",
    ) in entries
    _assert_absent(repr(entries), raw=[cipher], numbers=[big])


def test_k4_closed_message_has_no_text_marker() -> None:
    word = secrets.token_urlsafe(18)
    number = _free(DeviceUpdate_pb2.Location, 9)
    location = _reparse(
        DeviceUpdate_pb2.Location(altitude=3), _ld(number, word.encode())
    )

    entries = uft.describe_unknown_fields(location)

    assert entries == [("Location.#9", "len", f"len={len(word)}")]
    _assert_absent(repr(entries), words=[word])


def test_k4_closed_inherited_by_nested_message() -> None:
    number = _free(Common_pb2.EncryptedReport, 9)
    update = DeviceUpdate_pb2.DeviceUpdate()
    report = update.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations.recentLocation.geoLocation.encryptedReport
    report.isOwnReport = True
    report.CopyFrom(_reparse(report, _ld(number, _vf(1, 7))))

    assert _render(uft.describe_unknown_fields(update)) == [
        f"{_RECENT_PATH}.recentLocation.geoLocation.encryptedReport.#9=len=2"
    ]


def test_k4_first_secrets_creation_date_opened_with_time() -> None:
    number = _free(Common_pb2.Time, 3)
    update = DeviceUpdate_pb2.DeviceUpdate()
    registration = update.deviceMetadata.information.deviceRegistration
    registration.firstSecretsCreationDate = _time_bytes(1_690_000_000, 7) + _vf(
        number, 5
    )

    assert _render(uft.describe_unknown_fields(update)) == [
        "DeviceUpdate.deviceMetadata.information.deviceRegistration."
        "firstSecretsCreationDate.#3=varint:5"
    ]


def test_k4_open_unknown_with_cipher_shows_no_value(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _debug(caplog)
    cipher, big = _fake_cipher()
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(
        DeviceUpdate_pb2.DeviceMetadata(), _ld(number, _ld(19, _ld(1, cipher)))
    )

    uft.log_unknown_fields("device_list", secrets.token_hex(16), "Tag", [device])

    assert len(_lines(caplog)) == 1
    _assert_absent(caplog.text, raw=[cipher], numbers=[big])


def test_k4_form_names_not_in_secret_fields() -> None:
    for name in uft._FORM_BY_FIELD:
        assert name.rsplit(".", 1)[1] not in decoder._SECRET_PROTO_FIELDS, name


def test_k4_closed_messages_listed() -> None:
    closed = uft._CLOSED_MESSAGES
    assert {
        "EncryptedUserSecrets",
        "EncryptedOwnerKeyAndMetadata",
        "SharingKeys",
        "DeviceRegistration.KeyRotationInformation",
        "DeviceRegistration.KeyRotationInformation.RotatedEIK",
        "AccessInformation",
        "DeviceDescription.ContactInfo",
        "GeoLocation",
        "Location",
    } <= set(closed)


# --------------------------------------------------------------------------
# K5: schemaless subtree under an unknown field, no values (V-5)
# --------------------------------------------------------------------------


def test_k5_subtree_without_values() -> None:
    number = _free(DeviceUpdate_pb2.RecentLocationAndNetworkLocations, 12)
    update = DeviceUpdate_pb2.DeviceUpdate()
    recent = update.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations
    recent.CopyFrom(_reparse(recent, _ld(number, _vf(1, 7))))

    assert uft.describe_unknown_fields(update) == [
        (f"{_RECENT_PATH}.#12", "len", "len=2 msg{#1=varint}")
    ]


def test_k5_closed_location_length_only() -> None:
    number = _free(DeviceUpdate_pb2.Location, 9)
    location = _reparse(DeviceUpdate_pb2.Location(altitude=3), _ld(number, _vf(1, 7)))

    assert uft.describe_unknown_fields(location) == [("Location.#9", "len", "len=2")]


def test_k5_depth_limit_on_level_one() -> None:
    cipher, big = _fake_cipher()
    inner = _ld(1, cipher)
    outer = _ld(19, inner)
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), _ld(number, outer))

    entries = uft.describe_unknown_fields(device)

    assert entries == [
        (
            "DeviceMetadata.#99",
            "len",
            f"len={len(outer)} msg{{#19=len={len(inner)} depth-limit}}",
        )
    ]
    _assert_absent(repr(entries), raw=[cipher], numbers=[big])


def test_k5_subtree_has_no_text_marker() -> None:
    word = secrets.token_urlsafe(9)
    payload = _vf(1, 200) + _ld(3, word.encode()) + _fixed32(4, secrets.token_bytes(4))
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    device = _reparse(DeviceUpdate_pb2.DeviceMetadata(), _ld(number, payload))

    entries = uft.describe_unknown_fields(device)

    assert entries == [
        (
            "DeviceMetadata.#99",
            "len",
            f"len={len(payload)} msg{{#1=varint, #3=len={len(word)} depth-limit, #4=fixed32}}",
        )
    ]
    _assert_absent(repr(entries), words=[word])
    assert "200" not in repr(entries)


def test_k5_constant_unknown_depth() -> None:
    assert uft._MAX_UNKNOWN_DEPTH == 1


# --------------------------------------------------------------------------
# K6: one line per signature change, keyed by (kind, device id), LRU 256
# --------------------------------------------------------------------------


def _time_like_unknown(nanos: int) -> tuple[Any, int]:
    payload = _vf(1, 1_700_000_000) + _vf(2, nanos)
    number = _free(DeviceUpdate_pb2.DeviceMetadata, 99)
    return _reparse(DeviceUpdate_pb2.DeviceMetadata(), _ld(number, payload)), len(
        payload
    )


def test_k6_length_change_is_silent(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    ref = secrets.token_hex(16)
    first, first_len = _time_like_unknown(123_456_789)
    second, second_len = _time_like_unknown(300_000_000)
    assert (first_len, second_len) == (11, 12)

    uft.log_unknown_fields("device_list", ref, "Tag", [first])
    uft.log_unknown_fields("device_list", ref, "Tag", [second])

    assert _payloads(caplog) == ["DeviceMetadata.#99=len=11 msg{#1=varint, #2=varint}"]


def test_k6_supported_actions_order_is_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _debug(caplog)
    ref = secrets.token_hex(16)
    unknown = _supported_action(_free(DeviceUpdate_pb2.ExecuteActionType, 99))
    forward = DeviceUpdate_pb2.DeviceMetadata()
    forward.supportedActions.extend([unknown, _locate_action()])
    backward = DeviceUpdate_pb2.DeviceMetadata()
    backward.supportedActions.extend([_locate_action(), unknown])

    uft.log_unknown_fields("device_list", ref, "Tag", [forward])
    uft.log_unknown_fields("device_list", ref, "Tag", [backward])

    assert len(_lines(caplog)) == 1


def _recent_with_varint(value: int) -> Any:
    number = _free(DeviceUpdate_pb2.RecentLocationAndNetworkLocations, 12)
    update = DeviceUpdate_pb2.DeviceUpdate()
    recent = update.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations
    recent.CopyFrom(_reparse(recent, _vf(number, value)))
    return update


def test_k6_small_value_change_writes(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    ref = secrets.token_hex(16)

    uft.log_unknown_fields("device_update", ref, "Tag", [_recent_with_varint(2)])
    uft.log_unknown_fields("device_update", ref, "Tag", [_recent_with_varint(3)])

    assert _payloads(caplog) == [
        f"{_RECENT_PATH}.#12=varint:2",
        f"{_RECENT_PATH}.#12=varint:3",
    ]


def test_k6_large_value_change_is_silent(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    ref = secrets.token_hex(16)

    uft.log_unknown_fields("device_update", ref, "Tag", [_recent_with_varint(300)])
    uft.log_unknown_fields("device_update", ref, "Tag", [_recent_with_varint(400)])

    assert _payloads(caplog) == [f"{_RECENT_PATH}.#12=varint:>255"]


def _location(extra: bytes) -> Any:
    return _reparse(DeviceUpdate_pb2.Location(altitude=3), extra)


def test_k6_reports_merged_per_response(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    ref = secrets.token_hex(16)
    marked = _location(_vf(_free(DeviceUpdate_pb2.Location, 9), 1))
    plain = _location(b"")

    uft.log_unknown_fields("location", ref, "Tag", [marked, plain])
    uft.log_unknown_fields("location", ref, "Tag", [plain, marked])
    uft.log_unknown_fields("location", ref, "Tag", [marked, plain])

    assert _payloads(caplog) == ["Location.#9=varint:1"]


def test_k6_signature_counts_multiplicity(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    ref = secrets.token_hex(16)
    marked = _location(_vf(_free(DeviceUpdate_pb2.Location, 9), 1))
    plain = _location(b"")

    uft.log_unknown_fields("location", ref, "Tag", [marked, plain])
    uft.log_unknown_fields("location", ref, "Tag", [marked, marked])

    assert len(_lines(caplog)) == 2


def test_k6_same_label_two_ids_two_lines(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    device = _reparse(
        DeviceUpdate_pb2.DeviceMetadata(),
        _vf(_free(DeviceUpdate_pb2.DeviceMetadata, 99), 1),
    )

    uft.log_unknown_fields("device_list", secrets.token_hex(16), "Keys", [device])
    uft.log_unknown_fields("device_list", secrets.token_hex(16), "Keys", [device])

    assert len(_lines(caplog)) == 2


def test_k6_first_sight_logs_none(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    ref = secrets.token_hex(16)

    uft.log_unknown_fields(
        "device_list", ref, "Tag", [DeviceUpdate_pb2.DeviceMetadata()]
    )
    uft.log_unknown_fields(
        "device_list", ref, "Tag", [DeviceUpdate_pb2.DeviceMetadata()]
    )

    assert _lines(caplog) == ["Unknown protobuf fields (device_list) for Tag: (none)"]


def test_k6_lru_keeps_256(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    assert uft._MAX_SIGNATURES == 256
    device = DeviceUpdate_pb2.DeviceMetadata()
    refs = [secrets.token_hex(16) for _ in range(256)]
    for ref in refs:
        uft.log_unknown_fields("device_list", ref, "Tag", [device])
    caplog.clear()

    uft.log_unknown_fields("device_list", refs[0], "Tag", [device])

    assert _lines(caplog) == []


def test_k6_lru_evicts_oldest_after_256(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    device = DeviceUpdate_pb2.DeviceMetadata()
    refs = [secrets.token_hex(16) for _ in range(257)]
    for ref in refs:
        uft.log_unknown_fields("device_list", ref, "Tag", [device])
    caplog.clear()

    uft.log_unknown_fields("device_list", refs[-1], "Tag", [device])
    assert _lines(caplog) == []
    uft.log_unknown_fields("device_list", refs[0], "Tag", [device])
    assert len(_lines(caplog)) == 1


def test_k6_no_id_window_in_log(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    ref = secrets.token_hex(16)
    device = _reparse(
        DeviceUpdate_pb2.DeviceMetadata(),
        _vf(_free(DeviceUpdate_pb2.DeviceMetadata, 99), 1),
    )

    uft.log_unknown_fields("device_list", ref, "Tag", [device])
    uft.log_unknown_fields(
        "device_list", ref, "Tag", [DeviceUpdate_pb2.DeviceMetadata()]
    )

    assert len(_lines(caplog)) == 2
    _assert_absent(caplog.text, words=[ref])


# --------------------------------------------------------------------------
# K7: zero cost below DEBUG; shape of the cost-measurement fixture
# --------------------------------------------------------------------------


def _count_walker(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []
    original = uft.describe_unknown_fields

    def _counting(message: Message) -> Any:
        calls.append(1)
        return original(message)

    monkeypatch.setattr(uft, "describe_unknown_fields", _counting)
    return calls


def test_k7_info_level_skips_walker(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_LOGGER_NAME)
    calls = _count_walker(monkeypatch)

    uft.log_unknown_fields(
        "device_list",
        secrets.token_hex(16),
        "Tag",
        list(build_device_list(3).deviceMetadata),
    )

    assert calls == []
    assert [r for r in caplog.records if r.name == _LOGGER_NAME] == []


def test_k7_debug_level_calls_walker(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _debug(caplog)
    calls = _count_walker(monkeypatch)

    uft.log_unknown_fields(
        "device_list",
        secrets.token_hex(16),
        "Tag",
        list(build_device_list(3).deviceMetadata),
    )

    assert len(calls) == 3
    assert len(_lines(caplog)) == 1


def test_k7_build_device_list_has_live_shape() -> None:
    device_list = build_device_list(11)

    assert len(device_list.deviceMetadata) == 11
    ids = {
        d.identifierInformation.canonicIds.canonicId[0].id
        for d in device_list.deviceMetadata
    }
    assert len(ids) == 11
    for device in device_list.deviceMetadata:
        assert _render(uft.describe_unknown_fields(device)) == [
            "DeviceMetadata.#99=varint:1",
            "DeviceMetadata.information.deviceRegistration.#98=len=2 msg{#1=varint}",
        ]


# --------------------------------------------------------------------------
# K8: four call sites, fail-safe
# --------------------------------------------------------------------------

_NAME = "Tag Alpha"


def _boom_walker(monkeypatch: pytest.MonkeyPatch, word: str) -> None:
    def _boom(_message: Message) -> Any:
        raise RuntimeError(word)

    monkeypatch.setattr(uft, "describe_unknown_fields", _boom)


def _assert_fail_safe_record(caplog: pytest.LogCaptureFixture, word: str) -> None:
    own = [r for r in caplog.records if r.name == _LOGGER_NAME]
    assert len(own) == 1, caplog.text
    assert own[0].levelno == logging.DEBUG
    assert "RuntimeError" in own[0].getMessage()
    _assert_absent(caplog.text, words=[word])


def _device_list_hex() -> tuple[str, Any]:
    device = _device(
        _NAME,
        secrets.token_hex(16),
        _vf(_free(DeviceUpdate_pb2.DeviceMetadata, 99), 5),
    )
    device_list = DeviceUpdate_pb2.DevicesList()
    device_list.deviceMetadata.add().CopyFrom(device)
    return device_list.SerializeToString().hex(), device_list


def _device_update_hex() -> tuple[str, Any]:
    device = _device(
        _NAME,
        secrets.token_hex(16),
        _vf(_free(DeviceUpdate_pb2.DeviceMetadata, 99), 5),
    )
    update = DeviceUpdate_pb2.DeviceUpdate()
    update.deviceMetadata.CopyFrom(device)
    return update.SerializeToString().hex(), update


def test_k8_device_list_logs_line(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    hex_string, _expected = _device_list_hex()

    decoder.parse_device_list_protobuf(hex_string)

    assert _lines(caplog) == [
        f"Unknown protobuf fields (device_list) for {_NAME}: DeviceMetadata.#99=varint:5"
    ]


def test_k8_device_list_fail_safe(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _debug(caplog)
    word = secrets.token_urlsafe(24)
    _boom_walker(monkeypatch, word)
    hex_string, expected = _device_list_hex()

    result = decoder.parse_device_list_protobuf(hex_string)

    assert result.SerializeToString() == expected.SerializeToString()
    _assert_fail_safe_record(caplog, word)


def test_k8_device_update_logs_line(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    hex_string, _expected = _device_update_hex()

    decoder.parse_device_update_protobuf(hex_string)

    assert _lines(caplog) == [
        f"Unknown protobuf fields (device_update) for {_NAME}: "
        "DeviceUpdate.deviceMetadata.#99=varint:5"
    ]


def test_k8_device_update_fail_safe(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _debug(caplog)
    word = secrets.token_urlsafe(24)
    _boom_walker(monkeypatch, word)
    hex_string, expected = _device_update_hex()

    result = decoder.parse_device_update_protobuf(hex_string)

    assert result.SerializeToString() == expected.SerializeToString()
    _assert_fail_safe_record(caplog, word)


async def _decrypt_one_location(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Run the decryption path once; the decrypted Location carries ``#9``."""
    base_now = 1_700_000_000.0
    monkeypatch.setattr(decrypt_locations.time, "time", lambda: base_now)
    location = DeviceUpdate_pb2.Location()
    location.latitude = int(1.0 * 1e7)
    location.longitude = int(2.0 * 1e7)
    location.altitude = 3
    location_bytes = location.SerializeToString() + _vf(
        _free(DeviceUpdate_pb2.Location, 9), 4
    )
    identity_key = secrets.token_bytes(32)

    async def _identity_key(*_args: object, **_kwargs: object) -> list[bytes]:
        return [identity_key]

    async def _offload(*_args: object, **_kwargs: object) -> bytes:
        return location_bytes

    monkeypatch.setattr(decrypt_locations, "async_retrieve_identity_key", _identity_key)
    monkeypatch.setattr(decrypt_locations, "_offload_decrypt_aes", _offload)

    update = DeviceUpdate_pb2.DeviceUpdate()
    update.deviceMetadata.userDefinedDeviceName = _NAME
    update.deviceMetadata.identifierInformation.canonicIds.canonicId.add().id = (
        secrets.token_hex(16)
    )
    update.deviceMetadata.information.deviceRegistration.SetInParent()
    recent = update.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations
    recent.recentLocationTimestamp.seconds = int(base_now - 60)
    report = recent.recentLocation
    report.status = Common_pb2.Status.LAST_KNOWN
    report.geoLocation.accuracy = 5.0
    report.geoLocation.encryptedReport.publicKeyRandom = b""
    report.geoLocation.encryptedReport.encryptedLocation = secrets.token_bytes(24)
    report.geoLocation.encryptedReport.isOwnReport = True

    result = await decrypt_locations.async_decrypt_location_response_locations(
        update,
        cache=cast(Any, object()),
    )
    return cast(list[Any], result)


@pytest.mark.asyncio
async def test_k8_location_logs_line(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _debug(caplog)

    result = await _decrypt_one_location(monkeypatch)

    assert len(result) == 1
    assert _lines(caplog) == [
        f"Unknown protobuf fields (location) for {_NAME}: Location.#9=varint:4"
    ]


@pytest.mark.asyncio
async def test_k8_location_fail_safe(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _debug(caplog)
    word = secrets.token_urlsafe(24)
    _boom_walker(monkeypatch, word)

    result = await _decrypt_one_location(monkeypatch)

    assert len(result) == 1
    assert result[0]["altitude"] == 3
    _assert_fail_safe_record(caplog, word)


def _eid_response_bytes() -> bytes:
    response = DeviceUpdate_pb2.GetEidInfoForE2eeDevicesResponse()
    response.encryptedOwnerKeyAndMetadata.encryptedOwnerKey = secrets.token_bytes(48)
    response.encryptedOwnerKeyAndMetadata.ownerKeyVersion = 1
    return response.SerializeToString() + _vf(
        _free(DeviceUpdate_pb2.GetEidInfoForE2eeDevicesResponse, 7), 3
    )


def _patch_spot(monkeypatch: pytest.MonkeyPatch, body: bytes) -> None:
    async def _spot(*_args: object, **_kwargs: object) -> bytes:
        return body

    monkeypatch.setattr(get_eid_info_request, "_spot_call_async", _spot)


@pytest.mark.asyncio
async def test_k8_eid_response_logs_line(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _debug(caplog)
    _patch_spot(monkeypatch, _eid_response_bytes())

    await get_eid_info_request.async_get_eid_info(cache=cast(Any, object()))

    assert _lines(caplog) == [
        "Unknown protobuf fields (eid_response) for account: "
        "GetEidInfoForE2eeDevicesResponse.#7=varint:3"
    ]


@pytest.mark.asyncio
async def test_k8_eid_response_fail_safe(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _debug(caplog)
    word = secrets.token_urlsafe(24)
    _boom_walker(monkeypatch, word)
    body = _eid_response_bytes()
    _patch_spot(monkeypatch, body)

    result = await get_eid_info_request.async_get_eid_info(cache=cast(Any, object()))

    assert result.SerializeToString() == body
    _assert_fail_safe_record(caplog, word)


def test_k8_helper_fail_safe(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _debug(caplog)
    word = secrets.token_urlsafe(24)
    _boom_walker(monkeypatch, word)

    uft.log_unknown_fields(
        "device_list", secrets.token_hex(16), "Tag", [DeviceUpdate_pb2.DeviceMetadata()]
    )

    _assert_fail_safe_record(caplog, word)


# --------------------------------------------------------------------------
# K9: log hygiene of the whole line
# --------------------------------------------------------------------------


def test_k9_format_string_and_logger(caplog: pytest.LogCaptureFixture) -> None:
    _debug(caplog)
    device = _reparse(
        DeviceUpdate_pb2.DeviceMetadata(),
        _vf(_free(DeviceUpdate_pb2.DeviceMetadata, 99), 1),
    )

    uft.log_unknown_fields("device_list", secrets.token_hex(16), "Tag", [device])

    own = [r for r in caplog.records if r.name == _LOGGER_NAME]
    assert len(own) == 1
    assert own[0].levelno == logging.DEBUG
    assert own[0].msg == _FORMAT
    assert own[0].args == ("device_list", "Tag", "DeviceMetadata.#99=varint:1")


def test_k9_no_payload_window_in_log(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="custom_components.googlefindmy")
    canonic_id = secrets.token_hex(16)
    word = secrets.token_urlsafe(30)
    big = (1 << 33) + secrets.randbelow(1 << 20)
    fixed = secrets.token_bytes(8)
    fixed_short = secrets.token_bytes(4)
    cipher, cipher_big = _fake_cipher()
    free = DeviceUpdate_pb2.DeviceMetadata
    extra = (
        _ld(_free(free, 97), word.encode())
        + _vf(_free(free, 96), big)
        + _fixed64(_free(free, 95), fixed)
        + _fixed32(_free(free, 93), fixed_short)
        + _ld(_free(free, 94), _ld(19, _ld(1, cipher)))
    )
    device = _device(_NAME, canonic_id, extra)
    device.information.deviceRegistration.encryptedUserSecrets.encryptedAccountKey = (
        cipher
    )
    device_list = DeviceUpdate_pb2.DevicesList()
    device_list.deviceMetadata.add().CopyFrom(device)

    decoder.parse_device_list_protobuf(device_list.SerializeToString().hex())

    payloads = _payloads(caplog)
    assert len(payloads) == 1
    assert "DeviceMetadata.#96=varint:>255" in payloads[0]
    assert f"DeviceMetadata.#97=len={len(word)} text" in payloads[0]
    _assert_absent(
        caplog.text,
        raw=[cipher, fixed, fixed_short],
        words=[word, canonic_id],
        numbers=[
            big,
            cipher_big,
            int.from_bytes(fixed, "little"),
            int.from_bytes(fixed_short, "little"),
        ],
    )
