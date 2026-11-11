# custom_components/googlefindmy/unknown_field_tree.py
"""Report protobuf fields the vendored schema does not know, at DEBUG only.

The schema in ``ProtoDecoders/`` is reverse-engineered, so a field Google adds
stays invisible unless it is reported. This module is the one place that does
it (``AGENTS.md`` section 5, Schema-drift diagnostics). Per unknown field a
line names the path of field names and ``#number`` (repeated entries as
``[]``) and the wire type; of the payload it shows only a varint value of at
most ``_VARINT_VALUE_MAX`` (else ``>255``), a length and whether the bytes are
printable text. Fixed32 and fixed64 values are never shown.

Two separate limits apply. The descent through known fields has no constant:
it is bounded by the schema and by the fields that are set. Below the first
unknown field, a length-delimited field (or group) outside the closed message
types is tried as one schemaless level (``_MAX_UNKNOWN_DEPTH``); inside that
level only field numbers, wire types and lengths appear, never a value and
never a text marker, because schemaless bytes may be ciphertext.

A known ``bytes`` field is opened only if it is listed in ``_FORM_BY_FIELD``
and then parsed with its declared form. Inside the message types in
``_CLOSED_MESSAGES`` and inside every message below one of them, an unknown
length-delimited field shows its length only.

A line is written only when the signature of the unknown fields of one
``(kind, device_ref)`` changes; ``device_ref`` names the comparison slot and
is never logged. ``tests/test_unknown_field_tree.py`` pins these limits.
"""

from __future__ import annotations

import logging
import re
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from importlib import import_module
from typing import Any, cast

from google.protobuf.unknown_fields import UnknownFieldSet

from google.protobuf.message import DecodeError, Message

# Imported at module load (integration setup runs in the executor, never on the
# event loop). Through ``import_module`` because the vendored stubs under
# ``google/protobuf/`` declare neither module; ``Empty`` is the well-known
# message without fields, so every field parsed into it stays unknown.
_message_factory: Any = import_module("google.protobuf.message_factory")
_EMPTY_MESSAGE: type[Message] = cast(
    type[Message], import_module("google.protobuf.empty_pb2").Empty
)

_LOGGER = logging.getLogger("custom_components.googlefindmy.unknown_fields")

_VARINT_VALUE_MAX = 255
_MAX_UNKNOWN_DEPTH = 1
_MAX_SIGNATURES = 256
_MAX_ENTRIES = 64

_WIRE_VARINT = 0
_WIRE_FIXED64 = 1
_WIRE_LEN = 2
_WIRE_GROUP = 3
_WIRE_FIXED32 = 5

# Known ``bytes`` fields that carry nested protobuf structure, by full field
# name, mapped to the full name of the declared form (the ``form:`` comment in
# ``DeviceUpdate.proto``). Key material and identifiers are never listed.
_FORM_BY_FIELD: Mapping[str, str] = {
    "DeviceMetadata.supportedActions": "DeviceMetadata.SupportedActions",
    "ImageInformation.componentImages": "ImageInformation.ComponentImage",
    "DeviceTypeInformation.deviceComponentsInformation": "DeviceComponentInformation",
    "DeviceTypeInformation.componentDetails": "DeviceComponentDetail",
    "DeviceComponentDetail.detail": "DeviceComponentInformation",
    "DeviceRegistration.capabilities": "DeviceCapabilities",
    "DeviceRegistration.firstSecretsCreationDate": "Time",
    "DeviceRegistration.lostModeInformation": "DeviceRegistration.LostModeInformation",
    "DeviceMetadata.metadataField12": "Time",
    "DeviceRegistration.registrationField40": "Time",
}

# Message types that hold or wrap key material or personal data. "Closed" is
# inherited by every message nested below one of them.
_CLOSED_MESSAGES: frozenset[str] = frozenset(
    {
        "EncryptedUserSecrets",
        "EncryptedOwnerKeyAndMetadata",
        "SharingKeys",
        "DeviceRegistration.KeyRotationInformation",
        "DeviceRegistration.KeyRotationInformation.RotatedEIK",
        "AccessInformation",
        "DeviceDescription.ContactInfo",
        "GeoLocation",
        "Location",
    }
)

# Display of a field on the schemaless level, by wire type (length-delimited
# fields add their length). A group is not opened there.
_LEVEL_ONE_FORMS: Mapping[int, str] = {
    _WIRE_VARINT: "varint",
    _WIRE_FIXED64: "fixed64",
    _WIRE_GROUP: "group depth-limit",
    _WIRE_FIXED32: "fixed32",
}

_LENGTH_RE = re.compile(r"len=\d+")

_Entry = tuple[str, str, str]
_Signature = tuple[tuple[str, str, str], ...]

_SIGNATURES: OrderedDict[tuple[str, str], _Signature] = OrderedDict()
_FORM_CLASSES: dict[str, type[Message]] = {}


def _reset_signatures_for_tests() -> None:
    """Forget every stored signature (test seam, also for the cost measurement)."""
    _SIGNATURES.clear()


def _is_text(raw: bytes) -> bool:
    if not raw:
        return False
    try:
        return raw.decode("utf-8").isprintable()
    except UnicodeDecodeError:
        return False


def _level_one_item(number: int, wire_type: int, data: Any) -> str:
    """One field of the schemaless level: number, wire type, length only."""
    if wire_type == _WIRE_LEN:
        return f"#{number}=len={len(data)} depth-limit"
    return f"#{number}={_LEVEL_ONE_FORMS[wire_type]}"


def _schemaless(fields: UnknownFieldSet) -> str:
    return ", ".join(
        _level_one_item(f.field_number, f.wire_type, f.data) for f in fields
    )


def _schemaless_from_bytes(raw: bytes) -> str | None:
    """Parse ``raw`` as one schemaless level; ``None`` unless it parses cleanly."""
    if not raw:
        return None
    holder = _EMPTY_MESSAGE()
    try:
        holder.ParseFromString(raw)
    except DecodeError:
        return None
    return _schemaless(UnknownFieldSet(holder))


def _opens_unknown(closed: bool) -> bool:
    """Whether an unknown length-delimited field or group gets a schemaless level.

    Never inside a closed message. At most one level below the first unknown
    field is ever opened: ``_MAX_UNKNOWN_DEPTH`` acts as a switch (0 opens
    nothing, any value of 1 or more opens that one level), and every
    length-delimited field or group in that level carries ``depth-limit``.
    """
    return not closed and _MAX_UNKNOWN_DEPTH >= 1


def _unknown_entry(path: str, wire_type: int, data: Any, closed: bool) -> _Entry:
    if wire_type == _WIRE_VARINT:
        shown = str(data) if data <= _VARINT_VALUE_MAX else ">255"
        return (path, "varint", f"varint:{shown}")
    if wire_type == _WIRE_FIXED32:
        return (path, "fixed32", "fixed32")
    if wire_type == _WIRE_FIXED64:
        return (path, "fixed64", "fixed64")
    if wire_type == _WIRE_LEN:
        raw = bytes(data)
        display = f"len={len(raw)}"
        if _opens_unknown(closed):
            if _is_text(raw):
                display += " text"
            else:
                level = _schemaless_from_bytes(raw)
                if level is not None:
                    display += f" msg{{{level}}}"
        return (path, "len", display)
    display = "group"
    if _opens_unknown(closed):
        level = _schemaless(data)
        if level:
            display += f" msg{{{level}}}"
    return (path, "group", display)


def _form_class(message: Message, form: str) -> type[Message]:
    cls = _FORM_CLASSES.get(form)
    if cls is None:
        descriptor = message.DESCRIPTOR.file.pool.FindMessageTypeByName(form)
        cls = cast(type[Message], _message_factory.GetMessageClass(descriptor))
        _FORM_CLASSES[form] = cls
    return cls


def _open_form(
    form_class: type[Message], raw: bytes, path: str, closed: bool, out: list[_Entry]
) -> None:
    nested = form_class()
    try:
        nested.ParseFromString(raw)
    except DecodeError:
        out.append((path, "len", f"form-mismatch len={len(raw)}"))
        return
    _walk(nested, path, closed, out)


def _walk(message: Message, path: str, closed: bool, out: list[_Entry]) -> None:
    owner_name = message.DESCRIPTOR.full_name
    closed = closed or owner_name in _CLOSED_MESSAGES
    for field in UnknownFieldSet(message):
        out.append(
            _unknown_entry(
                f"{path}.#{field.field_number}", field.wire_type, field.data, closed
            )
        )
    for descriptor, value in message.ListFields():
        name = descriptor.name
        if descriptor.message_type is not None:
            # The upb descriptor has no `label`: repeated is read from the value
            # (map fields yield their message values through `.values()`).
            if isinstance(value, Message):
                _walk(value, f"{path}.{name}", closed, out)
                continue
            items = value.values() if hasattr(value, "values") else value
            for item in items:
                if isinstance(item, Message):
                    _walk(item, f"{path}.{name}[]", closed, out)
        elif descriptor.type == descriptor.TYPE_BYTES:
            form = _FORM_BY_FIELD.get(f"{owner_name}.{name}")
            if form is None:
                continue
            form_class = _form_class(message, form)
            if isinstance(value, bytes):
                _open_form(form_class, value, f"{path}.{name}", closed, out)
            else:
                for item in value:
                    _open_form(form_class, item, f"{path}.{name}[]", closed, out)


def describe_unknown_fields(message: Message) -> list[_Entry]:
    """Return ``(path, wire type, display)`` for every unknown field of ``message``.

    Pure: no logging, no state. The path starts with the message type name.
    """
    out: list[_Entry] = []
    _walk(message, message.DESCRIPTOR.name, False, out)
    return out


def _signature(entries: Iterable[_Entry]) -> _Signature:
    """Multiset of the entries without lengths and without values above 255."""
    return tuple(
        sorted(
            (path, wire, _LENGTH_RE.sub("len", display).replace(":>255", ""))
            for path, wire, display in entries
        )
    )


def _line(entries: list[_Entry]) -> str:
    items = [f"{path}={display}" for path, _wire, display in entries]
    if not items:
        return "(none)"
    if len(items) > _MAX_ENTRIES:
        rest = len(items) - _MAX_ENTRIES
        items = [*items[:_MAX_ENTRIES], f"+{rest}"]
    return "; ".join(items)


def log_unknown_fields(
    kind: str, device_ref: str, label: str, messages: Iterable[Message]
) -> None:
    """Log the unknown fields of one response for one device if they changed.

    ``messages`` are merged (several reports of one device in one response give
    one signature). ``device_ref`` names the comparison slot and is never logged;
    ``label`` is the display name and appears at DEBUG only. Never raises an
    ``Exception``: diagnostics must not break decoding.
    """
    if not _LOGGER.isEnabledFor(logging.DEBUG):
        return
    try:
        entries: list[_Entry] = []
        for message in messages:
            entries.extend(describe_unknown_fields(message))
        signature = _signature(entries)
        slot = (kind, device_ref)
        if _SIGNATURES.get(slot) == signature:
            _SIGNATURES.move_to_end(slot)
            return
        _SIGNATURES[slot] = signature
        _SIGNATURES.move_to_end(slot)
        while len(_SIGNATURES) > _MAX_SIGNATURES:
            _SIGNATURES.popitem(last=False)
        _LOGGER.debug(
            "Unknown protobuf fields (%s) for %s: %s", kind, label, _line(entries)
        )
    except Exception as err:  # noqa: BLE001 - diagnostics never break decoding
        _LOGGER.debug(
            "Unknown protobuf fields (%s) skipped: %s", kind, type(err).__name__
        )
