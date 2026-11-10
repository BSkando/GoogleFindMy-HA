# tests/test_decoder_unknown_fields_log.py
"""The device-list decoder reports schema drift through one helper only.

A device message that carries fields the vendored `.proto` does not know used
to be reported twice over: `_unknown_field_numbers` collected the bare field
numbers, and `get_devices_with_location` logged them as an `UNKNOWN FIELDS`
record. Both are gone. `parse_device_list_protobuf` now hands every decoded
device to `log_unknown_fields` in
`custom_components/googlefindmy/unknown_field_tree.py`, which writes one DEBUG
line on the logger `custom_components.googlefindmy.unknown_fields` with the
path and wire type of each unknown field and, of the payload, only a varint
value up to 255, a length and a text marker (`AGENTS.md` section 5,
Schema-drift diagnostics). The limits of that line are pinned in
`tests/test_unknown_field_tree.py`; this file pins the decoder's side: the
old probe and record are removed, the new line names numbers and lengths
rather than values, and the walk reads the unknown-field set instead of the
text format (it runs on the event loop, `AGENTS.md` 11.3).
"""

from __future__ import annotations

import importlib
import logging
import secrets
from types import ModuleType
from typing import Any

import pytest

from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2, decoder
from google.protobuf import text_format

_MODULE = "custom_components.googlefindmy.unknown_field_tree"
_LOGGER_NAME = "custom_components.googlefindmy.unknown_fields"
_FORMAT = "Unknown protobuf fields (%s) for %s: %s"


@pytest.fixture(autouse=True)
def _reset_state() -> None:
    decoder._reset_canonicless_warning_state()


def _tree() -> ModuleType:
    """Import inside the test, so a missing module fails each case on its own."""
    module = importlib.import_module(_MODULE)
    module._reset_signatures_for_tests()
    return module


def _device_with_unknown_fields(secret: str) -> Any:
    device_list = DeviceUpdate_pb2.DevicesList()
    device = device_list.deviceMetadata.add()
    device.userDefinedDeviceName = "Pixel"
    device.identifierInformation.canonicIds.canonicId.add().id = secrets.token_hex(16)
    # Re-parse the device with two fields the schema does not know: 1023
    # (length-delimited, the secret) and 1024 (varint 7). Tag 1023<<3|2 is
    # b"\xfa\x3f", tag 1024<<3|0 is b"\x80\x40".
    for number in (1023, 1024):
        assert number not in DeviceUpdate_pb2.DeviceMetadata.DESCRIPTOR.fields_by_number
    raw = (
        device.SerializeToString()
        + b"\xfa\x3f"
        + bytes([len(secret)])
        + secret.encode()
        + b"\x80\x40\x07"
    )
    device.ParseFromString(raw)
    return device_list


def test_unknown_fields_show_numbers_not_values() -> None:
    tree = _tree()
    secret = secrets.token_urlsafe(9)
    device_list = _device_with_unknown_fields(secret)

    entries = tree.describe_unknown_fields(device_list.deviceMetadata[0])

    assert [f"{path}={display}" for path, _wire, display in entries] == [
        f"DeviceMetadata.#1023=len={len(secret)} text",
        "DeviceMetadata.#1024=varint:7",
    ]
    assert secret not in repr(entries)
    assert tree.describe_unknown_fields(DeviceUpdate_pb2.DeviceMetadata()) == []


def test_unknown_fields_record_omits_values(caplog: pytest.LogCaptureFixture) -> None:
    _tree()
    secret = secrets.token_urlsafe(24)
    device_list = _device_with_unknown_fields(secret)

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.parse_device_list_protobuf(device_list.SerializeToString().hex())

    records = [
        r.getMessage()
        for r in caplog.records
        if r.name == _LOGGER_NAME and r.msg == _FORMAT
    ]
    assert records == [
        "Unknown protobuf fields (device_list) for Pixel: "
        f"DeviceMetadata.#1023=len={len(secret)} text; DeviceMetadata.#1024=varint:7"
    ]
    assert secret not in caplog.text
    assert all(secret[i : i + 8] not in caplog.text for i in range(len(secret) - 7))


def test_get_devices_with_location_no_longer_logs_unknown_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    device_list = _device_with_unknown_fields(secrets.token_urlsafe(9))

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(device_list, cache=None)

    assert "UNKNOWN FIELDS" not in caplog.text


def test_decoder_has_no_unknown_field_numbers() -> None:
    assert not hasattr(decoder, "_unknown_field_numbers")


def test_unknown_fields_do_not_serialise_to_text(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The walk reads the unknown-field set, never the text format.

    It runs on the event loop inside the DEBUG branch, once per decoded
    message; a ``text_format`` pass over every device message would stall the
    loop, and so would a lazy ``import_module`` on first use (``AGENTS.md``
    11.3). Nested messages are walked as well.
    """
    tree = _tree()

    def _no_text_format(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("text_format must not be used for unknown fields")

    def _no_import(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("no lazy import on the event-loop path")

    monkeypatch.setattr(text_format, "MessageToString", _no_text_format)
    monkeypatch.setattr(text_format, "MessageToBytes", _no_text_format)
    monkeypatch.setattr(decoder, "_get_text_format", _no_text_format)
    monkeypatch.setattr(decoder, "import_module", _no_import)
    monkeypatch.setattr(importlib, "import_module", _no_import)
    device_list = _device_with_unknown_fields("s3cr3t-value")
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)

    paths = [
        path for path, _wire, _display in tree.describe_unknown_fields(device_list)
    ]
    tree.log_unknown_fields(
        "device_list", secrets.token_hex(16), "Pixel", list(device_list.deviceMetadata)
    )

    assert paths == [
        "DevicesList.deviceMetadata[].#1023",
        "DevicesList.deviceMetadata[].#1024",
    ]
    assert len([r for r in caplog.records if r.msg == _FORMAT]) == 1
    assert "AssertionError" not in caplog.text
