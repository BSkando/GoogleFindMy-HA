# tests/test_decoder_identifier_structure_log.py
"""The identifier-structure diagnostic names the shape, never the IDs.

`get_devices_with_location` picks one canonical-ID branch per device by
`identifierInformation.type`: `phoneInformation.canonicIds` for Android
phones, `canonicIds` for everything else. When that branch is empty the
device is dropped as canonicless, and until now no log showed the type or
whether the other branch carried the ID (BSkando/GoogleFindMy-HA#224, a
Wear OS watch). On the main poll the decoder now logs at DEBUG the enum name
of the type, whether `phoneInformation` is set, and the ID count of both
branches. Counts and presence only: the ID values never reach the log
(`AGENTS.md` section 5), and the capability probe pass stays silent (CQS).
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2, decoder

_SECRET = "c4n0n1c-1d-QxZ8pLmN3vR7tY2w"


@pytest.fixture(autouse=True)
def _reset_state() -> None:
    decoder._reset_canonicless_warning_state()


def _device_list(
    *,
    id_type: int,
    phone_ids: list[str],
    generic_ids: list[str],
) -> DeviceUpdate_pb2.DevicesList:
    device_list = DeviceUpdate_pb2.DevicesList()
    device = device_list.deviceMetadata.add()
    device.userDefinedDeviceName = "Watch"
    info = device.identifierInformation
    info.type = id_type
    for value in phone_ids:
        info.phoneInformation.canonicIds.canonicId.add(id=value)
    for value in generic_ids:
        info.canonicIds.canonicId.add(id=value)
    return device_list


def _structure_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if "IDENTIFIER STRUCTURE" in r.getMessage()]


@pytest.mark.parametrize(
    ("id_type", "phone_ids", "generic_ids", "expected"),
    [
        pytest.param(
            DeviceUpdate_pb2.IDENTIFIER_SPOT,
            [_SECRET],
            [],
            "type=IDENTIFIER_SPOT, has_phoneInformation=True, "
            "phoneInformation.canonicIds=1, canonicIds=0",
            id="spot-id-in-phone-branch",
        ),
        pytest.param(
            DeviceUpdate_pb2.IDENTIFIER_ANDROID,
            [],
            [_SECRET],
            "type=IDENTIFIER_ANDROID, has_phoneInformation=False, "
            "phoneInformation.canonicIds=0, canonicIds=1",
            id="android-id-in-generic-branch",
        ),
    ],
)
def test_structure_line_reports_both_branches_without_values(
    caplog: pytest.LogCaptureFixture,
    id_type: int,
    phone_ids: list[str],
    generic_ids: list[str],
    expected: str,
) -> None:
    device_list = _device_list(
        id_type=id_type, phone_ids=phone_ids, generic_ids=generic_ids
    )

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=True
        )

    records = _structure_records(caplog)
    assert len(records) == 1, caplog.text
    record = records[0]
    message = record.getMessage()
    assert record.levelno == logging.DEBUG
    assert expected in message
    assert "'Watch'" in message
    # Existing filters count DEBUG lines containing "canonical ID"; this line
    # must not join them.
    assert "canonical ID" not in message
    assert _SECRET not in caplog.text
    assert all(
        _SECRET[i : i + 8] not in caplog.text for i in range(len(_SECRET) - 7)
    )


def test_structure_line_absent_when_read_branch_has_an_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    device_list = _device_list(
        id_type=DeviceUpdate_pb2.IDENTIFIER_SPOT, phone_ids=[], generic_ids=[_SECRET]
    )

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=True
        )

    assert _structure_records(caplog) == []


def test_structure_line_absent_on_capability_probe_pass(
    caplog: pytest.LogCaptureFixture,
) -> None:
    device_list = _device_list(
        id_type=DeviceUpdate_pb2.IDENTIFIER_SPOT, phone_ids=[_SECRET], generic_ids=[]
    )

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=False
        )

    assert _structure_records(caplog) == []


class _ExplodingDevice:
    @property
    def identifierInformation(self) -> object:
        raise RuntimeError("broken accessor")


def test_identifier_structure_never_raises() -> None:
    structure = decoder._identifier_structure

    assert structure(None) == ("<absent>", False, 0, 0)
    assert structure(object()) == ("<absent>", False, 0, 0)
    assert structure(SimpleNamespace()) == ("<absent>", False, 0, 0)
    assert structure(_ExplodingDevice()) == ("<unreadable>", False, -1, -1)

    # Phone double as in the canonicless tests: no generic canonicIds field.
    phone_double = SimpleNamespace(
        identifierInformation=SimpleNamespace(
            type=DeviceUpdate_pb2.IDENTIFIER_ANDROID,
            phoneInformation=SimpleNamespace(
                canonicIds=SimpleNamespace(canonicId=[])
            ),
        )
    )
    assert structure(phone_double) == ("IDENTIFIER_ANDROID", True, 0, 0)

    # An enum value newer than the vendored schema has no name.
    unknown_double = SimpleNamespace(identifierInformation=SimpleNamespace(type=7))
    name, has_phone, phone_ids, generic_ids = structure(unknown_double)
    assert name.startswith("UNRECOGNIZED(7)")
    assert (has_phone, phone_ids, generic_ids) == (False, 0, 0)

    # Real messages: reading an unset sub-message must not report it as set,
    # and the open enum accepts values the schema does not name.
    empty = DeviceUpdate_pb2.DeviceMetadata()
    assert structure(empty) == ("IDENTIFIER_UNKNOWN", False, 0, 0)
    assert not empty.HasField("identifierInformation")
    future = DeviceUpdate_pb2.DeviceMetadata()
    future.identifierInformation.type = 7  # type: ignore[assignment]
    assert structure(future)[0] == "UNRECOGNIZED(7)"
