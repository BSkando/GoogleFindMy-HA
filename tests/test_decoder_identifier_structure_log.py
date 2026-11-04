# tests/test_decoder_identifier_structure_log.py
"""The identifier-structure diagnostic names the shape, never the IDs.

`get_devices_with_location` picks one canonical-ID branch per device by
`identifierInformation.type`: `phoneInformation.canonicIds` for Android
phones, `canonicIds` for everything else. When that branch yields no row (no
entry at all, only blank entries, or a blank first entry on a phone) the device
is dropped as canonicless, and until now no log showed the type or whether the
other branch carried the ID (BSkando/GoogleFindMy-HA#224, a Wear OS watch). For
such a device the decoder now logs at DEBUG the enum name of the type, whether
`phoneInformation` is set, and the usable ID count of both branches. This line
carries no ID values, so it can go into a public issue, and the capability
probe pass stays silent (CQS).
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import cast

import pytest

from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2, decoder
from google.protobuf.message import Message

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
        pytest.param(
            DeviceUpdate_pb2.IDENTIFIER_SPOT,
            [],
            [""],
            "type=IDENTIFIER_SPOT, has_phoneInformation=False, "
            "phoneInformation.canonicIds=0, canonicIds=0",
            id="spot-blank-id-in-read-branch",
        ),
        pytest.param(
            DeviceUpdate_pb2.IDENTIFIER_SPOT,
            [""],
            [],
            "type=IDENTIFIER_SPOT, has_phoneInformation=True, "
            "phoneInformation.canonicIds=0, canonicIds=0",
            id="spot-blank-id-in-other-branch-is-no-id",
        ),
        pytest.param(
            DeviceUpdate_pb2.IDENTIFIER_ANDROID,
            [""],
            [],
            "type=IDENTIFIER_ANDROID, has_phoneInformation=True, "
            "phoneInformation.canonicIds=0, canonicIds=0",
            id="android-blank-id-in-read-branch",
        ),
        pytest.param(
            DeviceUpdate_pb2.IDENTIFIER_ANDROID,
            ["", _SECRET],
            [],
            "type=IDENTIFIER_ANDROID, has_phoneInformation=True, "
            "phoneInformation.canonicIds=1, canonicIds=0",
            id="android-reads-only-the-blank-first-entry",
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
        rows = decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=True
        )

    # The line accompanies a dropped device.
    assert rows == []
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
    assert all(_SECRET[i : i + 8] not in caplog.text for i in range(len(_SECRET) - 7))


@pytest.mark.parametrize(
    "generic_ids",
    [
        pytest.param([_SECRET], id="id-only"),
        pytest.param(["", _SECRET], id="blank-before-id"),
    ],
)
def test_structure_line_absent_when_read_branch_has_an_id(
    caplog: pytest.LogCaptureFixture, generic_ids: list[str]
) -> None:
    device_list = _device_list(
        id_type=DeviceUpdate_pb2.IDENTIFIER_SPOT, phone_ids=[], generic_ids=generic_ids
    )

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        rows = decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=True
        )

    assert len(rows) == 1
    assert _structure_records(caplog) == []


def test_one_structure_line_per_dropped_device(
    caplog: pytest.LogCaptureFixture,
) -> None:
    device_list = DeviceUpdate_pb2.DevicesList()
    for name, generic_id in (("Dropped A", ""), ("Kept", _SECRET), ("Dropped B", "")):
        device = device_list.deviceMetadata.add()
        device.userDefinedDeviceName = name
        device.identifierInformation.type = DeviceUpdate_pb2.IDENTIFIER_SPOT
        device.identifierInformation.canonicIds.canonicId.add(id=generic_id)

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        rows = decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=True
        )

    assert len(rows) == 1
    messages = [r.getMessage() for r in _structure_records(caplog)]
    assert len(messages) == 2
    assert "'Dropped A'" in messages[0]
    assert "'Dropped B'" in messages[1]


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


class _CancelledDevice:
    @property
    def identifierInformation(self) -> object:
        raise asyncio.CancelledError


class _HasFieldRaises(Message):
    def HasField(self, field_name: str) -> bool:
        raise ValueError(field_name)


class _UnboolableResult:
    def __bool__(self) -> bool:
        raise TypeError("no truth value")


class _HasFieldUnboolable(Message):
    def HasField(self, field_name: str) -> bool:
        return cast(bool, _UnboolableResult())


class _LoudInt(int):
    def __format__(self, format_spec: str) -> str:
        return "LEAK"

    def __repr__(self) -> str:
        return "LEAK"

    __str__ = __repr__


def _info(**fields: object) -> SimpleNamespace:
    return SimpleNamespace(identifierInformation=SimpleNamespace(**fields))


@pytest.mark.parametrize(
    "hostile",
    [
        pytest.param(_ExplodingDevice(), id="raising-property"),
        pytest.param(
            _info(canonicIds=SimpleNamespace(canonicId=5)), id="generic-not-iterable"
        ),
        pytest.param(
            _info(
                phoneInformation=SimpleNamespace(
                    canonicIds=SimpleNamespace(canonicId=5)
                )
            ),
            id="phone-not-iterable",
        ),
        pytest.param(
            SimpleNamespace(identifierInformation=_HasFieldRaises()),
            id="hasfield-raises",
        ),
        pytest.param(
            SimpleNamespace(identifierInformation=_HasFieldUnboolable()),
            id="hasfield-result-has-no-truth-value",
        ),
    ],
)
def test_identifier_structure_degrades_on_hostile_objects(hostile: object) -> None:
    assert decoder._identifier_structure(hostile) == ("<unreadable>", False, -1, -1)


def test_identifier_structure_lets_cancellation_through() -> None:
    with pytest.raises(asyncio.CancelledError):
        decoder._identifier_structure(_CancelledDevice())


def test_identifier_structure_takes_no_text_from_the_type_value() -> None:
    structure = decoder._identifier_structure

    # bool is an int, but no enum value.
    assert structure(_info(type=True)) == ("<absent>", False, 0, 0)
    assert structure(_info(type=_LoudInt(7)))[0] == "UNRECOGNIZED(7)"


class _NoTruthValueList(list[object]):
    def __bool__(self) -> bool:
        raise RuntimeError("no truth value")

    def __len__(self) -> int:
        raise RuntimeError("no length")


def test_usable_id_count_needs_no_truth_value_of_the_container() -> None:
    # The poll calls this outside any try, on the same object the row loop
    # iterates, so it must not ask more of that object than the loop does.
    assert decoder._usable_id_count(None) == 0
    assert decoder._usable_id_count(_NoTruthValueList([SimpleNamespace(id="x")])) == 1


def test_identifier_structure_counts_what_the_row_loop_accepts() -> None:
    entries = [
        SimpleNamespace(id="usable"),
        SimpleNamespace(id=""),
        SimpleNamespace(id=5),
        SimpleNamespace(),
    ]
    device = _info(canonicIds=SimpleNamespace(canonicId=entries))

    assert decoder._identifier_structure(device) == ("<absent>", False, 0, 1)


def test_identifier_structure_never_raises() -> None:
    structure = decoder._identifier_structure

    assert structure(None) == ("<absent>", False, 0, 0)
    assert structure(object()) == ("<absent>", False, 0, 0)
    assert structure(SimpleNamespace()) == ("<absent>", False, 0, 0)

    # Phone double as in the canonicless tests: no generic canonicIds field.
    phone_double = SimpleNamespace(
        identifierInformation=SimpleNamespace(
            type=DeviceUpdate_pb2.IDENTIFIER_ANDROID,
            phoneInformation=SimpleNamespace(canonicIds=SimpleNamespace(canonicId=[])),
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
