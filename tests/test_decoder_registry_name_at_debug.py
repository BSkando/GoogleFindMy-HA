# tests/test_decoder_registry_name_at_debug.py
"""Device names stay at DEBUG in the decoder's WARNING records.

``ProtoDecoders.decoder.get_devices_with_location`` runs on every device list
refresh, without a user action.
AGENTS.md section 5 (b) therefore wants its records at INFO and above to name
the device by its canonical IDs, with the user-chosen display name in a DEBUG
companion record. This module pins that for the two WARNING records that used
to carry the name:

* the decrypt failure (``Failed to decrypt location for device with IDs``),
  reached by a stubbed decrypt helper that raises;
* the missing-key dump (``DEBUG STRUCTURE: Missing Key for device``) on the
  main poll pass.

Each case checks that no record at INFO or above carries the display name
(rendered message or ``repr(record.args)``), that the WARNING carries the
canonical ID, and that exactly one DEBUG companion carries both.

What it does not cover: the heal-failure ERROR in
``coordinator/registry._heal_tracker_device_subentry``. That branch cannot be
reached at runtime: ``_refresh_device_entry`` falls back to ``updated or
device``, and ``device`` is checked for ``None`` at the top of the helper, so
``healed_device`` is never ``None``. Its record is pinned statically by
``tests/test_guard_unattended_device_names.py``. The four ``-> ... fields``
records of the missing-key dump log protobuf field names, not device names,
and are not checked here.

Retire this module when the two records stop logging at INFO or above.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.googlefindmy.ProtoDecoders import decoder

DEVICE_NAME = "Sentinel Hall Tag"
CANONICAL_ID = "tracker-sentinel-1"


def _canonic_ids() -> SimpleNamespace:
    return SimpleNamespace(canonicId=[SimpleNamespace(id=CANONICAL_ID)])


def _fields(*names: str) -> list[tuple[SimpleNamespace, None]]:
    return [(SimpleNamespace(name=name), None) for name in names]


def _tracker_device(*, with_key: bool, with_reports: bool) -> SimpleNamespace:
    """Build a tracker-shaped device like ``tests/test_phone_device_key_handling.py``."""
    secrets = SimpleNamespace(
        encryptedIdentityKey=b"0" * 60 if with_key else b"",
        ownerKeyVersion=1 if with_key else 0,
        HasField=lambda field: with_key and field == "encryptedIdentityKey",
        ListFields=lambda: _fields("encryptedIdentityKey") if with_key else [],
    )
    registration = SimpleNamespace(
        encryptedUserSecrets=secrets,
        deviceTypeInformation=SimpleNamespace(deviceType=1),
        HasField=lambda field: (
            field in ("encryptedUserSecrets", "deviceTypeInformation")
        ),
        ListFields=lambda: _fields("encryptedUserSecrets", "deviceTypeInformation"),
    )
    recent = SimpleNamespace(
        networkLocations=[],
        HasField=lambda field: field == "recentLocation",
    )
    reports = SimpleNamespace(
        recentLocationAndNetworkLocations=recent,
        HasField=lambda field: field == "recentLocationAndNetworkLocations",
    )
    location_information = SimpleNamespace(
        reports=reports,
        HasField=lambda field: field == "reports",
        ListFields=lambda: _fields("reports"),
    )
    info_fields = {"deviceRegistration"} | (
        {"locationInformation"} if with_reports else set()
    )
    information = SimpleNamespace(
        deviceRegistration=registration,
        locationInformation=location_information,
        HasField=lambda field: field in info_fields,
        ListFields=lambda: _fields(*sorted(info_fields)),
    )
    return SimpleNamespace(
        identifierInformation=SimpleNamespace(type=2, canonicIds=_canonic_ids()),
        information=information,
        userDefinedDeviceName=DEVICE_NAME,
        imageInformation=SimpleNamespace(url=""),
        HasField=lambda field: field == "information",
        ListFields=lambda: _fields(
            "identifierInformation", "information", "userDefinedDeviceName"
        ),
    )


def _record_text(record: logging.LogRecord) -> str:
    return f"{record.getMessage()} {record.args!r}"


def _assert_name_only_at_debug(
    caplog: pytest.LogCaptureFixture, warning_prefix: str, companion_prefix: str
) -> None:
    loud = [r for r in caplog.records if r.levelno >= logging.INFO]
    assert [r for r in loud if DEVICE_NAME in _record_text(r)] == []
    warnings = [
        r
        for r in loud
        if r.levelno == logging.WARNING and r.getMessage().startswith(warning_prefix)
    ]
    assert len(warnings) == 1
    assert CANONICAL_ID in warnings[0].getMessage()

    companions = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and r.getMessage().startswith(companion_prefix)
    ]
    assert len(companions) == 1
    assert DEVICE_NAME in companions[0].getMessage()
    assert CANONICAL_ID in companions[0].getMessage()


def test_decrypt_failure_names_the_device_by_canonical_id(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def _raise(*_a: Any, **_k: Any) -> list[dict[str, Any]]:
        raise RuntimeError("boom")

    # The decoder imports the decrypt helper inside the function, so the patch
    # goes to the defining module. ``DeviceUpdate`` is replaced as well: the real
    # protobuf ``CopyFrom`` would reject the stub device before the helper runs.
    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker."
        "decrypt_locations.decrypt_location_response_locations",
        _raise,
    )
    copied: list[Any] = []
    fake_update = SimpleNamespace(
        deviceMetadata=SimpleNamespace(CopyFrom=copied.append)
    )
    monkeypatch.setattr(decoder.DeviceUpdate_pb2, "DeviceUpdate", lambda: fake_update)
    device_list = SimpleNamespace(
        deviceMetadata=[_tracker_device(with_key=True, with_reports=True)]
    )

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=SimpleNamespace(entry_id="entry-A")
        )

    assert len(copied) == 1
    _assert_name_only_at_debug(
        caplog,
        "Failed to decrypt location for device with IDs",
        "Decrypt failure above concerns",
    )
    decrypt_warning = next(
        r
        for r in caplog.records
        if r.getMessage().startswith("Failed to decrypt location")
    )
    assert "boom" in decrypt_warning.getMessage()


def test_missing_key_dump_names_the_device_by_canonical_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    device_list = SimpleNamespace(
        deviceMetadata=[_tracker_device(with_key=False, with_reports=False)]
    )

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list,
            cache=SimpleNamespace(entry_id="entry-A"),
            emit_canonicless_diagnostics=True,
        )

    _assert_name_only_at_debug(
        caplog,
        "DEBUG STRUCTURE: Missing Key for device",
        "Missing-key device above is",
    )
