# tests/test_sound_acknowledgement_push.py
"""Pin K13: a Play/Stop Sound acknowledgement push is not logged as a failed locate.

Nova answers every Play and Stop Sound with pushes that carry the play's request
UUID in ``fcmMetadata.requestUuid``, key material, and no location report
(measured 2026-10-17/18, ten rings). They still run through the decryptor,
because the key material feeds the device cache; only the INFO line
"No locations found" was misleading after every ring. The criterion is the
request UUID being one of OUR Play Sound keys: locate requests set a request
UUID as well, and an empty locate answer must keep its INFO line.
"""

from __future__ import annotations

import logging
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.googlefindmy.Auth import fcm_receiver_ha
from custom_components.googlefindmy.Auth.fcm_receiver_ha import FcmReceiverHA
from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator
from custom_components.googlefindmy.coordinator.helpers.cache import (
    SOUND_UUID_MAX_AGE_S,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker import (
    decrypt_locations,
)
from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2

_LOGGER_NAME = decrypt_locations.__name__
_OWN = "own-play-key"


def _coordinator_with_key(key: str | None, *, aged: bool = False) -> Any:
    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"dev-1": key} if key else {}  # type: ignore[attr-defined]
    coordinator._sound_request_timestamps = {  # type: ignore[attr-defined]
        "dev-1": time.time() - (SOUND_UUID_MAX_AGE_S + 60 if aged else 0)
    }
    return coordinator


@pytest.mark.parametrize(
    ("cached", "aged", "probe", "expected"),
    [
        pytest.param(_OWN, False, _OWN, True, id="own"),
        pytest.param(_OWN, True, _OWN, True, id="own-aged-still-cached"),
        pytest.param(_OWN, False, "locate-uuid", False, id="other"),
        pytest.param(_OWN, False, "", False, id="empty"),
        pytest.param(None, False, _OWN, False, id="no-cache"),
    ],
)
def test_coordinator_recognises_only_its_own_play_keys(
    cached: str | None, aged: bool, probe: str, expected: bool
) -> None:
    coordinator = _coordinator_with_key(cached, aged=aged)

    assert coordinator.is_own_sound_request_uuid(probe) is expected


def _receiver(*coordinators: Any) -> FcmReceiverHA:
    receiver = FcmReceiverHA()
    receiver.coordinators = list(coordinators)
    return receiver


def _with_entry(coordinator: Any, entry_id: str) -> Any:
    coordinator.config_entry = SimpleNamespace(entry_id=entry_id)
    return coordinator


def _update(request_uuid: str) -> Any:
    return SimpleNamespace(fcmMetadata=SimpleNamespace(requestUuid=request_uuid))


@pytest.mark.parametrize(
    ("request_uuid", "entry_of_key", "expected"),
    [
        pytest.param(_OWN, "entry-1", True, id="own-key-same-entry"),
        pytest.param("locate-uuid", "entry-1", False, id="locate-answer"),
        pytest.param("", "entry-1", False, id="stop-without-key"),
        pytest.param(_OWN, "entry-2", False, id="key-of-other-entry"),
    ],
)
def test_receiver_classifies_the_push(
    request_uuid: str, entry_of_key: str, expected: bool
) -> None:
    receiver = _receiver(_with_entry(_coordinator_with_key(_OWN), entry_of_key))

    assert receiver._is_sound_acknowledgement("entry-1", _update(request_uuid)) is (
        expected
    )


def test_receiver_check_never_breaks_the_push_path() -> None:
    """Any error in the check selects the old behaviour (INFO), nothing more."""

    broken = MagicMock()
    broken.config_entry = SimpleNamespace(entry_id="entry-1")
    broken.is_own_sound_request_uuid = MagicMock(side_effect=RuntimeError("boom"))
    receiver = _receiver(broken)

    assert receiver._is_sound_acknowledgement("entry-1", _update(_OWN)) is False
    assert receiver._is_sound_acknowledgement("entry-1", object()) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("own_key", [True, False])
async def test_receiver_passes_the_classification_to_the_decryptor(
    monkeypatch: pytest.MonkeyPatch, own_key: bool
) -> None:
    receiver = _receiver(_with_entry(_coordinator_with_key(_OWN), "entry-1"))
    receiver._entry_caches["entry-1"] = MagicMock()
    monkeypatch.setattr(
        fcm_receiver_ha.decoder_module,
        "parse_device_update_protobuf",
        MagicMock(return_value=_update(_OWN if own_key else "locate-uuid")),
    )
    decrypt = AsyncMock(return_value=[])
    monkeypatch.setattr(
        fcm_receiver_ha, "async_decrypt_location_response_locations", decrypt
    )

    await receiver._decode_background_location_async("entry-1", "deadbeef")

    assert decrypt.await_args is not None
    assert decrypt.await_args.kwargs["action_feedback"] is own_key


@pytest.mark.asyncio
@pytest.mark.parametrize("action_feedback", [False, True])
async def test_decryptor_only_changes_the_log_level(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    action_feedback: bool,
) -> None:
    """Same metadata-only result either way; INFO only for a non-acknowledgement."""

    base_now = 1_700_100_000.0
    monkeypatch.setattr(decrypt_locations.time, "time", lambda: base_now)

    async def fake_identity_key(*_args: object, **_kwargs: object) -> list[bytes]:
        return [b"\x41" * 32]

    monkeypatch.setattr(
        decrypt_locations, "async_retrieve_identity_key", fake_identity_key
    )
    update = DeviceUpdate_pb2.DeviceUpdate()
    registration = update.deviceMetadata.information.deviceRegistration
    registration.pairDate = int(base_now - 120)
    registration.encryptedUserSecrets.creationDate.seconds = int(base_now - 60)
    registration.encryptedUserSecrets.encryptedIdentityKey = b"\x00" * 32
    caplog.set_level(logging.DEBUG, logger=_LOGGER_NAME)

    result = await decrypt_locations.async_decrypt_location_response_locations(
        update, cache=object(), action_feedback=action_feedback
    )

    assert len(result) == 1
    assert result[0].get("metadata_only") is True
    levels = {
        record.getMessage(): record.levelno
        for record in caplog.records
        if record.name == _LOGGER_NAME
    }
    no_locations = levels["[DecryptLocations] No locations found."]
    acknowledgement = [m for m in levels if "acknowledgement push" in m]
    if action_feedback:
        assert no_locations == logging.DEBUG
        assert len(acknowledgement) == 1
        assert levels[acknowledgement[0]] == logging.DEBUG
    else:
        assert no_locations == logging.INFO
        assert acknowledgement == []
