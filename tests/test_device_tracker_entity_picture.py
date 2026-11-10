# tests/test_device_tracker_entity_picture.py
"""The main tracker shows Google's product image as ``entity_picture``.

Source of the address, in this order, the first non-empty allowed entry wins:
``DeviceMetadata.imageInformation.imageUrl``, then the entries of
``imageInformation.componentImages`` (bytes, form ``ImageInformation.ComponentImage``),
then the entries of ``deviceRegistration.deviceTypeInformation.deviceComponentsInformation``
(bytes, form ``DeviceComponentInformation``). A bytes entry that does not parse
is skipped.

An address is allowed only over ``https`` with a host that equals one of
``_IMAGE_HOSTS`` exactly (``urllib.parse.urlsplit(...).hostname``), without
userinfo and without an explicit port. Only ``lh3.googleusercontent.com`` was
measured (public, model-bound product images, no sign-in); ``lh4`` to ``lh6``
are pinned as rejected until somebody measures them.

Data path: decoder row ``image_url`` -> ``async_get_basic_device_list`` ->
poll cycle (``_device_caps[device_id]["image_url"]``) ->
``GoogleFindMyCoordinator.get_device_image_url`` -> ``_sync_location_attrs``.
A device list without an address keeps the stored one. The image is an entity
property, never a state attribute. The last-location tracker shows none.

All addresses here are made up; they only use the measured host.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.googlefindmy.const import DOMAIN, TRACKER_SUBENTRY_KEY
from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2
from custom_components.googlefindmy.ProtoDecoders import decoder as _decoder
from custom_components.googlefindmy.ProtoDecoders.decoder import (
    get_devices_with_location,
)
from tests.helpers.config_entries_stub import make_config_entry

_HOST = "lh3.googleusercontent.com"
_URL_MAIN = f"https://{_HOST}/test-main-image"
_URL_COMPONENT = f"https://{_HOST}/test-component-image"
_URL_REGISTRATION = f"https://{_HOST}/test-registration-image"


# ---------------------------------------------------------------------------
# Decoder: precedence and parsing
# ---------------------------------------------------------------------------


def _device_list(
    *,
    main: str | None = None,
    components: list[bytes] | None = None,
    registration: list[bytes] | None = None,
) -> DeviceUpdate_pb2.DevicesList:
    devices_list = DeviceUpdate_pb2.DevicesList()
    device = devices_list.deviceMetadata.add()
    device.userDefinedDeviceName = "Tracker"
    device.identifierInformation.canonicIds.canonicId.add().id = "dev-1"
    secrets = device.information.deviceRegistration.encryptedUserSecrets
    secrets.encryptedIdentityKey = b"\x01\x02\x03"
    secrets.ownerKeyVersion = 1
    if main is not None:
        device.imageInformation.imageUrl = main
    for raw in components or []:
        device.imageInformation.componentImages.append(raw)
    type_info = device.information.deviceRegistration.deviceTypeInformation
    for raw in registration or []:
        type_info.deviceComponentsInformation.append(raw)
    return devices_list


def _component(url: str) -> bytes:
    return DeviceUpdate_pb2.ImageInformation.ComponentImage(
        imageUrl=url
    ).SerializeToString()


def _registration(url: str) -> bytes:
    return DeviceUpdate_pb2.DeviceComponentInformation(imageUrl=url).SerializeToString()


def _decoded_image_url(devices_list: DeviceUpdate_pb2.DevicesList) -> Any:
    rows = get_devices_with_location(devices_list, cache=None)
    assert len(rows) == 1
    return rows[0]["image_url"]


def test_main_image_wins_over_component_and_registration() -> None:
    devices_list = _device_list(
        main=_URL_MAIN,
        components=[_component(_URL_COMPONENT)],
        registration=[_registration(_URL_REGISTRATION)],
    )
    assert _decoded_image_url(devices_list) == _URL_MAIN


def test_component_image_wins_over_registration() -> None:
    devices_list = _device_list(
        components=[_component(_URL_COMPONENT)],
        registration=[_registration(_URL_REGISTRATION)],
    )
    assert _decoded_image_url(devices_list) == _URL_COMPONENT


def test_registration_image_is_the_last_fallback() -> None:
    devices_list = _device_list(registration=[_registration(_URL_REGISTRATION)])
    assert _decoded_image_url(devices_list) == _URL_REGISTRATION


def test_rejected_main_image_falls_through_to_next_source() -> None:
    devices_list = _device_list(
        main="http://lh3.googleusercontent.com/test-main-image",
        components=[_component(_URL_COMPONENT)],
    )
    assert _decoded_image_url(devices_list) == _URL_COMPONENT


def test_broken_bytes_entry_is_skipped() -> None:
    # 0x1A announces field 3 (0x0A field 1) with 0x7F bytes that never come.
    broken = b"\x1a\x7f" + b"x" * 3
    devices_list = _device_list(
        components=[broken, _component(_URL_COMPONENT)],
        registration=[b"\x0a\x7f"],
    )
    assert _decoded_image_url(devices_list) == _URL_COMPONENT


def test_only_broken_bytes_entries_give_none() -> None:
    devices_list = _device_list(components=[b"\x1a\x7f"], registration=[b"\x0a\x7f"])
    assert _decoded_image_url(devices_list) is None


def test_device_without_image_gives_none() -> None:
    assert _decoded_image_url(_device_list()) is None


def test_foreign_host_in_every_source_gives_none() -> None:
    devices_list = _device_list(
        main="https://fremd.example/x",
        components=[_component("https://fremd.example/y")],
        registration=[_registration("https://fremd.example/z")],
    )
    assert _decoded_image_url(devices_list) is None


# ---------------------------------------------------------------------------
# Host check
# ---------------------------------------------------------------------------


def test_image_hosts_hold_only_the_measured_host() -> None:
    assert frozenset({_HOST}) == _decoder._IMAGE_HOSTS


@pytest.mark.parametrize(
    ("url", "allowed"),
    [
        (f"https://{_HOST}/x", True),
        (f"https://{_HOST}/x=w100", True),
        ("https://lh4.googleusercontent.com/x", False),
        ("https://lh5.googleusercontent.com/x", False),
        ("https://lh6.googleusercontent.com/x", False),
    ],
)
def test_cdn_variants(url: str, allowed: bool) -> None:
    assert _decoder.is_allowed_image_url(url) is allowed


@pytest.mark.parametrize(
    "url",
    [
        "http://lh3.googleusercontent.com/x",
        "https://fremd.example/x",
        "https://lh3.googleusercontent.com@fremd.example/x",
        "https://user@lh3.googleusercontent.com/x",
        "https://lh3.googleusercontent.com:8443/x",
        "https://lh3.googleusercontent.com:443/x",
        "https://xlh3.googleusercontent.com/x",
        "https://lh3.googleusercontent.com.fremd.example/x",
        "//lh3.googleusercontent.com/x",
        "lh3.googleusercontent.com/x",
        "https://lh3.googleusercontent.com:bad/x",
        "https://lh3.googleusercontent.com/x y",
        "",
        None,
        b"https://lh3.googleusercontent.com/x",
    ],
)
def test_rejected_addresses(url: Any) -> None:
    assert _decoder.is_allowed_image_url(url) is False


# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------


class _CoordinatorStub:
    """Coordinator serving one device row and one image address."""

    def __init__(self, image_url: Any, *, with_getter: bool = True) -> None:
        self.hass = SimpleNamespace()
        self.config_entry = make_config_entry(
            entry_id="entry-picture", options={}, runtime_data=None
        )
        self.row: dict[str, Any] = {
            "id": "dev-1",
            "device_id": "dev-1",
            "name": "Tracker",
            "latitude": 48.1,
            "longitude": 11.5,
            "accuracy": 10.0,
            "last_seen": 1_700_000_000.0,
        }
        self.image_url = image_url
        if with_getter:
            self.get_device_image_url = self._get_device_image_url

    def _get_device_image_url(self, device_id: str) -> Any:
        return self.image_url if device_id == "dev-1" else None

    def async_add_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        return lambda: None

    def is_device_visible_in_subentry(self, key: str, device_id: str) -> bool:
        return True

    def is_device_present(self, device_id: str) -> bool:
        return True

    def get_device_location_data_for_subentry(
        self, key: str | None, device_id: str
    ) -> dict[str, Any] | None:
        return dict(self.row)

    def get_display_location_data_for_subentry(
        self, key: str | None, device_id: str
    ) -> dict[str, Any] | None:
        return dict(self.row)

    def get_fresh_coarse_fix(self, device_id: str) -> dict[str, Any] | None:
        return None

    def stable_subentry_identifier(
        self, *, key: str | None = None, feature: str | None = None
    ) -> str | None:
        return key

    def get_subentry_metadata(
        self, *, key: str | None = None, feature: str | None = None
    ) -> Any:
        return SimpleNamespace(
            config_subentry_id=key,
            visible_device_ids=["dev-1"],
            enabled_device_ids=["dev-1"],
        )


def _tracker(coordinator: Any, *, last_location: bool = False) -> Any:
    from custom_components.googlefindmy import device_tracker

    cls = (
        device_tracker.GoogleFindMyLastLocationTracker
        if last_location
        else device_tracker.GoogleFindMyDeviceTracker
    )
    entity = cls(
        coordinator,
        {"id": "dev-1", "name": "Tracker"},
        subentry_key=TRACKER_SUBENTRY_KEY,
        subentry_identifier=TRACKER_SUBENTRY_KEY,
    )
    entity.hass = SimpleNamespace()
    entity.entity_id = "device_tracker.googlefindmy_tracker"
    return entity


def _picture(entity: Any) -> Any:
    entity._sync_location_attrs()
    return entity._attr_entity_picture


def test_entity_shows_the_image_unchanged() -> None:
    entity = _tracker(_CoordinatorStub(f"https://{_HOST}/test-image=w100"))
    assert _picture(entity) == f"https://{_HOST}/test-image=w100"


def test_entity_picture_is_no_state_attribute() -> None:
    entity = _tracker(_CoordinatorStub(_URL_MAIN))
    entity._sync_location_attrs()
    attributes = entity._attr_extra_state_attributes
    assert _URL_MAIN not in attributes.values()
    assert "image_url" not in attributes
    assert "entity_picture" not in attributes


@pytest.mark.parametrize(
    "image_url",
    [
        "http://lh3.googleusercontent.com/x",
        "https://fremd.example/x",
        "https://lh3.googleusercontent.com.fremd.example/x",
        None,
        "",
    ],
)
def test_entity_rejects_what_the_check_rejects(image_url: Any) -> None:
    entity = _tracker(_CoordinatorStub(image_url))
    assert _picture(entity) is None


def test_entity_without_coordinator_accessor_has_no_picture() -> None:
    entity = _tracker(_CoordinatorStub(_URL_MAIN, with_getter=False))
    assert _picture(entity) is None


def test_entity_picture_follows_the_coordinator() -> None:
    coordinator = _CoordinatorStub(None)
    entity = _tracker(coordinator)
    assert _picture(entity) is None
    coordinator.image_url = _URL_MAIN
    assert _picture(entity) == _URL_MAIN


def test_last_location_tracker_has_no_picture() -> None:
    entity = _tracker(_CoordinatorStub(_URL_MAIN), last_location=True)
    assert _picture(entity) is None


# ---------------------------------------------------------------------------
# Data path through the real coordinator poll
# ---------------------------------------------------------------------------


class _DummyAPI:
    def __init__(self) -> None:
        self.device_list: list[dict[str, Any]] = []
        self.fcm: Any | None = None

    async def async_get_basic_device_list(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.device_list]

    def is_push_ready(self) -> bool:
        return True


class _DummyCache:
    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    async def async_get_cached_value(self, key: str) -> Any:
        return self._store.get(key)

    async def async_set_cached_value(self, key: str, value: Any) -> None:
        self._store[key] = value


class _DummyEntry:
    def __init__(self) -> None:
        self.entry_id = "entry-picture"
        self.data: dict[str, Any] = {}

    def async_start_reauth(self, hass: Any) -> None:
        return None

    def async_create_background_task(self, hass: Any, coro: Any, *, name: str) -> Any:
        return hass.async_create_task(coro, name=name)


class _DummyConfigEntries:
    def async_get_subentries(self, _entry_id: str) -> list[Any]:
        return []


class _DummyHass:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.bus = SimpleNamespace(async_fire=lambda *_a, **_k: None)
        self.config_entries = _DummyConfigEntries()
        self.data: dict[str, dict[str, Any]] = {DOMAIN: {}}

    def async_create_task(self, coro: Any, *, name: str | None = None) -> Any:
        return self.loop.create_task(coro, name=name)


@pytest.fixture
def poll_coordinator(monkeypatch: pytest.MonkeyPatch) -> _DummyAPI:
    """Patch the coordinator module so it builds over ``_DummyAPI``."""

    api = _DummyAPI()
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.main.GoogleFindMyAPI",
        lambda *_a, **_k: api,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.GoogleFindMyCoordinator._async_load_stats",
        AsyncMock(return_value=None),
    )
    return api


def _coordinator() -> Any:
    """Build a coordinator on the running test loop (pytest-asyncio)."""

    from custom_components.googlefindmy.coordinator import (
        FcmStatus,
        GoogleFindMyCoordinator,
    )

    coord = GoogleFindMyCoordinator(
        _DummyHass(asyncio.get_running_loop()), cache=_DummyCache()
    )
    coord.config_entry = _DummyEntry()
    coord.async_set_updated_data = Mock()
    coord._async_build_device_snapshot_with_fallbacks = AsyncMock(return_value=[])
    coord._async_start_poll_cycle = AsyncMock()
    coord._ensure_registry_for_devices = lambda *_args, **_kwargs: 0
    coord._schedule_short_retry = lambda *_args, **_kwargs: None
    coord._get_ignored_set = set
    coord._is_fcm_ready_soft = lambda: True
    coord._set_fcm_status(FcmStatus.CONNECTED)
    # No debounced stats writer: it would outlive the test as a pending task.
    coord._schedule_stats_persist = lambda: None
    return coord


async def test_decoder_row_reaches_the_coordinator_and_survives_a_list_without_image(
    poll_coordinator: _DummyAPI,
) -> None:
    api = poll_coordinator
    coord = _coordinator()

    rows = get_devices_with_location(
        _device_list(components=[_component(_URL_COMPONENT)]), cache=None
    )
    api.device_list = rows
    await coord._async_update_data()
    assert coord.get_device_image_url("dev-1") == _URL_COMPONENT

    # A later list (or a locate row built by the same decoder) without an
    # address carries ``image_url: None``; it must not erase the stored one.
    api.device_list = get_devices_with_location(_device_list(), cache=None)
    assert api.device_list[0]["image_url"] is None
    await coord._async_update_data()
    assert coord.get_device_image_url("dev-1") == _URL_COMPONENT

    # The entity reads it through the coordinator accessor.
    coordinator_stub = _CoordinatorStub(coord.get_device_image_url("dev-1"))
    assert _picture(_tracker(coordinator_stub)) == _URL_COMPONENT


async def test_coordinator_ignores_a_rejected_address_from_the_list(
    poll_coordinator: _DummyAPI,
) -> None:
    api = poll_coordinator
    coord = _coordinator()
    api.device_list = [
        {"id": "dev-1", "name": "Tracker", "image_url": "https://fremd.example/x"}
    ]
    await coord._async_update_data()
    assert coord.get_device_image_url("dev-1") is None
    assert coord.get_device_image_url("dev-unknown") is None


async def test_purge_device_drops_the_image(poll_coordinator: _DummyAPI) -> None:
    api = poll_coordinator
    coord = _coordinator()
    api.device_list = [{"id": "dev-1", "name": "Tracker", "image_url": _URL_MAIN}]
    await coord._async_update_data()
    assert coord.get_device_image_url("dev-1") == _URL_MAIN
    coord.data = []
    coord.purge_device("dev-1")
    assert coord.get_device_image_url("dev-1") is None
