# tests/test_services_refresh_device_urls.py
"""Validate refresh_device_urls service token scoping and canonical identifiers."""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any

import pytest
from homeassistant.core import ServiceCall
from homeassistant.helpers.network import NoURLAvailableError

from custom_components.googlefindmy import const, services
from tests.helpers.config_entries_stub import make_config_entry
from tests.helpers.service_doubles import FakeServiceRegistry

_StubServices = FakeServiceRegistry


class _StubConfigEntries:
    """Config entry manager stub providing domain-filtered lookups."""

    def __init__(self, entries: list[SimpleNamespace]) -> None:
        self._entries = entries
        self.setup_calls: list[str] = []

    def async_entries(self, domain: str) -> list[SimpleNamespace]:  # noqa: D401 - simple passthrough
        return list(self._entries) if domain == const.DOMAIN else []

    def async_get_entry(self, entry_id: str) -> SimpleNamespace | None:
        for entry in self._entries:
            if entry.entry_id == entry_id:
                return entry
        return None

    def async_get_subentries(self, entry_id: str) -> list[Any]:
        entry = self.async_get_entry(entry_id)
        if entry is None:
            return []
        subentries = getattr(entry, "subentries", None)
        if isinstance(subentries, dict):
            return list(subentries.values())
        return []

    async def async_setup(self, entry_id: str) -> bool:
        self.setup_calls.append(entry_id)
        return True


class _StubDeviceRegistry:
    """Minimal device registry capturing configuration URL updates."""

    def __init__(self, devices: dict[str, SimpleNamespace]) -> None:
        self.devices = devices
        self.updated: dict[str, str] = {}

    def async_entries_for_config_entry(
        self, config_entry_id: str
    ) -> list[SimpleNamespace]:
        """Return the devices owned by ``config_entry_id``.

        The service asks per entry rather than scanning the whole registry, so
        the double has to answer the same question core answers.
        """
        return [
            device
            for device in self.devices.values()
            if config_entry_id in getattr(device, "config_entries", ())
        ]

    def async_update_device(self, *, device_id: str, configuration_url: str) -> None:
        self.updated[device_id] = configuration_url
        self.devices[device_id].configuration_url = configuration_url


def test_refresh_device_urls_uses_entry_scoped_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each device URL uses the owning entry's token policy and canonical identifier."""

    fake_now = 1_209_600  # Aligns with a deterministic week bucket.
    base_url = "https://example.test"

    entry_one = SimpleNamespace(
        entry_id="entry-1",
        options={},
        data={},
    )
    entry_one.runtime_data = SimpleNamespace(coordinator=SimpleNamespace())
    entry_two = SimpleNamespace(
        entry_id="entry-2",
        options={const.OPT_MAP_VIEW_TOKEN_EXPIRATION: True},
        data={},
    )
    entry_two.runtime_data = SimpleNamespace(coordinator=SimpleNamespace())
    config_entries = _StubConfigEntries([entry_one, entry_two])

    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = config_entries

    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }

    devices = {
        "ha-dev-1": SimpleNamespace(
            id="ha-dev-1",
            identifiers={(const.DOMAIN, "entry-1:device-alpha")},
            config_entries={"entry-1"},
            serial_number=None,
            name="Alpha",
            name_by_user=None,
        ),
        "ha-dev-2": SimpleNamespace(
            id="ha-dev-2",
            identifiers={(const.DOMAIN, "entry-2:device-beta")},
            config_entries={"entry-2"},
            serial_number="beta-serial",
            name="Beta",
            name_by_user="Backpack",
        ),
        "ha-service": SimpleNamespace(
            id="ha-service",
            identifiers={
                (const.DOMAIN, f"{const.SERVICE_DEVICE_IDENTIFIER_PREFIX}entry-1")
            },
            config_entries={"entry-1"},
            serial_number=None,
            name="Service",
            name_by_user=None,
        ),
    }
    device_registry = _StubDeviceRegistry(devices)

    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", lambda hass, **kwargs: base_url)
    monkeypatch.setattr(services.time, "time", lambda: fake_now)

    async def _run_refresh() -> None:
        await services.async_register_services(hass, ctx)
        handler = hass.services.registered[
            (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
        ]
        await handler(ServiceCall({}))

    asyncio.run(_run_refresh())

    expected_entry_one_token = const.map_token_hex_digest(
        const.map_token_secret_seed("ha-uuid", "entry-1", False)
    )
    expected_entry_two_token = const.map_token_hex_digest(
        const.map_token_secret_seed("ha-uuid", "entry-2", True, now=fake_now)
    )

    assert device_registry.updated == {
        "ha-dev-1": f"{base_url}/api/googlefindmy/map/device-alpha?token={expected_entry_one_token}",
        "ha-dev-2": f"{base_url}/api/googlefindmy/map/beta-serial?token={expected_entry_two_token}",
    }
    assert "ha-service" not in device_registry.updated


@pytest.mark.asyncio
async def test_refresh_device_urls_clears_url_for_a_disabled_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A device owned by a ``map_view_enabled=False`` entry gets no configuration_url.

    Mirrors ``entity.py``'s ``device_configuration_url``: the service must not
    hand out a link to an endpoint that refuses every request for this entry.
    """

    base_url = "https://example.test"

    entry = make_config_entry(
        entry_id="entry-1",
        options={const.OPT_MAP_VIEW_ENABLED: False},
        data={},
        runtime_data=SimpleNamespace(coordinator=SimpleNamespace()),
    )
    config_entries = _StubConfigEntries([entry])

    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = config_entries

    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }

    devices = {
        "ha-dev-1": SimpleNamespace(
            id="ha-dev-1",
            identifiers={(const.DOMAIN, "entry-1:device-alpha")},
            config_entries={"entry-1"},
            serial_number=None,
            name="Alpha",
            name_by_user=None,
            configuration_url=f"{base_url}/api/googlefindmy/map/device-alpha?token=stale",
        ),
    }
    device_registry = _StubDeviceRegistry(devices)

    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", lambda hass, **kwargs: base_url)

    async def _run_refresh() -> None:
        await services.async_register_services(hass, ctx)
        handler = hass.services.registered[
            (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
        ]
        await handler(ServiceCall({}))

    await _run_refresh()

    assert device_registry.updated == {"ha-dev-1": None}


@pytest.mark.asyncio
async def test_refresh_device_urls_uses_the_queried_entry_not_the_device_shim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The owning entry comes from the query, not from the device's own list.

    The device lists ``entry-2`` first and is answered for the ``entry-1``
    query; the token must be ``entry-1``'s.

    What this does **not** show is a difference to the code before AP-13. That
    code walked ``config_entries`` and stopped at the first member that was one
    of ours, so it picked ``entry-1`` here as well. The case where the two really
    part company is the one in
    ``test_refresh_device_urls_ignores_a_device_owned_by_a_foreign_entry``. This
    test pins the *source* of the owner id, which is what a later refactor is
    most likely to swap back.
    """
    fake_now = 1_209_600
    base_url = "https://example.test"

    entry_one = make_config_entry(entry_id="entry-1", data={}, options={})
    entry_one.runtime_data = SimpleNamespace(coordinator=SimpleNamespace())
    config_entries = _StubConfigEntries([entry_one])

    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = config_entries

    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }

    devices = {
        "ha-dev-1": SimpleNamespace(
            id="ha-dev-1",
            identifiers={(const.DOMAIN, "entry-1:device-alpha")},
            # A list rather than a set: the order has to be observable for the
            # assertion below to mean anything.
            config_entries=["entry-2", "entry-1"],
            serial_number=None,
            name="Alpha",
            name_by_user=None,
        ),
    }
    device_registry = _StubDeviceRegistry(devices)

    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", lambda hass, **kwargs: base_url)
    monkeypatch.setattr(services.time, "time", lambda: fake_now)

    await services.async_register_services(hass, ctx)
    handler = hass.services.registered[
        (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
    ]
    await handler(ServiceCall({}))

    expected = const.map_token_hex_digest(
        const.map_token_secret_seed("ha-uuid", "entry-1", False)
    )
    foreign = const.map_token_hex_digest(
        const.map_token_secret_seed("ha-uuid", "entry-2", False)
    )
    assert device_registry.updated["ha-dev-1"].endswith(f"?token={expected}")
    assert foreign not in device_registry.updated["ha-dev-1"]


@pytest.mark.asyncio
async def test_refresh_device_urls_ignores_a_device_owned_by_a_foreign_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A device carrying our identifier but owned by nobody of ours is left alone.

    This is the behaviour AP-13 changed. The registry-wide scan it replaced took
    the first member of ``DeviceEntry.config_entries`` when none of them was one
    of ours, and then signed the map URL with a token seeded from that foreign
    entry id. Asking per config entry cannot reach such a device at all, which is
    the point: it is registry residue, not one of ours.
    """
    fake_now = 1_209_600
    base_url = "https://example.test"

    entry_one = make_config_entry(entry_id="entry-1", data={}, options={})
    entry_one.runtime_data = SimpleNamespace(coordinator=SimpleNamespace())
    config_entries = _StubConfigEntries([entry_one])

    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = config_entries

    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }

    devices = {
        "ha-dev-1": SimpleNamespace(
            id="ha-dev-1",
            identifiers={(const.DOMAIN, "device-alpha")},
            config_entries=["foreign-entry"],
            serial_number=None,
            name="Alpha",
            name_by_user=None,
        ),
    }
    device_registry = _StubDeviceRegistry(devices)

    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", lambda hass, **kwargs: base_url)
    monkeypatch.setattr(services.time, "time", lambda: fake_now)

    await services.async_register_services(hass, ctx)
    handler = hass.services.registered[
        (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
    ]
    await handler(ServiceCall({}))

    assert device_registry.updated == {}


def test_refresh_device_urls_skips_when_base_url_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not rewrite configuration URLs when no external URL is available."""

    entry = SimpleNamespace(
        entry_id="entry-1",
        options={},
        data={},
    )
    entry.runtime_data = SimpleNamespace(coordinator=SimpleNamespace())
    config_entries = _StubConfigEntries([entry])

    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = config_entries

    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }

    devices = {
        "ha-dev-1": SimpleNamespace(
            id="ha-dev-1",
            identifiers={(const.DOMAIN, "entry-1:device-alpha")},
            config_entries={"entry-1"},
            serial_number=None,
            name="Alpha",
            name_by_user=None,
            configuration_url="https://existing.test",
        ),
    }
    device_registry = _StubDeviceRegistry(devices)

    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)

    def _raise_url_error(*_: Any, **__: Any) -> str:
        raise NoURLAvailableError()

    monkeypatch.setattr(services, "get_url", _raise_url_error)

    async def _run_refresh() -> None:
        await services.async_register_services(hass, ctx)
        handler = hass.services.registered[
            (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
        ]
        await handler(ServiceCall({}))

    asyncio.run(_run_refresh())

    assert device_registry.updated == {}
    assert devices["ha-dev-1"].configuration_url == "https://existing.test"


def test_refresh_device_urls_skips_when_base_url_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not rewrite configuration URLs when get_url returns ``None``."""

    entry = SimpleNamespace(
        entry_id="entry-1",
        options={},
        data={},
    )
    entry.runtime_data = SimpleNamespace(coordinator=SimpleNamespace())
    config_entries = _StubConfigEntries([entry])

    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = config_entries

    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }

    devices = {
        "ha-dev-1": SimpleNamespace(
            id="ha-dev-1",
            identifiers={(const.DOMAIN, "entry-1:device-alpha")},
            config_entries={"entry-1"},
            serial_number=None,
            name="Alpha",
            name_by_user=None,
            configuration_url="https://existing.test",
        ),
    }
    device_registry = _StubDeviceRegistry(devices)

    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", lambda *_, **__: None)

    async def _run_refresh() -> None:
        await services.async_register_services(hass, ctx)
        handler = hass.services.registered[
            (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
        ]
        await handler(ServiceCall({}))

    asyncio.run(_run_refresh())

    assert device_registry.updated == {}
    assert devices["ha-dev-1"].configuration_url == "https://existing.test"


@pytest.mark.asyncio
async def test_refresh_device_urls_visits_a_shared_device_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A device hanging on two of our entries is written once, not twice.

    Below Core 2026.8 a device can belong to several config entries at the same
    time, so the per-entry query returns it once per entry. The first entry asked
    wins and seeds the map token; ``_canonical_identifier`` strips any of our
    entries' namespaces from the device id.
    """
    fake_now = 1_209_600
    base_url = "https://example.test"

    entry_one = make_config_entry(entry_id="entry-1", data={}, options={})
    entry_one.runtime_data = SimpleNamespace(coordinator=SimpleNamespace())
    entry_two = make_config_entry(entry_id="entry-2", data={}, options={})
    entry_two.runtime_data = SimpleNamespace(coordinator=SimpleNamespace())
    config_entries = _StubConfigEntries([entry_one, entry_two])

    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = config_entries

    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }

    devices = {
        "ha-shared": SimpleNamespace(
            id="ha-shared",
            identifiers={(const.DOMAIN, "entry-1:device-alpha")},
            config_entries=["entry-1", "entry-2"],
            serial_number=None,
            name="Alpha",
            name_by_user=None,
        ),
        # No identifier of ours: owned by one of our entries, but not one of our
        # devices. The domain filter has to keep it out.
        "ha-foreign": SimpleNamespace(
            id="ha-foreign",
            identifiers={("other_domain", "whatever")},
            config_entries=["entry-1"],
            serial_number=None,
            name="Foreign",
            name_by_user=None,
        ),
    }
    device_registry = _StubDeviceRegistry(devices)

    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", lambda hass, **kwargs: base_url)
    monkeypatch.setattr(services.time, "time", lambda: fake_now)

    await services.async_register_services(hass, ctx)
    handler = hass.services.registered[
        (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
    ]
    await handler(ServiceCall({}))

    expected = const.map_token_hex_digest(
        const.map_token_secret_seed("ha-uuid", "entry-1", False)
    )
    assert device_registry.updated == {
        "ha-shared": f"{base_url}/api/googlefindmy/map/device-alpha?token={expected}"
    }


@pytest.mark.asyncio
async def test_refresh_device_urls_prefers_an_enabled_owner_of_a_shared_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A disabled owner asked first does not clear a shared device's link.

    Below Core 2026.8 a device can hang on two of our entries. When the first
    entry asked has Map View off and the second has it on, the device keeps a
    link signed for the enabled entry instead of being cleared. The device
    still carries the disabled entry's namespaced identifier; the link path
    must use the bare device id the map view looks up.
    """
    base_url = "https://example.test"

    entry_one = make_config_entry(
        entry_id="entry-1", data={}, options={const.OPT_MAP_VIEW_ENABLED: False}
    )
    entry_two = make_config_entry(entry_id="entry-2", data={}, options={})
    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = _StubConfigEntries([entry_one, entry_two])

    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }
    devices = {
        "ha-shared": SimpleNamespace(
            id="ha-shared",
            identifiers={(const.DOMAIN, "entry-1:device-alpha")},
            config_entries=["entry-1", "entry-2"],
            serial_number=None,
            name="Alpha",
            name_by_user=None,
            configuration_url=f"{base_url}/api/googlefindmy/map/device-alpha?token=old",
        ),
    }
    device_registry = _StubDeviceRegistry(devices)
    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", lambda hass, **kwargs: base_url)

    await services.async_register_services(hass, ctx)
    handler = hass.services.registered[
        (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
    ]
    await handler(ServiceCall({}))

    expected = const.map_token_hex_digest(
        const.map_token_secret_seed("ha-uuid", "entry-2", False)
    )
    assert device_registry.updated == {
        "ha-shared": f"{base_url}/api/googlefindmy/map/device-alpha?token={expected}"
    }


@pytest.mark.asyncio
async def test_refresh_device_urls_keeps_a_prefix_that_is_not_one_of_our_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only our own entries' namespaces are stripped from the device id.

    A colon-separated prefix that names no GoogleFindMy entry is part of the
    device id itself and stays in the link path.
    """
    base_url = "https://example.test"
    entry = make_config_entry(entry_id="entry-1", data={}, options={})
    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = _StubConfigEntries([entry])
    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }
    devices = {
        "ha-dev": SimpleNamespace(
            id="ha-dev",
            identifiers={(const.DOMAIN, "other:device-alpha")},
            config_entries=["entry-1"],
            serial_number=None,
            name="Alpha",
            name_by_user=None,
            configuration_url=None,
        ),
    }
    device_registry = _StubDeviceRegistry(devices)
    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", lambda hass, **kwargs: base_url)

    await services.async_register_services(hass, ctx)
    handler = hass.services.registered[
        (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
    ]
    await handler(ServiceCall({}))

    expected = const.map_token_hex_digest(
        const.map_token_secret_seed("ha-uuid", "entry-1", False)
    )
    assert device_registry.updated == {
        "ha-dev": f"{base_url}/api/googlefindmy/map/other:device-alpha?token={expected}"
    }


async def _run_disabled_entry_refresh(
    monkeypatch: pytest.MonkeyPatch,
    *,
    configuration_url: str | None,
    get_url_impl: Any,
) -> _StubDeviceRegistry:
    """Run the refresh service once for a single ``map_view_enabled=False`` entry."""

    entry = make_config_entry(
        entry_id="entry-1",
        options={const.OPT_MAP_VIEW_ENABLED: False},
        data={},
        runtime_data=SimpleNamespace(coordinator=SimpleNamespace()),
    )
    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = _StubConfigEntries([entry])

    ctx = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "opt": lambda entry, key, default: entry.options.get(key, default),
        "default_map_view_token_expiration": const.DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }
    devices = {
        "ha-dev-1": SimpleNamespace(
            id="ha-dev-1",
            identifiers={(const.DOMAIN, "entry-1:device-alpha")},
            config_entries={"entry-1"},
            serial_number=None,
            name="Alpha",
            name_by_user=None,
            configuration_url=configuration_url,
        ),
    }
    device_registry = _StubDeviceRegistry(devices)
    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", get_url_impl)

    async def _run_refresh() -> None:
        await services.async_register_services(hass, ctx)
        handler = hass.services.registered[
            (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
        ]
        await handler(ServiceCall({}))

    await _run_refresh()
    return device_registry


@pytest.mark.asyncio
async def test_refresh_device_urls_clears_a_disabled_entry_without_a_base_url(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Clearing a disabled entry's link needs no reachable HA URL.

    Every resolved device sits on a disabled entry, so the service clears the
    stale link and returns before the base-URL lookup that would fail here.
    """

    def _raise_url_error(*_: Any, **__: Any) -> str:
        raise NoURLAvailableError()

    caplog.set_level(logging.WARNING, logger=services.__name__)
    device_registry = await _run_disabled_entry_refresh(
        monkeypatch,
        configuration_url="https://example.test/api/googlefindmy/map/a?token=stale",
        get_url_impl=_raise_url_error,
    )

    assert device_registry.updated == {"ha-dev-1": None}
    assert not [
        record
        for record in caplog.records
        if record.levelno >= logging.WARNING and "reachable URL" in record.getMessage()
    ]


@pytest.mark.asyncio
async def test_refresh_device_urls_leaves_an_already_empty_link_alone(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An already-empty link on a disabled entry is neither written nor counted."""

    caplog.set_level(logging.INFO, logger=services.__name__)
    device_registry = await _run_disabled_entry_refresh(
        monkeypatch,
        configuration_url=None,
        get_url_impl=lambda hass, **kwargs: "https://example.test",
    )

    assert device_registry.updated == {}
    assert "Refreshed URLs for 0 Google Find My devices" in caplog.text


def _raising_opt(entry: Any, key: str, default: bool) -> bool:
    raise RuntimeError("opt reader broken")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "opt_reader",
    [None, _raising_opt],
    ids=["no-opt-reader", "opt-reader-raises"],
)
async def test_refresh_device_urls_reads_token_expiration_without_the_opt_reader(
    monkeypatch: pytest.MonkeyPatch, opt_reader: Any
) -> None:
    """Without a usable ``opt`` reader the expiration option comes from the entry.

    The token policy must still follow the entry: an entry with token
    expiration on gets the weekly token, not the static one.
    """

    fake_now = 1_209_600
    base_url = "https://example.test"
    entry = make_config_entry(
        entry_id="entry-1",
        options={const.OPT_MAP_VIEW_TOKEN_EXPIRATION: True},
        data={},
        runtime_data=SimpleNamespace(coordinator=SimpleNamespace()),
    )
    hass = SimpleNamespace()
    hass.data = {"core.uuid": "ha-uuid", const.DOMAIN: {"entries": {}}}
    hass.services = _StubServices()
    hass.config_entries = _StubConfigEntries([entry])

    ctx: dict[str, Any] = {
        "domain": const.DOMAIN,
        "resolve_canonical": lambda hass, device_id: (device_id, device_id),
        "is_active_entry": lambda entry: True,
        "primary_active_entry": lambda entries: entries[0] if entries else None,
        "default_map_view_token_expiration": False,
        "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        "redact_url_token": lambda url: url,
        "soft_migrate_entry": lambda hass, entry: None,
    }
    if opt_reader is not None:
        ctx["opt"] = opt_reader

    devices = {
        "ha-dev-1": SimpleNamespace(
            id="ha-dev-1",
            identifiers={(const.DOMAIN, "entry-1:device-alpha")},
            config_entries={"entry-1"},
            serial_number=None,
            name="Alpha",
            name_by_user=None,
            configuration_url=None,
        ),
    }
    device_registry = _StubDeviceRegistry(devices)
    monkeypatch.setattr(services.dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(services, "get_url", lambda hass, **kwargs: base_url)
    monkeypatch.setattr(services.time, "time", lambda: fake_now)

    await services.async_register_services(hass, ctx)
    handler = hass.services.registered[
        (const.DOMAIN, const.SERVICE_REFRESH_DEVICE_URLS)
    ]
    await handler(ServiceCall({}))

    weekly = const.map_token_hex_digest(
        const.map_token_secret_seed("ha-uuid", "entry-1", True, now=fake_now)
    )
    assert device_registry.updated == {
        "ha-dev-1": f"{base_url}/api/googlefindmy/map/device-alpha?token={weekly}"
    }
