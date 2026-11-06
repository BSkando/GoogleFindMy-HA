# tests/test_services_read_beacon_state.py
"""Contract tests for the ``read_beacon_state`` action."""

from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace
from typing import Any

import pytest
import voluptuous as vol
from homeassistant.core import ServiceCall, SupportsResponse
from homeassistant.exceptions import ServiceValidationError

from custom_components.googlefindmy import const, services
from custom_components.googlefindmy.beacon_actions import BeaconKeys
from tests.helpers.config_entries_stub import make_config_entry
from tests.helpers.service_doubles import FakeServiceRegistry

ACCOUNT_KEY = bytes.fromhex("04112233445566778899aabbccddeeff")
EIK = bytes(range(32))
DEVICE_ID = "device-1"


class _Harness:
    """Registers the services against doubles and exposes the beacon handler."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.registered: dict[str, tuple[Any, Any, Any]] = {}
        self.probe_calls: list[dict[str, Any]] = []
        self.keys = BeaconKeys(ACCOUNT_KEY, EIK)
        self.release: asyncio.Event | None = None

        def _register_admin(
            _hass: Any,
            _domain: str,
            service: str,
            func: Any,
            schema: Any = None,
            supports_response: Any = SupportsResponse.NONE,
            **_kwargs: Any,
        ) -> None:
            self.registered[service] = (func, schema, supports_response)

        async def _keys(_hass: Any, _runtime: Any, _canonical: str) -> BeaconKeys:
            return self.keys

        async def _probe(_hass: Any, keys: BeaconKeys, **kwargs: Any) -> dict[str, Any]:
            self.probe_calls.append({"keys": keys, **kwargs})
            if self.release is not None:
                await self.release.wait()
            return {"connection": "connected"}

        monkeypatch.setattr(services, "async_register_admin_service", _register_admin)
        monkeypatch.setattr(services, "_async_beacon_keys", _keys)
        monkeypatch.setattr(services, "async_probe_tracker", _probe)

        entry = make_config_entry(
            entry_id="entry-1",
            runtime_data=SimpleNamespace(
                coordinator=SimpleNamespace(get_device_display_name=lambda _id: "x")
            ),
        )
        manager = SimpleNamespace(
            async_entries=lambda _domain=None: [entry],
            async_get_entry=lambda _id: entry,
        )
        self.hass = SimpleNamespace(
            data={const.DOMAIN: {"entries": {"entry-1": entry.runtime_data}}},
            services=FakeServiceRegistry(),
            config_entries=manager,
            config=SimpleNamespace(components={"bluetooth"}),
        )
        self.ctx = {
            "domain": const.DOMAIN,
            "resolve_canonical": lambda _hass, device_id: (device_id, device_id),
            "is_active_entry": lambda _entry: True,
            "primary_active_entry": lambda entries: entries[0] if entries else None,
            "opt": lambda _entry, _key, default: default,
            "default_map_view_token_expiration": False,
            "opt_map_view_token_expiration_key": const.OPT_MAP_VIEW_TOKEN_EXPIRATION,
            "redact_url_token": lambda url: url,
            "soft_migrate_entry": lambda *_args: None,
        }

    async def register(self) -> None:
        await services.async_register_services(self.hass, self.ctx)

    @property
    def handler(self) -> Any:
        return self.registered[services.SERVICE_READ_BEACON_STATE][0]

    def call(self, **data: Any) -> ServiceCall:
        schema = self.registered[services.SERVICE_READ_BEACON_STATE][1]
        return ServiceCall(data=schema({"device_id": DEVICE_ID, **data}))


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> _Harness:
    return _Harness(monkeypatch)


@pytest.mark.asyncio
async def test_registered_through_the_admin_helper_with_a_required_response(
    harness: _Harness,
) -> None:
    await harness.register()
    _func, _schema, support = harness.registered[services.SERVICE_READ_BEACON_STATE]
    assert support is SupportsResponse.ONLY


@pytest.mark.asyncio
async def test_returns_the_probe_report_and_passes_its_arguments(
    harness: _Harness,
) -> None:
    await harness.register()
    result = await harness.handler(harness.call())
    assert result == {"connection": "connected"}
    assert harness.probe_calls[0]["address"] is None
    assert harness.probe_calls[0]["timeout"] == 45


@pytest.mark.asyncio
async def test_address_is_normalized(harness: _Harness) -> None:
    await harness.register()
    await harness.handler(harness.call(address=" aa:bb:cc:dd:ee:ff "))
    assert harness.probe_calls[0]["address"] == "AA:BB:CC:DD:EE:FF"


@pytest.mark.asyncio
async def test_invalid_address_is_rejected_without_echo(harness: _Harness) -> None:
    await harness.register()
    with pytest.raises(ServiceValidationError) as info:
        await harness.handler(harness.call(address="not-a-mac-secret"))
    assert info.value.translation_key == "beacon_invalid_address"
    assert "not-a-mac-secret" not in str(info.value)
    assert harness.probe_calls == []


@pytest.mark.parametrize("timeout", [4, 121])
@pytest.mark.asyncio
async def test_timeout_range_is_enforced(harness: _Harness, timeout: int) -> None:
    await harness.register()
    with pytest.raises(vol.Invalid):
        harness.call(timeout=timeout)


@pytest.mark.asyncio
async def test_missing_bluetooth_is_a_translated_error(harness: _Harness) -> None:
    await harness.register()
    harness.hass.config.components = set()
    with pytest.raises(ServiceValidationError) as info:
        await harness.handler(harness.call())
    assert info.value.translation_key == "beacon_bluetooth_unavailable"


@pytest.mark.asyncio
async def test_parallel_call_is_rejected_not_queued(harness: _Harness) -> None:
    await harness.register()
    harness.release = asyncio.Event()
    first = asyncio.create_task(harness.handler(harness.call()))
    for _ in range(100):
        if harness.probe_calls or first.done():
            break
        await asyncio.sleep(0)
    assert not first.done(), first.exception()
    with pytest.raises(ServiceValidationError) as info:
        await harness.handler(harness.call())
    assert info.value.translation_key == "beacon_diagnostic_busy"
    harness.release.set()
    await first
    assert len(harness.probe_calls) == 1


# --- key path ------------------------------------------------------------------------


class _Resolver:
    def __init__(self, identity: Any) -> None:
        self._identity = identity

    async def _normalize_identities(self, items: list[Any], *, cache: Any) -> list[Any]:
        return items

    async def _resolve_owner_key(self, _identity: Any, *, cache: Any) -> Any:
        return SimpleNamespace(key=bytes(32), version=1)


def _key_hass(identity: Any) -> tuple[SimpleNamespace, SimpleNamespace]:
    hass = SimpleNamespace(
        data={const.DOMAIN: {const.DATA_EID_RESOLVER: _Resolver(identity)}}
    )
    runtime = SimpleNamespace(
        coordinator=SimpleNamespace(
            get_active_device_identities=lambda: [identity], cache=object()
        )
    )
    return hass, runtime


def _identity(**kwargs: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "canonical_id": "entry:sub:abc",
        "registry_id": "r",
        "identity_key": EIK,
        "encrypted_account_key": None,
    }
    return SimpleNamespace(**{**defaults, **kwargs})


@pytest.mark.asyncio
async def test_keys_come_from_the_identity_with_a_namespaced_id() -> None:
    hass, runtime = _key_hass(_identity())
    keys = await services._async_beacon_keys(hass, runtime, "abc")
    assert keys.eik == EIK
    assert keys.account_key is None


@pytest.mark.asyncio
async def test_account_key_is_decrypted_with_the_owner_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from custom_components.googlefindmy.KeyBackup import cloud_key_decryptor

    monkeypatch.setattr(
        cloud_key_decryptor, "decrypt_account_key", lambda _owner, _blob: ACCOUNT_KEY
    )
    hass, runtime = _key_hass(_identity(encrypted_account_key=bytes(32)))
    keys = await services._async_beacon_keys(hass, runtime, "abc")
    assert keys.account_key == ACCOUNT_KEY


@pytest.mark.asyncio
async def test_unusable_account_key_blob_does_not_leak_or_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from custom_components.googlefindmy.KeyBackup import cloud_key_decryptor

    def _raise(_owner: bytes, _blob: bytes) -> bytes:
        raise ValueError("bad length")

    monkeypatch.setattr(cloud_key_decryptor, "decrypt_account_key", _raise)
    hass, runtime = _key_hass(_identity(encrypted_account_key=bytes(5)))
    keys = await services._async_beacon_keys(hass, runtime, "abc")
    assert keys.account_key is None
    assert keys.eik == EIK


@pytest.mark.parametrize("canonical", ["other", "entry:sub:other"])
@pytest.mark.asyncio
async def test_unknown_identity_is_a_translated_error(canonical: str) -> None:
    hass, runtime = _key_hass(_identity())
    with pytest.raises(ServiceValidationError) as info:
        await services._async_beacon_keys(hass, runtime, canonical)
    assert info.value.translation_key == "beacon_keys_unavailable"


@pytest.mark.asyncio
async def test_no_key_at_all_is_a_translated_error() -> None:
    hass, runtime = _key_hass(_identity(identity_key=None))
    with pytest.raises(ServiceValidationError) as info:
        await services._async_beacon_keys(hass, runtime, "abc")
    assert info.value.translation_key == "beacon_keys_unavailable"


def test_handler_source_has_no_log_call_above_debug() -> None:
    source = inspect.getsource(services._async_beacon_keys)
    for level in ("info", "warning", "error", "exception"):
        assert f"_LOGGER.{level}" not in source


@pytest.mark.asyncio
async def test_no_coordinator_or_resolver_is_a_translated_error() -> None:
    hass = SimpleNamespace(data={const.DOMAIN: {}})
    with pytest.raises(ServiceValidationError) as info:
        await services._async_beacon_keys(hass, SimpleNamespace(), "abc")
    assert info.value.translation_key == "beacon_keys_unavailable"


@pytest.mark.asyncio
async def test_no_owner_key_leaves_only_the_eik() -> None:
    hass, runtime = _key_hass(_identity(encrypted_account_key=bytes(32)))

    async def no_owner(_identity: Any, *, cache: Any) -> None:
        return None

    hass.data[const.DOMAIN][const.DATA_EID_RESOLVER]._resolve_owner_key = no_owner
    keys = await services._async_beacon_keys(hass, runtime, "abc")
    assert keys.account_key is None
    assert keys.eik == EIK


@pytest.mark.asyncio
async def test_real_registration_uses_the_admin_helper_with_response() -> None:
    source = inspect.getsource(services.async_register_services)
    assert "SERVICE_READ_BEACON_STATE" in source
    assert "supports_response=SupportsResponse.ONLY" in source
