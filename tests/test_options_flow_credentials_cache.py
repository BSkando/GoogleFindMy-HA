# tests/test_options_flow_credentials_cache.py
"""Regression tests for the options credential flow clearing cached AAS tokens."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import pytest
from homeassistant.config_entries import ConfigSubentry
from homeassistant.helpers import frame

from custom_components.googlefindmy import api as api_module
from custom_components.googlefindmy import config_flow
from custom_components.googlefindmy.api import GoogleFindMyAPI
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    CONF_OAUTH_TOKEN,
    DATA_AAS_TOKEN,
    DATA_SECRET_BUNDLE,
    DOMAIN,
    SERVICE_SUBENTRY_KEY,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_SUBENTRY_KEY,
    SoundDispatchOutcome,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.PlaySound import (
    start_sound_request as start_module,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.PlaySound import (
    stop_sound_request as stop_module,
)
from tests.helpers.config_flow import prepare_flow_hass_config_entries


def _stable_subentry_id(entry_id: str, key: str) -> str:
    """Return deterministic config_subentry ids for credential cache tests."""

    return f"{entry_id}-{key}-subentry"


class _MemoryCache:
    """In-memory cache implementing the token cache contract used by the flow."""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    async def get(self, name: str) -> Any:
        return self._data.get(name)

    async def async_set_cached_value(self, name: str, value: Any) -> None:
        if value is None:
            self._data.pop(name, None)
        else:
            self._data[name] = value

    def set(self, name: str, value: Any) -> None:
        if value is None:
            self._data.pop(name, None)
        else:
            self._data[name] = value


@dataclass
class _RuntimeData:
    """Runtime data stub providing a cache attribute."""

    token_cache: _MemoryCache

    @property
    def cache(self) -> _MemoryCache:
        return self.token_cache


class _DummyEntry:
    """Minimal ConfigEntry substitute for exercising the options flow."""

    def __init__(
        self, *, entry_id: str, data: dict[str, Any], cache: _MemoryCache
    ) -> None:
        self.entry_id = entry_id
        self.data = data
        self.options: dict[str, Any] = {}
        self.runtime_data = _RuntimeData(cache)
        self.title = data.get(CONF_GOOGLE_EMAIL, "Google Find My Device")
        self.subentries: dict[str, ConfigSubentry] = {}

        service_subentry = ConfigSubentry(
            data={"group_key": SERVICE_SUBENTRY_KEY},
            subentry_type=SUBENTRY_TYPE_SERVICE,
            title="Service",
            unique_id=f"{entry_id}-{SERVICE_SUBENTRY_KEY}",
            subentry_id=_stable_subentry_id(entry_id, SERVICE_SUBENTRY_KEY),
        )
        tracker_subentry = ConfigSubentry(
            data={"group_key": TRACKER_SUBENTRY_KEY, "feature_flags": {}},
            subentry_type=SUBENTRY_TYPE_TRACKER,
            title="Google Find My devices",
            unique_id=f"{entry_id}-{TRACKER_SUBENTRY_KEY}",
            subentry_id=_stable_subentry_id(entry_id, TRACKER_SUBENTRY_KEY),
        )
        self.subentries[service_subentry.subentry_id] = service_subentry
        self.subentries[tracker_subentry.subentry_id] = tracker_subentry


class _DummyConfigEntries:
    """Expose Home Assistant config entry helpers used by the flow under test."""

    def __init__(self, entry: _DummyEntry) -> None:
        self._entry = entry
        self.updated_payloads: list[dict[str, Any]] = []
        self.reloaded: list[str] = []
        self.updated_subentries: list[tuple[str, dict[str, Any]]] = []
        self.removed_subentries: list[str] = []
        self.setup_calls: list[str] = []

    def async_get_entry(self, entry_id: str) -> _DummyEntry | None:
        return self._entry if entry_id == self._entry.entry_id else None

    def async_get_subentries(self, entry_id: str) -> list[ConfigSubentry]:
        entry = self.async_get_entry(entry_id)
        if entry is None:
            return []
        return list(entry.subentries.values())

    def async_update_entry(self, entry: _DummyEntry, *, data: dict[str, Any]) -> None:
        assert entry is self._entry
        entry.data = data
        self.updated_payloads.append(data)

    def async_update_subentry(  # noqa: PLR0913
        self,
        entry: _DummyEntry,
        subentry: ConfigSubentry,
        *,
        data: dict[str, Any],
        title: str | None = None,
        unique_id: str | None = None,
        translation_key: str | None = None,
    ) -> None:
        assert entry is self._entry
        subentry.data = MappingProxyType(dict(data))
        if title is not None:
            subentry.title = title
        if unique_id is not None:
            subentry.unique_id = unique_id
        if translation_key is not None:
            subentry.translation_key = translation_key
        self.updated_subentries.append((subentry.subentry_id, dict(subentry.data)))

    def async_remove_subentry(self, entry: _DummyEntry, subentry_id: str) -> bool:
        assert entry is self._entry
        entry.subentries.pop(subentry_id, None)
        self.removed_subentries.append(subentry_id)
        return True

    async def async_reload(self, entry_id: str) -> bool:
        assert DATA_AAS_TOKEN not in self._entry.data
        assert await self._entry.runtime_data.cache.get(DATA_AAS_TOKEN) is None
        self.reloaded.append(entry_id)
        # The core returns ``bool`` and the release callback reads it: a falsy
        # result means "ended without reloading" and hands the latch back. A
        # double returning ``None`` would therefore report every successful
        # reload as a failed one and silently defeat the coalescing this file
        # asserts a few lines below.
        return True

    async def async_setup(self, entry_id: str) -> bool:
        self.setup_calls.append(entry_id)
        return True


class _DummyHass:
    """Small Home Assistant stub collecting scheduled tasks for inspection."""

    def __init__(self, entry: _DummyEntry, cache: _MemoryCache) -> None:
        prepare_flow_hass_config_entries(
            self,
            lambda: _DummyConfigEntries(entry),
            frame_module=frame,
        )
        self.data: dict[str, Any] = {
            DOMAIN: {"entries": {entry.entry_id: _RuntimeData(cache)}}
        }
        self._tasks: list[asyncio.Task[Any]] = []

    def async_create_task(self, coro: Awaitable[Any]) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._tasks.append(task)
        return task

    async def drain_tasks(self) -> None:
        if not self._tasks:
            return
        await asyncio.gather(*self._tasks)


def test_options_flow_rotating_token_clears_cached_aas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replacing credentials in the options flow must drop cached AAS tokens."""

    async def _exercise() -> None:
        cache = _MemoryCache()
        await cache.async_set_cached_value(DATA_AAS_TOKEN, "aas_et/OLD")

        entry = _DummyEntry(
            entry_id="entry-1",
            data={
                CONF_GOOGLE_EMAIL: "user@example.com",
                CONF_OAUTH_TOKEN: "oauth-original-token-123456",
                DATA_AAS_TOKEN: "aas_et/OLD",
            },
            cache=cache,
        )
        hass = _DummyHass(entry, cache)

        flow = config_flow.OptionsFlowHandler()
        flow.hass = hass  # type: ignore[assignment]
        flow.config_entry = entry  # type: ignore[attr-defined]

        async def _fake_pick(
            hass: Any,
            email: str,
            candidates: list[tuple[str, str]],
            *,
            secrets_bundle: dict[str, Any] | None = None,
        ) -> str | None:
            return candidates[0][1] if candidates else None

        monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)

        new_token = "oauth-token-rotate-123456"
        result = await flow.async_step_credentials({"new_oauth_token": new_token})
        if inspect.isawaitable(result):
            result = await result

        assert isinstance(result, dict)
        assert result.get("type") in {"abort", "form"}

        assert hass.config_entries.updated_payloads
        updated = hass.config_entries.updated_payloads[-1]
        assert updated[CONF_OAUTH_TOKEN] == new_token
        assert DATA_AAS_TOKEN not in updated
        assert entry.data[CONF_OAUTH_TOKEN] == new_token
        assert DATA_AAS_TOKEN not in entry.data
        assert await cache.get(DATA_AAS_TOKEN) is None
        # Credentials are account-wide: the update writes ``entry.data`` (above)
        # and no subentry.
        assert hass.config_entries.updated_subentries == []

        await hass.drain_tasks()
        assert hass.config_entries.reloaded == [entry.entry_id]

        # The reload is still on its way, and the entry update notifies the
        # credential update listener as well. ``async_schedule_reload`` does not
        # coalesce, so a second rotation must not add another unload/setup cycle.
        second = await flow.async_step_credentials(
            {
                "new_oauth_token": "oauth-token-rotate-654321",
            }
        )
        if inspect.isawaitable(second):
            second = await second

        await hass.drain_tasks()
        assert hass.config_entries.reloaded == [entry.entry_id], (
            "a reload is already on its way; a second one only tears the entry "
            "down twice"
        )

    asyncio.run(_exercise())


def test_options_flow_gives_the_latch_back_when_the_reload_dies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The options refresher reloads directly, so a dead task must free the latch.

    This step claims the shared latch and then reloads in a fire-and-forget
    task, which keeps the promise open for that task's whole lifetime. Home
    Assistant rejects an unload for an entry in a lifecycle state that forbids
    it, and the task dies with that rejection: none of the release points
    (unload, setup, entry removal) runs, so a latch kept here would be permanent
    and the *next* credential rotation would stand down with its new token
    ineffective.
    """

    async def _exercise() -> None:
        cache = _MemoryCache()
        entry = _DummyEntry(
            entry_id="entry-latch",
            data={
                CONF_GOOGLE_EMAIL: "user@example.com",
                CONF_OAUTH_TOKEN: "oauth-original-token-123456",
            },
            cache=cache,
        )
        hass = _DummyHass(entry, cache)

        rejected: list[str] = []

        async def _rejecting_reload(entry_id: str) -> None:
            rejected.append(entry_id)
            raise config_flow.OperationNotAllowed(entry_id)

        hass.config_entries.async_reload = _rejecting_reload  # type: ignore[assignment]

        flow = config_flow.OptionsFlowHandler()
        flow.hass = hass  # type: ignore[assignment]
        flow.config_entry = entry  # type: ignore[attr-defined]

        async def _fake_pick(
            hass: Any,
            email: str,
            candidates: list[tuple[str, str]],
            *,
            secrets_bundle: dict[str, Any] | None = None,
        ) -> str | None:
            return candidates[0][1] if candidates else None

        monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)

        async def _rotate(token: str) -> None:
            outcome = await flow.async_step_credentials({"new_oauth_token": token})
            if inspect.isawaitable(outcome):
                await outcome
            # The reload task raises; gathering it would re-raise here, and the
            # rejection is the point of this test, not a failure of it.
            await asyncio.gather(*hass._tasks, return_exceptions=True)
            # ``gather`` returns as soon as the task is done, which is not the
            # same instant as "every done-callback has run": the release
            # callback and gather's own are both queued through
            # ``loop.call_soon``. One extra turn removes the dependency on
            # their registration order.
            await asyncio.sleep(0)
            hass._tasks.clear()

        await _rotate("oauth-token-rotate-123456")
        assert rejected == [entry.entry_id]

        await _rotate("oauth-token-rotate-654321")
        assert rejected == [entry.entry_id] * 2, (
            "the first reload never arrived, so the latch had to go back; "
            "without that release this rotation stands down and its token "
            "stays ineffective until a restart"
        )

    asyncio.run(_exercise())


def test_fcm_token_lookup_uses_entry_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the API forwards the config entry ID to the shared FCM receiver."""

    class _CacheStub:
        """Minimal cache exposing an entry ID attribute for the API wrapper."""

        def __init__(self, entry_id: str) -> None:
            self.entry_id = entry_id

        async def async_get_cached_value(self, key: str) -> Any:
            return None

        async def async_set_cached_value(self, key: str, value: Any) -> None:
            return None

    class _Receiver:
        def __init__(self) -> None:
            self.calls: list[str | None] = []

        def get_fcm_token(self, entry_id: str | None = None) -> str:
            self.calls.append(entry_id)
            assert entry_id == "entry-primary"
            return "token-primary-abcdef"

    receiver = _Receiver()
    monkeypatch.setattr(api_module, "_FCM_ReceiverGetter", lambda: receiver)

    api = GoogleFindMyAPI(cache=_CacheStub("entry-primary"))

    token = api._get_fcm_token_for_action()

    assert token == "token-primary-abcdef"
    assert receiver.calls == ["entry-primary"]


def test_fcm_token_lookup_falls_back_without_entry_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure legacy receivers without entry ID support continue to function."""

    class _CacheStub:
        async def async_get_cached_value(self, key: str) -> Any:
            return None

        async def async_set_cached_value(self, key: str, value: Any) -> None:
            return None

    class _LegacyReceiver:
        def __init__(self) -> None:
            self.calls: list[str | None] = []

        def get_fcm_token(self) -> str:
            self.calls.append(None)
            return "legacy-token-abcdef"

    receiver = _LegacyReceiver()
    monkeypatch.setattr(api_module, "_FCM_ReceiverGetter", lambda: receiver)

    api = GoogleFindMyAPI(cache=_CacheStub())

    token = api._get_fcm_token_for_action()

    assert token == "legacy-token-abcdef"
    assert receiver.calls == [None]


def test_play_stop_sound_uses_entry_cache(  # noqa: PLR0915
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure Play/Stop Sound submissions use the provided TokenCache with namespacing."""

    class _FakeCache:
        """Minimal cache tracking get/set keys to verify namespacing."""

        def __init__(self, entry_id: str) -> None:
            self.entry_id = entry_id
            self._data: dict[str, Any] = {}
            self.get_calls: list[str] = []
            self.set_calls: list[tuple[str, Any]] = []

        async def async_get_cached_value(self, key: str) -> Any:
            self.get_calls.append(key)
            return self._data.get(key)

        async def async_set_cached_value(self, key: str, value: Any) -> None:
            self.set_calls.append((key, value))
            if value is None:
                self._data.pop(key, None)
            else:
                self._data[key] = value

    async def _fail_get(key: str) -> Any:
        raise AssertionError("Global cache fallback must not be used for Play Sound")

    async def _fail_set(key: str, value: Any) -> None:
        raise AssertionError("Global cache fallback must not be used for Play Sound")

    monkeypatch.setattr(start_module, "_cache_get_default", _fail_get, raising=False)
    monkeypatch.setattr(start_module, "_cache_set_default", _fail_set, raising=False)
    monkeypatch.setattr(stop_module, "_cache_get_default", _fail_get, raising=False)
    monkeypatch.setattr(stop_module, "_cache_set_default", _fail_set, raising=False)

    async def _exercise() -> None:
        cache_primary = _FakeCache("entry-one")
        cache_secondary = _FakeCache("entry-two")

        api_primary = GoogleFindMyAPI(cache=cache_primary)
        _ = GoogleFindMyAPI(cache=cache_secondary)

        monkeypatch.setattr(
            api_primary,
            "_get_fcm_token_for_action",
            lambda: "tok-1234567890",
            raising=False,
        )

        start_calls: list[tuple[str, str, dict[str, Any]]] = []
        stop_calls: list[tuple[str, str, dict[str, Any]]] = []

        async def _fake_start(
            scope: str, payload: str, **kwargs: Any
        ) -> tuple[str, str]:
            start_calls.append((scope, payload, kwargs))
            return "start-ok", "uuid-start"

        async def _fake_stop(scope: str, payload: str, **kwargs: Any) -> str:
            stop_calls.append((scope, payload, kwargs))
            return "stop-ok"

        monkeypatch.setattr(start_module, "async_nova_request", _fake_start)
        monkeypatch.setattr(stop_module, "async_nova_request", _fake_stop)

        play = await api_primary.async_play_sound("device-42")
        success, request_uuid = play.accepted, play.cancel_key
        assert success is True
        assert request_uuid is not None
        assert (
            await api_primary.async_stop_sound("device-42", request_uuid)
            is SoundDispatchOutcome.ACCEPTED
        )

        assert start_calls and stop_calls

        _, _, start_kwargs = start_calls[0]
        assert start_kwargs["cache"] is cache_primary
        assert start_kwargs["namespace"] == "entry-one"

        start_get = start_kwargs["cache_get"]
        start_set = start_kwargs["cache_set"]
        assert start_get is not None
        assert start_set is not None
        await start_get("ttl")
        assert cache_primary.get_calls[-1] == "entry-one:ttl"
        await start_set("ttl", "value")
        assert ("entry-one:ttl", "value") in cache_primary.set_calls

        _, _, stop_kwargs = stop_calls[0]
        assert stop_kwargs["cache"] is cache_primary
        assert stop_kwargs["namespace"] == "entry-one"

        stop_get = stop_kwargs["cache_get"]
        stop_set = stop_kwargs["cache_set"]
        assert stop_get is not None
        assert stop_set is not None
        await stop_get("ttl2")
        assert cache_primary.get_calls[-1] == "entry-one:ttl2"
        await stop_set("ttl2", "value2")
        assert ("entry-one:ttl2", "value2") in cache_primary.set_calls

    asyncio.run(_exercise())


def test_options_flow_secrets_reauth_strips_owner_key_space(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Options reauth with a pasted secrets bundle strips owner_key whitespace."""

    async def _exercise() -> None:
        cache = _MemoryCache()
        entry = _DummyEntry(
            entry_id="entry-1",
            data={
                CONF_GOOGLE_EMAIL: "user@example.com",
                CONF_OAUTH_TOKEN: "oauth-original-token-123456",
            },
            cache=cache,
        )
        hass = _DummyHass(entry, cache)

        flow = config_flow.OptionsFlowHandler()
        flow.hass = hass  # type: ignore[assignment]
        flow.config_entry = entry  # type: ignore[attr-defined]

        async def _fake_pick(
            hass: Any,
            email: str,
            candidates: list[tuple[str, str]],
            *,
            secrets_bundle: dict[str, Any] | None = None,
        ) -> str | None:
            # The bundle handed downstream is already whitespace-normalized.
            assert secrets_bundle is not None
            assert secrets_bundle["owner_key"] == "AABBCC"
            return candidates[0][1] if candidates else None

        monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)

        payload = {
            "google_email": "user@example.com",
            "oauth_token": "oauth-token-from-secrets-123456",
            "owner_key": "AA BB\nCC ",
            # A shared_key is required for the bundle to pass the single-key
            # import gate; this test only asserts whitespace normalization.
            "shared_key": "DDEEFF",
            "username": "  Keep Me  ",
        }
        result = await flow.async_step_credentials(
            {"new_secrets_json": json.dumps(payload)}
        )
        if inspect.isawaitable(result):
            result = await result
        assert isinstance(result, dict)

        updated = hass.config_entries.updated_payloads[-1]
        bundle = updated[DATA_SECRET_BUNDLE]
        assert bundle["owner_key"] == "AABBCC"
        assert bundle["username"] == "Keep Me"
        await hass.drain_tasks()

    asyncio.run(_exercise())


def test_options_flow_secrets_reauth_guard_error_fallback_normalizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The multi-entry-guard fallback path also normalizes the bundle."""

    async def _exercise() -> None:
        cache = _MemoryCache()
        entry = _DummyEntry(
            entry_id="entry-1",
            data={
                CONF_GOOGLE_EMAIL: "user@example.com",
                CONF_OAUTH_TOKEN: "oauth-original-token-123456",
            },
            cache=cache,
        )
        hass = _DummyHass(entry, cache)

        # Force the first update to raise a multi-entry guard error so the
        # fallback branch (which re-parses and re-normalizes) is exercised.
        calls = {"n": 0}
        original_update = hass.config_entries.async_update_entry

        def _maybe_raise(entry_: Any, *, data: dict[str, Any]) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("Multiple config entries active for this domain")
            original_update(entry_, data=data)

        hass.config_entries.async_update_entry = _maybe_raise  # type: ignore[assignment]

        flow = config_flow.OptionsFlowHandler()
        flow.hass = hass  # type: ignore[assignment]
        flow.config_entry = entry  # type: ignore[attr-defined]

        async def _fake_pick(
            hass: Any,
            email: str,
            candidates: list[tuple[str, str]],
            *,
            secrets_bundle: dict[str, Any] | None = None,
        ) -> str | None:
            return candidates[0][1] if candidates else None

        monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)

        payload = {
            "google_email": "user@example.com",
            "oauth_token": "oauth-token-from-secrets-123456",
            "owner_key": "AA BB CC",
            # A shared_key is required to pass the single-key import gate; this
            # test exercises the multi-entry-guard fallback normalization.
            "shared_key": "DDEEFF",
        }
        result = await flow.async_step_credentials(
            {"new_secrets_json": json.dumps(payload)}
        )
        if inspect.isawaitable(result):
            result = await result
        assert isinstance(result, dict)
        # First update raised, fallback re-attempted the update.
        assert calls["n"] >= 2
        updated = hass.config_entries.updated_payloads[-1]
        assert updated[DATA_SECRET_BUNDLE]["owner_key"] == "AABBCC"
        await hass.drain_tasks()

    asyncio.run(_exercise())


@pytest.mark.parametrize(
    "bundle_field",
    [
        pytest.param({}, id="key-absent"),
        pytest.param({"new_secrets_json": ""}, id="key-blank"),
    ],
)
@pytest.mark.asyncio
async def test_options_flow_guard_error_without_a_bundle_reports_instead_of_crashing(
    monkeypatch: pytest.MonkeyPatch,
    bundle_field: dict[str, str],
) -> None:
    """A guard error without a bundle submission must report, not raise.

    Both credential branches of ``async_step_credentials`` call
    ``_finalize_success`` inside the same ``try``, so the multi-entry-guard
    deferral can be entered from a submission that never carried a bundle.
    Rebuilding the payload from ``new_secrets_json`` then raises out of the
    ``except`` handler instead of reporting the error.

    This is defence in depth, not a live user path. The token field is
    commented out of both schema branches, ``vol.Schema`` defaults to
    ``PREVENT_EXTRA``, and the flow manager validates ``user_input`` against
    ``data_schema`` before the step sees it, so the shapes below cannot
    arrive through the form. They can arrive through a direct call like this
    one, and they would arrive through the form again the day the field is
    re-enabled.

    ``key-blank`` is the discriminating parameter: it is the only one that a
    mere key-presence check would still fail, which is why the production
    guard tests the *value* (``has_secrets``). ``key-absent`` is kept because
    it fails differently (``KeyError`` rather than ``JSONDecodeError``) and
    is the shape the original report described, not because it catches a
    regression the blank case misses.
    ``_count_supplied_credential_methods`` ignores blank fields, so both
    forms pass the exclusivity gate as token-only submissions.

    This uses the module-local ``_DummyEntry`` rather than the canonical
    ``make_config_entry``: the step resolves its choices through
    ``_subentry_choice_map``, and the factory does not model ``subentries``.
    """

    cache = _MemoryCache()
    entry = _DummyEntry(
        entry_id="entry-1",
        data={
            CONF_GOOGLE_EMAIL: "user@example.com",
            CONF_OAUTH_TOKEN: "oauth-original-token-123456",
        },
        cache=cache,
    )
    hass = _DummyHass(entry, cache)

    calls = {"n": 0}

    def _always_guard(entry_: Any, *, data: dict[str, Any]) -> None:
        calls["n"] += 1
        raise RuntimeError("Multiple config entries active for this domain")

    hass.config_entries.async_update_entry = _always_guard  # type: ignore[assignment]

    flow = config_flow.OptionsFlowHandler()
    flow.hass = hass  # type: ignore[assignment]
    flow.config_entry = entry  # type: ignore[attr-defined]

    async def _fake_pick(
        hass: Any,
        email: str,
        candidates: list[tuple[str, str]],
        *,
        secrets_bundle: dict[str, Any] | None = None,
    ) -> str | None:
        return candidates[0][1] if candidates else None

    monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)

    result = await flow.async_step_credentials(
        {
            "new_oauth_token": "oauth-token-probe-123456",
            **bundle_field,
        }
    )
    if inspect.isawaitable(result):
        result = await result

    assert isinstance(result, dict)
    assert result.get("type") == "form"
    assert result.get("errors")
    # The deferral was skipped, so no second write was attempted.
    assert calls["n"] == 1
    await hass.drain_tasks()
