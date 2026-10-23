# tests/test_fcm_registration_entry_sync.py
"""Regenerated FCM credentials must stay durable in the config entry.

Regression for the live report: pressing the "regenerate FCM token" button
obtained a working registration, but it only reached the entry-scoped token
cache. The next setup re-seeded the entry's (stale) secrets bundle into that
cache and brought the dead registration back, so FCM delivery stopped again.

The receiver now writes a refreshed registration back into the entry's bundle,
which is the value ``async_setup_entry`` seeds from. The tests drive the real
credentials-update path and then simulate the next setup by re-running
``_async_save_secrets_data`` from the entry data.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from importlib import import_module
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from custom_components.googlefindmy.Auth.fcm_receiver_ha import FcmReceiverHA
from custom_components.googlefindmy.const import CONF_GOOGLE_EMAIL, DATA_SECRET_BUNDLE
from tests.helpers.config_entries_stub import make_config_entry

ENTRY_ID = "entry-fcm-regeneration"


class _Cache:
    """Entry-scoped cache double covering both write surfaces in play."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    async def get(self, name: str) -> Any:
        return self.data.get(name)

    async def set(self, name: str, value: Any) -> None:
        self.data[name] = value

    async def async_set_cached_value(self, name: str, value: Any) -> None:
        self.data[name] = value


class _ConfigEntries:
    """In-memory stand-in for Home Assistant's config-entry manager.

    ``async_update_entry`` mirrors the core: the in-memory entry changes now and
    the debounced store save happens later. Only the in-memory half matters for
    a reload that follows, which is the sequence the bug report describes.
    """

    def __init__(self, entry: Any, *, fail_update: bool = False) -> None:
        self._entries = {entry.entry_id: entry}
        self.update_calls: list[tuple[str, dict[str, Any]]] = []
        self._fail_update = fail_update

    def async_get_entry(self, entry_id: str) -> Any | None:
        return self._entries.get(entry_id)

    def async_update_entry(self, entry: Any, **kwargs: Any) -> None:
        self.update_calls.append((entry.entry_id, dict(kwargs)))
        if self._fail_update:
            raise RuntimeError("config-entry store unavailable")
        data = kwargs.get("data")
        if data is not None:
            entry.data = dict(data)


def _integration_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Import the integration package, seeding the loader stub when needed.

    The stub is installed through ``monkeypatch`` so it never outlives the
    calling test: an import-time write into ``sys.modules`` would leak into
    every later test in the session.
    """

    if "homeassistant.loader" not in sys.modules:
        loader_module = ModuleType("homeassistant.loader")
        loader_module.async_get_integration = lambda *_args, **_kwargs: None
        monkeypatch.setitem(sys.modules, "homeassistant.loader", loader_module)
    return import_module("custom_components.googlefindmy")


def _registration(token: str) -> dict[str, Any]:
    """Return a complete FCM credentials dict carrying ``token``."""

    return {
        "gcm": {"android_id": "1234567890", "security_token": "9876543210"},
        "fcm": {"registration": {"token": token}},
    }


@pytest.mark.asyncio
async def test_a_refreshed_registration_survives_the_next_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A regenerated registration is written to the entry and survives reseeding.

    Without the fix the fresh value only reaches the cache: the entry keeps the
    dead registration, and the second ``_async_save_secrets_data`` call (the
    next setup) restores it, which is exactly the reported reload regression.
    """

    integration = _integration_module(monkeypatch)
    stale = _registration("dead-registration")
    fresh = _registration("fresh-registration")
    entry = make_config_entry(
        entry_id=ENTRY_ID,
        data={
            CONF_GOOGLE_EMAIL: "user@example.com",
            # The config flow writes the same value both into the bundle and
            # beside it, so both copies are faced with the refresh.
            "fcm_credentials": stale,
            DATA_SECRET_BUNDLE: {
                "username": "user@example.com",
                "fcm_credentials": stale,
            },
        },
    )
    manager = _ConfigEntries(entry)
    cache = _Cache()

    # First setup: the entry's bundle seeds the live cache.
    await integration._async_save_secrets_data(cache, entry.data[DATA_SECRET_BUNDLE])
    assert cache.data["fcm_credentials"] == stale

    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace(config_entries=manager))
    receiver._entry_caches[ENTRY_ID] = cache

    # The registration succeeds and the client reports the refreshed creds.
    receiver._on_credentials_updated_for_entry(ENTRY_ID, fresh)
    if receiver._active_tasks:
        await asyncio.gather(*list(receiver._active_tasks))

    assert cache.data["fcm_credentials"] == fresh
    assert entry.data[DATA_SECRET_BUNDLE]["fcm_credentials"] == fresh
    assert entry.data["fcm_credentials"] == fresh

    # Single-write contract: one refresh persists exactly one entry update, so
    # the durability fix does not fan out into repeated writes.
    assert len(manager.update_calls) == 1

    # Next setup: the seed reads the entry again and must not undo the refresh.
    await integration._async_save_secrets_data(cache, entry.data[DATA_SECRET_BUNDLE])
    stored = cache.data["fcm_credentials"]
    assert stored["fcm"]["registration"]["token"] == "fresh-registration"


def test_a_registration_refresh_is_not_a_credential_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The written-back registration must not schedule an entry reload.

    The live cache already carries the refreshed value, so the update listener
    has to stay silent; a reload would tear down the receiver that just obtained
    the registration. A change to a seeded bundle member still counts.
    """

    integration = _integration_module(monkeypatch)
    bundle = {
        "username": "user@example.com",
        "shared_key": "shared-key-value",
        "fcm_credentials": _registration("dead-registration"),
    }
    refreshed = {**bundle, "fcm_credentials": _registration("fresh-registration")}
    changed_shared_key = {**bundle, "shared_key": "another-shared-key"}

    base_fingerprint = integration._credential_fingerprint({DATA_SECRET_BUNDLE: bundle})

    assert (
        integration._credential_fingerprint({DATA_SECRET_BUNDLE: refreshed})
        == base_fingerprint
    )
    assert (
        integration._credential_fingerprint({DATA_SECRET_BUNDLE: changed_shared_key})
        != base_fingerprint
    )


def _receiver_with_entry(
    entry: Any, *, fail_update: bool = False
) -> tuple[FcmReceiverHA, _ConfigEntries]:
    """Return a receiver wired to ``entry`` through a config-entry double."""

    manager = _ConfigEntries(entry, fail_update=fail_update)
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace(config_entries=manager))
    return receiver, manager


def test_persist_credentials_without_a_bundle_does_not_invent_one() -> None:
    """An entry without a secrets bundle is left untouched.

    The seed reads the bundle, so individual-token sign-ins have nothing to
    refresh; inventing a bundle here would create a key the seed never wrote.
    """

    entry = make_config_entry(
        entry_id=ENTRY_ID,
        data={CONF_GOOGLE_EMAIL: "user@example.com"},
    )
    receiver, manager = _receiver_with_entry(entry)

    receiver._persist_credentials_to_entry(ENTRY_ID, _registration("fresh"))

    assert manager.update_calls == []
    assert entry.data == {CONF_GOOGLE_EMAIL: "user@example.com"}
    assert DATA_SECRET_BUNDLE not in entry.data


def test_persist_credentials_skips_non_mapping_entry_data() -> None:
    """Entry data that is not a mapping is a no-op rather than an error."""

    entry = make_config_entry(entry_id=ENTRY_ID)
    entry.data = ["not-a-mapping"]
    receiver, manager = _receiver_with_entry(entry)

    receiver._persist_credentials_to_entry(ENTRY_ID, _registration("fresh"))

    assert manager.update_calls == []
    assert entry.data == ["not-a-mapping"]


@pytest.mark.parametrize("failure", ["no_entry_manager", "update_raises"])
@pytest.mark.asyncio
async def test_a_failed_entry_write_is_swallowed_and_the_cache_still_updates(
    failure: str, caplog: pytest.LogCaptureFixture
) -> None:
    """A best-effort entry write never blocks the cache persistence path.

    Durability must not become a failure mode: whether the config-entry manager
    is missing or rejects the update, the entry stays as it was and the
    entry-scoped cache still receives the refreshed registration.
    """

    stale = _registration("dead-registration")
    fresh = _registration("fresh-registration")
    entry = make_config_entry(
        entry_id=ENTRY_ID,
        data={
            CONF_GOOGLE_EMAIL: "user@example.com",
            DATA_SECRET_BUNDLE: {"fcm_credentials": stale},
        },
    )
    receiver = FcmReceiverHA()
    if failure == "update_raises":
        manager = _ConfigEntries(entry, fail_update=True)
        receiver.attach_hass(SimpleNamespace(config_entries=manager))
    cache = _Cache()
    receiver._entry_caches[ENTRY_ID] = cache
    receiver.creds[ENTRY_ID] = fresh

    with caplog.at_level(
        logging.DEBUG,
        logger="custom_components.googlefindmy.Auth.fcm_receiver_ha",
    ):
        await receiver._async_save_credentials_for_entry(ENTRY_ID)

    assert cache.data["fcm_credentials"] == fresh
    assert entry.data[DATA_SECRET_BUNDLE]["fcm_credentials"] == stale
    if failure == "update_raises":
        assert "Failed to persist the refreshed FCM registration" in caplog.text


@pytest.mark.asyncio
async def test_a_teardown_between_dispatch_and_persist_skips_the_entry_write() -> None:
    """The TOCTOU re-check drops an entry write for a torn-down entry.

    The caller-side gate in ``_on_credentials_updated_for_entry`` runs before
    the persistence task is dispatched; an entry removed in between must not be
    written again. The cache write still proceeds: durability for a live cache
    is harmless, resurrecting a removed entry's bundle is not.
    """

    stale = _registration("dead-registration")
    fresh = _registration("fresh-registration")
    entry = make_config_entry(
        entry_id=ENTRY_ID,
        data={
            CONF_GOOGLE_EMAIL: "user@example.com",
            "fcm_credentials": stale,
            DATA_SECRET_BUNDLE: {
                "username": "user@example.com",
                "fcm_credentials": stale,
            },
        },
    )
    receiver, manager = _receiver_with_entry(entry)
    cache = _Cache()
    receiver._entry_caches[ENTRY_ID] = cache

    receiver._on_credentials_updated_for_entry(ENTRY_ID, fresh)
    # Synchronous teardown lands before the dispatched task runs.
    receiver._unregistered.add(ENTRY_ID)
    if receiver._active_tasks:
        await asyncio.gather(*list(receiver._active_tasks))

    assert cache.data["fcm_credentials"] == fresh
    assert entry.data[DATA_SECRET_BUNDLE]["fcm_credentials"] == stale
    assert entry.data["fcm_credentials"] == stale
    assert manager.update_calls == []


@pytest.mark.parametrize(
    "hass",
    [
        None,
        SimpleNamespace(),
        SimpleNamespace(config_entries=SimpleNamespace()),
        SimpleNamespace(
            config_entries=SimpleNamespace(
                async_get_entry=None, async_update_entry=None
            )
        ),
    ],
    ids=["hass-missing", "manager-missing", "hooks-missing", "hooks-not-callable"],
)
def test_persist_credentials_without_usable_entry_manager_is_noop(
    hass: Any,
) -> None:
    """Persistence without a usable entry manager returns quietly.

    Detached receivers (no hass), hass doubles without a config-entry manager,
    and managers whose lookup/update hooks are absent or not callable all hit
    the same best-effort guard: nothing is written and nothing raises.
    """

    receiver = FcmReceiverHA()
    if hass is not None:
        receiver.attach_hass(hass)

    receiver._persist_credentials_to_entry(ENTRY_ID, _registration("fresh"))


def test_persist_credentials_for_unknown_entry_is_noop() -> None:
    """A lookup miss (entry removed) is a quiet no-op, not an error."""

    stale = _registration("dead-registration")
    other = make_config_entry(
        entry_id="entry-removed-elsewhere",
        data={
            CONF_GOOGLE_EMAIL: "user@example.com",
            DATA_SECRET_BUNDLE: {"fcm_credentials": stale},
        },
    )
    receiver, manager = _receiver_with_entry(other)

    receiver._persist_credentials_to_entry(ENTRY_ID, _registration("fresh"))

    assert manager.update_calls == []
    assert other.data[DATA_SECRET_BUNDLE] == {"fcm_credentials": stale}


def test_persist_credentials_skips_non_mapping_bundle() -> None:
    """A bundle that is not a mapping is left alone, not rebuilt."""

    entry = make_config_entry(
        entry_id=ENTRY_ID,
        data={
            CONF_GOOGLE_EMAIL: "user@example.com",
            DATA_SECRET_BUNDLE: "not-a-bundle",
        },
    )
    receiver, manager = _receiver_with_entry(entry)

    receiver._persist_credentials_to_entry(ENTRY_ID, _registration("fresh"))

    assert manager.update_calls == []
    assert entry.data[DATA_SECRET_BUNDLE] == "not-a-bundle"


def test_persist_credentials_without_top_level_copy_does_not_invent_one() -> None:
    """Only the bundle is rewritten when no top-level copy exists.

    The seed reads the bundle, so a top-level key nothing reads must not be
    created as a side effect of the refresh.
    """

    stale = _registration("dead-registration")
    fresh = _registration("fresh-registration")
    entry = make_config_entry(
        entry_id=ENTRY_ID,
        data={
            CONF_GOOGLE_EMAIL: "user@example.com",
            DATA_SECRET_BUNDLE: {"fcm_credentials": stale},
        },
    )
    receiver, manager = _receiver_with_entry(entry)

    receiver._persist_credentials_to_entry(ENTRY_ID, fresh)

    assert len(manager.update_calls) == 1
    assert entry.data[DATA_SECRET_BUNDLE]["fcm_credentials"] == fresh
    assert "fcm_credentials" not in entry.data


@pytest.mark.asyncio
async def test_persist_credentials_without_hass_still_updates_the_cache() -> None:
    """A missing hass skips the entry write but not the cache write.

    Drives the real ``_async_save_credentials_for_entry`` path: the entry half
    is best-effort, the cache half is the live value the listener needs.
    """

    fresh = _registration("fresh-registration")
    receiver = FcmReceiverHA()
    cache = _Cache()
    receiver._entry_caches[ENTRY_ID] = cache
    receiver.creds[ENTRY_ID] = fresh

    await receiver._async_save_credentials_for_entry(ENTRY_ID)

    assert cache.data["fcm_credentials"] == fresh


@pytest.mark.asyncio
async def test_a_tokenless_update_persists_without_touching_the_entry() -> None:
    """A non-mapping credential update never reaches the entry writer.

    ``_on_credentials_updated_for_entry`` stores ``None`` for tokenless
    updates; the save path must skip the entry rewrite (which assumes a
    mapping) while still recording the cache value.
    """

    entry = make_config_entry(
        entry_id=ENTRY_ID,
        data={
            CONF_GOOGLE_EMAIL: "user@example.com",
            DATA_SECRET_BUNDLE: {"fcm_credentials": _registration("stale")},
        },
    )
    receiver, manager = _receiver_with_entry(entry)
    cache = _Cache()
    receiver._entry_caches[ENTRY_ID] = cache
    receiver.creds[ENTRY_ID] = None

    await receiver._async_save_credentials_for_entry(ENTRY_ID)

    assert manager.update_calls == []
    assert cache.data["fcm_credentials"] is None
