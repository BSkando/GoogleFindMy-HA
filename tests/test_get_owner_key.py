# tests/test_get_owner_key.py
"""Coverage + regression-lock tests for ``SpotApi/.../get_owner_key``.

These tests exercise the *real* orchestration of ``async_get_owner_key``,
``_retrieve_owner_key`` and ``_get_or_generate_user_owner_key_hex`` by mocking
only their **dependencies** (``async_get_eid_info``, ``async_get_shared_key``,
``decrypt_owner_key``, ``async_get_username``) — never the SUT functions
themselves. Mocking the SUT away is exactly the anti-pattern that leaves a
module at low coverage despite indirect tests (see
``test_e2ee_token_cache_propagation.py``, which monkeypatches
``async_get_owner_key`` as a whole).

Each ``RL-*`` test pins one member of the eight bug families distilled from the
12 fix commits in this file's git history so a regression would fail loudly:

    RL-1   cache=None -> ValueError (multi-account safety; no global facade)
    RL-2   explicit username= is used, no cache resolution
    RL-3   without username, cache.get(username_string) is preferred
    RL-4   empty username cache -> async_get_username(cache=cache) fallback
    RL-5   no username resolvable -> RuntimeError
    RL-6   per-user dict cache hit -> returned without retrieval
    RL-7   legacy non-scoped owner_key -> migrated to owner_key_<user>
    RL-8   force_refresh=True bypasses cache, retrieves fresh, writes cache
    RL-9   default path calls async_get_shared_key(cache=, username=)
    RL-10  SpotApiEmptyResponseError (trailers-only) -> ConfigEntryAuthFailed
    RL-11  SpotAuthPermanentError -> ConfigEntryAuthFailed (session invalid)
    RL-12  missing/empty shared_key -> RuntimeError
    RL-13  missing encryptedOwnerKeyAndMetadata -> RuntimeError
    RL-14  missing/empty encryptedOwnerKey -> RuntimeError
    RL-15  InvalidTag from decrypt -> propagated unwrapped (discriminator)
    RL-16  decrypted owner_key empty/wrong type -> RuntimeError
    RL-17  hex happy path -> OwnerKeyInfo(32 bytes, version)
    RL-18  base64url fallback + self-heal: cache normalized to hex
    RL-19  invalid format -> cache cleared (user+legacy) + RuntimeError
    RL-20  wrong length (16 bytes) -> cache cleared + RuntimeError
    RL-21  raw value not dict-with-key nor str -> cache cleared + RuntimeError
    RL-22  version normalization (int / digit-string / non-digit-string)
    RL-23  sync facade inside the event loop -> RuntimeError
    RL-24  sync facade outside the loop -> asyncio.run(async_get_owner_key)
"""

from __future__ import annotations

import base64
import logging
from binascii import Error as BinasciiError
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices import (
    get_owner_key as gok,
)

OwnerKeyInfo = gok.OwnerKeyInfo
_user_cache_key = gok._user_cache_key
_LEGACY_KEY = gok._OWNER_KEY_CACHE_PREFIX
USERNAME_KEY = gok.username_string

# --- Fixtures (deterministic key material) ---------------------------------
KEY32 = bytes(range(32))  # exactly 32 bytes (256 bit)
HEX64 = KEY32.hex()  # 64-char lowercase hex
B64URL = base64.urlsafe_b64encode(KEY32).decode()  # base64url of the same key
HEX16 = bytes(range(16)).hex()  # 32 hex chars -> 16 bytes (wrong length)
OTHER32 = bytes([0xAA] * 32)  # distinct key to prove force-refresh bypass
ENC = b"encrypted-owner-key-blob"  # opaque; decrypt is mocked
SHARED = b"\x11" * 32  # non-empty shared key bytes


class _FakeCache:
    """Minimal entry-scoped cache mirroring ``TokenCache`` semantics.

    ``set(key, None)`` clears (pop), matching the production clear-on-error
    behavior. ``generator_calls`` makes cache-hit vs. cache-miss observable.
    """

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self.generator_calls = 0

    def seed(self, key: str, value: Any) -> None:
        self._data[key] = value

    async def get(self, key: str) -> Any:
        return self._data.get(key)

    async def set(self, key: str, value: Any) -> None:
        if value is None:
            self._data.pop(key, None)
            return
        self._data[key] = value

    async def get_or_set(
        self, name: str, generator: Callable[[], Awaitable[Any] | Any]
    ) -> Any:
        if (existing := self._data.get(name)) is not None:
            return existing
        self.generator_calls += 1
        candidate = generator()
        if hasattr(candidate, "__await__"):
            candidate = await candidate
        self._data[name] = candidate
        return candidate


def _eid_info(*, encrypted: Any = ENC, version: Any = 7) -> SimpleNamespace:
    """Build a fake EID-info object with the metadata attribute chain."""
    metadata = SimpleNamespace(encryptedOwnerKey=encrypted, ownerKeyVersion=version)
    return SimpleNamespace(encryptedOwnerKeyAndMetadata=metadata)


def _patch_retrieval(
    monkeypatch: pytest.MonkeyPatch,
    *,
    eid_info: Any = None,
    shared: Any = SHARED,
    decrypted: Any = KEY32,
    eid_exc: BaseException | None = None,
) -> dict[str, Any]:
    """Patch the four module-level dependencies; return mocks for assertions."""
    if eid_info is None:
        eid_info = _eid_info()
    m_eid = (
        AsyncMock(side_effect=eid_exc) if eid_exc else AsyncMock(return_value=eid_info)
    )
    m_shared = AsyncMock(return_value=shared)
    m_decrypt = MagicMock(return_value=decrypted)
    m_username = AsyncMock(return_value="resolved@user.example")

    monkeypatch.setattr(gok, "async_get_eid_info", m_eid)
    monkeypatch.setattr(gok, "async_get_shared_key", m_shared)
    monkeypatch.setattr(gok, "decrypt_owner_key", m_decrypt)
    monkeypatch.setattr(gok, "async_get_username", m_username)
    return {
        "eid": m_eid,
        "shared": m_shared,
        "decrypt": m_decrypt,
        "username": m_username,
    }


# === RL-1: multi-account safety (no global facade) =========================
async def test_rl1_cache_none_raises_value_error() -> None:
    with pytest.raises(ValueError, match="multi-account safety"):
        await gok.async_get_owner_key(cache=None)


# === RL-2: explicit username= is used, no cache resolution =================
async def test_rl2_explicit_username_skips_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mocks = _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    cache.seed(_user_cache_key("explicit@user.example"), {"key": HEX64, "version": 1})

    info = await gok.async_get_owner_key(cache=cache, username="explicit@user.example")

    assert info.key == KEY32
    mocks["username"].assert_not_awaited()  # explicit username -> no resolution


# === RL-3: cache.get(username_string) preferred ============================
async def test_rl3_cached_username_preferred(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mocks = _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    cache.seed(USERNAME_KEY, "cached@user.example")
    cache.seed(_user_cache_key("cached@user.example"), {"key": HEX64, "version": 2})

    info = await gok.async_get_owner_key(cache=cache)

    assert info.key == KEY32
    mocks["username"].assert_not_awaited()  # cached username wins over fallback


# === RL-4: fallback to async_get_username(cache=cache) =====================
async def test_rl4_username_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    mocks = _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    cache.seed(_user_cache_key("resolved@user.example"), {"key": HEX64, "version": 1})

    info = await gok.async_get_owner_key(cache=cache)

    assert info.key == KEY32
    mocks["username"].assert_awaited_once_with(cache=cache)


# === RL-5: no username resolvable -> RuntimeError ==========================
async def test_rl5_unresolvable_username_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_retrieval(monkeypatch)
    monkeypatch.setattr(gok, "async_get_username", AsyncMock(return_value=None))
    cache = _FakeCache()

    with pytest.raises(RuntimeError, match="Username is not configured"):
        await gok.async_get_owner_key(cache=cache)


async def test_empty_username_string_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit empty username collapses to None -> RuntimeError (branch)."""
    _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    with pytest.raises(RuntimeError, match="Username is not configured"):
        await gok.async_get_owner_key(cache=cache, username="")


# === RL-6: per-user dict cache hit, no retrieval ===========================
async def test_rl6_dict_cache_hit_no_retrieval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mocks = _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    cache.seed(_user_cache_key("hit@user.example"), {"key": HEX64, "version": 9})

    info = await gok.async_get_owner_key(cache=cache, username="hit@user.example")

    assert info.key == KEY32
    mocks["eid"].assert_not_awaited()  # served from cache, no server call
    assert cache.generator_calls == 0


# === RL-7: legacy migration within the same cache scope ====================
async def test_rl7_legacy_dict_migration(monkeypatch: pytest.MonkeyPatch) -> None:
    mocks = _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    user = "legacy@user.example"
    cache.seed(_LEGACY_KEY, {"key": HEX64, "version": 3})

    info = await gok.async_get_owner_key(cache=cache, username=user)

    assert info.key == KEY32
    assert info.version == 3
    # Migrated into the per-user scope, no fresh retrieval.
    assert cache._data[_user_cache_key(user)] == {"key": HEX64, "version": 3}
    mocks["eid"].assert_not_awaited()


async def test_legacy_str_migration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy *string* value is migrated to the per-user scope (branch)."""
    mocks = _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    user = "legacystr@user.example"
    cache.seed(_LEGACY_KEY, HEX64)

    info = await gok.async_get_owner_key(cache=cache, username=user)

    assert info.key == KEY32
    mocks["eid"].assert_not_awaited()


# === RL-8: force_refresh bypasses cache, retrieves fresh, writes cache =====
async def test_rl8_force_refresh_bypasses_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mocks = _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    user = "u@e.co"  # <= 13 chars -> exercises the redaction "else" branch
    cache.seed(_user_cache_key(user), {"key": OTHER32.hex(), "version": 0})

    info = await gok.async_get_owner_key(cache=cache, username=user, force_refresh=True)

    assert info.key == KEY32  # fresh value, NOT the seeded OTHER32
    mocks["eid"].assert_awaited_once()  # retrieval happened despite cache hit
    assert cache._data[_user_cache_key(user)] == {"key": HEX64, "version": 7}


# === RL-9: default path forwards username= to async_get_shared_key =========
async def test_rl9_shared_key_called_with_username(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mocks = _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    user = "multi@acct.example.com"  # > 13 chars -> redaction "if" branch

    info = await gok.async_get_owner_key(cache=cache, username=user)

    assert info.key == KEY32
    mocks["shared"].assert_awaited_once_with(cache=cache, username=user)


# === Account in log records only through _mask_email_for_logs ==============
_GOK_LOGGER = gok.__name__


async def test_retrieved_owner_key_record_masks_the_account(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The INFO record names the account only in the shared masked form.

    ``AGENTS.md`` section 5 allows an account in a log only through the masking
    helpers; the former ad-hoc mask kept the first three characters and the
    last ten, which shows the whole domain and part of the local name.
    """
    _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    user = "user@example.com"
    caplog.set_level(logging.DEBUG, logger=_GOK_LOGGER)

    await gok.async_get_owner_key(cache=cache, username=user)

    own = [rec for rec in caplog.records if rec.name == _GOK_LOGGER]
    info = [rec.getMessage() for rec in own if rec.levelno == logging.INFO]
    assert any("u***@example.com" in message for message in info), info
    for rec in own:
        assert user not in rec.getMessage()
        assert "use***" not in rec.getMessage()


async def test_legacy_migration_record_masks_the_account(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The DEBUG records of the legacy migration name the account masked, too."""
    _patch_retrieval(monkeypatch)
    user = "legacy@example.com"
    caplog.set_level(logging.DEBUG, logger=_GOK_LOGGER)
    for seeded in ({"key": HEX64, "version": 3}, HEX64):
        cache = _FakeCache()
        cache.seed(_LEGACY_KEY, seeded)
        caplog.clear()

        await gok.async_get_owner_key(cache=cache, username=user)

        migrated = [
            rec.getMessage()
            for rec in caplog.records
            if rec.name == _GOK_LOGGER and "Migrated legacy" in rec.getMessage()
        ]
        assert migrated, type(seeded).__name__
        assert all("l***@example.com" in message for message in migrated)
        assert all(user not in message for message in migrated)


# === RL-10: trailers-only / empty SPOT body -> ConfigEntryAuthFailed =======
async def test_rl10_empty_response_maps_to_auth_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_retrieval(
        monkeypatch, eid_exc=gok.SpotApiEmptyResponseError("trailers only")
    )
    cache = _FakeCache()

    with pytest.raises(gok.ConfigEntryAuthFailed):
        await gok.async_get_owner_key(cache=cache, username="x@acct.example.com")


# === RL-11: permanent auth error -> ConfigEntryAuthFailed (session invalid) =
async def test_rl11_permanent_auth_error_maps_to_auth_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_retrieval(monkeypatch, eid_exc=gok.SpotAuthPermanentError("invalid session"))
    cache = _FakeCache()

    with pytest.raises(gok.ConfigEntryAuthFailed, match="session invalid"):
        await gok.async_get_owner_key(cache=cache, username="x@acct.example.com")


# === RL-12..RL-16, RL-22: validation/decrypt guards (getter-isolated) ======
def _getters(*, eid_info: Any = None, shared: Any = SHARED) -> dict[str, Any]:
    """Build happy-path getter callables to isolate downstream guards.

    Allowed by the plan for RL-12..RL-16/RL-22 (NOT for RL-9/10/11, which must
    exercise the default branch).
    """
    info = _eid_info() if eid_info is None else eid_info

    async def eid_getter() -> Any:
        return info

    async def shared_getter() -> Any:
        return shared

    return {"eid_info_getter": eid_getter, "shared_key_getter": shared_getter}


async def test_rl12_missing_shared_key_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gok, "decrypt_owner_key", MagicMock(return_value=KEY32))
    cache = _FakeCache()
    with pytest.raises(RuntimeError, match="Shared key is missing"):
        await gok.async_get_owner_key(
            cache=cache, username="g@acct.example.com", **_getters(shared=b"")
        )


async def test_rl13_missing_metadata_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gok, "decrypt_owner_key", MagicMock(return_value=KEY32))
    cache = _FakeCache()
    no_md = SimpleNamespace(encryptedOwnerKeyAndMetadata=None)
    with pytest.raises(RuntimeError, match="encryptedOwnerKeyAndMetadata"):
        await gok.async_get_owner_key(
            cache=cache,
            username="g@acct.example.com",
            **_getters(eid_info=no_md),
        )


async def test_rl14_missing_encrypted_owner_key_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gok, "decrypt_owner_key", MagicMock(return_value=KEY32))
    cache = _FakeCache()
    with pytest.raises(RuntimeError, match="encryptedOwnerKey"):
        await gok.async_get_owner_key(
            cache=cache,
            username="g@acct.example.com",
            **_getters(eid_info=_eid_info(encrypted=b"")),
        )


async def test_rl15_invalid_tag_propagates_unwrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RL-15: an AES-GCM InvalidTag during owner-key decryption propagates
    unchanged instead of being wrapped into RuntimeError.

    decrypt_locations relies on the unwrapped InvalidTag to map the failure to
    SharedKeyMismatchError and distinguish a wrong/stale shared key from a missing
    one. Wrapping it here would erase that discriminator.
    """
    monkeypatch.setattr(
        gok, "decrypt_owner_key", MagicMock(side_effect=gok.InvalidTag())
    )
    cache = _FakeCache()
    with pytest.raises(gok.InvalidTag):
        await gok.async_get_owner_key(
            cache=cache, username="g@acct.example.com", **_getters()
        )


async def test_rl16_empty_decrypted_key_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gok, "decrypt_owner_key", MagicMock(return_value=b""))
    cache = _FakeCache()
    with pytest.raises(RuntimeError, match="empty or invalid type"):
        await gok.async_get_owner_key(
            cache=cache, username="g@acct.example.com", **_getters()
        )


# === RL-17: hex happy path -> OwnerKeyInfo(32 bytes, int version) ==========
async def test_rl17_hex_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    cache.seed(_user_cache_key("hex@user.example"), {"key": HEX64, "version": 7})

    info = await gok.async_get_owner_key(cache=cache, username="hex@user.example")

    assert isinstance(info, OwnerKeyInfo)
    assert info.key == KEY32
    assert len(info.key) == 32
    assert info.version == 7


# === RL-18: base64url fallback + self-heal to hex ==========================
async def test_rl18_base64url_self_heal(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    user = "b64@user.example"
    cache.seed(_user_cache_key(user), B64URL)  # legacy non-hex string value

    info = await gok.async_get_owner_key(cache=cache, username=user)

    assert info.key == KEY32
    assert info.version is None
    # Self-heal: cache normalized to a hex dict for future consistency.
    assert cache._data[_user_cache_key(user)] == {"key": HEX64, "version": None}


# === RL-19: invalid format -> cache cleared (user + legacy) + RuntimeError ==
async def test_rl19_invalid_format_clears_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    user = "bad@user.example"
    cache.seed(_user_cache_key(user), "z")  # neither valid hex nor valid base64
    cache.seed(_LEGACY_KEY, "z")

    with pytest.raises(RuntimeError, match="Invalid owner_key format"):
        await gok.async_get_owner_key(cache=cache, username=user)

    assert _user_cache_key(user) not in cache._data
    assert _LEGACY_KEY not in cache._data


# === RL-20: wrong length (16 bytes) -> cache cleared + RuntimeError =========
async def test_rl20_wrong_length_clears_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    user = "short@user.example"
    cache.seed(_user_cache_key(user), HEX16)  # decodes to 16 bytes
    cache.seed(_LEGACY_KEY, HEX16)

    with pytest.raises(RuntimeError, match="exactly 32 bytes"):
        await gok.async_get_owner_key(cache=cache, username=user)

    assert _user_cache_key(user) not in cache._data
    assert _LEGACY_KEY not in cache._data


# === RL-21: raw value not dict-with-key nor str -> cleared + RuntimeError ===
async def test_rl21_non_string_key_value_clears_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    user = "weird@user.example"
    cache.seed(_user_cache_key(user), {"key": 12345, "version": 1})  # key not str
    cache.seed(_LEGACY_KEY, {"key": 12345})

    with pytest.raises(RuntimeError, match="missing or invalid"):
        await gok.async_get_owner_key(cache=cache, username=user)

    assert _user_cache_key(user) not in cache._data
    assert _LEGACY_KEY not in cache._data


# === RL-22: version normalization (digit-string / non-digit-string) ========
async def test_rl22_digit_string_version_coerced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    cache.seed(_user_cache_key("v@user.example"), {"key": HEX64, "version": "42"})

    info = await gok.async_get_owner_key(cache=cache, username="v@user.example")

    assert info.version == 42  # "42" coerced via .isdigit() -> int()


async def test_rl22_non_digit_string_version_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_retrieval(monkeypatch)
    cache = _FakeCache()
    cache.seed(_user_cache_key("v2@user.example"), {"key": HEX64, "version": "abc"})

    info = await gok.async_get_owner_key(cache=cache, username="v2@user.example")

    assert info.version is None


# === RL-23: sync facade inside the event loop -> RuntimeError ==============
async def test_rl23_sync_facade_in_event_loop_raises() -> None:
    cache = _FakeCache()
    with pytest.raises(RuntimeError, match="event loop"):
        gok.get_owner_key(cache=cache)


# === RL-24: sync facade outside the loop -> asyncio.run path ===============
def test_rl24_sync_facade_outside_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plain sync test (no running loop) -> exercises asyncio.run branch."""
    sentinel = OwnerKeyInfo(key=KEY32, version=1)
    monkeypatch.setattr(gok, "async_get_owner_key", AsyncMock(return_value=sentinel))
    cache = _FakeCache()

    result = gok.get_owner_key(cache=cache)

    assert result == sentinel


# === Helper branch tests: _try_hex =========================================
def test_try_hex_0x_prefix() -> None:
    assert gok._try_hex("0x" + HEX64) == KEY32


def test_try_hex_whitespace_ignored() -> None:
    spaced = " ".join(HEX64[i : i + 8] for i in range(0, len(HEX64), 8))
    assert gok._try_hex(spaced + "\n") == KEY32


def test_try_hex_odd_length_padded() -> None:
    # "abc" -> prepend "0" -> "0abc" -> bytes 0x0a 0xbc
    assert gok._try_hex("abc") == bytes([0x0A, 0xBC])


def test_try_hex_non_hex_raises() -> None:
    with pytest.raises(BinasciiError):
        gok._try_hex("nothexvalue")


def test_try_hex_empty_string() -> None:
    assert gok._try_hex("") == b""


# === Helper branch tests: _try_base64_like =================================
def test_try_base64_like_urlsafe() -> None:
    assert gok._try_base64_like(B64URL) == KEY32


def test_try_base64_like_strips_pem_headers() -> None:
    std_b64 = base64.b64encode(KEY32).decode()
    pem = f"-----BEGIN KEY-----\n{std_b64}\n-----END KEY-----\n"
    assert gok._try_base64_like(pem) == KEY32


def test_try_base64_like_invalid_raises() -> None:
    # A single data char can never form a base64 byte -> both decoders raise.
    with pytest.raises((ValueError, TypeError)):
        gok._try_base64_like("z")
