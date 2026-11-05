# tests/test_config_flow_secrets_normalization.py
"""Tests for whitespace normalization of pasted secrets.json bundles.

Copy/paste of a ``secrets.json`` into the config flow can inject stray
whitespace into credential values. A single space inside ``owner_key`` breaks
AES-GCM owner-key decryption ("Owner key decryption failed"), and the user had
to delete that space manually. ``normalize_secrets_bundle`` removes whitespace
from credential values while preserving interior spaces in free-text fields.
"""

from __future__ import annotations

import inspect
import json
from importlib import import_module
from typing import Any

import pytest
from homeassistant.helpers import frame

from custom_components.googlefindmy import config_flow
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    CONF_OAUTH_TOKEN,
    DATA_SECRET_BUNDLE,
)
from custom_components.googlefindmy.shared_helpers import normalize_secrets_bundle
from tests.helpers.config_flow import (
    ConfigEntriesDomainUniqueIdLookupMixin,
    attach_config_entries_flow_manager,
    prepare_flow_hass_config_entries,
    set_config_flow_unique_id,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("AABBCC", "AABBCC"),  # already clean -> unchanged
        (" AABBCC", "AABBCC"),  # leading space
        ("AABBCC ", "AABBCC"),  # trailing space
        ("AA BB CC", "AABBCC"),  # interior spaces (wrapped paste)
        ("AA\nBB\tCC", "AABBCC"),  # interior newline/tab
        ("  AA BB  ", "AABB"),  # combined leading/trailing/interior
    ],
)
def test_credential_value_all_whitespace_removed(raw: str, expected: str) -> None:
    """Whitelisted credential keys lose ALL whitespace, including interior."""
    assert normalize_secrets_bundle({"owner_key": raw})["owner_key"] == expected
    assert normalize_secrets_bundle({"shared_key": raw})["shared_key"] == expected


def test_non_credential_value_only_edge_trimmed() -> None:
    """Free-text fields keep interior spaces and are only edge-trimmed."""
    result = normalize_secrets_bundle(
        {"username": "  Jane Doe  ", "Email": " a b@x.io "}
    )
    assert result["username"] == "Jane Doe"
    assert result["Email"] == "a b@x.io"


def test_nested_fcm_token_normalized() -> None:
    """Whitelisted keys nested under fcm_credentials are normalized too."""
    bundle = {
        "fcm_credentials": {
            "installation": {"token": " inst alled "},
            "fcm": {"registration": {"token": "reg\nistration"}},
        }
    }
    result = normalize_secrets_bundle(bundle)
    assert result["fcm_credentials"]["installation"]["token"] == "installed"
    assert result["fcm_credentials"]["fcm"]["registration"]["token"] == "registration"


def test_nested_gcm_identity_normalized() -> None:
    """The numeric GCM identity (android_id / security_token under
    fcm_credentials.gcm) loses ALL whitespace: both are fed to int() on the FCM
    login path, so an interior space from a wrapped paste would break push
    registration (Codex finding on PR #182)."""
    bundle = {
        "fcm_credentials": {
            "gcm": {
                "android_id": "1234 5678 9012 3456",
                "security_token": " 9876\n543210 ",
                "app_id": "app-1",
            }
        }
    }
    result = normalize_secrets_bundle(bundle)
    gcm = result["fcm_credentials"]["gcm"]
    assert gcm["android_id"] == "1234567890123456"
    assert gcm["security_token"] == "9876543210"
    # app_id is a structured identifier (not numeric/token credential material)
    # and is not in the whitelist: only edge-trimmed, interior preserved.
    assert gcm["app_id"] == "app-1"


def test_idempotent() -> None:
    """Applying the normalization twice yields the same result."""
    bundle = {
        "owner_key": " AA BB ",
        "username": "  Jane Doe  ",
        "fcm_credentials": {"installation": {"token": " t ok "}},
    }
    once = normalize_secrets_bundle(bundle)
    assert normalize_secrets_bundle(once) == once


def test_input_not_mutated() -> None:
    """The input mapping is never mutated (safe for MappingProxyType)."""
    bundle = {"owner_key": " AA BB ", "nested": {"aas_token": " x y "}}
    normalize_secrets_bundle(bundle)
    assert bundle["owner_key"] == " AA BB "
    assert bundle["nested"]["aas_token"] == " x y "


def test_list_values_recursed() -> None:
    """Lists are walked element-wise, dicts inside them are normalized."""
    bundle = {"items": [{"token": " a b "}, {"username": "  keep me  "}]}
    result = normalize_secrets_bundle(bundle)
    assert result["items"][0]["token"] == "ab"
    assert result["items"][1]["username"] == "keep me"


def test_non_string_scalars_unchanged() -> None:
    """Non-string scalar values pass through unchanged."""
    bundle = {"count": 3, "enabled": True, "missing": None, "owner_key": " A B "}
    result = normalize_secrets_bundle(bundle)
    assert result["count"] == 3
    assert result["enabled"] is True
    assert result["missing"] is None
    assert result["owner_key"] == "AB"


def test_non_mapping_input_returned_unchanged() -> None:
    """A non-mapping top-level value is returned unchanged."""
    assert normalize_secrets_bundle("plain") == "plain"
    assert normalize_secrets_bundle(None) is None


# ---------------------------------------------------------------------------
# Scoped-shared-key promotion (producer/consumer contract repair).
#
# A legacy/older secrets.json may carry a *scoped* ``shared_key_<email>`` entry
# but no top-level ``shared_key``. The CLI producer and the runtime reader both
# accept the scoped form, but ``_secrets_key_status`` only inspects the
# top-level ``shared_key``, so the import gate would wrongly reject such a bundle
# as ``keys_missing``. ``normalize_secrets_bundle`` promotes an unambiguous
# scoped key to the canonical top-level slot so every import surface and the
# runtime reader converge on one key definition.
# ---------------------------------------------------------------------------

# A realistic 32-byte (64 hex chars) shared key value.
_SHARED_HEX = "a1b2c3d4e5f60718293a4b5c6d7e8f90112233445566778899aabbccddeeff00"
_SHARED_HEX_2 = "00ffeeddccbbaa998877665544332211090f8e7d6c5b4a3a291807f6e5d4c3b2a1"[
    :64
]


def test_scoped_only_shared_key_promoted_to_top_level() -> None:
    """A scoped-only shared key is promoted to the canonical top-level slot."""
    bundle = {
        "shared_key_user@example.com": _SHARED_HEX,
        "oauth_token": "aas_et/FROM_SECRETS",
    }
    result = normalize_secrets_bundle(bundle)
    assert result["shared_key"] == _SHARED_HEX
    # The scoped entry is preserved (runtime save path still writes it scoped).
    assert result["shared_key_user@example.com"] == _SHARED_HEX


def test_existing_top_level_shared_key_not_overwritten() -> None:
    """An existing non-empty top-level shared_key wins over any scoped entry."""
    bundle = {
        "shared_key": _SHARED_HEX,
        "shared_key_user@example.com": _SHARED_HEX_2,
    }
    result = normalize_secrets_bundle(bundle)
    assert result["shared_key"] == _SHARED_HEX


def test_multiple_scoped_same_value_promoted() -> None:
    """Multiple scoped entries sharing the SAME value still promote (no ambiguity)."""
    bundle = {
        "shared_key_a@example.com": _SHARED_HEX,
        "shared_key_b@example.com": _SHARED_HEX,
    }
    result = normalize_secrets_bundle(bundle)
    assert result["shared_key"] == _SHARED_HEX


def test_ambiguous_distinct_scoped_values_not_promoted() -> None:
    """Two DISTINCT scoped values are ambiguous and must NOT be promoted."""
    bundle = {
        "shared_key_a@example.com": _SHARED_HEX,
        "shared_key_b@example.com": _SHARED_HEX_2,
    }
    result = normalize_secrets_bundle(bundle)
    assert "shared_key" not in result


def test_bare_shared_key_prefix_not_matched_as_scoped() -> None:
    """The bare ``shared_key`` must not be mistaken for a scoped entry.

    Only keys with a non-empty suffix after the ``shared_key_`` prefix qualify;
    an empty (``""``) top-level shared_key alongside no scoped entry stays absent.
    """
    bundle = {"shared_key": "", "oauth_token": "aas_et/X"}
    result = normalize_secrets_bundle(bundle)
    # No scoped entry exists to promote, and the empty value is not "present".
    assert result["shared_key"] == ""


def test_scoped_empty_value_not_promoted() -> None:
    """A scoped entry whose value is empty/whitespace does not qualify."""
    bundle = {"shared_key_user@example.com": "   ", "oauth_token": "aas_et/X"}
    result = normalize_secrets_bundle(bundle)
    assert "shared_key" not in result


def test_promotion_does_not_descend_into_nested_dicts() -> None:
    """Promotion is a top-level-only step; nested scoped keys are untouched."""
    bundle = {
        "oauth_token": "aas_et/X",
        "nested": {"shared_key_user@example.com": _SHARED_HEX},
    }
    result = normalize_secrets_bundle(bundle)
    # No top-level shared_key is synthesized from a nested scoped entry.
    assert "shared_key" not in result
    assert result["nested"]["shared_key_user@example.com"] == _SHARED_HEX
    assert "shared_key" not in result["nested"]


def test_promotion_idempotent() -> None:
    """Promotion is idempotent: a second pass yields an identical bundle."""
    bundle = {
        "shared_key_user@example.com": _SHARED_HEX,
        "oauth_token": "aas_et/X",
    }
    once = normalize_secrets_bundle(bundle)
    assert normalize_secrets_bundle(once) == once


def test_promotion_does_not_mutate_input() -> None:
    """The scoped-only input mapping is never mutated by promotion."""
    bundle = {
        "shared_key_user@example.com": _SHARED_HEX,
        "oauth_token": "aas_et/X",
    }
    normalize_secrets_bundle(bundle)
    assert "shared_key" not in bundle


def test_promoted_scoped_value_is_whitespace_normalized() -> None:
    """A promoted scoped key is normalized like the canonical ``shared_key``.

    Scoped ``shared_key_<id>`` keys are not whitelisted, so the recursive
    whitespace pass only edge-trims them. Promotion must apply the canonical
    credential normalization (all whitespace removed) so a wrapped-paste interior
    space cannot survive into the top-level key and break AES-GCM decryption.
    """
    bundle = {"shared_key_user@example.com": " a b\nc ", "oauth_token": "aas_et/X"}
    result = normalize_secrets_bundle(bundle)
    assert result["shared_key"] == "abc"


def test_scoped_values_differing_only_by_whitespace_not_ambiguous() -> None:
    """Two scoped entries that normalize to the same key are not a conflict."""
    bundle = {
        "shared_key_a@example.com": "AA BB",
        "shared_key_b@example.com": "AABB",
    }
    result = normalize_secrets_bundle(bundle)
    # Both normalize to "AABB", so promotion proceeds with that single value.
    assert result["shared_key"] == "AABB"


def test_promotion_tolerates_non_string_keys() -> None:
    """Promotion never raises on non-string keys (contract: input is ``Any``).

    A bundle may carry non-string keys; the scoped-key scan must skip them rather
    than call ``str``-only methods on them.
    """
    bundle: dict[Any, Any] = {
        7: "numeric-key-value",
        "shared_key_user@example.com": _SHARED_HEX,
    }
    result = normalize_secrets_bundle(bundle)
    assert result["shared_key"] == _SHARED_HEX
    assert result[7] == "numeric-key-value"


def test_scoped_only_bundle_passes_key_status_gate() -> None:
    """End-to-end: a scoped-only bundle is importable after normalization.

    ``_secrets_key_status`` reads only the top-level ``shared_key``; the
    promotion at the normalization chokepoint makes the scoped key visible to
    the gate so ``has_shared`` is True and ``_interpret_credentials_choice`` does
    not return ``keys_missing``.
    """
    raw = {
        "google_email": "user@example.com",
        "aas_token": "aas_et/FROM_SECRETS",
        "shared_key_user@example.com": _SHARED_HEX,
    }
    normalized = normalize_secrets_bundle(raw)
    has_shared, _has_owner = config_flow._secrets_key_status(normalized)
    assert has_shared is True

    _method, _email, _cands, err = config_flow._interpret_credentials_choice(
        {"secrets_json": json.dumps(raw)},
        secrets_field="secrets_json",
        token_field=CONF_OAUTH_TOKEN,
        email_field=CONF_GOOGLE_EMAIL,
    )
    assert err != "keys_missing"


@pytest.mark.asyncio
async def test_config_flow_round_trip_strips_owner_key_space(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: a pasted owner_key with a stray space is persisted clean.

    This is the regression guard for the reported bug: the user pasted a valid
    secrets.json but a copy/paste space inside owner_key broke decryption. The
    persisted secrets bundle must contain the whitespace-free owner_key.
    """
    secrets_payload = {
        "google_email": "user@example.com",
        "aas_token": "aas_et/FROM_SECRETS",
        "owner_key": "AA BB\nCC ",
        "shared_key": " DD EE ",
        "username": "  Keep Me  ",
    }

    async def _fake_pick(
        hass: Any,
        email: str,
        candidates: list[tuple[str, str]],
        *,
        secrets_bundle: dict[str, Any] | None = None,
    ) -> str | None:
        # The bundle handed downstream is already normalized.
        assert secrets_bundle is not None
        assert secrets_bundle["owner_key"] == "AABBCC"
        return candidates[0][1]

    monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)

    class _ConfigEntries(ConfigEntriesDomainUniqueIdLookupMixin):
        def __init__(self) -> None:
            attach_config_entries_flow_manager(self)

        def async_entries(self, domain: str) -> list[Any]:
            assert domain == config_flow.DOMAIN
            return []

    class _FlowHass:
        def __init__(self) -> None:
            prepare_flow_hass_config_entries(
                self,
                _ConfigEntries,
                frame_module=frame,
            )

    captured: dict[str, Any] = {}

    async def _create_entry(
        *,
        title: str,
        data: dict[str, Any],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        captured["result"] = {"title": title, "data": data, "options": options}
        return {"type": "create_entry", "title": title, "data": data}

    hass = _FlowHass()
    flow = config_flow.ConfigFlow()
    flow.hass = hass  # type: ignore[assignment]
    flow.context = {}
    flow._available_devices = [("Device", "device-id")]  # type: ignore[attr-defined]
    set_config_flow_unique_id(flow, None)

    async def _set_unique_id(value: str, *, raise_on_progress: bool = False) -> None:
        set_config_flow_unique_id(flow, value)

    flow.async_set_unique_id = _set_unique_id  # type: ignore[assignment]
    flow._abort_if_unique_id_configured = lambda **_: None  # type: ignore[attr-defined]
    flow.async_create_entry = _create_entry  # type: ignore[assignment]

    first = await flow.async_step_secrets_json(
        {"secrets_json": json.dumps(secrets_payload)}
    )
    if inspect.isawaitable(first):
        first = await first
    assert isinstance(first, dict)
    assert first.get("type") == "form"

    final = await flow.async_step_device_selection({})
    if inspect.isawaitable(final):
        final = await final
    assert isinstance(final, dict)
    assert final.get("type") == "create_entry"

    payload = captured.get("result")
    assert payload, "Expected config entry payload to be captured"
    data = payload["data"]
    bundle = data[DATA_SECRET_BUNDLE]
    assert bundle["owner_key"] == "AABBCC"
    assert bundle["shared_key"] == "DDEE"
    # Free-text field keeps interior spaces, only edge-trimmed.
    assert bundle["username"] == "Keep Me"
    # Token persisted from the (clean) aas_token.
    assert data[CONF_OAUTH_TOKEN] == "aas_et/FROM_SECRETS"
    assert data[CONF_GOOGLE_EMAIL] == "user@example.com"


# ---------------------------------------------------------------------------
# D1/D2 single-key enforcement tests.
#
# Design decision (single-key inversion, v5): the presence of ``shared_key`` is
# the only gate that decides whether a pasted secrets bundle is importable. An
# owner-only bundle without a ``shared_key`` is a non-renewable dead end and must
# be blocked; conversely a bundle that carries a ``shared_key`` but lacks (or has
# a stale) ``owner_key`` is tolerated because the integration can fetch/refresh
# the owner key itself. The D2 seeding warning names the real outage scope
# (immediate FMDN loss plus the rotation-driven late outage for own devices).
# ---------------------------------------------------------------------------


def test_d1_shared_missing_blocked() -> None:
    """The D1 single-key rule: shared_key-less bundles block under v5.

    ``_interpret_credentials_choice`` is a pure function, so no ConfigEntry or
    hass stub is required. Two input classes are pinned, both of which carry a
    valid email and a plausible token and must block once the single-key rule
    is enforced:

    * (a) shared_key missing AND owner_key missing.
    * (b) shared_key missing BUT owner_key present.
    """
    # Class (a): neither shared_key nor owner_key present.
    bundle_no_owner = {
        "google_email": "user@example.com",
        "aas_token": "aas_et/FROM_SECRETS",
    }
    # Class (b): owner_key present, shared_key still missing.
    bundle_with_owner = {
        "google_email": "user@example.com",
        "aas_token": "aas_et/FROM_SECRETS",
        "owner_key": "AABBCC",
    }

    for bundle in (bundle_no_owner, bundle_with_owner):
        _method, _email, _cands, err = config_flow._interpret_credentials_choice(
            {"secrets_json": json.dumps(bundle)},
            secrets_field="secrets_json",
            token_field=CONF_OAUTH_TOKEN,
            email_field=CONF_GOOGLE_EMAIL,
        )
        # Target (v5) behavior: a missing shared_key blocks the import.
        assert err == "keys_missing", (
            "shared_key-less bundle must be rejected with 'keys_missing' "
            f"(owner_key present={'owner_key' in bundle})"
        )


class _CapturingCache:
    """Minimal cache stub recording values written by the secrets migrator.

    Mirrors the ``_CapturingCache`` pattern in
    ``tests/test_token_cache_secrets.py`` so the seeding helper can be exercised
    without a real ``TokenCache`` or ConfigEntry.
    """

    def __init__(self) -> None:
        self.saved: dict[str, Any] = {}

    async def async_set_cached_value(self, name: str, value: Any) -> None:
        self.saved[name] = value


def _seeding_shared_missing_warnings(
    caplog: pytest.LogCaptureFixture,
) -> list[str]:
    """Return the emitted missing-shared_key warning messages."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelname == "WARNING" and "shared_key" in record.getMessage()
    ]


@pytest.mark.asyncio
async def test_d2_seeding_warning_names_rotation_outage_when_owner_present(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """D2 fix: an owner-only (shared_key missing) bundle warns about the
    rotation-driven late outage, not only the immediate FMDN restriction."""
    integration_init = import_module("custom_components.googlefindmy")

    cache = _CapturingCache()
    secrets_bundle = {
        "google_email": "user@example.com",
        "username": "user@example.com",
        "owner_key": "AABBCC",
        # shared_key intentionally absent -> triggers the seeding warning.
    }

    with caplog.at_level("WARNING"):
        await integration_init._async_save_secrets_data(cache, secrets_bundle)

    warnings = _seeding_shared_missing_warnings(caplog)
    assert warnings, "Expected a missing-shared_key warning to be emitted"
    message = warnings[0]
    # The corrected wording now surfaces the rotation-driven late outage.
    assert "rotat" in message.lower()
    assert "will fail" in message


@pytest.mark.asyncio
async def test_d2_seeding_warning_names_total_outage_when_owner_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """D2 fix: a bundle with neither key warns that no location can be
    decrypted at all."""
    integration_init = import_module("custom_components.googlefindmy")

    cache = _CapturingCache()
    secrets_bundle = {
        "google_email": "user@example.com",
        "username": "user@example.com",
        # neither owner_key nor shared_key present.
    }

    with caplog.at_level("WARNING"):
        await integration_init._async_save_secrets_data(cache, secrets_bundle)

    warnings = _seeding_shared_missing_warnings(caplog)
    assert warnings, "Expected a missing-shared_key warning to be emitted"
    message = warnings[0]
    assert "No location can be decrypted" in message
    # The total-outage variant must not claim a (non-existent) rotation grace
    # period.
    assert "rotat" not in message.lower()


@pytest.mark.asyncio
async def test_d2_seeding_warning_silent_when_shared_present(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """D2 fix: a bundle that carries a shared_key emits no seeding warning."""
    integration_init = import_module("custom_components.googlefindmy")

    cache = _CapturingCache()
    secrets_bundle = {
        "google_email": "user@example.com",
        "username": "user@example.com",
        "owner_key": "AABBCC",
        "shared_key": "DDEEFF",
    }

    with caplog.at_level("WARNING"):
        await integration_init._async_save_secrets_data(cache, secrets_bundle)

    assert not _seeding_shared_missing_warnings(caplog), (
        "A complete bundle (shared_key present) must not warn about a missing "
        "shared_key."
    )


# ---------------------------------------------------------------------------
# AP2: unit coverage for the _secrets_key_status helper (four input classes).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("bundle", "expected"),
    [
        ({}, (False, False)),  # both missing
        ({"shared_key": "AABB"}, (True, False)),  # shared only
        ({"owner_key": "CCDD"}, (False, True)),  # owner only
        ({"shared_key": "AABB", "owner_key": "CCDD"}, (True, True)),  # both
    ],
)
def test_secrets_key_status_four_classes(
    bundle: dict[str, Any], expected: tuple[bool, bool]
) -> None:
    """The helper reports presence for each of the four key combinations."""
    assert config_flow._secrets_key_status(bundle) == expected


@pytest.mark.parametrize(
    "value",
    ["", "   ", "\n\t", 123, None, b"AABB", ["AABB"]],
)
def test_secrets_key_status_typechecks_presence(value: Any) -> None:
    """Only a non-empty string (after stripping) counts as a present key."""
    has_shared, has_owner = config_flow._secrets_key_status(
        {"shared_key": value, "owner_key": value}
    )
    assert has_shared is False
    assert has_owner is False


# ---------------------------------------------------------------------------
# AP3: call-site integration coverage for async_step_secrets_json.
#
# shared-missing (owner missing OR present) -> errors["base"] == "keys_missing"
# shared-present -> the flow proceeds to the device-selection entry path.
# ---------------------------------------------------------------------------


def _make_secrets_flow(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, dict[str, Any]]:
    """Build a ConfigFlow wired with the canonical _FlowHass stub pattern."""

    captured: dict[str, Any] = {}

    async def _fake_pick(
        hass: Any,
        email: str,
        candidates: list[tuple[str, str]],
        *,
        secrets_bundle: dict[str, Any] | None = None,
    ) -> str | None:
        return candidates[0][1] if candidates else None

    monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)

    class _ConfigEntries(ConfigEntriesDomainUniqueIdLookupMixin):
        def __init__(self) -> None:
            attach_config_entries_flow_manager(self)

        def async_entries(self, domain: str) -> list[Any]:
            assert domain == config_flow.DOMAIN
            return []

    class _FlowHass:
        def __init__(self) -> None:
            prepare_flow_hass_config_entries(
                self,
                _ConfigEntries,
                frame_module=frame,
            )

    hass = _FlowHass()
    flow = config_flow.ConfigFlow()
    flow.hass = hass  # type: ignore[assignment]
    flow.context = {}
    flow._available_devices = [("Device", "device-id")]  # type: ignore[attr-defined]
    set_config_flow_unique_id(flow, None)

    async def _set_unique_id(value: str, *, raise_on_progress: bool = False) -> None:
        set_config_flow_unique_id(flow, value)

    async def _create_entry(
        *,
        title: str,
        data: dict[str, Any],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        captured["result"] = {"title": title, "data": data, "options": options}
        return {"type": "create_entry", "title": title, "data": data}

    flow.async_set_unique_id = _set_unique_id  # type: ignore[assignment]
    flow._abort_if_unique_id_configured = lambda **_: None  # type: ignore[attr-defined]
    flow.async_create_entry = _create_entry  # type: ignore[assignment]
    return flow, captured


@pytest.mark.asyncio
@pytest.mark.parametrize("with_owner", [False, True])
async def test_secrets_json_step_blocks_when_shared_missing(
    monkeypatch: pytest.MonkeyPatch, with_owner: bool
) -> None:
    """async_step_secrets_json rejects a shared_key-less paste with keys_missing.

    Both owner classes (owner missing and owner present) are blocked, since the
    single-key rule gates only on the shared_key.
    """
    flow, captured = _make_secrets_flow(monkeypatch)

    secrets_payload: dict[str, Any] = {
        "google_email": "user@example.com",
        "aas_token": "aas_et/FROM_SECRETS",
    }
    if with_owner:
        secrets_payload["owner_key"] = "AABBCC"

    result = await flow.async_step_secrets_json(
        {"secrets_json": json.dumps(secrets_payload)}
    )
    if inspect.isawaitable(result):
        result = await result

    assert isinstance(result, dict)
    assert result.get("type") == "form"
    assert result.get("errors") == {"base": "keys_missing"}
    assert "result" not in captured


@pytest.mark.asyncio
async def test_secrets_json_step_accepts_when_shared_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """async_step_secrets_json accepts a shared_key bundle and proceeds to entry."""
    flow, captured = _make_secrets_flow(monkeypatch)

    secrets_payload = {
        "google_email": "user@example.com",
        "aas_token": "aas_et/FROM_SECRETS",
        "shared_key": "DDEEFF",
    }

    first = await flow.async_step_secrets_json(
        {"secrets_json": json.dumps(secrets_payload)}
    )
    if inspect.isawaitable(first):
        first = await first
    assert isinstance(first, dict)
    # No key error: the flow advanced to the device-selection form.
    assert first.get("errors", {}) == {}

    final = await flow.async_step_device_selection({})
    if inspect.isawaitable(final):
        final = await final
    assert isinstance(final, dict)
    assert final.get("type") == "create_entry"
    assert captured["result"]["data"][DATA_SECRET_BUNDLE]["shared_key"] == "DDEEFF"


# ---------------------------------------------------------------------------
# AP3e: call-site integration coverage for async_step_credentials
# (options "Refresh credentials"). Reuses the canonical options-flow harness.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("with_owner", [False, True])
async def test_credentials_step_blocks_when_shared_missing(
    monkeypatch: pytest.MonkeyPatch, with_owner: bool
) -> None:
    """async_step_credentials rejects a shared_key-less refresh in the field slot.

    The error lands in the new_secrets_json field slot (not base) and no entry
    update / reload is performed.
    """
    from tests.test_options_flow_credentials_cache import (
        _DummyEntry,
        _DummyHass,
        _MemoryCache,
    )

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

    async def _fail_pick(*args: Any, **kwargs: Any) -> str | None:
        raise AssertionError("token validation must not run for a blocked bundle")

    monkeypatch.setattr(config_flow, "async_pick_working_token", _fail_pick)

    payload: dict[str, Any] = {
        "google_email": "user@example.com",
        "oauth_token": "oauth-token-from-secrets-123456",
    }
    if with_owner:
        payload["owner_key"] = "AABBCC"

    result = await flow.async_step_credentials(
        {"new_secrets_json": json.dumps(payload)}
    )
    if inspect.isawaitable(result):
        result = await result

    assert isinstance(result, dict)
    assert result.get("type") == "form"
    assert result.get("errors") == {"new_secrets_json": "keys_missing"}
    assert not hass.config_entries.updated_payloads
    assert not hass.config_entries.reloaded


@pytest.mark.asyncio
async def test_credentials_step_accepts_when_shared_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """async_step_credentials accepts a shared_key refresh and finalizes."""
    from tests.test_options_flow_credentials_cache import (
        _DummyEntry,
        _DummyHass,
        _MemoryCache,
    )

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
        return candidates[0][1] if candidates else None

    monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)

    payload = {
        "google_email": "user@example.com",
        "oauth_token": "oauth-token-from-secrets-123456",
        "shared_key": "DDEEFF",
    }

    result = await flow.async_step_credentials(
        {"new_secrets_json": json.dumps(payload)}
    )
    if inspect.isawaitable(result):
        result = await result

    assert isinstance(result, dict)
    # _finalize_success was reached: the entry was updated with the new bundle.
    assert hass.config_entries.updated_payloads
    updated = hass.config_entries.updated_payloads[-1]
    assert updated[DATA_SECRET_BUNDLE]["shared_key"] == "DDEEFF"
    await hass.drain_tasks()


@pytest.mark.asyncio
async def test_credentials_step_shared_present_but_no_token_is_invalid_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shared_key-bearing bundle with no plausible token still fails on tokens.

    This pins the branch that follows the single-key gate: the gate passes (the
    shared_key is present) but the candidate-token check then rejects the bundle
    with ``invalid_token`` in the base slot.
    """
    from tests.test_options_flow_credentials_cache import (
        _DummyEntry,
        _DummyHass,
        _MemoryCache,
    )

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

    async def _fail_pick(*args: Any, **kwargs: Any) -> str | None:
        raise AssertionError("token validation must not run without candidates")

    monkeypatch.setattr(config_flow, "async_pick_working_token", _fail_pick)

    payload = {
        "google_email": "user@example.com",
        # shared_key present so the gate passes, but no plausible token.
        "shared_key": "DDEEFF",
    }

    result = await flow.async_step_credentials(
        {"new_secrets_json": json.dumps(payload)}
    )
    if inspect.isawaitable(result):
        result = await result

    assert isinstance(result, dict)
    assert result.get("errors") == {"base": "invalid_token"}
    assert not hass.config_entries.updated_payloads


# ---------------------------------------------------------------------------
# AP3c: discovery validator coverage for the single-key gate.
#
# shared-missing (owner missing OR present) -> DiscoveryFlowError("keys_missing")
# shared-present -> the discovery payload validates normally.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("with_owner", [False, True])
def test_discovery_payload_blocks_when_shared_missing(with_owner: bool) -> None:
    """A discovered bundle without a shared_key is rejected with keys_missing."""
    secrets: dict[str, Any] = {
        "google_email": "user@example.com",
        "aas_token": "aas_et/DISCOVERY",
    }
    if with_owner:
        secrets["owner_key"] = "AABBCC"

    with pytest.raises(config_flow.DiscoveryFlowError) as exc_info:
        config_flow._normalize_and_validate_discovery_payload(
            {DATA_SECRET_BUNDLE: secrets}
        )
    assert exc_info.value.reason == "keys_missing"


def test_discovery_payload_accepts_when_shared_present() -> None:
    """A discovered bundle with a shared_key validates normally."""
    secrets = {
        "google_email": "user@example.com",
        "aas_token": "aas_et/DISCOVERY",
        "shared_key": "DDEEFF",
    }
    result = config_flow._normalize_and_validate_discovery_payload(
        {DATA_SECRET_BUNDLE: secrets}
    )
    assert result.email == "user@example.com"
    assert result.secrets_bundle is not None
    assert result.secrets_bundle["shared_key"] == "DDEEFF"


@pytest.mark.asyncio
async def test_secrets_json_step_dead_token_reports_cannot_connect(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A complete bundle whose tokens all fail the probe shows ``cannot_connect``.

    The validation failure is logged with the candidate sources only, without
    the address.
    """
    flow, captured = _make_secrets_flow(monkeypatch)

    async def _no_working_token(*_args: Any, **_kwargs: Any) -> str | None:
        return None

    monkeypatch.setattr(config_flow, "async_pick_working_token", _no_working_token)
    payload = {
        "google_email": "user@example.com",
        "aas_token": "aas_et/FROM_SECRETS",
        "shared_key": _SHARED_HEX,
    }

    with caplog.at_level("WARNING", logger=config_flow.__name__):
        result = await flow.async_step_secrets_json(
            {"secrets_json": json.dumps(payload)}
        )
        if inspect.isawaitable(result):
            result = await result

    assert isinstance(result, dict)
    assert result.get("type") == "form"
    assert result.get("errors") == {"base": "cannot_connect"}
    assert "result" not in captured
    failures = [
        r for r in caplog.records if "Token validation failed" in r.getMessage()
    ]
    assert len(failures) == 1
    assert failures[0].__dict__["candidate_sources"]
    assert "email" not in failures[0].__dict__
