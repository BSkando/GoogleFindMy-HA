# tests/test_config_flow_basics.py
"""Basics coverage for :mod:`custom_components.googlefindmy.config_flow` (Phase 4 AP-L).

Targets pure-function helpers that are reachable without instantiating a
full Home Assistant flow manager: validators, normalizers, payload builders,
error mapping, credential interpreters, discovery helpers and the lightweight
dataclass surface. The 25 helpers tested here are all reachable from the flow
steps, so every test exercises a code path that real onboarding traverses
(Aniche RV-G3: user-setup path, every branch is onboarding critical).

Empiricism trace (CA-MOCK-001 / CA-ASSERTION-EMPIRIE-001):
- All assertions are grounded against the production implementation in
  ``custom_components/googlefindmy/config_flow.py`` (read at commit
  a2edc23d). Line anchors are noted on the test classes.
- No mocks of pure-function behaviour; only thin shims for the
  ``HomeAssistant``/``ConfigEntry`` boundary where the helpers touch HA APIs
  (``_find_entry_by_email``, ``_resolve_entry_email_for_lookup``,
  ``_ensure_optional_entry_attributes``).
"""

from __future__ import annotations

import ast
import json
import logging
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.googlefindmy import config_flow as cf

# ---------------------------------------------------------------------------
# Block A: validators (cf.py lines 944-956)
# ---------------------------------------------------------------------------


class TestEmailValid:
    """Empiricism: ``_email_valid`` returns ``bool(_EMAIL_RE.match(value or ""))``."""

    @pytest.mark.parametrize(
        "value",
        [
            "user@example.com",
            "first.last@sub.example.co.uk",
            "user+tag@example.com",
            "U_S_E_R@example.io",
        ],
    )
    def test_accepts_well_formed_addresses(self, value: str) -> None:
        assert cf._email_valid(value) is True

    @pytest.mark.parametrize(
        "value",
        [
            "",  # empty
            "not-an-email",  # no @
            "@example.com",  # missing local part
            "user@",  # missing domain
            "user@nope",  # missing TLD
            "user@.com",  # empty subdomain label
            "a@b.c",  # TLD too short
            " user@example.com",  # leading whitespace blocks regex
        ],
    )
    def test_rejects_malformed_addresses(self, value: str) -> None:
        assert cf._email_valid(value) is False

    def test_handles_none_via_value_or_empty(self) -> None:
        # The implementation defends against ``None`` via ``value or ""``.
        assert cf._email_valid(None) is False  # type: ignore[arg-type]


class TestTokenPlausible:
    """Empiricism: ``_TOKEN_RE`` requires ``\\S{16,}`` (no whitespace, len>=16)."""

    def test_accepts_long_token_without_whitespace(self) -> None:
        assert cf._token_plausible("a" * 16) is True
        assert cf._token_plausible("aas_et/" + "x" * 64) is True

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "short",  # too short
            "a" * 15,  # one shy of minimum length
            "with whitespace1234567890",  # contains space
            "with\ttab12345678",  # contains tab
        ],
    )
    def test_rejects_short_or_whitespace_tokens(self, value: str) -> None:
        assert cf._token_plausible(value) is False

    def test_handles_none(self) -> None:
        assert cf._token_plausible(None) is False  # type: ignore[arg-type]


class TestLooksLikeJwt:
    """Empiricism: ``count(".") >= 2 and value[:3] == 'eyJ'``."""

    def test_classic_jwt_shape_accepted(self) -> None:
        assert cf._looks_like_jwt("eyJabc.eyJdef.signature") is True

    def test_two_dots_but_wrong_prefix_rejected(self) -> None:
        assert cf._looks_like_jwt("aaa.bbb.ccc") is False

    def test_correct_prefix_but_no_dots_rejected(self) -> None:
        assert cf._looks_like_jwt("eyJabcdefghijklmnop") is False

    def test_empty_string_rejected(self) -> None:
        # Slicing ``""[:3]`` yields ``""`` which is not ``"eyJ"`` — no IndexError.
        assert cf._looks_like_jwt("") is False


# ---------------------------------------------------------------------------
# Block B: normalizers (cf.py lines 964-988)
# ---------------------------------------------------------------------------


class TestNormalizeFeatureList:
    """Empiricism: dedupes, strips, lowercases, sorts; skips non-str."""

    def test_strips_lowercases_dedupes_and_sorts(self) -> None:
        result = cf._normalize_feature_list(
            ["  Maps ", "device_tracker", "MAPS", "Sensor"]
        )
        assert result == ["device_tracker", "maps", "sensor"]

    def test_skips_non_string_entries(self) -> None:
        # Non-str entries are silently dropped (no exception, no insertion).
        result = cf._normalize_feature_list(["sensor", 42, None, "button"])  # type: ignore[list-item]
        assert result == ["button", "sensor"]

    def test_drops_empty_after_strip(self) -> None:
        result = cf._normalize_feature_list(["", "  ", "sensor"])
        assert result == ["sensor"]

    def test_empty_input_returns_empty_list(self) -> None:
        assert cf._normalize_feature_list([]) == []


class TestNormalizeVisibleIds:
    """Empiricism: strips and dedupes (no case-folding!), sorts."""

    def test_strips_and_dedupes(self) -> None:
        result = cf._normalize_visible_ids(["  abc ", "abc", "def"])
        assert result == ["abc", "def"]

    def test_preserves_case(self) -> None:
        # Visible IDs are case-sensitive device identifiers.
        result = cf._normalize_visible_ids(["ABC", "abc"])
        assert result == ["ABC", "abc"]

    def test_skips_non_strings_and_blanks(self) -> None:
        result = cf._normalize_visible_ids(["abc", 0, None, "", " "])  # type: ignore[list-item]
        assert result == ["abc"]

    def test_empty_input(self) -> None:
        assert cf._normalize_visible_ids([]) == []


# ---------------------------------------------------------------------------
# Block C: feature settings and payload builder (cf.py lines 991-1063)
# ---------------------------------------------------------------------------


class TestDeriveFeatureSettings:
    """Empiricism: per-key precedence options_payload > defaults > module DEFAULT."""

    def test_explicit_options_override_defaults(self) -> None:
        opt_key = cf.OPT_GOOGLE_HOME_FILTER_ENABLED
        if opt_key is None:
            pytest.skip("OPT_GOOGLE_HOME_FILTER_ENABLED not available in this build")

        opt_stats = cf.OPT_ENABLE_STATS_ENTITIES
        opt_map = cf.OPT_MAP_VIEW_TOKEN_EXPIRATION

        has_filter, flags = cf._derive_feature_settings(
            options_payload={
                opt_key: True,
                opt_stats: False,
                opt_map: True,
            },
            defaults={opt_stats: True},
        )
        assert has_filter is True
        assert flags[opt_stats] is False  # options wins over defaults
        assert flags[opt_map] is True
        assert flags[opt_key] is True

    def test_defaults_apply_when_options_silent(self) -> None:
        opt_key = cf.OPT_GOOGLE_HOME_FILTER_ENABLED
        if opt_key is None:
            pytest.skip("OPT_GOOGLE_HOME_FILTER_ENABLED not available in this build")
        opt_stats = cf.OPT_ENABLE_STATS_ENTITIES

        has_filter, flags = cf._derive_feature_settings(
            options_payload={},
            defaults={opt_key: True, opt_stats: True},
        )
        assert has_filter is True
        assert flags[opt_stats] is True

    def test_module_default_used_when_options_and_defaults_silent(self) -> None:
        opt_key = cf.OPT_GOOGLE_HOME_FILTER_ENABLED
        if opt_key is None or cf.DEFAULT_GOOGLE_HOME_FILTER_ENABLED is None:
            pytest.skip("Module default not configured in this build")

        has_filter, _flags = cf._derive_feature_settings(
            options_payload={}, defaults={}
        )
        assert has_filter is bool(cf.DEFAULT_GOOGLE_HOME_FILTER_ENABLED)

    def test_contributor_mode_passthrough(self) -> None:
        has_filter, flags = cf._derive_feature_settings(
            options_payload={cf.OPT_CONTRIBUTOR_MODE: "high_traffic"},
            defaults={},
        )
        assert flags[cf.OPT_CONTRIBUTOR_MODE] == "high_traffic"
        # contributor_mode does not affect the filter flag itself.
        assert isinstance(has_filter, bool)


class TestBuildSubentryPayload:
    """Empiricism: lines 1040-1063 — composes group_key/features/flags dict."""

    def test_basic_payload_without_visible_ids(self) -> None:
        payload = cf._build_subentry_payload(
            group_key="trackers",
            features=["device_tracker", "Sensor"],
            entry_title="Acme",
            has_google_home_filter=True,
            feature_flags={"enable_stats": True},
        )
        assert payload["group_key"] == "trackers"
        assert payload["features"] == ["device_tracker", "sensor"]
        assert payload["fcm_push_enabled"] is False
        assert payload["has_google_home_filter"] is True
        assert payload["feature_flags"] == {"enable_stats": True}
        assert payload["entry_title"] == "Acme"
        # Without visible_device_ids, key must NOT be present.
        assert "visible_device_ids" not in payload

    def test_visible_device_ids_included_when_provided(self) -> None:
        payload = cf._build_subentry_payload(
            group_key="trackers",
            features=[],
            entry_title="T",
            has_google_home_filter=False,
            feature_flags={},
            visible_device_ids=["dev_b", "dev_a", "dev_a"],
        )
        assert payload["visible_device_ids"] == ["dev_a", "dev_b"]

    def test_empty_visible_ids_after_normalization_drops_key(self) -> None:
        # All-empty visible_device_ids list becomes [] after normalization → dropped.
        payload = cf._build_subentry_payload(
            group_key="g",
            features=[],
            entry_title="t",
            has_google_home_filter=False,
            feature_flags={},
            visible_device_ids=["", "   "],
        )
        assert "visible_device_ids" not in payload

    def test_feature_flags_is_copied_not_referenced(self) -> None:
        flags = {"a": True}
        payload = cf._build_subentry_payload(
            group_key="g",
            features=[],
            entry_title="t",
            has_google_home_filter=False,
            feature_flags=flags,
        )
        flags["b"] = False  # mutate source after build
        assert payload["feature_flags"] == {"a": True}


# ---------------------------------------------------------------------------
# Block D: token persistence guard + multi-entry guard (cf.py lines 1072-1088)
# ---------------------------------------------------------------------------


class TestDisqualifiesForPersistence:
    """Empiricism: rejects JWT-shaped tokens, accepts everything else."""

    def test_jwt_rejected(self) -> None:
        reason = cf._disqualifies_for_persistence("eyJabc.eyJdef.signature")
        assert isinstance(reason, str)
        assert "JWT" in reason or "installation" in reason

    def test_aas_token_accepted(self) -> None:
        assert cf._disqualifies_for_persistence("aas_et/" + "x" * 64) is None

    def test_generic_token_accepted(self) -> None:
        assert cf._disqualifies_for_persistence("a" * 32) is None


class TestIsMultiEntryGuardError:
    """Empiricism: returns True only for substring match of two guard phrases."""

    def test_recognizes_multi_entry_phrase(self) -> None:
        assert cf._is_multi_entry_guard_error(
            Exception("Multiple config entries active here")
        )

    def test_recognizes_runtime_data_phrase(self) -> None:
        assert cf._is_multi_entry_guard_error(Exception("pass entry.runtime_data"))

    def test_unrelated_error_not_recognized(self) -> None:
        assert cf._is_multi_entry_guard_error(Exception("not found")) is False

    def test_works_with_non_exception_repr(self) -> None:
        # _is_multi_entry_guard_error stringifies with f"{err}".
        class _Custom(Exception):
            def __str__(self) -> str:
                return "Multiple config entries active"

        assert cf._is_multi_entry_guard_error(_Custom()) is True


# ---------------------------------------------------------------------------
# Block E: API exception mapping (cf.py lines 1094-1127)
# ---------------------------------------------------------------------------


class TestMapApiExcToErrorKey:
    """Empiricism: 5-branch dispatcher — DependencyNotReady > name > status > aiohttp > guard > unknown."""

    def test_dependency_not_ready_wins(self) -> None:
        err = cf.DependencyNotReady("deps missing")
        assert cf._map_api_exc_to_error_key(err) == "dependency_not_ready"

    @pytest.mark.parametrize(
        "exc_cls_name",
        ["AuthError", "UnauthorizedAccess", "ForbiddenRequest", "CredentialFailure"],
    )
    def test_auth_keywords_in_name(self, exc_cls_name: str) -> None:
        cls = type(exc_cls_name, (Exception,), {})
        assert cf._map_api_exc_to_error_key(cls("x")) == "invalid_auth"

    def test_status_401_maps_to_invalid_auth(self) -> None:
        err = type("HttpError", (Exception,), {})("nope")
        err.status = 401  # type: ignore[attr-defined]
        assert cf._map_api_exc_to_error_key(err) == "invalid_auth"

    def test_status_code_attribute_403(self) -> None:
        err = type("HttpError", (Exception,), {})("nope")
        err.status_code = 403  # type: ignore[attr-defined]
        assert cf._map_api_exc_to_error_key(err) == "invalid_auth"

    def test_status_as_string_digit(self) -> None:
        err = type("HttpError", (Exception,), {})("nope")
        err.status = "401"  # type: ignore[attr-defined]
        assert cf._map_api_exc_to_error_key(err) == "invalid_auth"

    def test_status_as_bool_true_is_one(self) -> None:
        # Empiricism (lines 1108-1109): bool is checked BEFORE int — True -> 1, not 401.
        err = type("HttpError", (Exception,), {})("nope")
        err.status = True  # type: ignore[attr-defined]
        result = cf._map_api_exc_to_error_key(err)
        # 1 is not in (401, 403) → falls through to network heuristic → name has no
        # timeout/connect → ends in "unknown".
        assert result == "unknown"

    def test_non_digit_status_string_falls_through(self) -> None:
        err = type("HttpError", (Exception,), {})("nope")
        err.status = "Service Unavailable"  # type: ignore[attr-defined]
        # Not digit → status_int stays None → falls through.
        assert cf._map_api_exc_to_error_key(err) == "unknown"

    def test_timeout_in_class_name_maps_to_cannot_connect(self) -> None:
        cls = type("ReadTimeoutError", (Exception,), {})
        assert cf._map_api_exc_to_error_key(cls("x")) == "cannot_connect"

    def test_connection_in_class_name(self) -> None:
        cls = type("ConnectionRefusedError", (Exception,), {})
        assert cf._map_api_exc_to_error_key(cls("x")) == "cannot_connect"

    def test_multi_entry_guard_returns_unknown(self) -> None:
        # Even though the guard is recognized, the mapping is still "unknown".
        assert (
            cf._map_api_exc_to_error_key(Exception("Multiple config entries active"))
            == "unknown"
        )

    def test_plain_runtime_error_unknown(self) -> None:
        assert cf._map_api_exc_to_error_key(RuntimeError("boom")) == "unknown"

    def test_the_two_mapper_rows_the_device_list_narrowing_relies_on(self) -> None:
        """Characterisation of the MAPPER only; it does not pin the narrowing.

        Read the name literally: this asserts the two rows of
        `_map_api_exc_to_error_key` that the device-list change depends on, and
        nothing more. Both rows are true with or without that change, so
        reverting `api.py`'s client-error branch leaves this test green. The end
        to end pin lives in
        `tests/test_api_basics.py::TestAsyncBasicDeviceListErrorMapping::test_a_rejected_probe_reaches_the_config_flow_as_a_non_auth_key`,
        which takes the exception from the narrowed handler itself.

        The device-list handler used to raise ConfigEntryAuthFailed for every
        non-retryable 4xx. That class carries "auth" in its name, so a probe
        rejected with 400/404/422 showed the user ``invalid_auth`` and sent an
        intact sign-in to the re-authentication form. It now raises
        UpdateFailed, which matches neither the name test nor the status test
        above and therefore ends at ``unknown``.

        Both rows are already true today; this test pins the pair so the shift
        is a recorded decision rather than a silent change of a user-facing
        message. Retire it when a dedicated error key for "the server refused
        the request" exists, which needs strings.json and the full translation
        sync of its own.
        """
        from homeassistant.exceptions import ConfigEntryAuthFailed
        from homeassistant.helpers.update_coordinator import UpdateFailed

        assert cf._map_api_exc_to_error_key(UpdateFailed("x")) == "unknown"
        assert (
            cf._map_api_exc_to_error_key(ConfigEntryAuthFailed("x")) == "invalid_auth"
        )


# ---------------------------------------------------------------------------
# Block F: secrets extractors (cf.py lines 1167-1251)
# ---------------------------------------------------------------------------


class TestExtractEmailFromSecrets:
    """Empiricism: flat key priority list, then nested ``account.email`` fallback."""

    def test_prefers_google_home_username(self) -> None:
        data: dict[str, Any] = {
            "googleHomeUsername": "primary@example.com",
            cf.CONF_GOOGLE_EMAIL: "other@example.com",
            "email": "third@example.com",
        }
        assert cf._extract_email_from_secrets(data) == "primary@example.com"

    def test_falls_through_to_conf_google_email(self) -> None:
        data = {cf.CONF_GOOGLE_EMAIL: "via_conf@example.com"}
        assert cf._extract_email_from_secrets(data) == "via_conf@example.com"

    def test_nested_account_email_fallback(self) -> None:
        data: dict[str, Any] = {"account": {"email": "nested@example.com"}}
        assert cf._extract_email_from_secrets(data) == "nested@example.com"

    def test_nested_lookup_swallows_exceptions(self) -> None:
        # account is a string → indexing raises → swallowed → None.
        assert cf._extract_email_from_secrets({"account": "not-a-dict"}) is None

    def test_non_email_values_are_rejected(self) -> None:
        data = {"email": "no_at_sign"}
        assert cf._extract_email_from_secrets(data) is None

    def test_returns_none_when_no_candidates(self) -> None:
        assert cf._extract_email_from_secrets({}) is None


class TestExtractOauthCandidatesFromSecrets:
    """Empiricism: aas_token first, then flat keys, then fcm fallbacks; de-dup."""

    def test_aas_token_listed_first(self) -> None:
        data: dict[str, Any] = {
            cf.CONF_OAUTH_TOKEN: "oauth_token_value_with_padding_xxxx",
            "aas_token": "aas_token_value_with_padding_xxxx",
        }
        cands = cf._extract_oauth_candidates_from_secrets(data)
        labels = [label for label, _token in cands]
        assert labels[0] == "aas_token"
        assert cf.CONF_OAUTH_TOKEN in labels

    def test_deduplicates_repeated_token_value(self) -> None:
        token = "shared_token_value_padding_xxxx"
        data = {
            "aas_token": token,
            "oauth_token": token,
            "access_token": token,
        }
        cands = cf._extract_oauth_candidates_from_secrets(data)
        # All three keys point to the same token → only first survives dedup.
        assert len(cands) == 1
        assert cands[0][0] == "aas_token"

    def test_skips_implausible_tokens(self) -> None:
        data: dict[str, Any] = {
            "aas_token": "short",  # < 16 chars
            "oauth_token": None,  # not a string
            "token": "valid_token_padding_xxxxxxxxxxxx",
        }
        cands = cf._extract_oauth_candidates_from_secrets(data)
        assert [label for label, _token in cands] == ["token"]

    def test_fcm_credentials_used_as_fallback(self) -> None:
        data = {
            "fcm_credentials": {
                "installation": {"token": "fcm_install_token_padding_xxxx"},
                "fcm": {"registration": {"token": "fcm_register_token_padding_xx"}},
            }
        }
        cands = cf._extract_oauth_candidates_from_secrets(data)
        labels = {label for label, _token in cands}
        assert {"fcm_installation", "fcm_registration"} <= labels

    def test_partial_fcm_paths_swallowed(self) -> None:
        # Missing nested key raises KeyError, caught by except.
        data = {"fcm_credentials": {"installation": "not-a-dict"}}
        # Should not raise; just return whatever else is plausible (here: nothing).
        cands = cf._extract_oauth_candidates_from_secrets(data)
        assert cands == []

    def test_empty_payload(self) -> None:
        assert cf._extract_oauth_candidates_from_secrets({}) == []


# ---------------------------------------------------------------------------
# Block G: candidate labels + credential interpreters (cf.py lines 1360-1478)
# ---------------------------------------------------------------------------


class TestCandLabels:
    """Empiricism: lines 1360-1365 — sorted unique source labels or 'none'."""

    def test_returns_none_for_empty_list(self) -> None:
        assert cf._cand_labels([]) == "none"

    def test_returns_none_when_all_labels_blank(self) -> None:
        # Sources falsy → filtered out → empty set → "none".
        assert cf._cand_labels([("", "tok1"), ("", "tok2")]) == "none"

    def test_sorted_unique(self) -> None:
        cands = [
            ("oauth_token", "a" * 16),
            ("aas_token", "b" * 16),
            ("oauth_token", "c" * 16),  # duplicate label
        ]
        assert cf._cand_labels(cands) == "aas_token, oauth_token"


class TestLogTokenValidationFailure:
    """Empiricism: emits a single warning with the candidate sources only."""

    def test_emits_warning_with_candidate_sources(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.WARNING, logger=cf._LOGGER.name)
        cf._log_token_validation_failure(
            candidates=[("aas_token", "x" * 16)],
        )
        assert any(
            "Token validation failed" in record.getMessage()
            for record in caplog.records
        )


class TestInterpretCredentialsChoice:
    """Empiricism: lines 1385-1435 — secrets XOR (token AND email)."""

    def test_mixing_secrets_and_token_returns_choose_one(self) -> None:
        method, email, cands, err = cf._interpret_credentials_choice(
            {"secrets_json": "{}", "oauth_token": "tok", "email": ""},
            secrets_field="secrets_json",
            token_field="oauth_token",
            email_field="email",
        )
        assert (method, email, cands, err) == (None, None, None, "choose_one")

    def test_completely_empty_input_returns_choose_one(self) -> None:
        method, email, cands, err = cf._interpret_credentials_choice(
            {"secrets_json": "", "oauth_token": "", "email": ""},
            secrets_field="secrets_json",
            token_field="oauth_token",
            email_field="email",
        )
        assert err == "choose_one"
        assert method is None and email is None and cands is None

    def test_invalid_json_returns_invalid_json(self) -> None:
        method, email, cands, err = cf._interpret_credentials_choice(
            {"secrets_json": "not json", "oauth_token": "", "email": ""},
            secrets_field="secrets_json",
            token_field="oauth_token",
            email_field="email",
        )
        assert (method, email, cands, err) == ("secrets", None, None, "invalid_json")

    def test_secrets_not_a_dict_returns_invalid_json(self) -> None:
        # JSON arrays parse successfully but fail the dict check.
        method, _email, _cands, err = cf._interpret_credentials_choice(
            {"secrets_json": "[1, 2]", "oauth_token": "", "email": ""},
            secrets_field="secrets_json",
            token_field="oauth_token",
            email_field="email",
        )
        assert (method, err) == ("secrets", "invalid_json")

    def test_secrets_path_happy(self) -> None:
        secrets = {
            "googleHomeUsername": "user@example.com",
            "aas_token": "aas_token_value_padding_xxxxxxx",
            # A shared_key is required to pass the single-key import gate.
            "shared_key": "DDEEFF",
        }
        method, email, cands, err = cf._interpret_credentials_choice(
            {
                "secrets_json": json.dumps(secrets),
                "oauth_token": "",
                "email": "",
            },
            secrets_field="secrets_json",
            token_field="oauth_token",
            email_field="email",
        )
        assert method == "secrets"
        assert email == "user@example.com"
        assert cands and cands[0][0] == "aas_token"
        assert err is None

    def test_secrets_path_invalid_token(self) -> None:
        # Email valid but no plausible tokens → invalid_token. A shared_key is
        # present so the single-key gate passes and the token check is reached.
        secrets = {
            "googleHomeUsername": "user@example.com",
            "aas_token": "short",
            "shared_key": "DDEEFF",
        }
        method, _email, _cands, err = cf._interpret_credentials_choice(
            {
                "secrets_json": json.dumps(secrets),
                "oauth_token": "",
                "email": "",
            },
            secrets_field="secrets_json",
            token_field="oauth_token",
            email_field="email",
        )
        assert (method, err) == ("secrets", "invalid_token")

    def test_manual_path_happy(self) -> None:
        method, email, cands, err = cf._interpret_credentials_choice(
            {
                "secrets_json": "",
                "oauth_token": "a" * 32,
                "email": "user@example.com",
            },
            secrets_field="secrets_json",
            token_field="oauth_token",
            email_field="email",
        )
        assert method == "manual"
        assert email == "user@example.com"
        assert cands == [("manual", "a" * 32)]
        assert err is None

    def test_manual_path_rejects_jwt(self) -> None:
        method, _email, _cands, err = cf._interpret_credentials_choice(
            {
                "secrets_json": "",
                "oauth_token": "eyJabc.eyJdef.signature_padding_xxxx",
                "email": "user@example.com",
            },
            secrets_field="secrets_json",
            token_field="oauth_token",
            email_field="email",
        )
        # JWT-shaped tokens cannot be persisted.
        assert (method, err) == ("manual", "invalid_token")

    def test_manual_path_invalid_email(self) -> None:
        method, _email, _cands, err = cf._interpret_credentials_choice(
            {
                "secrets_json": "",
                "oauth_token": "a" * 32,
                "email": "not-an-email",
            },
            secrets_field="secrets_json",
            token_field="oauth_token",
            email_field="email",
        )
        assert (method, err) == ("manual", "invalid_token")


class TestInterpretReauthChoice:
    """Empiricism: lines 1445-1478 — secrets XOR (currently disabled) token."""

    def test_both_paths_active_returns_choose_one(self) -> None:
        method, data, err = cf._interpret_reauth_choice(
            {"secrets_json": "{}", "new_oauth_token": "tok"}
        )
        assert (method, data, err) == (None, None, "choose_one")

    def test_neither_path_returns_choose_one(self) -> None:
        method, data, err = cf._interpret_reauth_choice(
            {"secrets_json": "", "new_oauth_token": ""}
        )
        assert (method, data, err) == (None, None, "choose_one")

    def test_secrets_path_happy(self) -> None:
        payload = {
            "googleHomeUsername": "user@example.com",
            "aas_token": "aas_token_value_padding_xxxxxxx",
        }
        method, data, err = cf._interpret_reauth_choice(
            {"secrets_json": json.dumps(payload), "new_oauth_token": ""}
        )
        assert method == "secrets"
        assert (
            isinstance(data, dict) and data["googleHomeUsername"] == "user@example.com"
        )
        assert err is None

    def test_secrets_path_invalid_json(self) -> None:
        method, data, err = cf._interpret_reauth_choice(
            {"secrets_json": "garbage", "new_oauth_token": ""}
        )
        assert (method, data, err) == (None, None, "invalid_json")

    def test_secrets_path_array_not_dict(self) -> None:
        method, data, err = cf._interpret_reauth_choice(
            {"secrets_json": "[]", "new_oauth_token": ""}
        )
        assert (method, data, err) == (None, None, "invalid_json")

    def test_secrets_invalid_token_returns_invalid_token(self) -> None:
        # Email present but no plausible candidate token.
        payload = {"googleHomeUsername": "user@example.com"}
        method, data, err = cf._interpret_reauth_choice(
            {"secrets_json": json.dumps(payload), "new_oauth_token": ""}
        )
        assert (method, data, err) == (None, None, "invalid_token")

    def test_token_only_path_falls_through_to_choose_one(self) -> None:
        # The manual reauth token path was removed, not merely disabled (see
        # agents/config_flow/AGENTS.md and tests/test_manual_reauth_removal_guard.py).
        # The field is still read, but for this shape the read changes nothing:
        # without it the same submission would end at the exclusivity check with
        # the same "choose_one". The read is load-bearing only when both halves
        # arrive at once, where it stops the bundle half from winning. Reviving
        # the path needs an amendment to that contract, not a re-enable.
        method, data, err = cf._interpret_reauth_choice(
            {"secrets_json": "", "new_oauth_token": "a" * 32}
        )
        assert (method, data, err) == (None, None, "choose_one")


# ---------------------------------------------------------------------------
# Block H: entry resolver, masking, callbacks (cf.py lines 1481-1606, 745-774)
# ---------------------------------------------------------------------------


class TestMaskEmailForLogs:
    """Empiricism: privacy-preserving mask for logs."""

    def test_typical_email_masked(self) -> None:
        assert cf._mask_email_for_logs("user@example.com") == "u***@example.com"

    def test_single_char_local_part(self) -> None:
        assert cf._mask_email_for_logs("a@example.com") == "*@example.com"

    def test_missing_local_returns_star_at_domain(self) -> None:
        # "@example.com" → local="" → returns "*@example.com" (line 771).
        assert cf._mask_email_for_logs("@example.com") == "*@example.com"

    def test_none_returns_unknown(self) -> None:
        assert cf._mask_email_for_logs(None) == "<unknown>"

    def test_empty_returns_unknown(self) -> None:
        assert cf._mask_email_for_logs("") == "<unknown>"

    def test_no_at_returns_unknown(self) -> None:
        assert cf._mask_email_for_logs("no-at-sign") == "<unknown>"


class TestTypedCallback:
    """Empiricism: lines 745-748 — preserves the wrapped callable via HA callback."""

    def test_returns_decorated_callable(self) -> None:
        def fn(x: int) -> int:
            return x * 2

        wrapped = cf._typed_callback(fn)
        assert callable(wrapped)
        # The decorator from HA returns the same function instance (it just tags it),
        # so behaviour must be preserved.
        assert wrapped(3) == 6


class TestIsDiscoveryUpdateInfo:
    """Empiricism: lines 751-760 — recognizes two source identifiers."""

    def test_recognizes_primary_source(self) -> None:
        assert cf._is_discovery_update_info({"source": cf.DISCOVERY_UPDATE_SOURCE})

    def test_recognizes_legacy_source(self) -> None:
        assert cf._is_discovery_update_info(
            {"source": cf.LEGACY_DISCOVERY_UPDATE_SOURCE}
        )

    def test_rejects_unrelated_source(self) -> None:
        assert cf._is_discovery_update_info({"source": "user"}) is False

    def test_rejects_none(self) -> None:
        assert cf._is_discovery_update_info(None) is False

    def test_rejects_non_mapping(self) -> None:
        assert cf._is_discovery_update_info("not-a-mapping") is False  # type: ignore[arg-type]


class TestRegisterDependencyError:
    """Empiricism: lines 653-663 — single-shot record for "base" field."""

    def test_records_import_failed_once(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.ERROR, logger=cf._LOGGER.name)
        errors: dict[str, str] = {}
        cf._register_dependency_error(errors, ImportError("boom"))
        assert errors == {"base": "import_failed"}
        # Second call with same field is a no-op.
        cf._register_dependency_error(errors, ImportError("again"))
        assert errors == {"base": "import_failed"}

    def test_uses_custom_field(self) -> None:
        errors: dict[str, str] = {}
        cf._register_dependency_error(errors, ImportError("x"), field="oauth")
        assert errors == {"oauth": "import_failed"}


class TestEnsureOptionalEntryAttributes:
    """Empiricism: lines 1586-1605 — fills missing attrs, defends against slot stubs."""

    def test_none_entry_is_noop(self) -> None:
        # No raise, no side effects.
        cf._ensure_optional_entry_attributes(None)  # type: ignore[arg-type]

    def test_missing_source_is_populated(self) -> None:
        entry = SimpleNamespace()
        cf._ensure_optional_entry_attributes(entry)  # type: ignore[arg-type]
        assert hasattr(entry, "source")
        assert entry.source is None  # type: ignore[attr-defined]

    def test_existing_source_is_preserved(self) -> None:
        entry = SimpleNamespace(source="user")
        cf._ensure_optional_entry_attributes(entry)  # type: ignore[arg-type]
        assert entry.source == "user"

    def test_slots_object_does_not_raise(self) -> None:
        # A class with __slots__ that does NOT include "source" must not crash
        # the helper; the defensive try/except swallows the AttributeError.
        class Slotted:
            __slots__ = ("entry_id",)

            def __init__(self) -> None:
                self.entry_id = "e1"

        cf._ensure_optional_entry_attributes(Slotted())  # type: ignore[arg-type]


class TestResolveEntryEmailForLookup:
    """Empiricism: lines 1481-1528 — caches resolver, defensively guards exceptions."""

    @pytest.fixture(autouse=True)
    def _reset_resolver_cache(self) -> Any:
        """Reset the module-level lazy cache between tests."""
        original_resolve = cf._RESOLVE_ENTRY_EMAIL
        original_coalesce = cf._COALESCE_ENTRIES
        cf._RESOLVE_ENTRY_EMAIL = None
        cf._COALESCE_ENTRIES = None
        yield
        cf._RESOLVE_ENTRY_EMAIL = original_resolve
        cf._COALESCE_ENTRIES = original_coalesce

    def test_uses_data_then_options_for_email(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Force the fallback resolver by making the integration import return a
        # module without _resolve_entry_email.
        fake_pkg = SimpleNamespace()
        monkeypatch.setattr(
            "custom_components.googlefindmy.config_flow.import_integration_package",
            lambda: fake_pkg,
            raising=True,
        )

        entry = SimpleNamespace(
            data={cf.CONF_GOOGLE_EMAIL: "  Found@Example.com  "},
            options={},
            entry_id="entry-1",
        )
        raw, normalized = cf._resolve_entry_email_for_lookup(entry)  # type: ignore[arg-type]
        assert raw == "Found@Example.com"
        assert normalized == "found@example.com"

    def test_options_used_when_data_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_pkg = SimpleNamespace()
        monkeypatch.setattr(
            "custom_components.googlefindmy.config_flow.import_integration_package",
            lambda: fake_pkg,
            raising=True,
        )

        entry = SimpleNamespace(
            data={},
            options={cf.CONF_GOOGLE_EMAIL: "opts@example.com"},
            entry_id="entry-2",
        )
        raw, normalized = cf._resolve_entry_email_for_lookup(entry)  # type: ignore[arg-type]
        assert raw == "opts@example.com"
        assert normalized == "opts@example.com"

    def test_no_email_returns_none_pair(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_pkg = SimpleNamespace()
        monkeypatch.setattr(
            "custom_components.googlefindmy.config_flow.import_integration_package",
            lambda: fake_pkg,
            raising=True,
        )
        entry = SimpleNamespace(data={}, options={}, entry_id="entry-3")
        raw, normalized = cf._resolve_entry_email_for_lookup(entry)  # type: ignore[arg-type]
        assert raw is None and normalized is None

    def test_custom_resolver_from_integration_is_used(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        called: list[Any] = []

        def custom_resolver(entry: Any) -> tuple[str | None, str | None]:
            called.append(entry)
            return ("Raw@X.com", "raw@x.com")

        fake_pkg = SimpleNamespace(_resolve_entry_email=custom_resolver)
        monkeypatch.setattr(
            "custom_components.googlefindmy.config_flow.import_integration_package",
            lambda: fake_pkg,
            raising=True,
        )
        entry = SimpleNamespace(entry_id="e")
        raw, normalized = cf._resolve_entry_email_for_lookup(entry)  # type: ignore[arg-type]
        assert (raw, normalized) == ("Raw@X.com", "raw@x.com")
        assert called and called[0] is entry


class TestFindEntryByEmail:
    """Empiricism: lines 1531-1542 — match against normalized email."""

    @pytest.fixture(autouse=True)
    def _reset_resolver_cache(self) -> Any:
        original = cf._RESOLVE_ENTRY_EMAIL
        cf._RESOLVE_ENTRY_EMAIL = None
        yield
        cf._RESOLVE_ENTRY_EMAIL = original

    def _make_hass_with_entries(self, entries: list[Any]) -> Any:
        async_entries = lambda _domain: entries  # noqa: E731
        return SimpleNamespace(
            config_entries=SimpleNamespace(async_entries=async_entries)
        )

    def test_empty_target_returns_none(self) -> None:
        hass = self._make_hass_with_entries([])
        assert cf._find_entry_by_email(hass, "") is None

    def test_matches_normalized_email(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_pkg = SimpleNamespace()
        monkeypatch.setattr(
            "custom_components.googlefindmy.config_flow.import_integration_package",
            lambda: fake_pkg,
            raising=True,
        )
        e1 = SimpleNamespace(
            data={cf.CONF_GOOGLE_EMAIL: "Match@Example.com"},
            options={},
            entry_id="e1",
        )
        e2 = SimpleNamespace(
            data={cf.CONF_GOOGLE_EMAIL: "other@example.com"},
            options={},
            entry_id="e2",
        )
        hass = self._make_hass_with_entries([e2, e1])
        result = cf._find_entry_by_email(hass, "MATCH@example.com")
        assert result is e1

    def test_no_match_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_pkg = SimpleNamespace()
        monkeypatch.setattr(
            "custom_components.googlefindmy.config_flow.import_integration_package",
            lambda: fake_pkg,
            raising=True,
        )
        e1 = SimpleNamespace(
            data={cf.CONF_GOOGLE_EMAIL: "x@example.com"},
            options={},
            entry_id="e1",
        )
        hass = self._make_hass_with_entries([e1])
        assert cf._find_entry_by_email(hass, "y@example.com") is None


# ---------------------------------------------------------------------------
# Block I: discovery helpers (cf.py lines 1622-1740)
# ---------------------------------------------------------------------------


class TestSubentryOptionProperty:
    """Empiricism: lines 901-916 — subentry_id falls through to None when missing."""

    def test_subentry_id_returns_attribute_when_present(self) -> None:
        sub = SimpleNamespace(subentry_id="abc")
        opt = cf._SubentryOption(
            key="k",
            label="L",
            subentry=sub,
            visible_device_ids=(),  # type: ignore[arg-type]
        )
        assert opt.subentry_id == "abc"

    def test_subentry_id_none_when_subentry_missing(self) -> None:
        opt = cf._SubentryOption(
            key="k", label="L", subentry=None, visible_device_ids=()
        )
        assert opt.subentry_id is None

    def test_subentry_id_none_when_attribute_absent(self) -> None:
        # A subentry object without subentry_id attribute → getattr returns None.
        opt = cf._SubentryOption(
            key="k",
            label="L",
            subentry=SimpleNamespace(),
            visible_device_ids=(),  # type: ignore[arg-type]
        )
        assert opt.subentry_id is None


class TestDiscoveryFlowError:
    """Empiricism: lines 1613-1618 — exposes ``reason`` attribute."""

    def test_reason_is_captured(self) -> None:
        err = cf.DiscoveryFlowError("invalid_discovery_info")
        assert err.reason == "invalid_discovery_info"
        assert "invalid_discovery_info" in str(err)


class TestNormalizeAndValidateDiscoveryPayload:
    """Empiricism: lines 1649-1740 — multi-branch normalizer for discovery info."""

    def test_non_mapping_raises_invalid_discovery_info(self) -> None:
        with pytest.raises(cf.DiscoveryFlowError) as exc_info:
            cf._normalize_and_validate_discovery_payload(None)
        assert exc_info.value.reason == "invalid_discovery_info"

    def test_missing_email_raises(self) -> None:
        with pytest.raises(cf.DiscoveryFlowError) as exc_info:
            cf._normalize_and_validate_discovery_payload({"candidate_tokens": "tok"})
        assert exc_info.value.reason == "invalid_discovery_info"

    def test_invalid_secrets_json_raises(self) -> None:
        with pytest.raises(cf.DiscoveryFlowError) as exc_info:
            cf._normalize_and_validate_discovery_payload(
                {cf.CONF_GOOGLE_EMAIL: "user@example.com", "secrets_json": "{bad"}
            )
        assert exc_info.value.reason == "invalid_discovery_info"

    def test_email_only_without_candidates_raises_cannot_connect(self) -> None:
        with pytest.raises(cf.DiscoveryFlowError) as exc_info:
            cf._normalize_and_validate_discovery_payload(
                {cf.CONF_GOOGLE_EMAIL: "user@example.com"}
            )
        # Valid email but no token candidates → cannot_connect.
        assert exc_info.value.reason == "cannot_connect"

    def test_secrets_provide_email_and_candidates(self) -> None:
        secrets = {
            "googleHomeUsername": "user@example.com",
            "aas_token": "aas_token_value_padding_xxxxxxx",
            # A shared_key is required to pass the single-key discovery gate.
            "shared_key": "DDEEFF",
        }
        payload = {cf.DATA_SECRET_BUNDLE: secrets}
        result = cf._normalize_and_validate_discovery_payload(payload)
        assert result.email == "user@example.com"
        assert result.unique_id.startswith("acct:")
        labels = {label for label, _token in result.candidates}
        assert "aas_token" in labels
        assert result.secrets_bundle is not None

    def test_secrets_json_string_is_parsed(self) -> None:
        secrets = {
            "googleHomeUsername": "user@example.com",
            "aas_token": "aas_token_value_padding_xxxxxxx",
            # A shared_key is required to pass the single-key discovery gate.
            "shared_key": "DDEEFF",
        }
        payload = {
            cf.CONF_GOOGLE_EMAIL: "user@example.com",
            "secrets_json": json.dumps(secrets),
        }
        result = cf._normalize_and_validate_discovery_payload(payload)
        assert result.email == "user@example.com"

    def test_candidate_tokens_string_form(self) -> None:
        payload = {
            cf.CONF_GOOGLE_EMAIL: "user@example.com",
            "candidate_tokens": "a" * 32,
        }
        result = cf._normalize_and_validate_discovery_payload(payload)
        labels = {label for label, _token in result.candidates}
        assert "candidate_tokens" in labels

    def test_candidate_tokens_mapping_form(self) -> None:
        payload = {
            cf.CONF_GOOGLE_EMAIL: "user@example.com",
            "candidates": {"primary": "a" * 32, "secondary": "b" * 32},
        }
        result = cf._normalize_and_validate_discovery_payload(payload)
        labels = {label for label, _token in result.candidates}
        assert {"primary", "secondary"} <= labels

    def test_candidate_tokens_iterable_of_mappings(self) -> None:
        payload = {
            cf.CONF_GOOGLE_EMAIL: "user@example.com",
            "tokens": [
                {"label": "first", "token": "a" * 32},
                {"source": "second", "token": "b" * 32},
                {"token": "c" * 32},  # neither label nor source → falls back to key
            ],
        }
        result = cf._normalize_and_validate_discovery_payload(payload)
        labels = {label for label, _token in result.candidates}
        assert {"first", "second", "tokens"} <= labels

    def test_candidate_tokens_iterable_of_strings(self) -> None:
        payload = {
            cf.CONF_GOOGLE_EMAIL: "user@example.com",
            "tokens": ["a" * 32, "b" * 32],
        }
        result = cf._normalize_and_validate_discovery_payload(payload)
        labels = {label for label, _token in result.candidates}
        # Strings in an iterable produce labels of the form "{key}_{idx}".
        assert any(label.startswith("tokens_") for label in labels)

    def test_direct_oauth_token_keys(self) -> None:
        payload = {
            cf.CONF_GOOGLE_EMAIL: "user@example.com",
            cf.CONF_OAUTH_TOKEN: "a" * 32,
        }
        result = cf._normalize_and_validate_discovery_payload(payload)
        assert any(label == cf.CONF_OAUTH_TOKEN for label, _token in result.candidates)

    def test_title_propagated_when_string(self) -> None:
        payload = {
            cf.CONF_GOOGLE_EMAIL: "user@example.com",
            cf.CONF_OAUTH_TOKEN: "a" * 32,
            "title": "My Account",
        }
        result = cf._normalize_and_validate_discovery_payload(payload)
        assert result.title == "My Account"

    def test_non_string_title_becomes_none(self) -> None:
        payload = {
            cf.CONF_GOOGLE_EMAIL: "user@example.com",
            cf.CONF_OAUTH_TOKEN: "a" * 32,
            "title": 123,
        }
        result = cf._normalize_and_validate_discovery_payload(payload)
        assert result.title is None

    def test_invalid_email_raises_invalid_discovery_info(self) -> None:
        # candidate_tokens valid, email malformed.
        payload = {
            cf.CONF_GOOGLE_EMAIL: "not-an-email",
            cf.CONF_OAUTH_TOKEN: "a" * 32,
        }
        with pytest.raises(cf.DiscoveryFlowError) as exc_info:
            cf._normalize_and_validate_discovery_payload(payload)
        assert exc_info.value.reason == "invalid_discovery_info"


# ---------------------------------------------------------------------------
# Block J: module-level constants (cheap, smoke-tests imports)
# ---------------------------------------------------------------------------


class TestModuleConstants:
    """Sanity-check module-level constants to lock the surface."""

    def test_email_regex_is_compiled(self) -> None:
        assert isinstance(cf._EMAIL_RE, re.Pattern)

    def test_token_regex_is_compiled(self) -> None:
        assert isinstance(cf._TOKEN_RE, re.Pattern)

    def test_auth_method_constants_present(self) -> None:
        assert cf._AUTH_METHOD_SECRETS == "secrets_json"
        assert cf._AUTH_METHOD_INDIVIDUAL == "individual_tokens"

    def test_discovery_update_sources_distinct(self) -> None:
        assert cf.DISCOVERY_UPDATE_SOURCE != cf.LEGACY_DISCOVERY_UPDATE_SOURCE

    def test_default_subentry_titles_cover_known_keys(self) -> None:
        assert cf.TRACKER_SUBENTRY_KEY in cf._DEFAULT_SUBENTRY_TITLES
        assert cf.SERVICE_SUBENTRY_KEY in cf._DEFAULT_SUBENTRY_TITLES

    def test_step_user_schema_validates_secrets_method(self) -> None:
        # Smoke-test the schema accepts the secrets_json auth method.
        validated = cf.STEP_USER_DATA_SCHEMA({"auth_method": "secrets_json"})
        assert validated == {"auth_method": "secrets_json"}

    def test_step_secrets_schema_passes_through_string(self) -> None:
        validated = cf.STEP_SECRETS_DATA_SCHEMA({"secrets_json": "{}"})
        assert validated == {"secrets_json": "{}"}


class TestLoggingNeverLeaksAccountAddresses:
    """No log call in the account-facing modules may pass a raw address.

    `AGENTS.md` section 5 forbids logging email addresses outright. Written as
    an AST walk rather than as a pin on the sites that were found, because the
    leak reappears with every new log line, in a shape nobody predicted: the
    two sites fixed here were a bare name, but an f-string, an attribute, a
    subscript or a helpfully wrapping `str()` leak exactly as much. A value is
    considered handled only when a *masking* helper wraps it; any other call
    around it is treated as still leaking.

    Scope is the modules this feature owns, plus the owner-key lookup in
    `SpotApi/GetEidInfoForE2eeDevices/get_owner_key.py`, which logs the account
    on every key retrieval. Older subsystems (`NovaApi`, the rest of `SpotApi`,
    `Auth`) carry the same class of leak and are deliberately NOT silently
    included here: widening the rule without fixing them would only produce a
    red suite, and fixing them belongs in its own change.
    """

    _MODULES = (
        "config_flow.py",
        "__init__.py",
        "discovery.py",
        "SpotApi/GetEidInfoForE2eeDevices/get_owner_key.py",
    )
    _EMAIL_NAMES = frozenset(
        {
            "account_email",
            "email",
            "email_key",
            "extracted_email",
            "fixed_email",
            "google_email",
            "normalised_email",
            "normalized_email",
            "raw_email",
            "user_email",
            "username",
        }
    )
    _MASKERS = frozenset({"_mask_email_for_logs", "_redact_account_for_log"})

    @classmethod
    def _leaks(cls, node: ast.AST) -> list[str]:
        """Names of address-carrying leaves reachable without a masker."""

        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else getattr(func, "id", "")
            )
            if name in cls._MASKERS:
                return []

        found: list[str] = []
        for child in ast.iter_child_nodes(node):
            found.extend(cls._leaks(child))
        if isinstance(node, ast.Name) and node.id in cls._EMAIL_NAMES:
            found.append(node.id)
        elif isinstance(node, ast.Attribute) and node.attr in cls._EMAIL_NAMES:
            found.append(node.attr)
        elif (
            isinstance(node, ast.Subscript)
            and isinstance(node.slice, ast.Constant)
            and node.slice.value in cls._EMAIL_NAMES
        ):
            found.append(str(node.slice.value))
        return found

    def test_no_log_call_passes_an_unmasked_address(self) -> None:
        component = Path(cf.__file__).resolve().parent

        offenders: list[str] = []
        masked_calls = 0
        for module in self._MODULES:
            path = component / module
            assert path.is_file(), f"{module} not found; the guard would be vacuous"
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                receiver = (
                    func.value.id
                    if isinstance(func, ast.Attribute)
                    and isinstance(func.value, ast.Name)
                    else ""
                )
                if "LOG" not in receiver.upper():
                    continue
                arguments: list[ast.AST] = [
                    *node.args,
                    *(kw.value for kw in node.keywords),
                ]
                if any(
                    isinstance(a, ast.Call)
                    and getattr(a.func, "id", getattr(a.func, "attr", ""))
                    in self._MASKERS
                    for a in arguments
                ):
                    masked_calls += 1
                for argument in arguments:
                    for leaf in self._leaks(argument):
                        offenders.append(f"{module}:{argument.lineno}: {leaf}")

        # Vacuum control: a renamed logger or a moved module would make the walk
        # find nothing and report success. The masked calls prove it arrived.
        assert masked_calls, "the walk reached no masked log call; the guard is vacuous"
        assert not offenders, (
            "log calls interpolate an unmasked address; wrap it in one of "
            f"{sorted(self._MASKERS)}:\n" + "\n".join(sorted(set(offenders)))
        )
