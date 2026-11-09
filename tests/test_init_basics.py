# tests/test_init_basics.py
"""Basics coverage for :mod:`custom_components.googlefindmy.__init__` (Phase 4 AP-K).

Leverage rationale (CA-COV-HEBEL-001): ``__init__.py`` is the largest module in the
codebase (~4266 statements, 63 % baseline). Sixteen pure-function helpers are
currently completely untested. They split into five families, all of which are
risk-prioritized per Aniche RV-G3 because they execute on every config-entry
setup/unload cycle:

1. Entity-deduplication scoring (``_compute_entity_score`` /
   ``_pick_canonical_entity_entry``): silent canonical-entity drift would hide
   duplicate trackers from the user.
2. Config-entry health scoring (``_entry_schema_score`` /
   ``_entry_creation_timestamp``): drives the coalesce algorithm that decides
   which legacy entry survives a merge.
3. Platform/subentry identifier helpers (``_feature_name_from_platform`` /
   ``_subentry_entry_id`` / ``_default_button_subentry_identifier``): mapping
   bugs would route entities to the wrong subentry and corrupt the device
   registry.
4. Domain-data accessors (``_get_fcm_refcount`` / ``_get_nova_refcount`` and
   their setters / receivers map / ``_sync_receiver_default_entry``): wrong
   refcount math leaks FCM connections; this is exactly the bug class behind
   the open memory-leak report on HA Core 2026.6.x.
5. Button unique-id parsing and tracker identifier candidates
   (``_normalize_legacy_button_remainder`` / ``_parse_button_unique_id`` /
   ``_iter_tracker_identifier_candidates`` / ``_normalize_device_identifier``):
   these power the button-relink heuristics across schema migrations.

Pre-Push-Sweep (testing-strategien.md § 18.1) and CA-MOCK-001 + CA-ASSERTION-
EMPIRIE-001 enforce that every assertion is grounded against the production
source ranges (``__init__.py`` lines 564-784, 1053-1092, 2296-2316, 2882-3142,
3230-3258, 3537-3704). No ``asyncio.run`` is used (CA-ASYNCIO-RUN-001) — all
async helpers covered here are deliberately exercised through their synchronous
contract or via ``pytest.mark.asyncio``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN

from custom_components.googlefindmy import (
    _DEFAULT_SUBENTRY_IDENTIFIER,
    ConfigEntrySubentryDefinition,
    RuntimeData,
    _ButtonUniqueIdParts,
    _compute_entity_score,
    _default_button_subentry_identifier,
    _entry_creation_timestamp,
    _entry_schema_score,
    _feature_name_from_platform,
    _get_fcm_refcount,
    _get_fcm_refcounts,
    _get_nova_refcount,
    _get_retry_attempts,
    _get_retry_handles,
    _iter_tracker_identifier_candidates,
    _normalize_device_identifier,
    _normalize_legacy_button_remainder,
    _parse_button_unique_id,
    _pick_canonical_entity_entry,
    _runtime_data,
    _set_fcm_refcount,
    _set_nova_refcount,
    _subentry_entry_id,
    _sync_receiver_default_entry,
)
from custom_components.googlefindmy.const import (
    CONF_OAUTH_TOKEN,
    DATA_AAS_TOKEN,
    DATA_AUTH_METHOD,
    DATA_SECRET_BUNDLE,
    DOMAIN,
    SUBENTRY_TYPE_TRACKER,
)

# ---------------------------------------------------------------------------
# Lightweight fakes (no Home Assistant fixtures needed for pure helpers)
# ---------------------------------------------------------------------------


class _FakeStates:
    """Minimal ``hass.states`` substitute returning a configurable state."""

    def __init__(self, mapping: dict[str, Any] | None = None) -> None:
        self._mapping = mapping or {}

    def get(self, entity_id: str) -> Any:
        return self._mapping.get(entity_id)


def _make_hass(states: dict[str, Any] | None = None) -> SimpleNamespace:
    """Return a lightweight ``hass`` double exposing only ``.states.get``."""

    return SimpleNamespace(states=_FakeStates(states or {}))


def _make_entity(
    entity_id: str,
    *,
    translation_key: str | None = None,
    disabled_by: Any = None,
) -> SimpleNamespace:
    """Return a registry-entry double covering the score-relevant attributes."""

    return SimpleNamespace(
        entity_id=entity_id,
        translation_key=translation_key,
        disabled_by=disabled_by,
    )


def _make_state(state: str = "on") -> SimpleNamespace:
    """Return a state object whose ``.state`` attribute is the supplied string."""

    return SimpleNamespace(state=state)


def _make_entry(
    *,
    data: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    entry_id: str = "entry-1",
) -> SimpleNamespace:
    """Return a config-entry double exposing the attributes touched by helpers."""

    return SimpleNamespace(
        entry_id=entry_id,
        data=data or {},
        options=options or {},
        created_at=created_at,
        updated_at=updated_at,
    )


# ---------------------------------------------------------------------------
# Block A — Entity duplicate scoring (Lines 669-705)
# ---------------------------------------------------------------------------


class TestComputeEntityScoreBasics:
    """``_compute_entity_score`` weights translation_key (4) + active (3) + state (3)."""

    @pytest.mark.parametrize(
        ("translation_key", "disabled_by", "state_value", "expected"),
        [
            (None, "user", None, 0),  # nothing
            ("my_key", "user", None, 4),  # translation_key only
            (None, None, None, 3),  # enabled only
            (None, "user", "on", 3),  # active state only
            ("my_key", None, "on", 10),  # full house
            (
                "my_key",
                None,
                STATE_UNAVAILABLE,
                7,
            ),  # state unavailable strips +3
            (
                "my_key",
                None,
                STATE_UNKNOWN,
                7,
            ),  # state unknown strips +3
            ("my_key", "user", "off", 7),  # disabled strips +3
        ],
    )
    def test_score_components_combine_additively(
        self,
        translation_key: str | None,
        disabled_by: Any,
        state_value: str | None,
        expected: int,
    ) -> None:
        entity = _make_entity(
            "sensor.foo",
            translation_key=translation_key,
            disabled_by=disabled_by,
        )
        states = {"sensor.foo": _make_state(state_value)} if state_value else {}
        hass = _make_hass(states)

        assert _compute_entity_score(hass, entity) == expected

    def test_missing_state_object_does_not_credit_state_points(self) -> None:
        """A state lookup miss must not award the +3 active-state points."""

        entity = _make_entity(
            "sensor.never_seen", translation_key="k", disabled_by=None
        )
        hass = _make_hass({})  # no state recorded

        # translation_key (+4) + enabled (+3) + no state ⇒ 7
        assert _compute_entity_score(hass, entity) == 7


class TestPickCanonicalEntityEntryBasics:
    """``_pick_canonical_entity_entry`` keeps the highest-scoring entry; ties stay first."""

    def test_returns_first_when_all_scores_tie(self) -> None:
        first = _make_entity("sensor.a")
        second = _make_entity("sensor.b")
        hass = _make_hass({})

        assert _pick_canonical_entity_entry(hass, [first, second]) is first

    def test_prefers_higher_score(self) -> None:
        weaker = _make_entity("sensor.weak", disabled_by="user")
        stronger = _make_entity("sensor.strong", translation_key="k", disabled_by=None)
        hass = _make_hass({"sensor.strong": _make_state("on")})

        canonical = _pick_canonical_entity_entry(hass, [weaker, stronger])

        assert canonical is stronger

    def test_single_entry_returns_itself(self) -> None:
        only = _make_entity("sensor.only", translation_key="k")
        hass = _make_hass({})

        assert _pick_canonical_entity_entry(hass, [only]) is only


# ---------------------------------------------------------------------------
# Block B — Config-entry health scoring (Lines 751-784)
# ---------------------------------------------------------------------------


class TestEntrySchemaScoreBasics:
    """``_entry_schema_score`` sums bundle, auth, oauth, aas, and option counts."""

    def test_empty_entry_scores_zero(self) -> None:
        entry = _make_entry(data={}, options={})

        assert _entry_schema_score(entry) == 0

    def test_mapping_bundle_scores_five(self) -> None:
        entry = _make_entry(
            data={DATA_SECRET_BUNDLE: {"secrets_data": {"x": 1}}},
            options={},
        )

        # bundle is Mapping ⇒ +5; no auth/oauth/aas ⇒ total 5
        assert _entry_schema_score(entry) == 5

    def test_non_mapping_bundle_scores_two(self) -> None:
        entry = _make_entry(
            data={DATA_SECRET_BUNDLE: "raw-string-bundle"},
            options={},
        )

        # bundle not Mapping but truthy ⇒ +2
        assert _entry_schema_score(entry) == 2

    def test_auth_oauth_aas_all_add(self) -> None:
        entry = _make_entry(
            data={
                DATA_AUTH_METHOD: "secrets_json",
                CONF_OAUTH_TOKEN: "oauth-x",
                DATA_AAS_TOKEN: "aas-x",
            },
            options={},
        )

        # +2 (auth) +1 (oauth) +1 (aas) ⇒ 4
        assert _entry_schema_score(entry) == 4

    def test_options_length_is_added_separately(self) -> None:
        entry = _make_entry(
            data={},
            options={"opt_a": 1, "opt_b": 2, "opt_c": 3},
        )

        # options-Mapping container scans through all 5 checks (zero), then
        # len(entry.options) ⇒ 3 is added at the end
        assert _entry_schema_score(entry) == 3

    def test_non_mapping_data_is_skipped(self) -> None:
        """Non-Mapping ``entry.data`` must short-circuit without errors."""

        entry = SimpleNamespace(data="not-a-mapping", options={})

        # Only ``len(entry.options) == 0`` remains. Production source line 768
        # short-circuits the non-Mapping container with ``continue``.
        assert _entry_schema_score(entry) == 0


class TestEntryCreationTimestampBasics:
    """``_entry_creation_timestamp`` prefers ``created_at`` then ``updated_at`` then inf."""

    def test_created_at_takes_precedence(self) -> None:
        created = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        updated = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
        entry = _make_entry(created_at=created, updated_at=updated)

        assert _entry_creation_timestamp(entry) == created.timestamp()

    def test_falls_back_to_updated_at_when_created_missing(self) -> None:
        updated = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        entry = _make_entry(created_at=None, updated_at=updated)

        assert _entry_creation_timestamp(entry) == updated.timestamp()

    def test_returns_inf_when_no_timestamp_available(self) -> None:
        entry = _make_entry(created_at=None, updated_at=None)

        assert _entry_creation_timestamp(entry) == float("inf")

    def test_non_datetime_falls_back_to_updated_at(self) -> None:
        """A string ``created_at`` triggers the ``updated_at`` fallback path."""

        updated = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        entry = SimpleNamespace(created_at="2026-01-01", updated_at=updated)

        assert _entry_creation_timestamp(entry) == updated.timestamp()


# ---------------------------------------------------------------------------
# Block C — Platform / subentry identifier helpers (Lines 1053-1092, 3537-3548)
# ---------------------------------------------------------------------------


class TestFeatureNameFromPlatformBasics:
    """``_feature_name_from_platform`` returns the platform's domain string."""

    def test_enum_with_string_value_returns_value(self) -> None:
        platform = SimpleNamespace(value="sensor")

        assert _feature_name_from_platform(platform) == "sensor"

    def test_strips_dotted_prefix_when_value_missing(self) -> None:
        class _Stub:
            def __str__(self) -> str:
                return "Platform.SENSOR"

        # No ``.value`` attribute (slots not used here, no fallback string).
        platform = _Stub()

        assert _feature_name_from_platform(platform) == "sensor"

    def test_lowercases_plain_str_repr(self) -> None:
        class _Stub:
            def __str__(self) -> str:
                return "SENSOR"

        assert _feature_name_from_platform(_Stub()) == "sensor"

    def test_non_string_value_is_ignored(self) -> None:
        """A non-string ``.value`` must fall through to the ``__str__`` path."""

        class _Stub:
            value = 42

            def __str__(self) -> str:
                return "domain.button"

        # Value is int, so the first branch is skipped; ``str()`` yields the
        # dotted form that gets split.
        assert _feature_name_from_platform(_Stub()) == "button"


class TestSubentryEntryIdBasics:
    """``_subentry_entry_id`` prefers ``subentry_id`` then ``entry_id`` then ``None``."""

    def test_returns_subentry_id_when_present(self) -> None:
        subentry = SimpleNamespace(subentry_id="sub-1", entry_id="entry-1")

        assert _subentry_entry_id(subentry) == "sub-1"

    def test_falls_back_to_entry_id(self) -> None:
        subentry = SimpleNamespace(subentry_id=None, entry_id="entry-1")

        assert _subentry_entry_id(subentry) == "entry-1"

    def test_returns_none_when_both_missing(self) -> None:
        subentry = SimpleNamespace(subentry_id=None, entry_id=None)

        assert _subentry_entry_id(subentry) is None

    def test_empty_string_is_rejected(self) -> None:
        """Empty strings must not satisfy the truthiness guard."""

        subentry = SimpleNamespace(subentry_id="", entry_id="")

        assert _subentry_entry_id(subentry) is None

    def test_non_string_subentry_id_falls_through(self) -> None:
        """``isinstance(str)`` check forces the fallback when type mismatches."""

        subentry = SimpleNamespace(subentry_id=42, entry_id="entry-1")

        assert _subentry_entry_id(subentry) == "entry-1"


class TestDefaultButtonSubentryIdentifierBasics:
    """``_default_button_subentry_identifier`` checks button → device_tracker → default."""

    def test_returns_button_identifier_when_present(self) -> None:
        result = _default_button_subentry_identifier(
            {"button": "btn-id", "device_tracker": "tracker-id"}
        )

        assert result == "btn-id"

    def test_falls_back_to_device_tracker(self) -> None:
        result = _default_button_subentry_identifier({"device_tracker": "tracker-id"})

        assert result == "tracker-id"

    def test_returns_module_default_when_both_missing(self) -> None:
        result = _default_button_subentry_identifier({})

        assert result == _DEFAULT_SUBENTRY_IDENTIFIER
        # The literal value of _DEFAULT_SUBENTRY_IDENTIFIER in __init__.py.
        assert result == "core_tracking"

    def test_skips_non_string_identifier(self) -> None:
        """Non-string identifiers must not satisfy the truthy/type guard."""

        result = _default_button_subentry_identifier(
            {"button": 0, "device_tracker": ""}
        )

        # Both candidates fail the ``isinstance(str) and identifier`` guard, so
        # the module-default fires.
        assert result == _DEFAULT_SUBENTRY_IDENTIFIER


# ---------------------------------------------------------------------------
# Block D — RuntimeData accessors (Lines 2296-2316)
# ---------------------------------------------------------------------------


class TestRuntimeDataAccessorBasics:
    """``_runtime_data`` / ``_get_retry_attempts`` / ``_get_retry_handles`` route via runtime_data."""

    def _make_runtime(self) -> RuntimeData:
        # RuntimeData is a slots dataclass — fill required fields with magic
        # mocks because the helpers under test only touch ``subentry_retry_*``.
        return RuntimeData(
            coordinator=MagicMock(),
            token_cache=MagicMock(),
            subentry_manager=MagicMock(),
        )

    def test_runtime_data_returns_runtime_data_object(self) -> None:
        runtime = self._make_runtime()
        entry = SimpleNamespace(runtime_data=runtime)

        assert _runtime_data(entry) is runtime

    def test_runtime_data_asserts_on_wrong_type(self) -> None:
        entry = SimpleNamespace(runtime_data="not a RuntimeData")

        with pytest.raises(AssertionError):
            _runtime_data(entry)

    def test_get_retry_attempts_returns_runtime_field(self) -> None:
        runtime = self._make_runtime()
        runtime.subentry_retry_attempts["sub-1"] = {"forward": 2}
        entry = SimpleNamespace(runtime_data=runtime)

        attempts = _get_retry_attempts(entry)

        assert attempts is runtime.subentry_retry_attempts
        assert attempts["sub-1"]["forward"] == 2

    def test_get_retry_handles_returns_runtime_field(self) -> None:
        runtime = self._make_runtime()
        handle = MagicMock()
        runtime.subentry_retry_handles["sub-1"] = handle
        entry = SimpleNamespace(runtime_data=runtime)

        handles = _get_retry_handles(entry)

        assert handles is runtime.subentry_retry_handles
        assert handles["sub-1"] is handle


# ---------------------------------------------------------------------------
# Block E — Domain-data accessors: FCM refcounts (Lines 3076-3122)
# ---------------------------------------------------------------------------


class TestFcmRefcountsBasics:
    """``_get_fcm_refcount`` / ``_set_fcm_refcount`` enforce per-entry refcounting."""

    def test_get_fcm_refcounts_normalizes_invalid_entries(self) -> None:
        bucket: dict[str, Any] = {
            "fcm_refcounts": {
                "good": 3,
                42: 5,  # non-string key
                "bad-value": "not-an-int",
            }
        }

        refcounts = _get_fcm_refcounts(bucket)  # type: ignore[arg-type]

        # Production source line 3083 keeps only str→int pairs.
        assert refcounts == {"good": 3}
        # ``bucket`` is mutated in place to hold the sanitized dict.
        assert bucket["fcm_refcounts"] is refcounts

    def test_get_fcm_refcounts_creates_dict_when_missing(self) -> None:
        bucket: dict[str, Any] = {}

        refcounts = _get_fcm_refcounts(bucket)  # type: ignore[arg-type]

        assert refcounts == {}
        assert bucket["fcm_refcounts"] is refcounts

    def test_get_fcm_refcount_returns_value_for_existing_entry(self) -> None:
        bucket: dict[str, Any] = {"fcm_refcounts": {"entry-1": 7}}

        assert _get_fcm_refcount(bucket, "entry-1") == 7  # type: ignore[arg-type]

    def test_get_fcm_refcount_returns_zero_for_unknown_entry(self) -> None:
        bucket: dict[str, Any] = {"fcm_refcounts": {"entry-1": 7}}

        assert _get_fcm_refcount(bucket, "entry-2") == 0  # type: ignore[arg-type]

    def test_set_fcm_refcount_writes_and_mirrors_default(self) -> None:
        bucket: dict[str, Any] = {}

        _set_fcm_refcount(bucket, "default", 4)  # type: ignore[arg-type]

        assert bucket["fcm_refcounts"]["default"] == 4
        # default-entry must mirror to legacy ``fcm_refcount`` key (line 3105)
        assert bucket["fcm_refcount"] == 4

    def test_set_fcm_refcount_does_not_touch_legacy_for_non_default(self) -> None:
        bucket: dict[str, Any] = {}

        _set_fcm_refcount(bucket, "entry-x", 9)  # type: ignore[arg-type]

        assert bucket["fcm_refcounts"]["entry-x"] == 9
        assert "fcm_refcount" not in bucket


class TestNovaRefcountBasics:
    """``_get_nova_refcount`` / ``_set_nova_refcount`` cover the Nova session counter."""

    def test_get_returns_stored_int(self) -> None:
        bucket: dict[str, Any] = {"nova_refcount": 5}

        assert _get_nova_refcount(bucket) == 5  # type: ignore[arg-type]

    def test_get_returns_zero_when_missing(self) -> None:
        assert _get_nova_refcount({}) == 0  # type: ignore[arg-type]

    def test_get_returns_zero_when_value_is_not_int(self) -> None:
        """A corrupt non-int stored value must not crash the helper."""

        bucket: dict[str, Any] = {"nova_refcount": "not-an-int"}

        assert _get_nova_refcount(bucket) == 0  # type: ignore[arg-type]

    def test_set_persists_value(self) -> None:
        bucket: dict[str, Any] = {}

        _set_nova_refcount(bucket, 3)  # type: ignore[arg-type]

        assert bucket["nova_refcount"] == 3


class TestSyncReceiverDefaultEntryBasics:
    """``_sync_receiver_default_entry`` prefers ``set_default_entry_id`` over setattr."""

    def test_uses_setter_when_available(self) -> None:
        setter = MagicMock()
        receiver = SimpleNamespace(set_default_entry_id=setter)

        _sync_receiver_default_entry(receiver, "entry-1")

        setter.assert_called_once_with("entry-1")

    def test_falls_back_to_setattr_when_setter_missing(self) -> None:
        receiver = SimpleNamespace()

        _sync_receiver_default_entry(receiver, "entry-2")

        assert receiver.default_entry_id == "entry-2"

    def test_none_entry_is_silently_ignored(self) -> None:
        setter = MagicMock()
        receiver = SimpleNamespace(set_default_entry_id=setter)

        _sync_receiver_default_entry(receiver, None)

        setter.assert_not_called()
        assert not hasattr(receiver, "default_entry_id")

    def test_setter_exception_falls_through_to_attribute_assignment(self) -> None:
        """When the setter raises, the helper still writes ``default_entry_id``."""

        setter = MagicMock(side_effect=RuntimeError("boom"))
        receiver = SimpleNamespace(set_default_entry_id=setter)

        _sync_receiver_default_entry(receiver, "entry-x")

        # Fallback path (line 3138-3141) writes the attribute despite the
        # setter failure.
        assert receiver.default_entry_id == "entry-x"


# ---------------------------------------------------------------------------
# Block F — Device identifier normalization (Lines 3230-3258)
# ---------------------------------------------------------------------------


class TestNormalizeDeviceIdentifierBasics:
    """``_normalize_device_identifier`` strips entry/subentry prefixes from device IDs."""

    def test_empty_identifier_returns_unchanged(self) -> None:
        assert _normalize_device_identifier(SimpleNamespace(), "") == ""

    def test_identifier_without_colon_passes_through(self) -> None:
        device = SimpleNamespace(config_entries=set())

        assert (
            _normalize_device_identifier(device, "integration_entry-1")
            == "integration_entry-1"
        )

    def test_strips_known_config_entry_prefix(self) -> None:
        device = SimpleNamespace(config_entries={"entry-1"})

        assert (
            _normalize_device_identifier(device, "entry-1:sub-1:google-device-7")
            == "google-device-7"
        )

    def test_only_last_segment_is_returned(self) -> None:
        """Without matching config_entries, last segment after split is kept."""

        device = SimpleNamespace(config_entries=set())

        assert _normalize_device_identifier(device, "foo:bar:baz") == "baz"

    def test_trailing_empty_segment_falls_back_to_ident(self) -> None:
        """Empty last segment must not blank the identifier."""

        device = SimpleNamespace(config_entries=set())

        result = _normalize_device_identifier(device, "foo:")

        # ``last or ident`` (production line 3258) returns the original.
        assert result == "foo:"


# ---------------------------------------------------------------------------
# Block G — Legacy button remainder + button unique-id parser (Lines 3569-3704)
# ---------------------------------------------------------------------------


class TestNormalizeLegacyButtonRemainderBasics:
    """``_normalize_legacy_button_remainder`` strips legacy ``tracker_`` prefixes."""

    def test_no_action_suffix_returns_remainder_unchanged(self) -> None:
        result = _normalize_legacy_button_remainder(
            "tracker_devicexlocate",
            identifier="core_tracking",
            suffixes=("play_sound", "stop_sound", "locate_device"),
        )

        # No matching suffix ⇒ early return.
        assert result == "tracker_devicexlocate"

    def test_strips_legacy_tracker_prefix_when_action_matches(self) -> None:
        result = _normalize_legacy_button_remainder(
            "tracker_device123play_sound",
            identifier="core_tracking",
            suffixes=("play_sound",),
        )

        # Production line 3594: returns ``{trimmed}{action_suffix}``.
        assert result == "device123play_sound"

    def test_keeps_remainder_when_payload_already_identifier_prefixed(self) -> None:
        result = _normalize_legacy_button_remainder(
            "core_tracking_device123play_sound",
            identifier="core_tracking",
            suffixes=("play_sound",),
        )

        # Payload starts with ``{identifier}_`` ⇒ early return at line 3587.
        assert result == "core_tracking_device123play_sound"

    def test_legacy_prefix_with_empty_remainder_falls_through(self) -> None:
        """Trimming to empty must NOT strip the legacy prefix."""

        result = _normalize_legacy_button_remainder(
            "tracker_play_sound",
            identifier="core_tracking",
            suffixes=("play_sound",),
        )

        # ``trimmed`` ⇒ empty after slicing ``tracker_`` ⇒ break ⇒ original
        # remainder (line 3597 reachable only when prefix absent or trimmed
        # empty).
        assert result == "tracker_play_sound"


class TestParseButtonUniqueIdBasics:
    """``_parse_button_unique_id`` decodes legacy + namespaced unique IDs."""

    def _entry(self) -> SimpleNamespace:
        return SimpleNamespace(entry_id="entry-1")

    def test_returns_none_for_empty_unique_id(self) -> None:
        assert _parse_button_unique_id("", self._entry(), {}, "core_tracking") is None

    def test_returns_none_for_non_string_unique_id(self) -> None:
        assert (
            _parse_button_unique_id(
                42,  # type: ignore[arg-type]
                self._entry(),
                {},
                "core_tracking",
            )
            is None
        )

    def test_full_namespaced_unique_id(self) -> None:
        """``<domain>_<entry_id>:<sub_id>:<device_id>_<action>`` parses cleanly."""

        unique = f"{DOMAIN}_entry-1:sub-7:device-A_play_sound"

        parts = _parse_button_unique_id(unique, self._entry(), {}, "core_tracking")

        assert parts is not None
        assert parts.entry_id == "entry-1"
        assert parts.subentry_id == "sub-7"
        assert parts.google_device_id == "device-A"
        assert parts.action == "play_sound"

    def test_unknown_subentry_falls_back_to_default(self) -> None:
        """If no subentry token resolves, ``fallback_subentry_id`` is used."""

        unique = f"{DOMAIN}_entry-1_device-B_play_sound"

        parts = _parse_button_unique_id(
            unique, self._entry(), {}, fallback_subentry_id="core_tracking"
        )

        assert parts is not None
        assert parts.subentry_id == "core_tracking"
        assert parts.google_device_id == "device-B"
        assert parts.action == "play_sound"

    def test_subentry_map_resolves_known_token(self) -> None:
        """``subentry_map`` values can match a longest-prefix token in the remainder."""

        unique = f"{DOMAIN}_entry-1_sub-known_device-Z_play_sound"
        subentry_map = {"button": "sub-known"}

        parts = _parse_button_unique_id(
            unique, self._entry(), subentry_map, "core_tracking"
        )

        assert parts is not None
        assert parts.subentry_id == "sub-known"
        assert parts.google_device_id == "device-Z"

    def test_mismatched_entry_id_returns_none(self) -> None:
        """Namespaced unique ID belonging to a different entry must not resolve."""

        unique = f"{DOMAIN}_other-entry:sub-1:device_play_sound"

        result = _parse_button_unique_id(unique, self._entry(), {}, "core_tracking")

        assert result is None

    def test_unknown_action_falls_back_to_split(self) -> None:
        """When no known action suffix matches, ``rsplit('_', 1)`` is used."""

        unique = "entry-1_device-X_custom"  # ``custom`` is not in suffix tuple

        parts = _parse_button_unique_id(unique, self._entry(), {}, "core_tracking")

        assert parts is not None
        assert parts.action == "custom"
        assert parts.google_device_id == "device-X"

    def test_returns_none_for_unparseable_unique_id(self) -> None:
        """Single-token unique IDs without underscore must fail cleanly."""

        result = _parse_button_unique_id(
            "lonely-token", self._entry(), {}, "core_tracking"
        )

        assert result is None

    def test_empty_entry_id_returns_none(self) -> None:
        """``entry.entry_id`` must be a non-empty string (line 3635)."""

        entry = SimpleNamespace(entry_id="")

        result = _parse_button_unique_id(
            f"{DOMAIN}_entry-1_device_play_sound",
            entry,
            {},
            "core_tracking",
        )

        assert result is None


class TestIterTrackerIdentifierCandidatesBasics:
    """``_iter_tracker_identifier_candidates`` yields three layered registry lookups."""

    def test_returns_three_layered_candidates(self) -> None:
        parts = _ButtonUniqueIdParts(
            entry_id="entry-1",
            subentry_id="sub-7",
            google_device_id="device-A",
            action="play_sound",
        )

        candidates = _iter_tracker_identifier_candidates(parts)

        assert candidates == (
            (DOMAIN, "entry-1:sub-7:device-A"),
            (DOMAIN, "entry-1:device-A"),
            (DOMAIN, "device-A"),
        )


# ---------------------------------------------------------------------------
# Block H — ConfigEntrySubentryDefinition dataclass smoke (Lines 1095-1108)
# ---------------------------------------------------------------------------


class TestConfigEntrySubentryDefinitionBasics:
    """``ConfigEntrySubentryDefinition`` defaults to the tracker subentry type."""

    def test_default_subentry_type_is_tracker(self) -> None:
        definition = ConfigEntrySubentryDefinition(
            key="trackers",
            title="Trackers",
            data={"some": "data"},
        )

        assert definition.key == "trackers"
        assert definition.title == "Trackers"
        assert definition.data == {"some": "data"}
        assert definition.subentry_type == SUBENTRY_TYPE_TRACKER

    def test_subentry_type_can_be_overridden(self) -> None:
        definition = ConfigEntrySubentryDefinition(
            key="trackers",
            title="Trackers",
            data={},
            subentry_type="custom_type",
        )

        assert definition.subentry_type == "custom_type"
