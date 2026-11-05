# tests/test_options_flow_credentials_outcomes.py
"""One test per branch of the credential options step that this change touched.

The step validates one of two credential methods (a pasted secrets bundle or a
manual token) and then either stores it for the whole account or answers with a
form error. Removing the feature group selector dedented its validation
branches, so they became part of the diff; several of them had no test
anywhere. The outcomes that neighbouring files already pin stay there
(``keys_missing`` and ``invalid_token`` in
``tests/test_config_flow_secrets_normalization.py``, the plain token success and
``credentials_saved_not_reloaded`` in
``tests/test_options_flow_credentials_reload.py``, a guard error without a
bundle in ``tests/test_options_flow_credentials_cache.py``).

Each error test asserts the exact ``errors`` mapping *and* that nothing was
written, claimed or reloaded, so a branch that silently stores half a credential
fails here. Each success test asserts the stored payload itself, not merely the
abort reason, because the branches differ only in what they store (the chosen
token, its JWT-free alternative, the AAS token kept or dropped).

The manual-token field is commented out of the form schema, so the token
branches are reached only by a direct call. They stay tested because the code
stays: ``async_step_credentials`` keeps both branches and the guard fallback
documents a token-only submission as defence in depth.

Deliberately a new file, for the same reason as its neighbours:
``tests/test_options_flow_credentials_cache.py`` is on the ``LEGACY_ALLOWLIST``
of ``test_guard_asyncio_run_antipattern.py``. Entry, hass and latch doubles are
imported from ``tests/test_options_flow_credentials_reload.py`` rather than
copied.
"""

from __future__ import annotations

import inspect
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from custom_components.googlefindmy import config_flow
from custom_components.googlefindmy.const import (
    CONF_OAUTH_TOKEN,
    DATA_AAS_TOKEN,
    DATA_AUTH_METHOD,
    DATA_SECRET_BUNDLE,
)
from tests.test_options_flow_credentials_reload import (
    _DummyHass,
    _LatchRecorder,
    _make_entry,
    _MemoryCache,
)

pytestmark = pytest.mark.asyncio

_ORIGINAL_TOKEN = "oauth-original-token-123456"
_PLAIN_TOKEN = "oauth-token-outcomes-123456"
_JWT_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl"
_AAS_TOKEN = "aas_et/outcomes-master-token-123456"
_STALE_AAS_TOKEN = "aas_et/stale-master-token-123456"

_Pick = Callable[..., Awaitable[str | None]]


def _returning(value: str | None) -> _Pick:
    """A token picker that answers ``value`` whatever it is offered."""

    async def _pick(
        _hass: Any,
        _email: str,
        _candidates: list[tuple[str, str]],
        *,
        secrets_bundle: dict[str, Any] | None = None,
    ) -> str | None:
        return value

    return _pick


def _raising(exc: BaseException) -> _Pick:
    """A token picker that raises ``exc`` on every call."""

    async def _pick(
        _hass: Any,
        _email: str,
        _candidates: list[tuple[str, str]],
        *,
        secrets_bundle: dict[str, Any] | None = None,
    ) -> str | None:
        raise exc

    return _pick


def _bundle(**tokens: str) -> str:
    """A secrets bundle that passes the single-key rule, plus ``tokens``."""

    return json.dumps({"shared_key": "DDEEFF", **tokens})


async def _submit(
    monkeypatch: pytest.MonkeyPatch,
    user_input: dict[str, Any],
    pick: _Pick,
    *,
    stale_aas: bool = False,
    stale_bundle: bool = False,
) -> tuple[Any, Any, _DummyHass, _LatchRecorder]:
    """Run one credential submission and return outcome, entry, hass, latch.

    ``stale_aas`` seeds an AAS token from an earlier login, so a success test
    can show that a non-AAS credential drops it instead of carrying it along.
    ``stale_bundle`` seeds a secrets bundle from an earlier login, so the manual
    token branch can show that it drops the bundle it replaces.
    """

    cache = _MemoryCache()
    entry = _make_entry(entry_id="entry-credential-outcomes", cache=cache)
    if stale_aas:
        entry.data = {**entry.data, DATA_AAS_TOKEN: _STALE_AAS_TOKEN}
    if stale_bundle:
        entry.data = {**entry.data, DATA_SECRET_BUNDLE: {"shared_key": "AABBCC"}}
    hass = _DummyHass(entry, cache)
    latch = _LatchRecorder()
    monkeypatch.setattr(
        config_flow, "import_integration_package", lambda: latch, raising=True
    )
    monkeypatch.setattr(config_flow, "async_pick_working_token", pick)

    flow = config_flow.OptionsFlowHandler()
    flow.hass = hass  # type: ignore[assignment]
    flow.config_entry = entry  # type: ignore[attr-defined]

    outcome = await flow.async_step_credentials(user_input)
    if inspect.isawaitable(outcome):
        outcome = await outcome
    await hass.settle()
    return outcome, entry, hass, latch


def _assert_rejected(
    outcome: Any, entry: Any, hass: _DummyHass, latch: _LatchRecorder
) -> None:
    """The form comes back and nothing was stored, claimed or reloaded."""

    assert outcome["type"] == "form"
    assert outcome["step_id"] == "credentials"
    assert hass.config_entries.updated_payloads == []
    assert entry.data[CONF_OAUTH_TOKEN] == _ORIGINAL_TOKEN
    assert latch.claims == []
    assert hass.config_entries.reloads == []


def _assert_stored(
    outcome: Any, hass: _DummyHass, latch: _LatchRecorder
) -> dict[str, Any]:
    """Exactly one write, claimed and reloaded; returns the stored payload."""

    assert outcome["type"] == "abort"
    assert outcome["reason"] == "reconfigure_successful"
    assert len(hass.config_entries.updated_payloads) == 1
    assert latch.claims == ["entry-credential-outcomes"]
    assert hass.config_entries.reloads == ["entry-credential-outcomes"]
    return hass.config_entries.updated_payloads[0]


# --- Exactly one method per submission ---------------------------------------


@pytest.mark.parametrize(
    "user_input",
    [
        pytest.param({}, id="none"),
        pytest.param(
            {
                "new_secrets_json": _bundle(oauth_token=_PLAIN_TOKEN),
                "new_oauth_token": _PLAIN_TOKEN,
            },
            id="both",
        ),
    ],
)
async def test_zero_or_two_methods_are_refused_before_anything_is_tried(
    monkeypatch: pytest.MonkeyPatch, user_input: dict[str, Any]
) -> None:
    called: list[Any] = []

    async def _pick(*args: Any, **kwargs: Any) -> str | None:
        called.append(args)
        return _PLAIN_TOKEN

    outcome, entry, hass, latch = await _submit(monkeypatch, user_input, _pick)

    _assert_rejected(outcome, entry, hass, latch)
    assert outcome["errors"] == {"base": "choose_one"}
    # Refused before validation, not after a failed one.
    assert called == []


# --- The manual token branch -------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(config_flow.DependencyNotReady("api missing"), id="dependency"),
        pytest.param(ImportError("api missing"), id="import"),
    ],
)
async def test_token_validation_without_dependencies_reports_import_failed(
    monkeypatch: pytest.MonkeyPatch, exc: Exception
) -> None:
    outcome, entry, hass, latch = await _submit(
        monkeypatch, {"new_oauth_token": _PLAIN_TOKEN}, _raising(exc)
    )

    _assert_rejected(outcome, entry, hass, latch)
    assert outcome["errors"] == {"base": "import_failed"}


async def test_a_token_no_endpoint_accepts_reports_cannot_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome, entry, hass, latch = await _submit(
        monkeypatch, {"new_oauth_token": _PLAIN_TOKEN}, _returning(None)
    )

    _assert_rejected(outcome, entry, hass, latch)
    assert outcome["errors"] == {"base": "cannot_connect"}


async def test_a_validated_jwt_token_is_stored_with_a_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger=config_flow._LOGGER.name)

    outcome, _entry, hass, latch = await _submit(
        monkeypatch,
        {"new_oauth_token": _JWT_TOKEN},
        _returning(_JWT_TOKEN),
        stale_aas=True,
        stale_bundle=True,
    )

    stored = _assert_stored(outcome, hass, latch)
    assert stored[CONF_OAUTH_TOKEN] == _JWT_TOKEN
    assert stored[DATA_AUTH_METHOD] == config_flow._AUTH_METHOD_INDIVIDUAL
    assert DATA_AAS_TOKEN not in stored
    assert DATA_SECRET_BUNDLE not in stored
    assert "token looks like a JWT; persisting anyway" in caplog.text


async def test_a_validated_aas_token_is_also_stored_as_the_aas_token(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger=config_flow._LOGGER.name)

    outcome, _entry, hass, latch = await _submit(
        monkeypatch, {"new_oauth_token": _AAS_TOKEN}, _returning(_AAS_TOKEN)
    )

    stored = _assert_stored(outcome, hass, latch)
    assert stored[CONF_OAUTH_TOKEN] == _AAS_TOKEN
    assert stored[DATA_AAS_TOKEN] == _AAS_TOKEN
    assert stored[DATA_AUTH_METHOD] == config_flow._AUTH_METHOD_INDIVIDUAL
    # Control for the JWT test: an AAS token raises no JWT warning.
    assert "looks like a JWT" not in caplog.text


# --- The secrets bundle branch -----------------------------------------------


@pytest.mark.parametrize(
    "pasted",
    [
        pytest.param("[1, 2]", id="json-but-not-an-object"),
        pytest.param("{not json", id="not-json"),
    ],
)
async def test_a_bundle_that_is_not_a_json_object_is_invalid_json(
    monkeypatch: pytest.MonkeyPatch, pasted: str
) -> None:
    outcome, entry, hass, latch = await _submit(
        monkeypatch, {"new_secrets_json": pasted}, _returning(_PLAIN_TOKEN)
    )

    _assert_rejected(outcome, entry, hass, latch)
    assert outcome["errors"] == {"new_secrets_json": "invalid_json"}


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(config_flow.DependencyNotReady("api missing"), id="dependency"),
        pytest.param(ImportError("api missing"), id="import"),
    ],
)
async def test_bundle_validation_without_dependencies_reports_import_failed(
    monkeypatch: pytest.MonkeyPatch, exc: Exception
) -> None:
    outcome, entry, hass, latch = await _submit(
        monkeypatch,
        {"new_secrets_json": _bundle(oauth_token=_PLAIN_TOKEN)},
        _raising(exc),
    )

    _assert_rejected(outcome, entry, hass, latch)
    assert outcome["errors"] == {"base": "import_failed"}


async def test_a_bundle_whose_tokens_all_fail_reports_cannot_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome, entry, hass, latch = await _submit(
        monkeypatch,
        {"new_secrets_json": _bundle(oauth_token=_PLAIN_TOKEN)},
        _returning(None),
    )

    _assert_rejected(outcome, entry, hass, latch)
    assert outcome["errors"] == {"base": "cannot_connect"}


async def test_a_validated_jwt_from_a_bundle_gives_way_to_a_plain_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome, _entry, hass, latch = await _submit(
        monkeypatch,
        {
            "new_secrets_json": _bundle(
                oauth_token=_JWT_TOKEN, access_token=_PLAIN_TOKEN
            )
        },
        _returning(_JWT_TOKEN),
        stale_aas=True,
    )

    stored = _assert_stored(outcome, hass, latch)
    assert stored[CONF_OAUTH_TOKEN] == _PLAIN_TOKEN
    assert stored[DATA_AUTH_METHOD] == config_flow._AUTH_METHOD_SECRETS
    assert stored[DATA_SECRET_BUNDLE]["access_token"] == _PLAIN_TOKEN
    assert DATA_AAS_TOKEN not in stored


async def test_a_validated_jwt_without_an_alternative_is_kept(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome, _entry, hass, latch = await _submit(
        monkeypatch,
        {"new_secrets_json": _bundle(oauth_token=_JWT_TOKEN)},
        _returning(_JWT_TOKEN),
    )

    stored = _assert_stored(outcome, hass, latch)
    assert stored[CONF_OAUTH_TOKEN] == _JWT_TOKEN
    assert stored[DATA_AUTH_METHOD] == config_flow._AUTH_METHOD_SECRETS


async def test_a_validated_aas_token_from_a_bundle_keeps_the_aas_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome, _entry, hass, latch = await _submit(
        monkeypatch,
        {"new_secrets_json": _bundle(aas_token=_AAS_TOKEN)},
        _returning(_AAS_TOKEN),
    )

    stored = _assert_stored(outcome, hass, latch)
    assert stored[CONF_OAUTH_TOKEN] == _AAS_TOKEN
    assert stored[DATA_AAS_TOKEN] == _AAS_TOKEN
    assert stored[DATA_AUTH_METHOD] == config_flow._AUTH_METHOD_SECRETS


# --- The multi-entry guard fallback ------------------------------------------


@pytest.mark.parametrize(
    ("tokens", "expected_token", "expected_aas"),
    [
        pytest.param({"aas_token": _AAS_TOKEN}, _AAS_TOKEN, _AAS_TOKEN, id="aas-only"),
        pytest.param(
            {"oauth_token": _PLAIN_TOKEN}, _PLAIN_TOKEN, None, id="plain-only"
        ),
        # Two candidates, the AAS token written last: the fallback takes the
        # first *candidate* (``_extract_oauth_candidates_from_secrets`` puts
        # ``aas_token`` first), not the first or the last key of the bundle.
        pytest.param(
            {"oauth_token": _PLAIN_TOKEN, "aas_token": _AAS_TOKEN},
            _AAS_TOKEN,
            _AAS_TOKEN,
            id="two-candidates",
        ),
    ],
)
async def test_the_guard_fallback_stores_the_first_candidate_of_the_bundle(
    monkeypatch: pytest.MonkeyPatch,
    tokens: dict[str, str],
    expected_token: str,
    expected_aas: str | None,
) -> None:
    outcome, _entry, hass, latch = await _submit(
        monkeypatch,
        {"new_secrets_json": _bundle(**tokens)},
        _raising(RuntimeError("Multiple config entries active for this domain")),
        stale_aas=True,
    )

    stored = _assert_stored(outcome, hass, latch)
    assert stored[CONF_OAUTH_TOKEN] == expected_token
    assert stored[DATA_AUTH_METHOD] == config_flow._AUTH_METHOD_SECRETS
    assert stored.get(DATA_AAS_TOKEN) == expected_aas


async def test_any_other_error_during_bundle_validation_stores_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control for the fallback: only the guard error stores unvalidated."""

    outcome, entry, hass, latch = await _submit(
        monkeypatch,
        {"new_secrets_json": _bundle(oauth_token=_PLAIN_TOKEN)},
        _raising(RuntimeError("boom")),
    )

    _assert_rejected(outcome, entry, hass, latch)
    assert outcome["errors"] == {"base": "unknown"}
