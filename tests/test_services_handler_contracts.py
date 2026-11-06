# tests/test_services_handler_contracts.py
"""Contract tests for the googlefindmy service handlers.

``async_register_services(hass, ctx)`` wires seven service handlers as closures
over an injected ``ctx`` (a 14-key context passed by ``__init__.py`` to avoid
import cycles). These tests build that full ctx with mocked callables, capture
the registered handlers, and pin:

* input validation (``device_id`` / ``request_uuid`` type + emptiness guards),
* the ``_resolve_runtime_for_device_id`` outcomes (no active entry, resolver
  ``HomeAssistantError`` -> not-found, coordinator dispatch, suppressed ops),
* token redaction in ``refresh_device_urls`` (the raw token-bearing URL is what
  is handed to ``ctx["redact_url_token"]`` for logging),
* the ``rebuild_registry`` payload-type guards (invalid entry_id / device_ids).

Fixture note: ``dr.async_get`` and ``get_url`` are patched at the module level so
handlers run without a live Home Assistant device registry / network helper.
"""

from __future__ import annotations

import ast
import json
import pathlib
import string
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from custom_components.googlefindmy import services
from custom_components.googlefindmy.const import (
    PlaySoundOutcome,
    StopSoundOutcome,
    StopSoundUncorrelatedReason,
)
from custom_components.googlefindmy.services import (
    HomeAssistantError,
    ServiceValidationError,
)
from tests.helpers.config_entries_stub import make_config_entry
from tests.helpers.service_doubles import FakeServiceCall, FakeServiceRegistry

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


_FakeCall = FakeServiceCall


def _make_hass(entries: list[Any] | None = None) -> SimpleNamespace:
    """Build a fake hass with a capturing service registry and entry list."""
    entry_list = list(entries or [])
    return SimpleNamespace(
        services=FakeServiceRegistry(),
        config_entries=SimpleNamespace(async_entries=lambda _domain: entry_list),
        data={},
    )


@pytest.fixture
def full_ctx() -> dict[str, Any]:
    """A complete 14-key ctx with every callable stubbed.

    Mirrors the keys documented on ``async_register_services``; every callable
    is a ``Mock``/``AsyncMock`` so a handler that reaches for any of them gets a
    benign stub rather than a ``KeyError``.
    """
    return {
        "domain": services.DOMAIN,
        "resolve_canonical": mock.Mock(return_value=("CANON", "Friendly Name")),
        "is_active_entry": mock.Mock(return_value=True),
        "primary_active_entry": mock.Mock(return_value=None),
        # Mirrors the real _opt(entry, key, default): with no matching option
        # set on the (empty options/data) test entries, it always falls
        # through to the caller's own default -- a flat return_value=False
        # would silently satisfy every bool-typed option read the same way.
        "opt": mock.Mock(side_effect=lambda entry, key, default: default),
        "default_map_view_token_expiration": False,
        "opt_map_view_token_expiration_key": "map_view_token_expiration",
        "redact_url_token": mock.Mock(return_value="<redacted-url>"),
        "soft_migrate_entry": mock.AsyncMock(),
        "migrate_unique_ids": mock.AsyncMock(),
        "relink_button_devices": mock.AsyncMock(),
        "relink_subentry_entities": mock.AsyncMock(),
        "coalesce_account_entries": mock.AsyncMock(),
        "extract_normalized_email": mock.Mock(return_value=None),
    }


def _register(hass: SimpleNamespace, ctx: dict[str, Any]) -> dict[str, Any]:
    """Register services and return {service_name: handler}.

    ``async_register_services`` is declared ``async`` but its body performs only
    synchronous work (defining closures + calling ``async_register``). Driving
    the coroutine with a single ``send(None)`` runs it to completion without a
    loop, so this works both in a plain fixture and inside a running event loop
    (``asyncio.run`` would raise there).
    """
    registry = FakeServiceRegistry()
    hass.services = registry
    coro = services.async_register_services(hass, ctx)
    try:
        coro.send(None)
    except StopIteration:
        pass
    else:  # pragma: no cover - registration must not await
        coro.close()
        raise RuntimeError("async_register_services unexpectedly awaited")
    return {
        service: handler for (_domain, service), handler in registry.handlers.items()
    }


@pytest.fixture
def handlers(full_ctx: dict[str, Any]) -> dict[str, Any]:
    """Handlers registered against a fake hass with no config entries."""
    hass = _make_hass()
    return _register(hass, full_ctx)


# ---------------------------------------------------------------------------
# Registration contract
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_all_services_registered(self, handlers: dict[str, Any]) -> None:
        """Every documented service name is wired exactly once."""
        assert set(handlers) == {
            services.SERVICE_LOCATE_DEVICE,
            services.SERVICE_LOCATE_EXTERNAL,
            services.SERVICE_PLAY_SOUND,
            services.SERVICE_STOP_SOUND,
            services.SERVICE_REFRESH_DEVICE_URLS,
            services.SERVICE_REBUILD_DEVICE_REGISTRY,
            services.SERVICE_REBUILD_REGISTRY,
            services.SERVICE_READ_BEACON_STATE,
        }


# ---------------------------------------------------------------------------
# Input validation (no resolver reached)
# ---------------------------------------------------------------------------


class TestDeviceIdValidation:
    """The three device-scoped handlers reject a missing/blank device_id."""

    @pytest.mark.parametrize(
        "service_const",
        [
            "SERVICE_LOCATE_DEVICE",
            "SERVICE_LOCATE_EXTERNAL",
            "SERVICE_PLAY_SOUND",
            "SERVICE_STOP_SOUND",
        ],
    )
    @pytest.mark.parametrize("bad", [None, "", 123, {"nested": 1}])
    @pytest.mark.asyncio
    async def test_missing_or_non_str_device_id_raises_not_found(
        self, handlers: dict[str, Any], service_const: str, bad: Any
    ) -> None:
        handler = handlers[getattr(services, service_const)]
        with pytest.raises(ServiceValidationError) as excinfo:
            await handler(_FakeCall({"device_id": bad}))
        assert excinfo.value.translation_key == "device_not_found"

    @pytest.mark.asyncio
    async def test_stop_sound_rejects_non_str_request_uuid(
        self, handlers: dict[str, Any]
    ) -> None:
        """A non-string ``request_uuid`` fails closed before any dispatch.

        The key changed from ``stop_sound_failed`` to its own
        ``stop_sound_invalid_uuid``: the former template carries an ``{error}``
        placeholder that this call site cannot fill, and Home Assistant
        swallows the resulting ``KeyError``, so the user was shown the raw
        template. See ``TestSoundTranslationPlaceholders``.
        """
        handler = handlers[services.SERVICE_STOP_SOUND]
        with pytest.raises(ServiceValidationError) as excinfo:
            await handler(_FakeCall({"device_id": "dev1", "request_uuid": 999}))
        assert excinfo.value.translation_key == "stop_sound_invalid_uuid"


# ---------------------------------------------------------------------------
# Resolver + dispatch contracts
# ---------------------------------------------------------------------------


def _hass_with_coordinator(coord: Any) -> SimpleNamespace:
    """Fake hass whose single entry exposes ``runtime_data.coordinator``."""
    runtime = SimpleNamespace(coordinator=coord)
    entry = SimpleNamespace(entry_id="e1", runtime_data=runtime, title="Account")
    return _make_hass([entry])


class TestResolverDispatch:
    @pytest.mark.asyncio
    async def test_no_active_entry_raises(self, full_ctx: dict[str, Any]) -> None:
        """With zero runtimes the handler raises the no_active_entry error."""
        hass = _make_hass()  # no entries -> no runtimes
        handlers = _register(hass, full_ctx)
        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_LOCATE_DEVICE](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == "no_active_entry"

    @pytest.mark.asyncio
    async def test_resolver_home_assistant_error_maps_to_not_found(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A ``HomeAssistantError`` from the canonical resolver becomes a
        translated device_not_found error."""
        full_ctx["resolve_canonical"] = mock.Mock(
            side_effect=HomeAssistantError("bad id")
        )
        coord = SimpleNamespace()
        hass = _hass_with_coordinator(coord)
        handlers = _register(hass, full_ctx)
        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_LOCATE_DEVICE](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == "device_not_found"

    @pytest.mark.asyncio
    async def test_locate_dispatches_to_coordinator(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A resolvable device dispatches ``async_locate_device`` with the
        canonical id from the resolver."""
        coord = SimpleNamespace(
            async_locate_device=mock.AsyncMock(),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        # No device-registry hit -> fall through to the coordinator scan.
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        await handlers[services.SERVICE_LOCATE_DEVICE](_FakeCall({"device_id": "dev1"}))
        coord.async_locate_device.assert_awaited_once_with("CANON")

    @pytest.mark.asyncio
    async def test_play_sound_suppressed_raises(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A coordinator that suppresses surfaces as a play_sound_suppressed error.

        Not ``play_sound_failed``: that template needs an ``{error}`` this call
        site has no value for, so the user would be shown the raw brace.
        """
        coord = SimpleNamespace(
            async_play_sound=mock.AsyncMock(return_value=PlaySoundOutcome.SUPPRESSED),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_PLAY_SOUND](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == "play_sound_suppressed"

    @pytest.mark.asyncio
    async def test_play_sound_failed_raises_its_own_key(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FAILED and SUPPRESSED must not share a message.

        They differ in what the user has to do: wait, or read the log and act.
        Before ``PlaySoundOutcome`` existed both arrived as one ``False`` and
        were answered with one text that had to list every possible cause.
        """
        coord = SimpleNamespace(
            async_play_sound=mock.AsyncMock(return_value=PlaySoundOutcome.FAILED),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_PLAY_SOUND](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == "play_sound_rejected"

    @pytest.mark.asyncio
    async def test_play_sound_accepted_raises_nothing(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The positive half of the closed set, so the branch cannot swallow success."""
        coord = SimpleNamespace(
            async_play_sound=mock.AsyncMock(return_value=PlaySoundOutcome.ACCEPTED),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        await handlers[services.SERVICE_PLAY_SOUND](_FakeCall({"device_id": "dev1"}))

    @pytest.mark.asyncio
    async def test_play_sound_value_outside_the_enum_is_not_reported_as_success(
        self,
        full_ctx: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Closed set, closed handling -- the same rule the stop path states.

        A stale double returning the old ``True`` is the realistic case. Silence
        here would report a ring that never started as started; the breach also
        has to be visible to whoever maintains the double, so it is logged.
        """
        coord = SimpleNamespace(
            async_play_sound=mock.AsyncMock(return_value=True),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        with caplog.at_level("ERROR"), pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_PLAY_SOUND](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == "play_sound_rejected"
        assert "not a PlaySoundOutcome" in caplog.text
        # AGENTS.md section 5: no device ids in logs. Pinned rather than trusted,
        # for the same reason as in the coordinator.
        assert "dev1" not in caplog.text

    @pytest.mark.parametrize(
        ("service_const", "coord_attr", "translation_key"),
        [
            ("SERVICE_LOCATE_DEVICE", "async_locate_device", "locate_failed"),
            ("SERVICE_LOCATE_EXTERNAL", "async_locate_device", "locate_failed"),
            ("SERVICE_PLAY_SOUND", "async_play_sound", "play_sound_failed"),
            ("SERVICE_STOP_SOUND", "async_stop_sound", "stop_sound_failed"),
        ],
    )
    @pytest.mark.asyncio
    async def test_downstream_failure_wrapped_as_translated_error(
        self,
        full_ctx: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
        service_const: str,
        coord_attr: str,
        translation_key: str,
    ) -> None:
        """Any coordinator exception is surfaced as the handler's translated
        ``*_failed`` error, never as a raw traceback."""
        coord = SimpleNamespace(
            **{coord_attr: mock.AsyncMock(side_effect=RuntimeError("boom"))},
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[getattr(services, service_const)](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == translation_key

    @pytest.mark.asyncio
    async def test_stop_sound_passes_request_uuid_through(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A valid ``request_uuid`` is forwarded verbatim to the coordinator."""
        coord = SimpleNamespace(
            async_stop_sound=mock.AsyncMock(return_value=StopSoundOutcome.CANCELLED),
            classify_stop_correlation=mock.Mock(return_value=None),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        await handlers[services.SERVICE_STOP_SOUND](
            _FakeCall({"device_id": "dev1", "request_uuid": "req-7"})
        )
        coord.async_stop_sound.assert_awaited_once_with("CANON", "req-7")

    @pytest.mark.asyncio
    async def test_uncorrelated_stop_is_reported_not_swallowed(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stop without a cancel key must not be reported as plain success.

        The submission was accepted, but nothing proves an effect on the
        device. Returning silently would tell the user the ring was stopped
        when it may well keep playing (BSkando#195).
        """

        coord = SimpleNamespace(
            async_stop_sound=mock.AsyncMock(return_value=StopSoundOutcome.UNCORRELATED),
            classify_stop_correlation=mock.Mock(return_value=None),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_STOP_SOUND](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == "stop_sound_uncorrelated"

    @pytest.mark.asyncio
    async def test_suppressed_stop_reports_its_own_cause(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stop that was never sent gets its own, placeholder-correct key.

        ``stop_sound_failed`` carries an ``{error}`` placeholder that this call
        site cannot fill, so it would render with a stray literal.
        """

        coord = SimpleNamespace(
            async_stop_sound=mock.AsyncMock(return_value=StopSoundOutcome.SUPPRESSED),
            classify_stop_correlation=mock.Mock(return_value=None),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_STOP_SOUND](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == "stop_sound_suppressed"

    @pytest.mark.asyncio
    async def test_an_unknown_outcome_is_never_reported_as_success(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The enum is a closed set, so its handling is closed too.

        Three `is` comparisons and no default meant that any other value --
        a bool from a stale test double, a member added later without visiting
        this call site -- fell through silently and reached the user as a
        successful stop. That is BSkando#195 reintroduced by omission, so the
        default must be an error.
        """

        coord = SimpleNamespace(
            async_stop_sound=mock.AsyncMock(return_value=True),
            classify_stop_correlation=mock.Mock(return_value=None),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_STOP_SOUND](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == "stop_sound_uncorrelated"

    @pytest.mark.asyncio
    async def test_rejected_stop_does_not_borrow_the_suppressed_advice(
        self, full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A refused stop must not be reported as a local, transient condition.

        SUPPRESSED means "the push transport is not up yet, retry shortly".
        FAILED means the attempt was made and failed, most often because the
        sign-in expired. Sharing one message would give the wrong advice to
        whichever half is not the actual cause. Note that "was it sent" is not
        the criterion -- SoundDispatchOutcome.NOT_SENT is a FAILED as well.
        """

        coord = SimpleNamespace(
            async_stop_sound=mock.AsyncMock(return_value=StopSoundOutcome.FAILED),
            classify_stop_correlation=mock.Mock(return_value=None),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)
        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_STOP_SOUND](
                _FakeCall({"device_id": "dev1"})
            )
        assert excinfo.value.translation_key == "stop_sound_rejected"


# ---------------------------------------------------------------------------
# Token redaction contract (refresh_device_urls)
# ---------------------------------------------------------------------------


class TestRefreshUrlRedaction:
    @pytest.mark.asyncio
    async def test_redactor_receives_raw_token_url(
        self,
        full_ctx: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The refresh handler hands the *raw* token-bearing URL to
        ``ctx["redact_url_token"]``; the persisted configuration_url carries the
        token, and only the redactor's masked output reaches the log (the raw
        token never appears in the emitted DEBUG record)."""
        entry = make_config_entry(entry_id="e1", title="Account")
        hass = _make_hass([entry])

        device = SimpleNamespace(
            id="devid",
            identifiers={(services.DOMAIN, "e1:CANON123")},
            config_entries=["e1"],
            serial_number=None,
            name="Tag",
            name_by_user=None,
        )
        update_mock = mock.Mock()
        # The handler asks per config entry instead of scanning the whole
        # registry, so the double answers that question rather than exposing a
        # ``devices`` mapping.
        fake_reg = SimpleNamespace(
            async_update_device=update_mock,
            async_entries_for_config_entry=lambda entry_id: (
                [device] if entry_id in device.config_entries else []
            ),
        )
        monkeypatch.setattr(services.dr, "async_get", lambda _h: fake_reg)
        monkeypatch.setattr(services, "get_url", lambda *a, **k: "http://ha.local:8123")

        redactor = mock.Mock(return_value="http://ha.local:8123/...?token=***")
        full_ctx["redact_url_token"] = redactor
        handlers = _register(hass, full_ctx)

        with caplog.at_level("DEBUG", logger=services._LOGGER.name):
            await handlers[services.SERVICE_REFRESH_DEVICE_URLS](_FakeCall({}))

        # The device URL was updated with a token-bearing URL...
        update_mock.assert_called_once()
        written_url = update_mock.call_args.kwargs["configuration_url"]
        assert "/api/googlefindmy/map/CANON123?token=" in written_url
        # ...that exact raw URL is what the redactor was asked to mask...
        redactor.assert_called_once_with(written_url)
        # ...and the raw token must not leak into the emitted log record; only
        # the redactor's masked output is logged.
        raw_token = written_url.split("token=", 1)[1]
        assert raw_token not in caplog.text
        assert "token=***" in caplog.text


# ---------------------------------------------------------------------------
# rebuild_registry payload guards
# ---------------------------------------------------------------------------


class TestRebuildRegistryGuards:
    @pytest.mark.asyncio
    async def test_non_iterable_entry_id_payload_hits_type_guard(
        self, full_ctx: dict[str, Any], caplog: pytest.LogCaptureFixture
    ) -> None:
        """A non-iterable ``entry_id`` payload (e.g. an int) trips the payload
        *type* guard specifically and returns without a reload.

        A ``dict`` would be iterable and slip past the type guard into the
        unresolvable-id branch, so an int is used to pin the type guard itself.
        """
        reload_mock = mock.AsyncMock()
        hass = _make_hass()
        hass.config_entries.async_reload = reload_mock  # type: ignore[attr-defined]
        handlers = _register(hass, full_ctx)
        with caplog.at_level("WARNING"):
            await handlers[services.SERVICE_REBUILD_REGISTRY](
                _FakeCall({"entry_id": 123})
            )
        reload_mock.assert_not_awaited()
        assert "Invalid entry_id payload type" in caplog.text

    @pytest.mark.asyncio
    async def test_invalid_device_ids_payload_returns_without_reload(
        self, full_ctx: dict[str, Any], caplog: pytest.LogCaptureFixture
    ) -> None:
        """A non-iterable ``device_ids`` payload trips its type guard and returns
        without a reload."""
        reload_mock = mock.AsyncMock()
        hass = _make_hass()
        hass.config_entries.async_reload = reload_mock  # type: ignore[attr-defined]
        handlers = _register(hass, full_ctx)
        with caplog.at_level("WARNING"):
            await handlers[services.SERVICE_REBUILD_REGISTRY](
                _FakeCall({"device_ids": 123})
            )
        reload_mock.assert_not_awaited()
        assert "Invalid device_ids payload type" in caplog.text


class TestSoundTranslationPlaceholders:
    """Every raised message must be able to render.

    Covers the stop AND the play path: the class claim is only as strong as the
    branches it actually enumerates.

    Home Assistant formats an exception message with
    ``with suppress(KeyError): message.format(**placeholders)``. A template
    whose placeholders the call site does not supply therefore does not fail
    loudly -- the user is simply shown the raw template, braces and all. This
    pins the whole class rather than the one branch that was noticed.
    """

    @staticmethod
    def _template_placeholders(translation_key: str) -> set[str]:
        strings_path = pathlib.Path(services.__file__).parent / "strings.json"
        message = json.loads(strings_path.read_text(encoding="utf-8"))["exceptions"][
            translation_key
        ]["message"]
        return {field for _, field, _, _ in string.Formatter().parse(message) if field}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("call_data", "outcome", "translation_key"),
        [
            ({"device_id": "dev1", "request_uuid": 7}, None, "stop_sound_invalid_uuid"),
            (
                {"device_id": "dev1"},
                StopSoundOutcome.SUPPRESSED,
                "stop_sound_suppressed",
            ),
            ({"device_id": "dev1"}, StopSoundOutcome.FAILED, "stop_sound_rejected"),
            (
                {"device_id": "dev1"},
                StopSoundOutcome.UNCORRELATED,
                "stop_sound_uncorrelated",
            ),
            ({"device_id": "dev1"}, RuntimeError("boom"), "stop_sound_failed"),
        ],
    )
    async def test_every_raised_key_gets_all_its_placeholders(
        self,
        full_ctx: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
        call_data: dict[str, Any],
        outcome: Any,
        translation_key: str,
    ) -> None:
        """Each reachable stop-sound error supplies every placeholder it needs."""

        if isinstance(outcome, Exception):
            stop = mock.AsyncMock(side_effect=outcome)
        else:
            stop = mock.AsyncMock(return_value=outcome)
        coord = SimpleNamespace(
            async_stop_sound=stop,
            classify_stop_correlation=mock.Mock(return_value=None),
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)

        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_STOP_SOUND](_FakeCall(call_data))

        assert excinfo.value.translation_key == translation_key
        supplied = set(excinfo.value.translation_placeholders or {})
        required = self._template_placeholders(translation_key)
        assert required <= supplied, (
            f"{translation_key} would render with a stray literal: "
            f"missing {sorted(required - supplied)}"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("outcome", "translation_key"),
        [
            (PlaySoundOutcome.SUPPRESSED, "play_sound_suppressed"),
            (PlaySoundOutcome.FAILED, "play_sound_rejected"),
            (RuntimeError("boom"), "play_sound_failed"),
        ],
    )
    async def test_play_sound_keys_get_all_their_placeholders(
        self,
        full_ctx: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
        outcome: Any,
        translation_key: str,
    ) -> None:
        """The play path belongs to the same class and was its last gap.

        ``play_sound_failed`` carries an ``{error}`` the suppression branch
        cannot fill. Until button.py stopped swallowing ServiceValidationError
        the defect was invisible; pinning only the stop keys left it standing.

        Parametrised over every reachable play key rather than the one branch
        that was noticed first: ``play_sound_rejected`` joined the class with
        the remedy split, and a guard that grows one method per key is a guard
        that stops growing.
        """

        if isinstance(outcome, Exception):
            play = mock.AsyncMock(side_effect=outcome)
        else:
            play = mock.AsyncMock(return_value=outcome)
        coord = SimpleNamespace(
            async_play_sound=play,
            get_device_display_name=mock.Mock(return_value="Tag"),
        )
        hass = _hass_with_coordinator(coord)
        monkeypatch.setattr(
            services.dr,
            "async_get",
            lambda _h: SimpleNamespace(async_get=lambda _d: None),
        )
        handlers = _register(hass, full_ctx)

        with pytest.raises(ServiceValidationError) as excinfo:
            await handlers[services.SERVICE_PLAY_SOUND](
                _FakeCall({"device_id": "dev1"})
            )

        assert excinfo.value.translation_key == translation_key
        supplied = set(excinfo.value.translation_placeholders or {})
        required = self._template_placeholders(translation_key)
        assert required <= supplied, (
            f"{translation_key} would render with a stray literal: "
            f"missing {sorted(required - supplied)}"
        )


# ---------------------------------------------------------------------------
# Admin registration and the target set of the permission check
# ---------------------------------------------------------------------------


class TestAdminRegistration:
    """Exactly the three maintenance actions go through the admin wrapper."""

    def test_maintenance_services_are_admin_with_passthrough_schema(
        self, full_ctx: dict[str, Any]
    ) -> None:
        registry = FakeServiceRegistry()
        hass = _make_hass()
        hass.services = registry
        coro = services.async_register_services(hass, full_ctx)
        with pytest.raises(StopIteration):
            coro.send(None)

        domain = services.DOMAIN
        admin = {
            (domain, services.SERVICE_REFRESH_DEVICE_URLS),
            (domain, services.SERVICE_REBUILD_DEVICE_REGISTRY),
            (domain, services.SERVICE_REBUILD_REGISTRY),
        }
        # The diagnostic is admin too, but validates its own fields.
        diagnostic = (domain, services.SERVICE_READ_BEACON_STATE)
        assert registry.admin_services == admin | {diagnostic}
        assert registry.schemas[diagnostic]({"device_id": "d"})["timeout"] == 45
        payload = {"mode": "migrate", "entry_id": ["a"], "device_ids": ["d"]}
        for key in admin:
            schema = registry.schemas[key]
            assert schema is not None
            assert schema(dict(payload)) == payload
        for key in set(registry.handlers) - admin - {diagnostic}:
            assert registry.schemas[key] is None


class TestControlTargetSet:
    """Target set of the permission check with the integration's real resolver.

    ``_control_entity_ids_for_target`` is fed the real
    ``_resolve_canonical_from_any`` (the resolver the buttons use), so the
    reverse lookup over canonical ids runs against the production
    normalisation, which the real-Home-Assistant tests cannot reach.
    """

    CANON1_ENTITIES = {"button.canon1_play", "button.canon1_locate"}

    @pytest.fixture
    def resolver_ctx(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
        import custom_components.googlefindmy as integration

        domain = services.DOMAIN
        devices = {
            "dev-canon1": SimpleNamespace(
                id="dev-canon1",
                identifiers={(domain, "entry1:sub1:CANON1")},
                name="Keys",
                name_by_user=None,
                config_entry_id="entry1",
            ),
            "dev-canon2": SimpleNamespace(
                id="dev-canon2",
                identifiers={(domain, "entry1:sub1:CANON2")},
                name="Bag",
                name_by_user=None,
                config_entry_id="entry1",
            ),
        }

        def _entity(entity_id: str, device_id: str, **kw: Any) -> SimpleNamespace:
            return SimpleNamespace(
                entity_id=entity_id,
                platform=kw.get("platform", domain),
                device_id=device_id,
                disabled=kw.get("disabled", False),
            )

        entities = {
            "button.canon1_play": _entity("button.canon1_play", "dev-canon1"),
            "button.canon1_locate": _entity("button.canon1_locate", "dev-canon1"),
            "button.canon1_off": _entity(
                "button.canon1_off", "dev-canon1", disabled=True
            ),
            "sensor.canon1_other": _entity(
                "sensor.canon1_other", "dev-canon1", platform="other"
            ),
            "button.canon2_play": _entity("button.canon2_play", "dev-canon2"),
        }
        dev_reg = SimpleNamespace(async_get=devices.get)
        ent_reg = SimpleNamespace(entities=entities, async_get=entities.get)
        monkeypatch.setattr(services.dr, "async_get", lambda _h: dev_reg)
        monkeypatch.setattr(services.er, "async_get", lambda _h: ent_reg)
        assert integration.dr is services.dr
        assert integration.er is services.er
        return {"resolve_canonical": integration._resolve_canonical_from_any}

    @pytest.mark.parametrize("value", ["dev-canon1", "button.canon1_play", "CANON1"])
    def test_canon1_target_set_for_every_input_form(
        self, resolver_ctx: dict[str, Any], value: str
    ) -> None:
        result = services._control_entity_ids_for_target(object(), value, resolver_ctx)
        assert result == {"button.canon1_play", "button.canon1_locate"}
        assert "button.canon2_play" not in result

    @pytest.mark.parametrize("value", ["dev-canon2", "button.canon2_play", "CANON2"])
    def test_canon2_target_set_for_every_input_form(
        self, resolver_ctx: dict[str, Any], value: str
    ) -> None:
        result = services._control_entity_ids_for_target(object(), value, resolver_ctx)
        assert result == {"button.canon2_play"}

    @pytest.mark.parametrize("value", ["", None, ["dev-canon1"], "UNKNOWN"])
    def test_no_target_yields_empty_set(
        self, resolver_ctx: dict[str, Any], value: Any
    ) -> None:
        assert (
            services._control_entity_ids_for_target(object(), value, resolver_ctx)
            == set()
        )


_DEVICE_HANDLERS = (
    "async_locate_device_service",
    "async_play_sound_service",
    "async_stop_sound_service",
    "async_locate_external_service",
)
# Admin-only: the helper's wrapper authorizes before the handler body runs, so no
# per-device permission check spans its resolution.
_ADMIN_DEVICE_HANDLERS = ("async_read_beacon_state_service",)
_SYNC_RESOLVERS = ("_resolve_runtime_for_device_id", "_iter_runtimes")
_SUSPENDING = (ast.Await, ast.AsyncFor, ast.AsyncWith)


def _start(node: ast.AST) -> tuple[int, int]:
    return (node.lineno, node.col_offset)  # type: ignore[attr-defined]


def _end(node: ast.AST) -> tuple[int, int]:
    return (node.end_lineno or 0, node.end_col_offset or 0)  # type: ignore[attr-defined]


def _awaited_call(node: ast.AST) -> str:
    """Return the source of the call an ``await`` node awaits, else ``""``."""

    if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
        return ast.unparse(node.value.func)
    return ""


def _suspension_violations(source: str) -> list[str]:
    """Return places where control may yield between check and dispatch.

    Positions are compared as (line, column) start and end pairs, so an await
    nested inside the resolver's or the coordinator's arguments counts, and
    the span of each device handler runs from the permission check to the end
    of the first awaited coordinator call.
    """

    functions = {
        node.name: node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    violations: list[str] = []
    for name in _SYNC_RESOLVERS:
        if any(isinstance(n, _SUSPENDING) for n in ast.walk(functions[name])):
            violations.append(name)

    check = functions["_async_authorize_device_call"]
    targets = [
        n
        for n in ast.walk(check)
        if isinstance(n, ast.Call)
        and getattr(n.func, "id", "") == "_control_entity_ids_for_target"
    ]
    if len(targets) != 1:
        violations.append("_async_authorize_device_call: target set not found")
    else:
        for n in ast.walk(check):
            if isinstance(n, _SUSPENDING) and _end(n) > _start(targets[0]):
                violations.append(f"_async_authorize_device_call:{n.lineno}")

    for name in _device_handler_names(source):
        nodes = sorted(
            (n for n in ast.walk(functions[name]) if isinstance(n, _SUSPENDING)),
            key=_start,
        )
        start = next(
            (n for n in nodes if _awaited_call(n) == "_async_authorize_device_call"),
            None,
        )
        resolver = next(
            (n for n in nodes if _awaited_call(n) == "_resolve_runtime_for_device_id"),
            None,
        )
        dispatch = None
        if resolver is not None:
            dispatch = next(
                (
                    n
                    for n in nodes
                    if _start(n) > _start(resolver)
                    and ".coordinator." in _awaited_call(n)
                ),
                None,
            )
        if start is None or resolver is None or dispatch is None:
            violations.append(f"{name}: check, resolver or coordinator await not found")
            continue
        violations.extend(
            f"{name}:{n.lineno}"
            for n in nodes
            if n is not start
            and n is not resolver
            and n is not dispatch
            and _start(n) > _start(start)
            and _end(n) <= _end(dispatch)
        )
    return violations


def _device_handler_names(source: str) -> list[str]:
    """Return the registered handlers that read ``device_id``, in source order."""

    register = next(
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "async_register_services"
    )
    handlers = {
        node.name: node
        for node in ast.walk(register)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    names: list[str] = []
    for node in ast.walk(register):
        if not isinstance(node, ast.Call):
            continue
        func = ast.unparse(node.func)
        if func not in ("hass.services.async_register", "async_register_admin_service"):
            continue
        for arg in node.args:
            if (
                isinstance(arg, ast.Name)
                and arg.id in handlers
                and arg.id not in _ADMIN_DEVICE_HANDLERS
            ):
                if "'device_id'" in ast.unparse(handlers[arg.id]):
                    names.append(arg.id)
    return names


def _first_statement_violations(source: str) -> list[str]:
    """Return device handlers whose first statement is not the permission check."""

    functions = {
        node.name: node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    violations: list[str] = []
    for name in _device_handler_names(source):
        body = functions[name].body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body = body[1:]
        first = body[0] if body else None
        if not (
            isinstance(first, ast.Expr)
            and _awaited_call(first.value) == "_async_authorize_device_call"
        ):
            violations.append(name)
    return violations


class TestNoSuspensionBetweenCheckAndDispatch:
    """Pin the invariant documented on ``_async_authorize_device_call``.

    The permission check and the runtime resolution must see the same registry
    state, so nothing between them may yield to the event loop.
    """

    _SOURCE = pathlib.Path(services.__file__).read_text(encoding="utf-8")

    def test_source_has_no_suspension_point(self) -> None:
        assert _suspension_violations(self._SOURCE) == []

    def test_probe_detects_an_inserted_await(self) -> None:
        anchor = "        runtimes = list(_iter_runtimes(hass))\n"
        assert self._SOURCE.count(anchor) == 1
        mutated = self._SOURCE.replace(
            anchor, "        await asyncio.sleep(0)\n" + anchor, 1
        )
        assert _suspension_violations(mutated) == ["_resolve_runtime_for_device_id"]

    def test_probe_detects_an_await_in_a_handler_span(self) -> None:
        anchor = (
            "        await _async_authorize_device_call(hass, call, "
            'call.data.get("device_id"), ctx)\n'
            '        raw_device_id = call.data.get("device_id")\n'
        )
        assert self._SOURCE.count(anchor) == 4
        mutated = self._SOURCE.replace(
            anchor, anchor + "        await asyncio.sleep(0)\n", 1
        )
        violations = _suspension_violations(mutated)
        assert len(violations) == 1
        assert violations[0].startswith("async_locate_device_service:")

    def test_probe_detects_an_await_between_resolver_and_coordinator(
        self,
    ) -> None:
        anchor = (
            "            runtime, canonical_id = await "
            "_resolve_runtime_for_device_id(raw_device_id)\n"
            "            outcome = await runtime.coordinator.async_play_sound("
        )
        assert self._SOURCE.count(anchor) == 1
        mutated = self._SOURCE.replace(
            anchor,
            anchor.replace(
                "            outcome = await",
                "            await asyncio.sleep(0)\n            outcome = await",
            ),
            1,
        )
        violations = _suspension_violations(mutated)
        assert len(violations) == 1
        assert violations[0].startswith("async_play_sound_service:")

    def test_probe_detects_an_await_in_the_resolver_argument(self) -> None:
        anchor = (
            "            runtime, canonical_id = await "
            "_resolve_runtime_for_device_id(raw_device_id)\n"
            "            outcome = await runtime.coordinator.async_play_sound("
        )
        assert self._SOURCE.count(anchor) == 1
        mutated = self._SOURCE.replace(
            anchor,
            anchor.replace(
                "_resolve_runtime_for_device_id(raw_device_id)",
                "_resolve_runtime_for_device_id(\n"
                "                await asyncio.sleep(0, raw_device_id)\n"
                "            )",
            ),
            1,
        )
        violations = _suspension_violations(mutated)
        assert len(violations) == 1
        assert violations[0].startswith("async_play_sound_service:")

    def test_handler_set_comes_from_the_registrations(self) -> None:
        assert sorted(_device_handler_names(self._SOURCE)) == sorted(_DEVICE_HANDLERS)
        assert _first_statement_violations(self._SOURCE) == []

    def test_probe_sees_an_unlisted_device_handler(self) -> None:
        anchor = (
            "    hass.services.async_register(DOMAIN, SERVICE_PLAY_SOUND, "
            "async_play_sound_service)\n"
        )
        assert self._SOURCE.count(anchor) == 1
        extra = (
            "    async def async_extra_service(call: ServiceCall) -> None:\n"
            '        raw_device_id = call.data.get("device_id")\n'
            "        del raw_device_id\n"
            '    hass.services.async_register(DOMAIN, "extra", async_extra_service)\n'
        )
        mutated = self._SOURCE.replace(anchor, anchor + extra, 1)
        assert "async_extra_service" in _device_handler_names(mutated)
        assert _first_statement_violations(mutated) == ["async_extra_service"]
        assert any(
            v.startswith("async_extra_service:")
            for v in _suspension_violations(mutated)
        )

    def test_probe_detects_an_await_after_the_target_set(self) -> None:
        anchor = "    entity_ids = _control_entity_ids_for_target(hass, raw_device_id, ctx)\n"
        assert self._SOURCE.count(anchor) == 1
        mutated = self._SOURCE.replace(
            anchor, anchor + "    await asyncio.sleep(0)\n", 1
        )
        violations = _suspension_violations(mutated)
        assert len(violations) == 1
        assert violations[0].startswith("_async_authorize_device_call:")


# ---------------------------------------------------------------------------
# K10: an uncorrelated stop names its case
# ---------------------------------------------------------------------------

_STOP_REASON_KEYS = {
    StopSoundUncorrelatedReason.NO_KEY: "stop_sound_no_cancel_key",
    StopSoundUncorrelatedReason.AGED_KEY: "stop_sound_cancel_key_aged",
    StopSoundUncorrelatedReason.FOREIGN_KEY: "stop_sound_cancel_key_foreign",
}


@pytest.mark.asyncio
@pytest.mark.parametrize(("reason", "translation_key"), list(_STOP_REASON_KEYS.items()))
async def test_handler_raises_the_key_of_the_case(
    full_ctx: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    reason: StopSoundUncorrelatedReason,
    translation_key: str,
) -> None:
    """Each case reaches the user as its own error, never as success."""

    order: list[str] = []

    def _classify(device_id: str, request_uuid: str | None) -> Any:
        order.append(f"classify:{device_id}:{request_uuid}")
        return reason

    async def _stop(device_id: str, request_uuid: str | None) -> StopSoundOutcome:
        order.append(f"stop:{device_id}:{request_uuid}")
        return StopSoundOutcome.UNCORRELATED

    coord = SimpleNamespace(
        async_stop_sound=_stop,
        classify_stop_correlation=_classify,
        get_device_display_name=mock.Mock(return_value="Tag"),
    )
    hass = _hass_with_coordinator(coord)
    monkeypatch.setattr(
        services.dr, "async_get", lambda _h: SimpleNamespace(async_get=lambda _d: None)
    )
    handlers = _register(hass, full_ctx)

    with pytest.raises(ServiceValidationError) as excinfo:
        await handlers[services.SERVICE_STOP_SOUND](
            _FakeCall({"device_id": "dev1", "request_uuid": "k"})
        )

    assert excinfo.value.translation_key == translation_key
    # Read before the stop, with the same arguments the stop receives.
    assert order == ["classify:CANON:k", "stop:CANON:k"]
    message = json.loads(
        (pathlib.Path(services.__file__).parent / "strings.json").read_text(
            encoding="utf-8"
        )
    )["exceptions"][translation_key]["message"]
    required = {f for _, f, _, _ in string.Formatter().parse(message) if f}
    assert required <= set(excinfo.value.translation_placeholders or {})


@pytest.mark.asyncio
async def test_a_correlated_stop_ignores_the_case(
    full_ctx: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The case only selects a message; a CANCELLED stop still succeeds."""

    coord = SimpleNamespace(
        async_stop_sound=mock.AsyncMock(return_value=StopSoundOutcome.CANCELLED),
        classify_stop_correlation=mock.Mock(
            return_value=StopSoundUncorrelatedReason.NO_KEY
        ),
        get_device_display_name=mock.Mock(return_value="Tag"),
    )
    hass = _hass_with_coordinator(coord)
    monkeypatch.setattr(
        services.dr, "async_get", lambda _h: SimpleNamespace(async_get=lambda _d: None)
    )
    handlers = _register(hass, full_ctx)

    await handlers[services.SERVICE_STOP_SOUND](_FakeCall({"device_id": "dev1"}))
