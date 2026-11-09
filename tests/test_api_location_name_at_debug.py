# tests/test_api_location_name_at_debug.py
"""Device names stay at DEBUG in the failure records of the location lookup.

``GoogleFindMyAPI.async_get_device_location`` runs on every poll cycle, not only
when somebody presses a button. AGENTS.md section 5 (b) therefore wants its
records at INFO and above to name the device by id, with the user-chosen
display name in a DEBUG companion record. This module pins that for each of the
eleven failure branches that log at WARNING or ERROR: the stubbed
``get_location_data_for_device`` raises the branch's exception, and the test
checks that

(a) no record at INFO or above carries the display name, neither in the
    rendered message nor in ``repr(record.args)``;
(b) a record at the branch's level carries the device id;
(c) exactly one DEBUG record with the prefix ``Location lookup failure for
    device`` carries both the id and the display name;
(d) the branch still returns or raises what it did before.

What it does not cover: records logged by the callers (``coordinator/polling``
and ``coordinator/locate``), the DEBUG records of the success path, the
client-error branch that already logs at DEBUG, and the sync wrapper
``get_device_location``, whose error context is built by ``_run_sync_helper``.
A display name that reaches a record through an exception message is not
followed either; the stubbed exceptions carry no name.

Retire this module when the eleven branches stop logging at INFO or above, or
when ``tests/test_guard_unattended_device_names.py`` checks the records of
``async_get_device_location`` at runtime as well.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from aiohttp import ClientError
from homeassistant.exceptions import ConfigEntryAuthFailed

from custom_components.googlefindmy import api as api_module
from custom_components.googlefindmy.api import GoogleFindMyAPI
from custom_components.googlefindmy.NovaApi.nova_request import (
    NovaAuthError,
    NovaAuthPermanentError,
    NovaHTTPError,
    NovaLogicError,
    NovaProtobufDecodeError,
    NovaRateLimitError,
)
from tests.helpers.api_stub import StubCache

DEVICE_ID = "dev-sentinel-1"
DEVICE_NAME = "Sentinel Hall Tag"
COMPANION_PREFIX = "Location lookup failure for device"

# (case id, exception raised by the stub, expected level, expected outcome)
# The outcome is either the exception type the branch raises or ``None`` for
# the branches that return an empty dict.
CASES: list[tuple[str, BaseException, int, type[BaseException] | None]] = [
    (
        "auth-permanent-subclass",
        NovaAuthPermanentError(401, "perm"),
        logging.ERROR,
        ConfigEntryAuthFailed,
    ),
    (
        "auth-permanent-flag",
        NovaAuthError(401, "perm", is_permanent=True),
        logging.ERROR,
        ConfigEntryAuthFailed,
    ),
    ("auth-transient", NovaAuthError(401, "transient"), logging.WARNING, NovaAuthError),
    ("http-401", NovaHTTPError(401, "http"), logging.ERROR, ConfigEntryAuthFailed),
    ("http-500", NovaHTTPError(500, "http"), logging.WARNING, None),
    ("rate-limit", NovaRateLimitError("rate"), logging.WARNING, None),
    ("protobuf-decode", NovaProtobufDecodeError("decode"), logging.ERROR, None),
    ("nova-logic", NovaLogicError(5, "logic"), logging.ERROR, None),
    ("network", ClientError("network"), logging.ERROR, None),
    ("runtime", RuntimeError("runtime"), logging.ERROR, None),
    ("other", ValueError("other"), logging.ERROR, None),
]


def _record_text(record: logging.LogRecord) -> str:
    return f"{record.getMessage()} {record.args!r}"


@pytest.mark.parametrize(
    ("exc", "level", "outcome"),
    [pytest.param(exc, level, outcome, id=case) for case, exc, level, outcome in CASES],
)
@pytest.mark.asyncio
async def test_failure_records_name_the_device_by_id(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    exc: BaseException,
    level: int,
    outcome: type[BaseException] | None,
) -> None:
    async def _raise(*_a: Any, **_k: Any) -> list[Any]:
        raise exc

    monkeypatch.setattr(api_module, "get_location_data_for_device", _raise)
    api = GoogleFindMyAPI(cache=StubCache(entry_id="entry-1"), session=None)

    with caplog.at_level(logging.DEBUG):
        if outcome is None:
            assert await api.async_get_device_location(DEVICE_ID, DEVICE_NAME) == {}
        else:
            with pytest.raises(outcome):
                await api.async_get_device_location(DEVICE_ID, DEVICE_NAME)

    loud = [r for r in caplog.records if r.levelno >= logging.INFO]
    assert [r for r in loud if DEVICE_NAME in _record_text(r)] == []
    assert [r for r in loud if r.levelno == level and DEVICE_ID in r.getMessage()]

    companions = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and r.getMessage().startswith(COMPANION_PREFIX)
    ]
    assert len(companions) == 1
    assert DEVICE_ID in companions[0].getMessage()
    assert DEVICE_NAME in companions[0].getMessage()
