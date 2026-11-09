# tests/test_stop_sound_uncorrelated_reason.py
"""Pin K10: an uncorrelated stop names its case instead of listing every cause.

``StopSoundOutcome.UNCORRELATED`` stays one outcome and stays an error for the
caller (BSkando#195). What changed is the message: "no key", "aged key" and
"foreign key" have different remedies, so each gets its own translation key.
The case is read by ``classify_stop_correlation`` from the same state that
``async_stop_sound`` decides on; these tests pin that both agree, that the
service handler reads the case before the stop, and that every new key is
present, complete and localized.
"""

from __future__ import annotations

import json
import pathlib
import time
from typing import Any

import pytest

from custom_components.googlefindmy import services
from custom_components.googlefindmy.const import (
    StopSoundOutcome,
    StopSoundUncorrelatedReason,
)
from custom_components.googlefindmy.coordinator.helpers.cache import (
    SOUND_UUID_MAX_AGE_S,
)
from tests.test_stop_sound_keep_key_and_defer import _coordinator, events  # noqa: F401

_DEV = "dev-1"
_OWN = "own-key"
_NEW_KEYS = {
    StopSoundUncorrelatedReason.NO_KEY: "stop_sound_no_cancel_key",
    StopSoundUncorrelatedReason.AGED_KEY: "stop_sound_cancel_key_aged",
    StopSoundUncorrelatedReason.FOREIGN_KEY: "stop_sound_cancel_key_foreign",
}
_COMPONENT = pathlib.Path(services.__file__).parent


def _aged(coordinator: Any) -> None:
    coordinator._sound_request_timestamps[_DEV] = (
        time.time() - SOUND_UUID_MAX_AGE_S - 60
    )


# (cached key, aged?, supplied key, expected reason)
_CASES = [
    pytest.param(_OWN, False, None, None, id="own-fresh-implicit"),
    pytest.param(_OWN, False, _OWN, None, id="own-fresh-supplied"),
    pytest.param(None, False, None, StopSoundUncorrelatedReason.NO_KEY, id="no-key"),
    pytest.param(None, False, "  ", StopSoundUncorrelatedReason.NO_KEY, id="blank"),
    pytest.param(
        _OWN, True, None, StopSoundUncorrelatedReason.AGED_KEY, id="aged-implicit"
    ),
    pytest.param(
        _OWN, True, _OWN, StopSoundUncorrelatedReason.AGED_KEY, id="aged-supplied"
    ),
    pytest.param(
        _OWN, False, "other", StopSoundUncorrelatedReason.FOREIGN_KEY, id="foreign"
    ),
    pytest.param(
        None,
        False,
        "other",
        StopSoundUncorrelatedReason.FOREIGN_KEY,
        id="foreign-no-cache",
    ),
]


@pytest.mark.asyncio
@pytest.mark.usefixtures("events")
@pytest.mark.parametrize(("cached", "aged", "supplied", "expected"), _CASES)
async def test_reason_matches_the_outcome_of_the_stop(
    cached: str | None,
    aged: bool,
    supplied: str | None,
    expected: StopSoundUncorrelatedReason | None,
) -> None:
    """``None`` exactly when the stop is CANCELLED; otherwise the named case."""

    coordinator = _coordinator([], uuids={_DEV: cached} if cached else None)
    if aged:
        _aged(coordinator)

    reason = coordinator.classify_stop_correlation(_DEV, supplied)
    outcome = await coordinator.async_stop_sound(_DEV, supplied)

    assert reason is expected
    if expected is None:
        assert outcome is StopSoundOutcome.CANCELLED
    else:
        assert outcome is StopSoundOutcome.UNCORRELATED


@pytest.mark.asyncio
@pytest.mark.usefixtures("events")
async def test_a_repeated_stop_with_our_key_is_not_a_case() -> None:
    """Since the key is kept, a second stop is correlated again (K9 meets K10)."""

    coordinator = _coordinator([], uuids={_DEV: _OWN})
    await coordinator.async_stop_sound(_DEV, None)

    assert coordinator.classify_stop_correlation(_DEV, None) is None


def test_every_reason_has_its_own_message() -> None:
    """A new reason without a message would silently fall back to the generic text."""

    assert set(services._STOP_UNCORRELATED_MESSAGES) == set(StopSoundUncorrelatedReason)
    assert {key for _, key in services._STOP_UNCORRELATED_MESSAGES.values()} == set(
        _NEW_KEYS.values()
    )


def test_new_keys_exist_in_every_language_and_are_localized() -> None:
    """strings.json equals en.json; the nine other files carry their own text."""

    strings = json.loads((_COMPONENT / "strings.json").read_text(encoding="utf-8"))
    files = sorted((_COMPONENT / "translations").glob("*.json"))
    assert len(files) == 10
    loaded = {f.stem: json.loads(f.read_text(encoding="utf-8")) for f in files}
    for key in _NEW_KEYS.values():
        english = strings["exceptions"][key]["message"]
        assert loaded["en"]["exceptions"][key]["message"] == english
        for lang, data in loaded.items():
            text = data["exceptions"][key]["message"]
            assert "{device_id}" in text, (lang, key)
            if lang != "en":
                assert text != english, (lang, key)


def test_aged_message_names_the_actual_age_limit() -> None:
    """The aged text says "30 minutes"; it must follow the constant it describes."""

    strings = json.loads((_COMPONENT / "strings.json").read_text(encoding="utf-8"))
    minutes = SOUND_UUID_MAX_AGE_S // 60
    assert (
        f"{minutes} minutes"
        in strings["exceptions"]["stop_sound_cancel_key_aged"]["message"]
    )
    assert (
        f"{minutes} minutes"
        in services._STOP_UNCORRELATED_MESSAGES[StopSoundUncorrelatedReason.AGED_KEY][0]
    )
