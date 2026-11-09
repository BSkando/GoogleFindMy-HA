# tests/test_decoder_canonicless_counts.py
"""Transition-independent canonicless drop counters (P1-2a).

The existing WARNING logic counts only warn-worthy AND transition-affected drops
(``warn_worthy = (not benign) or was_visible``) and de-duplicates them across
polls. For the diagnostics aggregate we need a separate, *deterministic* tally:

* ``total`` -- every canonicless drop in the main poll,
* ``benign`` -- ``is_android_device or not has_information_block`` drops,
* ``warn`` -- the remaining (tracker-shaped) drops,

with the invariant ``total == benign + warn`` and **no** ``was_visible``
component. The counters live in the emit-gated drop block, so only the main poll
(``emit_canonicless_diagnostics=True``) writes them; the capability probe
(``False``) must leave them untouched (CQS / V6).

The decoder exposes the per-entry tally via
``decoder.get_canonicless_counts(entry_id)`` (entry-scoped module state mirroring
``_last_canonicless_count_by_entry``), reset by
``decoder._reset_canonicless_warning_state``.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2, decoder


@pytest.fixture(autouse=True)
def _reset_canonicless_warning_state() -> None:
    """Clear all process-wide canonicless guards (incl. the new counts) per test."""
    decoder._reset_canonicless_warning_state()


def _make_phone_device(
    *,
    name: str,
    canonic_ids: list[SimpleNamespace] | None = None,
) -> SimpleNamespace:
    """Benign class: Android phone with a configurable (default empty) ID list."""
    return SimpleNamespace(
        identifierInformation=SimpleNamespace(
            type=DeviceUpdate_pb2.IDENTIFIER_ANDROID,
            phoneInformation=SimpleNamespace(
                canonicIds=SimpleNamespace(canonicId=canonic_ids or [])
            ),
        ),
        userDefinedDeviceName=name,
        imageInformation=SimpleNamespace(url=""),
        HasField=lambda field_name: (
            field_name not in ("information", "deviceRegistration")
        ),
        ListFields=lambda: [
            (SimpleNamespace(name="identifierInformation"), None),
            (SimpleNamespace(name="userDefinedDeviceName"), None),
            (SimpleNamespace(name="imageInformation"), None),
        ],
    )


def _make_buds_device(*, name: str) -> SimpleNamespace:
    """Benign class: reduced-listing accessory with no information block."""
    return SimpleNamespace(
        identifierInformation=SimpleNamespace(
            type=DeviceUpdate_pb2.IDENTIFIER_UNKNOWN,
            canonicIds=SimpleNamespace(canonicId=[]),
        ),
        userDefinedDeviceName=name,
        imageInformation=SimpleNamespace(url=""),
        HasField=lambda field_name: False,
        ListFields=lambda: [
            (SimpleNamespace(name="identifierInformation"), None),
            (SimpleNamespace(name="userDefinedDeviceName"), None),
            (SimpleNamespace(name="imageInformation"), None),
        ],
    )


def _make_tracker_device(*, name: str) -> SimpleNamespace:
    """Warn-worthy class: non-Android device with an information block."""
    information = SimpleNamespace(
        HasField=lambda field_name: False,
        ListFields=lambda: [],
    )
    return SimpleNamespace(
        identifierInformation=SimpleNamespace(
            type=DeviceUpdate_pb2.IDENTIFIER_SPOT,
            canonicIds=SimpleNamespace(canonicId=[]),
        ),
        information=information,
        userDefinedDeviceName=name,
        imageInformation=SimpleNamespace(url=""),
        HasField=lambda field_name: field_name == "information",
        ListFields=lambda: [
            (SimpleNamespace(name="identifierInformation"), None),
            (SimpleNamespace(name="information"), None),
            (SimpleNamespace(name="userDefinedDeviceName"), None),
        ],
    )


def _canonicless_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the rendered canonicless WARNING messages."""
    return [
        rec.getMessage()
        for rec in caplog.records
        if rec.levelno == logging.WARNING and "canonical ID" in rec.getMessage()
    ]


# ---------------------------------------------------------------------------
# Split tally: total == benign + warn, transition-independent
# ---------------------------------------------------------------------------


def test_counts_split_total_benign_warn() -> None:
    """A2: N benign + M tracker drops give total=N+M, benign=N, warn=M."""
    device_list = SimpleNamespace(
        deviceMetadata=[
            _make_phone_device(name="Phone"),  # benign
            _make_buds_device(name="Buds"),  # benign
            _make_tracker_device(name="Tracker A"),  # warn
            _make_tracker_device(name="Tracker B"),  # warn
        ]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    decoder.get_devices_with_location(
        device_list, cache=cache, emit_canonicless_diagnostics=True
    )

    counts = decoder.get_canonicless_counts("entry-A")
    assert counts["total"] == 4
    assert counts["benign"] == 2
    assert counts["warn"] == 2
    assert counts["total"] == counts["benign"] + counts["warn"]


def test_counts_are_transition_independent() -> None:
    """A2: a previously-visible benign drop counts as benign, not warn.

    The existing WARNING logic would treat the "was visible, now gone" phone as
    warn-worthy; the diagnostics tally must NOT -- benign stays benign regardless
    of the transition.
    """
    cache = SimpleNamespace(entry_id="entry-A")
    visible = SimpleNamespace(
        deviceMetadata=[
            _make_phone_device(name="X", canonic_ids=[SimpleNamespace(id="cid-x")])
        ]
    )
    dropped = SimpleNamespace(deviceMetadata=[_make_phone_device(name="X")])

    decoder.get_devices_with_location(
        visible, cache=cache, emit_canonicless_diagnostics=True
    )
    decoder.get_devices_with_location(
        dropped, cache=cache, emit_canonicless_diagnostics=True
    )

    counts = decoder.get_canonicless_counts("entry-A")
    assert counts["total"] == 1
    assert counts["benign"] == 1
    assert counts["warn"] == 0


def test_counts_default_zero_for_unknown_entry() -> None:
    """A2: an entry that never dropped a device reports all-zero counts."""
    counts = decoder.get_canonicless_counts("never-seen")
    assert counts == {"total": 0, "benign": 0, "warn": 0}


def test_counts_absolute_per_poll_not_accumulated() -> None:
    """A2: each main poll overwrites the tally (absolute, not incremental)."""
    cache = SimpleNamespace(entry_id="entry-A")
    two = SimpleNamespace(
        deviceMetadata=[
            _make_tracker_device(name="A"),
            _make_tracker_device(name="B"),
        ]
    )
    one = SimpleNamespace(deviceMetadata=[_make_tracker_device(name="A")])

    decoder.get_devices_with_location(
        two, cache=cache, emit_canonicless_diagnostics=True
    )
    assert decoder.get_canonicless_counts("entry-A")["warn"] == 2

    decoder.get_devices_with_location(
        one, cache=cache, emit_canonicless_diagnostics=True
    )
    assert decoder.get_canonicless_counts("entry-A")["warn"] == 1
    assert decoder.get_canonicless_counts("entry-A")["total"] == 1


# ---------------------------------------------------------------------------
# CQS: the capability probe (emit=False) must not touch the counters (V6)
# ---------------------------------------------------------------------------


def test_probe_pass_does_not_write_counts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """V6: emit=False leaves the tally at zero; the emit=True poll then fills it."""
    device_list = SimpleNamespace(
        deviceMetadata=[
            _make_phone_device(name="Phone"),
            _make_tracker_device(name="Tracker"),
        ]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        # Capability probe: silent and counter-free.
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=False
        )
        assert decoder.get_canonicless_counts("entry-A") == {
            "total": 0,
            "benign": 0,
            "warn": 0,
        }
        assert _canonicless_warnings(caplog) == []

        # Main poll: fills the tally exactly once (no double counting).
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )

    counts = decoder.get_canonicless_counts("entry-A")
    assert counts == {"total": 2, "benign": 1, "warn": 1}


def test_reset_clears_counts() -> None:
    """The argument-free reset clears the counts map (test isolation)."""
    cache = SimpleNamespace(entry_id="entry-A")
    device_list = SimpleNamespace(deviceMetadata=[_make_tracker_device(name="Tracker")])
    decoder.get_devices_with_location(
        device_list, cache=cache, emit_canonicless_diagnostics=True
    )
    assert decoder.get_canonicless_counts("entry-A")["total"] == 1

    decoder._reset_canonicless_warning_state()
    assert decoder.get_canonicless_counts("entry-A") == {
        "total": 0,
        "benign": 0,
        "warn": 0,
    }


def test_entry_scoped_reset_isolates_counts() -> None:
    """An entry-scoped reset clears only that entry's tally."""
    list_a = SimpleNamespace(deviceMetadata=[_make_tracker_device(name="A")])
    list_b = SimpleNamespace(deviceMetadata=[_make_tracker_device(name="B")])
    decoder.get_devices_with_location(
        list_a,
        cache=SimpleNamespace(entry_id="entry-A"),
        emit_canonicless_diagnostics=True,
    )
    decoder.get_devices_with_location(
        list_b,
        cache=SimpleNamespace(entry_id="entry-B"),
        emit_canonicless_diagnostics=True,
    )

    decoder._reset_canonicless_warning_state("entry-A")

    assert decoder.get_canonicless_counts("entry-A")["total"] == 0
    assert decoder.get_canonicless_counts("entry-B")["total"] == 1


# ---------------------------------------------------------------------------
# Dropped names for the stale-device removal (S-R3d)
# ---------------------------------------------------------------------------


def test_names_of_every_drop_are_recorded_per_poll() -> None:
    """S-R3d: every canonicless drop leaves its name, benign class included.

    The stale-device removal must not take a device that Google still lists,
    only without a canonical id, for one that left the account. The class
    tally cannot tell: a phone that was a tracker device before drops as
    ``benign``. Its name is the only trace, and each main poll replaces it.
    """
    cache = SimpleNamespace(entry_id="entry-A")
    first = SimpleNamespace(
        deviceMetadata=[
            _make_phone_device(name="Phone"),
            _make_buds_device(name="Buds"),
            _make_tracker_device(name="Tracker A"),
            _make_phone_device(
                name="Visible", canonic_ids=[SimpleNamespace(id="cid-v")]
            ),
            _make_buds_device(name="Buds"),
        ]
    )
    decoder.get_devices_with_location(
        first, cache=cache, emit_canonicless_diagnostics=True
    )
    # Two devices share the name ``Buds``: both drops count (R2-6). Each drop
    # carries the class of its device, so a role swap of two same-named
    # devices of different classes shows (R3-5).
    assert decoder.get_canonicless_name_counts("entry-A") == {
        ("Phone", "android"): 1,
        ("Buds", "no_information"): 2,
        ("Tracker A", "tracker"): 1,
    }

    second = SimpleNamespace(deviceMetadata=[_make_buds_device(name="Buds")])
    decoder.get_devices_with_location(
        second, cache=cache, emit_canonicless_diagnostics=True
    )
    buds = ("Buds", "no_information")
    assert decoder.get_canonicless_name_counts("entry-A") == {buds: 1}
    copy = decoder.get_canonicless_name_counts("entry-A")
    copy[buds] = 9
    assert decoder.get_canonicless_name_counts("entry-A") == {buds: 1}


def test_names_follow_the_probe_rule_and_the_resets() -> None:
    """S-R3d: the probe writes no names; both reset forms clear them."""
    device_list = SimpleNamespace(deviceMetadata=[_make_buds_device(name="Buds")])
    assert decoder.get_canonicless_name_counts("entry-A") == {}

    decoder.get_devices_with_location(
        device_list,
        cache=SimpleNamespace(entry_id="entry-A"),
        emit_canonicless_diagnostics=False,
    )
    assert decoder.get_canonicless_name_counts("entry-A") == {}

    for entry_id in ("entry-A", "entry-B"):
        decoder.get_devices_with_location(
            device_list,
            cache=SimpleNamespace(entry_id=entry_id),
            emit_canonicless_diagnostics=True,
        )
    decoder._reset_canonicless_warning_state("entry-A")
    assert decoder.get_canonicless_name_counts("entry-A") == {}
    assert decoder.get_canonicless_name_counts("entry-B") == {
        ("Buds", "no_information"): 1
    }

    decoder._reset_canonicless_warning_state()
    assert decoder.get_canonicless_name_counts("entry-B") == {}
