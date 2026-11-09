# tests/test_decoder_canonicless_device_warning.py
"""Severity of the canonicless-device signal is coupled to actionability.

When Google's device list contains a device but provides no canonical ID for it
(empty ``canonicIds`` list, or only blank/invalid IDs), ``get_devices_with_location``
emits zero rows for that device, so it silently disappears from Home Assistant.

Not every such drop is actionable, though, and a class-blind WARNING trains the
operator to ignore the signal (alert fatigue). These tests pin a severity policy that
tracks *handling relevance*:

* **Benign classes** (Android phones and reduced-listing Bluetooth accessories such as
  Pixel Buds) are not warn-worthy for their device class and therefore produce **only a
  DEBUG line**, never a WARNING, as long as they were not previously visible. The class
  is a guess from the payload shape (a Wear OS watch can land in it, BSkando#224), so
  that line asks for an issue instead of saying no action is required.
* **Warn-worthy drops** still raise the de-duplicated, count-only WARNING:
  a tracker-shaped device (has an ``information`` block, not Android) that lacks a
  canonical ID, OR any device that was visible in the previous run and now disappears
  (a real regression). The per-device name stays at DEBUG (privacy).

Valid devices stay silent on both tiers (no behavioural change to the happy path).

Command-Query-Separation contract (``emit_canonicless_diagnostics``):
    ``get_devices_with_location`` runs twice per poll with the same cache: once as a
    capability probe (``api.py:361``, ``_build_can_ring_index``) and once as the main
    poll (``api.py:702``). Only the main poll passes ``emit_canonicless_diagnostics=True``
    and is allowed to emit the canonicless diagnostics (DEBUG lines, the count WARNING)
    and to mutate the module-level visibility/count maps. The capability probe runs with
    the default ``False`` and is diagnostically silent and side-effect free, so the
    main poll reads the correct previous-poll visibility set instead of one already
    overwritten by the probe pass.
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Callable
from types import SimpleNamespace

import pytest

from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2, decoder


@pytest.fixture(autouse=True)
def _reset_canonicless_warning_state() -> None:
    """Clear the process-wide canonicless guards before each test.

    The decoder de-duplicates the canonicless WARNING via a module-level count map and
    tracks previously-visible device names in a second module-level map. The argument-free
    reset clears both, so every test stays order-independent.
    """
    decoder._reset_canonicless_warning_state()


def _make_phone_device(
    *,
    name: str,
    canonic_ids: list[SimpleNamespace],
) -> SimpleNamespace:
    """Build a minimal Android phone device (benign class) with a configurable ID list."""
    return SimpleNamespace(
        identifierInformation=SimpleNamespace(
            type=DeviceUpdate_pb2.IDENTIFIER_ANDROID,
            phoneInformation=SimpleNamespace(
                canonicIds=SimpleNamespace(canonicId=canonic_ids)
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


def _make_buds_device(
    *,
    name: str,
    canonic_ids: list[SimpleNamespace] | None = None,
) -> SimpleNamespace:
    """Build a reduced-listing Bluetooth accessory (benign class, no information block).

    Mirrors the live Pixel Buds payload: ``identifierInformation`` plus a user-defined
    name, but no ``information`` block (so ``HasField("information")`` is False) and an
    empty generic canonic-ID list.
    """
    return SimpleNamespace(
        identifierInformation=SimpleNamespace(
            type=DeviceUpdate_pb2.IDENTIFIER_UNKNOWN,
            canonicIds=SimpleNamespace(canonicId=canonic_ids or []),
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


def _make_tracker_device(
    *,
    name: str,
    canonic_ids: list[SimpleNamespace] | None = None,
) -> SimpleNamespace:
    """Build a tracker-shaped device (warn-worthy class): non-Android, has an info block.

    The ``information`` substructure is present (``HasField("information")`` True) so the
    device is NOT in the benign ``not has_information_block`` class, but its nested
    ``HasField`` returns False and ``ListFields`` is empty so the DEBUG dump and the
    registration path stay inert. With ``cache=None`` no decryption is attempted.
    """
    information = SimpleNamespace(
        HasField=lambda field_name: False,
        ListFields=lambda: [],
    )
    return SimpleNamespace(
        identifierInformation=SimpleNamespace(
            type=DeviceUpdate_pb2.IDENTIFIER_SPOT,
            canonicIds=SimpleNamespace(canonicId=canonic_ids or []),
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
    """Return WARNING messages that announce a canonicless device drop (rendered)."""
    return [
        rec.getMessage()
        for rec in caplog.records
        if rec.levelno == logging.WARNING and "canonical ID" in rec.getMessage()
    ]


def _canonicless_debug_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return DEBUG messages that name a canonicless device (rendered)."""
    return [
        rec.getMessage()
        for rec in caplog.records
        if rec.levelno == logging.DEBUG and "canonical ID" in rec.getMessage()
    ]


def _missing_key_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the ``DEBUG STRUCTURE: Missing Key`` WARNING records (rendered).

    These are the hidden-key diagnostic dump emitted for a tracker-shaped device that
    lacks an ``encryptedIdentityKey``. Like the canonicless diagnostics, they are a
    main-poll concern: the capability probe pass must stay diagnostically silent.
    """
    return [
        rec.getMessage()
        for rec in caplog.records
        if rec.levelno == logging.WARNING and "Missing Key" in rec.getMessage()
    ]


# ----------------------------------------------------------------------------------------
# Benign classes (phone, accessory): silent WARNING tier, named DEBUG line, no remediation
#
# These model the MAIN poll, so they pass emit_canonicless_diagnostics=True: only the main
# poll is allowed to emit the per-device DEBUG line the benign tier asserts.
# ----------------------------------------------------------------------------------------


def test_phone_canonicless_never_visible_is_silent_with_benign_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A never-before-visible phone with an empty canonic list warns NOT, only DEBUG (i).

    The benign DEBUG line names the device but must not carry the actionable remediation
    ("reload" / "findable again"), which would be misleading for a phone whose ID is known
    via the Locate flow.
    """
    device = _make_phone_device(name="Lost Pixel", canonic_ids=[])
    device_list = SimpleNamespace(deviceMetadata=[device])

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        results = decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=True
        )

    assert results == []
    assert _canonicless_warnings(caplog) == []

    debug_lines = _canonicless_debug_lines(caplog)
    assert any("Lost Pixel" in line for line in debug_lines)
    benign = [line for line in debug_lines if "Lost Pixel" in line]
    assert benign, "expected a benign DEBUG line naming the device"
    for line in benign:
        assert "reload" not in line
        assert "findable again" not in line


def test_buds_canonicless_never_visible_is_silent_with_benign_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A never-visible Bluetooth accessory (no information block) warns NOT, only DEBUG (ii)."""
    device = _make_buds_device(name="Pixel Buds Pro")
    device_list = SimpleNamespace(deviceMetadata=[device])

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        results = decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=True
        )

    assert results == []
    assert _canonicless_warnings(caplog) == []
    debug_lines = _canonicless_debug_lines(caplog)
    assert any("Pixel Buds Pro" in line for line in debug_lines)
    for line in [d for d in debug_lines if "Pixel Buds Pro" in d]:
        assert "reload" not in line
        assert "findable again" not in line


def test_blank_canonic_id_phone_is_silent_when_never_visible(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A blank-only ID list on a benign phone is treated as canonicless and stays silent."""
    device = _make_phone_device(
        name="Blank ID Phone", canonic_ids=[SimpleNamespace(id="")]
    )
    device_list = SimpleNamespace(deviceMetadata=[device])

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        results = decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=True
        )

    assert results == []
    assert _canonicless_warnings(caplog) == []
    assert any("Blank ID Phone" in line for line in _canonicless_debug_lines(caplog))


def test_never_visible_benign_device_no_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Explicit (v): a never-visible benign device produces zero WARNING records.

    Left at the default emit_canonicless_diagnostics=False so it additionally pins the
    capability-probe pass as WARNING-silent.
    """
    device_list = SimpleNamespace(deviceMetadata=[_make_buds_device(name="Earbuds")])
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(device_list, cache=cache)

    assert _canonicless_warnings(caplog) == []


@pytest.mark.parametrize(
    "make_device",
    [
        pytest.param(
            lambda: _make_phone_device(name="Benign Device", canonic_ids=[]),
            id="android-phone",
        ),
        pytest.param(
            lambda: _make_buds_device(name="Benign Device"),
            id="no-information-block",
        ),
    ],
)
def test_benign_debug_line_asks_for_a_report_instead_of_no_action(
    caplog: pytest.LogCaptureFixture,
    make_device: Callable[[], SimpleNamespace],
) -> None:
    """The benign DEBUG line must not wave the drop off as needing no action.

    The benign class is inferred from the payload shape. A Wear OS watch that Find My
    Device locates and rings on its own arrives without an ``information`` block and
    lands in the same class (BSkando/GoogleFindMy-HA#224). The line therefore names the
    common cases, excludes phones and tablets, and asks for an issue with the
    ``IDENTIFIER STRUCTURE`` line instead of promising that nothing needs to be done.
    The tier and the count stay as before: one DEBUG line, no WARNING, no remediation.
    """
    device_list = SimpleNamespace(deviceMetadata=[make_device()])

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        results = decoder.get_devices_with_location(
            device_list, cache=None, emit_canonicless_diagnostics=True
        )

    assert results == []
    assert _canonicless_warnings(caplog) == []

    records = [
        rec
        for rec in caplog.records
        if "canonical ID" in rec.getMessage() and "Benign Device" in rec.getMessage()
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    line = records[0].getMessage()
    for forbidden in (
        "no action is required",
        "phones and Bluetooth accessories",
        "reload",
        "findable again",
    ):
        assert forbidden not in line
    for required in (
        "open an issue",
        "'IDENTIFIER STRUCTURE'",
        "not a phone or tablet",
        "on its own",
    ):
        assert required in line


def test_warn_worthy_debug_line_carries_no_report_request(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The warn-worthy DEBUG line keeps its remediation and asks for no issue."""
    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Garage Tracker")]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )

    lines = [
        line for line in _canonicless_debug_lines(caplog) if "Garage Tracker" in line
    ]
    assert len(lines) == 1
    assert "reload the integration" in lines[0]
    assert "open an issue" not in lines[0]
    assert "IDENTIFIER STRUCTURE" not in lines[0]


# ----------------------------------------------------------------------------------------
# Warn-worthy: tracker-shaped drop, and previously-visible device disappearing
# ----------------------------------------------------------------------------------------


def test_tracker_canonicless_emits_count_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A tracker-shaped device without a canonical ID warns once with count 1 (iii).

    The WARNING reports the count, keeps the actionable remediation ("reload the
    integration"), and must NOT leak the device name; the name appears only at DEBUG.
    """
    device = _make_tracker_device(name="Garage Tracker")
    device_list = SimpleNamespace(deviceMetadata=[device])
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        results = decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )

    assert results == []
    warnings = _canonicless_warnings(caplog)
    assert len(warnings) == 1
    assert "1" in warnings[0]
    assert "Garage Tracker" not in warnings[0]
    assert "reload the integration" in warnings[0]
    assert any("Garage Tracker" in line for line in _canonicless_debug_lines(caplog))


def test_previously_visible_benign_device_drop_emits_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A benign device that was visible and now drops is warn-worthy (iv).

    Even though a phone is the benign class, a real disappearance ("was visible, now
    gone") is a regression and must surface exactly one WARNING. Both calls model main
    polls (emit_canonicless_diagnostics=True).
    """
    cache = SimpleNamespace(entry_id="entry-A")
    visible = SimpleNamespace(
        deviceMetadata=[
            _make_phone_device(name="X", canonic_ids=[SimpleNamespace(id="cid-x")])
        ]
    )
    dropped = SimpleNamespace(
        deviceMetadata=[_make_phone_device(name="X", canonic_ids=[])]
    )

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        first = decoder.get_devices_with_location(
            visible, cache=cache, emit_canonicless_diagnostics=True
        )
        assert len(first) == 1
        decoder.get_devices_with_location(
            dropped, cache=cache, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 1


def test_previously_visible_benign_device_drop_warns_once_then_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Anti-spam (iv-extended): the transition warns once, then the redrop stays silent.

    Run 1 makes "X" visible; run 2 drops it (one WARNING); run 3 drops it again. Because
    the visibility set is rebuilt per run, "X" is no longer "previously visible" in run 3,
    so it falls back to the benign class and emits no further WARNING. Every run models a
    main poll (emit_canonicless_diagnostics=True).
    """
    cache = SimpleNamespace(entry_id="entry-A")
    visible = SimpleNamespace(
        deviceMetadata=[
            _make_phone_device(name="X", canonic_ids=[SimpleNamespace(id="cid-x")])
        ]
    )
    dropped = SimpleNamespace(
        deviceMetadata=[_make_phone_device(name="X", canonic_ids=[])]
    )

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            visible, cache=cache, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            dropped, cache=cache, emit_canonicless_diagnostics=True
        )
        assert len(_canonicless_warnings(caplog)) == 1
        decoder.get_devices_with_location(
            dropped, cache=cache, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 1


def test_transition_warns_exactly_once_per_poll_despite_double_call(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Multi-run-per-poll invariant (vii): the capability probe must not consume it.

    A poll calls ``get_devices_with_location`` twice with the same cache: the capability
    probe (emit_canonicless_diagnostics=False) and the main poll (True). After "X" was
    visible, the probe pass on the dropped list must NOT touch the visibility map, so the
    main pass still sees "X" as previously visible and fires the transition WARNING exactly
    once (``== 1``, not the pre-fix ``<= 1`` where the probe clobbered the map).
    """
    cache = SimpleNamespace(entry_id="entry-A")
    visible = SimpleNamespace(
        deviceMetadata=[
            _make_phone_device(name="X", canonic_ids=[SimpleNamespace(id="cid-x")])
        ]
    )
    dropped = SimpleNamespace(
        deviceMetadata=[_make_phone_device(name="X", canonic_ids=[])]
    )

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        # Poll 1: main poll makes "X" visible.
        decoder.get_devices_with_location(
            visible, cache=cache, emit_canonicless_diagnostics=True
        )
        # Poll 2: capability probe (silent) then main poll (emits) on the dropped list.
        decoder.get_devices_with_location(
            dropped, cache=cache, emit_canonicless_diagnostics=False
        )
        decoder.get_devices_with_location(
            dropped, cache=cache, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 1


def test_double_decode_mixed_drop_warns_once_with_stable_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Mixed-device double-decode (viii): aggregate count is stable across the two passes.

    This pins the exact regression Codex flagged for the within-poll double-decode in
    ``api.py::_process_device_list_response`` (capability probe with
    ``emit_canonicless_diagnostics=False`` followed by the main poll with ``True`` on the
    SAME payload): a poll where a previously visible phone disappears WHILE a tracker-shaped
    device is missing must surface the aggregate WARNING exactly once with count 2, not
    twice (count 2 then count 1). Because every cross-poll mutation (the visibility set and
    the count guard) is gated to the main poll, the probe pass cannot pop the just-gone
    phone out of the previous-run set before the main pass reads it, so the transition stays
    intact and the count does not decay between the two decodes.
    """
    cache = SimpleNamespace(entry_id="entry-A")

    # Run 1 (main poll): make the phone "Pixel" visible so it enters the visibility set.
    visible = SimpleNamespace(
        deviceMetadata=[
            _make_phone_device(name="Pixel", canonic_ids=[SimpleNamespace(id="cid-p")])
        ]
    )
    decoder.get_devices_with_location(
        visible, cache=cache, emit_canonicless_diagnostics=True
    )

    # Run 2 payload: "Pixel" disappears (benign class but previously visible -> warn-worthy)
    # WHILE the tracker "Tracker" is canonicless (warn-worthy by class). Both warn, so the
    # aggregate count is 2 on the main pass.
    poll = SimpleNamespace(
        deviceMetadata=[
            _make_phone_device(name="Pixel", canonic_ids=[]),
            _make_tracker_device(name="Tracker"),
        ]
    )

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        # Decode the SAME payload twice, exactly as api.py does within one poll:
        decoder.get_devices_with_location(
            poll, cache=cache, emit_canonicless_diagnostics=False
        )  # capability probe: must stay silent and leave the guards untouched
        decoder.get_devices_with_location(
            poll, cache=cache, emit_canonicless_diagnostics=True
        )  # main poll: the single diagnostic emission for this poll

    warnings = _canonicless_warnings(caplog)
    assert len(warnings) == 1, warnings
    assert "2 device(s)" in warnings[0], warnings[0]


def test_valid_canonic_id_produces_row_and_no_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative control: a valid canonic ID yields one row and no canonicless signal.

    Left at the default emit_canonicless_diagnostics=False: the happy path is identical on
    both passes, so this also pins that the probe pass produces the row without diagnostics.
    """
    device = _make_phone_device(
        name="Healthy Phone", canonic_ids=[SimpleNamespace(id="valid-canonic-id")]
    )
    device_list = SimpleNamespace(deviceMetadata=[device])

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        results = decoder.get_devices_with_location(device_list, cache=None)

    assert len(results) == 1
    assert _canonicless_warnings(caplog) == []
    assert _canonicless_debug_lines(caplog) == []


# ----------------------------------------------------------------------------------------
# Command-Query-Separation: the capability probe pass (emit=False) is silent and pure
# ----------------------------------------------------------------------------------------


def test_emit_false_pass_leaves_count_state_for_main_pass(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T2: the capability probe pass does not pre-load or pop the entry's count state.

    A poll runs the probe (emit=False) before the main poll (emit=True) on the same
    canonicless tracker list. The probe must leave ``_last_canonicless_count_by_entry``
    untouched (no premature dedup, no spurious pop), so the subsequent main pass fires the
    WARNING exactly once and records the count.
    """
    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Garage Tracker")]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    assert decoder._last_canonicless_count_by_entry.get("entry-A") is None

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        # Capability probe pass: silent, no state change.
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=False
        )
        assert _canonicless_warnings(caplog) == []
        assert _canonicless_debug_lines(caplog) == []
        assert decoder._last_canonicless_count_by_entry.get("entry-A") is None

        # Main poll pass: fires exactly one WARNING and records the count.
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 1
    assert decoder._last_canonicless_count_by_entry.get("entry-A") == 1


def test_emit_false_mutates_no_module_state_and_logs_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T3: an emit=False call is side-effect free and logs neither WARNING nor DEBUG.

    Pre-seeds both module maps, then runs a canonicless tracker through the probe pass and
    asserts: (a) the visibility map is unchanged (not overwritten with this run's set),
    (b) the count map is unchanged (its pre-seeded entry is NOT popped by the 0-count
    ``else`` branch of the count guard), and (c) no canonicless WARNING/DEBUG is emitted.
    """
    cache = SimpleNamespace(entry_id="entry-A")
    decoder._last_canonicless_count_by_entry["entry-A"] = 1
    decoder._visible_device_names_by_entry["entry-A"] = {"Previously Seen"}

    count_snapshot = copy.deepcopy(decoder._last_canonicless_count_by_entry)
    visible_snapshot = copy.deepcopy(decoder._visible_device_names_by_entry)

    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Garage Tracker")]
    )

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=False
        )

    assert decoder._last_canonicless_count_by_entry == count_snapshot
    assert decoder._visible_device_names_by_entry == visible_snapshot
    assert _canonicless_warnings(caplog) == []
    assert _canonicless_debug_lines(caplog) == []


def test_emit_false_rows_match_emit_true(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T4: the returned rows are identical between the probe and the main pass.

    Only the diagnostics differ between the two passes; the produced rows (the query
    result) must not. A list with a valid phone plus a canonicless tracker yields exactly
    the valid phone's row in both passes.
    """

    def _build_list() -> SimpleNamespace:
        return SimpleNamespace(
            deviceMetadata=[
                _make_phone_device(
                    name="Healthy", canonic_ids=[SimpleNamespace(id="valid-id")]
                ),
                _make_tracker_device(name="Garage Tracker"),
            ]
        )

    rows_probe = decoder.get_devices_with_location(
        _build_list(), cache=None, emit_canonicless_diagnostics=False
    )
    decoder._reset_canonicless_warning_state()
    rows_main = decoder.get_devices_with_location(
        _build_list(), cache=None, emit_canonicless_diagnostics=True
    )

    assert len(rows_probe) == 1
    assert rows_probe == rows_main


def test_missing_key_diagnostics_silent_on_probe_pass(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T5: the hidden-key diagnostic dump is suppressed on the capability probe pass.

    A tracker-shaped device (has an ``information`` block, not Android) that lacks an
    ``encryptedIdentityKey`` triggers the ``DEBUG STRUCTURE: Missing Key`` WARNING dump.
    That dump is a diagnostic, so the probe pass (emit_canonicless_diagnostics=False) must
    stay silent: otherwise the capability probe duplicates the WARNING once per poll for
    that device class, defeating the diagnostically-silent contract.
    """
    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Keyless Tracker")]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=False
        )

    assert _missing_key_warnings(caplog) == []


def test_missing_key_diagnostics_emitted_on_main_pass(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T6: the hidden-key diagnostic dump is preserved on the main poll.

    The companion to T5: gating the dump must not silence it on the main poll
    (emit_canonicless_diagnostics=True), where the tracker-shaped keyless device is a real
    signal worth surfacing. The WARNING names the device by its canonical IDs only; the
    display name goes to a DEBUG companion, because this path runs on every unattended
    device list refresh (AGENTS.md section 5 (b)).
    """
    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Keyless Tracker")]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )

    warnings = _missing_key_warnings(caplog)
    assert len(warnings) == 1
    assert "Keyless Tracker" not in warnings[0]
    companions = [
        rec.getMessage()
        for rec in caplog.records
        if rec.levelno == logging.DEBUG
        and rec.getMessage().startswith("Missing-key device above is")
    ]
    assert len(companions) == 1
    assert "Keyless Tracker" in companions[0]


# ----------------------------------------------------------------------------------------
# Count-guard re-arm mechanics, now exercised on the warn-worthy (tracker) population.
# All model the main poll, so each call passes emit_canonicless_diagnostics=True.
# ----------------------------------------------------------------------------------------


def test_tracker_repeated_call_does_not_warn_twice(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The count WARNING is de-duplicated across repeated decoder calls (warn-worthy)."""
    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Garage Tracker")]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 1


def test_tracker_same_count_in_distinct_entries_each_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Two config entries each with a canonicless tracker each get their own WARNING."""
    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Shared Name")]
    )
    cache_a = SimpleNamespace(entry_id="entry-A")
    cache_b = SimpleNamespace(entry_id="entry-B")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=cache_a, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            device_list, cache=cache_b, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 2


def test_tracker_multiple_canonicless_in_one_list_warn_once_with_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Two warn-worthy trackers in one list yield a single aggregate WARNING of count 2."""
    device_list = SimpleNamespace(
        deviceMetadata=[
            _make_tracker_device(name="Twin"),
            _make_tracker_device(name="Twin"),
        ]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.DEBUG, logger="custom_components.googlefindmy"):
        results = decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )

    assert results == []
    warnings = _canonicless_warnings(caplog)
    assert len(warnings) == 1
    assert "2" in warnings[0]
    assert "Twin" not in warnings[0]
    assert len(_canonicless_debug_lines(caplog)) == 2


def test_tracker_changed_count_in_one_entry_warns_again(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A changed canonicless count re-surfaces the WARNING (new information)."""
    one = SimpleNamespace(deviceMetadata=[_make_tracker_device(name="A")])
    two = SimpleNamespace(
        deviceMetadata=[
            _make_tracker_device(name="A"),
            _make_tracker_device(name="B"),
        ]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            one, cache=cache, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            two, cache=cache, emit_canonicless_diagnostics=True
        )

    warnings = _canonicless_warnings(caplog)
    assert len(warnings) == 2
    assert "1" in warnings[0]
    assert "2" in warnings[1]


def test_tracker_count_returning_to_prior_value_warns_again(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A count that returns to an earlier value re-surfaces the WARNING (1 -> 2 -> 1)."""
    one = SimpleNamespace(deviceMetadata=[_make_tracker_device(name="A")])
    two = SimpleNamespace(
        deviceMetadata=[
            _make_tracker_device(name="A"),
            _make_tracker_device(name="B"),
        ]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            one, cache=cache, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            two, cache=cache, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            one, cache=cache, emit_canonicless_diagnostics=True
        )

    warnings = _canonicless_warnings(caplog)
    assert len(warnings) == 3
    assert "1" in warnings[0]
    assert "2" in warnings[1]
    assert "1" in warnings[2]


def test_tracker_recovery_then_redrop_warns_again(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A full recovery (count 0) followed by a new drop re-surfaces the WARNING."""
    missing = SimpleNamespace(deviceMetadata=[_make_tracker_device(name="A")])
    healthy = SimpleNamespace(
        deviceMetadata=[
            _make_tracker_device(name="A", canonic_ids=[SimpleNamespace(id="ok")])
        ]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            missing, cache=cache, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            healthy, cache=cache, emit_canonicless_diagnostics=True
        )
        assert len(_canonicless_warnings(caplog)) == 1
        decoder.get_devices_with_location(
            missing, cache=cache, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 2


def test_mixed_run_only_benign_left_pops_guard(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """(vi) When a tracker recovers and only never-visible benign drops remain, count is 0.

    Run 1: a tracker drops (count 1, WARNING). Run 2: the tracker recovers and a benign,
    never-visible accessory drops -- the warn-worthy count is 0, so the guard pops and no
    second WARNING fires. Both runs model the main poll.
    """
    cache = SimpleNamespace(entry_id="entry-A")
    run1 = SimpleNamespace(deviceMetadata=[_make_tracker_device(name="Tracker")])
    run2 = SimpleNamespace(
        deviceMetadata=[
            _make_tracker_device(
                name="Tracker", canonic_ids=[SimpleNamespace(id="ok")]
            ),
            _make_buds_device(name="Earbuds"),
        ]
    )

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            run1, cache=cache, emit_canonicless_diagnostics=True
        )
        assert len(_canonicless_warnings(caplog)) == 1
        decoder.get_devices_with_location(
            run2, cache=cache, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 1
    assert decoder._last_canonicless_count_by_entry.get("entry-A") is None


def test_tracker_same_count_in_one_entry_warns_once_across_calls(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The entry-scoped count key de-duplicates a stable count across poll calls."""
    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Garage Tracker")]
    )
    cache = SimpleNamespace(entry_id="entry-A")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            device_list, cache=cache, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 1


def test_reset_with_entry_id_re_arms_only_that_entry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An entry-scoped reset re-arms that entry's warning without touching others."""
    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Garage Tracker")]
    )
    cache_a = SimpleNamespace(entry_id="entry-A")
    cache_b = SimpleNamespace(entry_id="entry-B")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=cache_a, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            device_list, cache=cache_b, emit_canonicless_diagnostics=True
        )
        decoder._reset_canonicless_warning_state("entry-A")
        decoder.get_devices_with_location(
            device_list, cache=cache_a, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            device_list, cache=cache_b, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 3


def test_reset_without_entry_id_clears_all(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A reset with no entry id clears the whole guard (test-isolation contract)."""
    device_list = SimpleNamespace(
        deviceMetadata=[_make_tracker_device(name="Garage Tracker")]
    )
    cache_a = SimpleNamespace(entry_id="entry-A")
    cache_b = SimpleNamespace(entry_id="entry-B")

    with caplog.at_level(logging.WARNING, logger="custom_components.googlefindmy"):
        decoder.get_devices_with_location(
            device_list, cache=cache_a, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            device_list, cache=cache_b, emit_canonicless_diagnostics=True
        )
        decoder._reset_canonicless_warning_state()
        decoder.get_devices_with_location(
            device_list, cache=cache_a, emit_canonicless_diagnostics=True
        )
        decoder.get_devices_with_location(
            device_list, cache=cache_b, emit_canonicless_diagnostics=True
        )

    assert len(_canonicless_warnings(caplog)) == 4
