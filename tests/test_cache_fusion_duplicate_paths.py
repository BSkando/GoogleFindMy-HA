# tests/test_cache_fusion_duplicate_paths.py
"""A report that already entered the cache is not fused again, on every path.

The fusion averages an incoming fix with the cached position. When the same
report arrives again (the next poll returns it unchanged, a manual locate gets
it once more, the push channel delivers what a poll already brought in), the
cached position already IS the average of that report, and averaging again
moved the published position towards the raw fix on every delivery - without a
new location and with a fresh state write each time. ``person`` then treated
the tracker as freshly updated.

These tests drive the three real entry points into the fusion (poll cycle,
manual locate, ``update_device_cache`` for push), the propagation to shared
devices and the purge, without stubbing ``_apply_weighted_location_fusion``.
Unless a test says otherwise (the clear jump does not overlap on purpose), a
scenario starts from a cached fix that is OLDER than the report, overlaps it and
carries the same source rank: with an equal timestamp a higher cached rank
would block the merge, and without a cached timestamp the merge would judge by
distance, and either would hide the defect.
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from custom_components.googlefindmy.const import OPT_SEMANTIC_LOCATIONS
from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator
from tests.helpers.config_entries_stub import make_config_entry

_DEVICE = "dev-1"
_SIBLING = "dev-2"
_SHARED_KEY = bytes(range(32))


class _DummyCache:
    async def async_get_cached_value(self, _key: str) -> None:  # pragma: no cover
        return None

    async def async_set_cached_value(
        self, _key: str, _value: object
    ) -> None:  # pragma: no cover
        return None


class _DummyBus:
    def async_listen(
        self, *_args: object, **_kwargs: object
    ) -> Any:  # pragma: no cover
        return lambda: None

    def async_fire(self, *_args: object, **_kwargs: object) -> None:  # pragma: no cover
        return None


class _DummyHass:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.bus = _DummyBus()
        self.data: dict[str, Any] = {}

    def async_create_task(self, coro: Any, *, name: str | None = None) -> Any:
        return self.loop.create_task(coro, name=name)


class _Api:
    """Returns the same report on every request, as a stuck server answer does."""

    def __init__(self, report: dict[str, Any]) -> None:
        self.report = report

    async def async_get_device_location(
        self, _dev_id: str, _dev_name: str
    ) -> dict[str, Any]:
        return dict(self.report)


def _coordinator(
    monkeypatch: pytest.MonkeyPatch, report: dict[str, Any]
) -> tuple[GoogleFindMyCoordinator, dict[str, int]]:
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.GoogleFindMyCoordinator._async_load_stats",
        AsyncMock(return_value=None),
    )
    coordinator = GoogleFindMyCoordinator(
        _DummyHass(asyncio.get_running_loop()), cache=_DummyCache()
    )
    coordinator.config_entry = make_config_entry(
        entry_id="entry-id", options={}, data={}, title="Test Entry"
    )
    coordinator.api = _Api(report)
    coordinator._get_google_home_filter = lambda: None
    coordinator._is_fcm_ready_soft = lambda: True
    coordinator._get_ignored_set = set
    devices = [{"id": _DEVICE, "name": "Tag"}]
    coordinator._last_device_list = list(devices)
    coordinator.data = []
    coordinator.last_update_success = True
    coordinator.last_exception = None
    coordinator.async_set_update_error = lambda _exc: None
    coordinator.async_set_updated_data = lambda _data: None
    counts: dict[str, int] = {}

    def _increment(stat_name: str) -> None:
        counts[stat_name] = counts.get(stat_name, 0) + 1

    coordinator.increment_stat = _increment  # type: ignore[method-assign]
    return coordinator, counts


async def _settle(coordinator: GoogleFindMyCoordinator) -> None:
    save_task = coordinator._stats_save_task
    if save_task is not None:
        save_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await save_task


def _prior_fix(now: float) -> dict[str, Any]:
    return {
        "latitude": 49.866,
        "longitude": 10.839,
        "accuracy": 50.0,
        "last_seen": now - 600.0,
        "status": "coordinate",
    }


def _report(now: float, **overrides: Any) -> dict[str, Any]:
    """A report five minutes after the prior fix, about 33 m north of it."""

    report: dict[str, Any] = {
        "latitude": 49.8663,
        "longitude": 10.839,
        "accuracy": 50.0,
        "last_seen": now - 300.0,
        "status": "coordinate",
    }
    report.update(overrides)
    return report


def _position(coordinator: GoogleFindMyCoordinator, device_id: str) -> tuple[Any, Any]:
    cached = coordinator._device_location_data[device_id]
    return cached["latitude"], cached["longitude"]


async def _poll(coordinator: GoogleFindMyCoordinator) -> None:
    await coordinator._async_start_poll_cycle(
        [{"id": _DEVICE, "name": "Tag"}], force=True
    )
    await _settle(coordinator)


async def _locate(coordinator: GoogleFindMyCoordinator) -> None:
    # The cooldowns a successful locate sets are cleared through the fields
    # themselves; waiting them out would make the test slow and clock-bound.
    coordinator._locate_cooldown_until.pop(_DEVICE, None)
    coordinator._device_poll_cooldown_until.pop(_DEVICE, None)
    await coordinator.async_locate_device(_DEVICE)
    await _settle(coordinator)


@pytest.mark.asyncio
async def test_poll_path_repeat_is_fused_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second poll returning the same report leaves the position where it was."""

    now = time.time()
    coordinator, counts = _coordinator(monkeypatch, _report(now))
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))

    await _poll(coordinator)
    after_first = _position(coordinator, _DEVICE)
    await _poll(coordinator)

    assert _position(coordinator, _DEVICE) == after_first
    assert after_first[0] == pytest.approx(49.86615, abs=1e-9)
    assert counts.get("fused_updates") == 1


@pytest.mark.asyncio
async def test_locate_path_repeat_is_fused_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second manual locate with the same answer leaves the position alone."""

    now = time.time()
    coordinator, counts = _coordinator(monkeypatch, _report(now))
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))

    await _locate(coordinator)
    after_first = _position(coordinator, _DEVICE)
    await _locate(coordinator)

    assert _position(coordinator, _DEVICE) == after_first
    assert after_first[0] == pytest.approx(49.86615, abs=1e-9)
    assert counts.get("fused_updates") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_overrides",
    [
        # The poll path normalises the stamp before the fusion, the push path
        # only after it; the identity must not depend on which one ran.
        pytest.param({"last_seen_ms": True}, id="ms_timestamp"),
        # Poll and locate drop an error-code accuracy before the fusion, the
        # push path carries 0.0 into it.
        pytest.param({"accuracy": 0.0}, id="zero_accuracy"),
    ],
)
async def test_poll_then_push_same_report_is_a_duplicate(
    monkeypatch: pytest.MonkeyPatch, raw_overrides: dict[str, Any]
) -> None:
    """The push channel delivering what a poll already brought in changes nothing."""

    now = time.time()
    raw = _report(now)
    if raw_overrides.pop("last_seen_ms", False):
        raw["last_seen"] = int(raw["last_seen"] * 1000)
    raw.update(raw_overrides)
    coordinator, counts = _coordinator(monkeypatch, raw)
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))

    await _poll(coordinator)
    after_poll = _position(coordinator, _DEVICE)
    fused_after_poll = counts.get("fused_updates")
    coordinator.update_device_cache(_DEVICE, dict(raw))

    assert _position(coordinator, _DEVICE) == after_poll
    assert fused_after_poll == 1
    assert counts.get("fused_updates") == 1


@pytest.mark.asyncio
async def test_clear_jump_is_booked_like_a_fusion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A report taken over unaveraged is remembered as well.

    About 1.1 km from the cached fix with 50 m radii on both sides: no overlap,
    so the first delivery replaces the position without averaging. The second
    delivery would overlap the new position exactly and was fused with itself.
    """

    now = time.time()
    jump = _report(now, latitude=49.876)
    coordinator, counts = _coordinator(monkeypatch, jump)
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))

    coordinator.update_device_cache(_DEVICE, dict(jump))
    coordinator.update_device_cache(_DEVICE, dict(jump))

    cached = coordinator._device_location_data[_DEVICE]
    assert cached["status"] == "coordinate"
    assert cached["latitude"] == pytest.approx(49.876, abs=1e-9)
    assert counts.get("fused_updates", 0) == 0


@pytest.mark.asyncio
async def test_report_from_before_a_jump_does_not_pull_the_position_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A clear jump keeps the ring, so an earlier report stays a repeat.

    A report without a timestamp cannot be ordered against the jump, so nothing
    but the ring stops it from taking the position back to where it was before.
    """

    now = time.time()
    stampless = _report(now, last_seen=None)
    jump = _report(now, latitude=49.876, last_seen=now - 100.0)
    coordinator, _counts = _coordinator(monkeypatch, jump)
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))
    coordinator.update_device_cache(_DEVICE, dict(stampless))
    coordinator.update_device_cache(_DEVICE, dict(jump))
    assert _position(coordinator, _DEVICE)[0] == pytest.approx(49.876, abs=1e-9)

    coordinator.update_device_cache(_DEVICE, dict(stampless))

    assert _position(coordinator, _DEVICE)[0] == pytest.approx(49.876, abs=1e-9)


@pytest.mark.asyncio
async def test_report_from_before_a_trusted_takeover_stays_a_repeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A trusted takeover keeps the ring, like a clear jump.

    The trusted report is about 1.1 km away, so without the ring the earlier
    report would not be snapped back to the anchor but would pass the gate as
    a clear jump of its own and take the position back.
    """

    now = time.time()
    stampless = _report(now, last_seen=None)
    anchor = _report(
        now, latitude=49.876, last_seen=now - 100.0, location_type="trusted"
    )
    coordinator, _counts = _coordinator(monkeypatch, anchor)
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))
    coordinator.update_device_cache(_DEVICE, dict(stampless))
    coordinator.update_device_cache(_DEVICE, dict(anchor))
    assert _position(coordinator, _DEVICE)[0] == pytest.approx(49.876, abs=1e-9)
    assert coordinator._device_location_data[_DEVICE]["location_type"] == "trusted"

    coordinator.update_device_cache(_DEVICE, dict(stampless))

    assert _position(coordinator, _DEVICE)[0] == pytest.approx(49.876, abs=1e-9)


@pytest.mark.asyncio
async def test_trusted_anchor_with_the_same_geometry_is_not_a_repeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A semantic anchor promotes the row even if it matches an ordinary report.

    The semantic mapping writes the configured position and radius into the
    report, so an ordinary report can carry the same geometry. Both arrive
    without a stamp here: with the cached stamp the semantic report would be a
    replay and skip the mapping. Taken as the ordinary report's repeat, the
    anchor would never promote the row to ``trusted``, and a later overlapping
    fix would be fused instead of snapping back to the anchor.
    """

    now = time.time()
    coordinator, _counts = _coordinator(monkeypatch, _report(now))
    coordinator.config_entry = make_config_entry(
        entry_id="entry-id",
        options={
            OPT_SEMANTIC_LOCATIONS: {
                "Home": {"latitude": 49.8663, "longitude": 10.839, "accuracy": 20.0}
            }
        },
        data={},
        title="Test Entry",
    )
    plain = _report(now, accuracy=20.0)
    del plain["last_seen"]
    coordinator.update_device_cache(_DEVICE, plain)

    coordinator.update_device_cache(
        _DEVICE, {"semantic_name": "Home", "status": "semantic"}
    )

    cached = coordinator._device_location_data[_DEVICE]
    assert cached["location_type"] == "trusted"
    assert _position(coordinator, _DEVICE)[0] == pytest.approx(49.8663, abs=1e-9)

    coordinator.update_device_cache(
        _DEVICE, _report(now, latitude=49.8666, last_seen=now - 60.0)
    )

    assert _position(coordinator, _DEVICE)[0] == pytest.approx(49.8663, abs=1e-9)


@pytest.mark.asyncio
async def test_measured_report_is_not_a_repeat_of_an_estimated_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real 200 m fix clears the estimate a fallback 200 m radius left behind.

    The fallback radius is the same number as a real fix of that size, so the
    two reports below differ only in the flag. Taken as a repeat, the measured
    report would keep the row marked estimated and the accuracy gate would go
    on treating the position as unreliable.
    """

    now = time.time()
    coordinator, _counts = _coordinator(monkeypatch, _report(now))
    coordinator.update_device_cache(
        _DEVICE, _report(now, accuracy=200.0, accuracy_estimated=True)
    )
    assert coordinator._device_location_data[_DEVICE]["accuracy_estimated"] is True

    coordinator.update_device_cache(
        _DEVICE, _report(now, accuracy=200.0, accuracy_estimated=False)
    )

    assert coordinator._device_location_data[_DEVICE]["accuracy_estimated"] is False


@pytest.mark.asyncio
async def test_no_transient_marker_reaches_the_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Whatever the fusion pencils onto a report stays out of the cached row."""

    now = time.time()
    coordinator, _counts = _coordinator(monkeypatch, _report(now))
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))

    await _poll(coordinator)
    await _locate(coordinator)
    coordinator.update_device_cache(_DEVICE, _report(now, last_seen=now - 200.0))
    coordinator.update_device_cache(_DEVICE, _report(now, last_seen=now - 200.0))

    leaked = [
        key for key in coordinator._device_location_data[_DEVICE] if key.startswith("_")
    ]
    assert leaked == []


@pytest.mark.asyncio
async def test_purge_forgets_fused_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    """A device re-added under the same id does not inherit what was fused before."""

    now = time.time()
    coordinator, counts = _coordinator(monkeypatch, _report(now))
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))
    coordinator.update_device_cache(_DEVICE, _report(now))

    coordinator.purge_device(_DEVICE)
    coordinator.update_device_cache(
        _DEVICE, _prior_fix(now) | {"latitude": 49.8657, "last_seen": now - 900.0}
    )
    coordinator.update_device_cache(_DEVICE, _report(now))

    assert counts.get("fused_updates") == 2


@pytest.mark.asyncio
async def test_shared_sibling_does_not_refuse_propagated_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The copy a shared device receives counts as the report having arrived there.

    Two accounts track one physical tag. A report committed on the first is
    propagated to the second; when the second account then receives the very
    same report itself, it is already in its cache and must not be averaged
    with the propagated (already averaged) position.
    """

    now = time.time()
    coordinator, _counts = _coordinator(monkeypatch, _report(now))
    for device_id in (_DEVICE, _SIBLING):
        coordinator.update_device_cache(
            device_id, _prior_fix(now) | {"identity_key": _SHARED_KEY}
        )

    coordinator.update_device_cache(
        _DEVICE, _report(now) | {"identity_key": _SHARED_KEY}
    )
    propagated = _position(coordinator, _SIBLING)
    coordinator.update_device_cache(
        _SIBLING, _report(now) | {"identity_key": _SHARED_KEY}
    )

    assert propagated[0] == pytest.approx(49.86615, abs=1e-9)
    assert _position(coordinator, _SIBLING) == propagated


@pytest.mark.asyncio
async def test_sibling_that_skipped_propagation_still_fuses_the_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shared device that did not adopt the row has not received the report.

    The sibling already holds its own fix of the same second, so the
    propagation skips it. When the report then reaches the sibling directly,
    it is new there and is fused with the sibling's own fix.
    """

    now = time.time()
    coordinator, _counts = _coordinator(monkeypatch, _report(now))
    for device_id in (_DEVICE, _SIBLING):
        coordinator.update_device_cache(
            device_id, _prior_fix(now) | {"identity_key": _SHARED_KEY}
        )
    own_fix = _report(now, latitude=49.866, longitude=10.8394)
    coordinator.update_device_cache(_SIBLING, own_fix | {"identity_key": _SHARED_KEY})
    sibling_before = _position(coordinator, _SIBLING)

    coordinator.update_device_cache(
        _DEVICE, _report(now) | {"identity_key": _SHARED_KEY}
    )
    assert _position(coordinator, _SIBLING) == sibling_before
    coordinator.update_device_cache(
        _SIBLING, _report(now) | {"identity_key": _SHARED_KEY}
    )

    assert _position(coordinator, _SIBLING) != sibling_before
    assert coordinator._device_location_data[_SIBLING]["status"] == "Fused (Weighted)"


@pytest.mark.asyncio
async def test_primed_position_forgets_fused_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restored position no longer contains the reports fused before it.

    On a restart the first refresh can commit the current report before the
    tracker restores an older state over it. The current report arriving again
    must then be fused with the restored position, not refused as a repeat
    that keeps the restored position under the newer timestamp.
    """

    now = time.time()
    coordinator, _counts = _coordinator(monkeypatch, _report(now))
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))
    coordinator.update_device_cache(_DEVICE, _report(now))

    restored = {"latitude": 49.8657, "longitude": 10.839, "accuracy": 50.0}
    coordinator.prime_device_location_cache(
        _DEVICE, restored | {"last_seen": now - 900.0}
    )
    coordinator.update_device_cache(_DEVICE, _report(now))

    cached = coordinator._device_location_data[_DEVICE]
    assert (cached["latitude"], cached["longitude"]) != (49.8657, 10.839)
    assert cached["status"] == "Fused (Weighted)"


@pytest.mark.asyncio
async def test_primed_metadata_keeps_fused_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Priming without a position leaves the cached geometry, and the ring, alone."""

    now = time.time()
    coordinator, counts = _coordinator(monkeypatch, _report(now))
    coordinator.update_device_cache(_DEVICE, _prior_fix(now))
    coordinator.update_device_cache(_DEVICE, _report(now))
    after_first = _position(coordinator, _DEVICE)

    coordinator.prime_device_location_cache(_DEVICE, {"battery_level": 0.5})
    coordinator.update_device_cache(_DEVICE, _report(now))

    assert _position(coordinator, _DEVICE) == after_first
    assert counts.get("fused_updates") == 1


# Every function the guard below finds writing a cache row, and what it does
# about the remembered reports. A remembered report must not be fused again into
# a row that already contains it, and must not be refused against a row older
# than it. So a writer books what it commits; keeps the ring when it replaces the
# geometry with a newer report (a clear jump or a trusted takeover, where the
# older reports in the ring are then refused); forgets the ring when it can put
# an older position in place (priming a restored state); or writes no geometry.
# A new writer is added here with its reason.
_ROW_WRITERS = {
    ("coordinator/cache.py", "update_device_cache"): "books after the commit",
    ("coordinator/cache.py", "_propagate_location_to_shared_devices"): (
        "books for each adopting target"
    ),
    ("coordinator/cache.py", "prime_device_location_cache"): (
        "forgets the ring when it primes a position"
    ),
    ("coordinator/cache.py", "_persist_anchor_metadata"): "writes metadata only",
    ("coordinator/polling.py", "_async_start_poll_cycle"): (
        "fallback for coordinator doubles without update_device_cache"
    ),
    ("Auth/fcm_receiver_ha.py", "_write_coordinator_payload"): (
        "fallback for coordinator doubles without update_device_cache"
    ),
    ("coordinator/main.py", "__init__"): (
        "creates the empty cache, before the ring exists"
    ),
    ("coordinator/main.py", "purge_device"): "removes the row and forgets the ring",
    ("fmdn_finder/location_uploader.py", "_update_semantic_name"): (
        "writes the semantic name only"
    ),
}

_CACHE_ATTR = "_device_location_data"
_MUTATING_METHODS = frozenset(
    {"pop", "popitem", "update", "setdefault", "clear", "__setitem__", "__delitem__"}
)


def _is_cache_ref(node: ast.AST, cache_names: set[str]) -> bool:
    """``x._device_location_data``, ``getattr(x, "_device_location_data", ...)``,
    ``... or {}`` around either, or a local name bound to one of them."""

    if isinstance(node, ast.Attribute):
        return node.attr == _CACHE_ATTR
    if isinstance(node, ast.Name):
        return node.id in cache_names
    if isinstance(node, ast.BoolOp):
        return any(_is_cache_ref(value, cache_names) for value in node.values)
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 2
        and isinstance(node.args[1], ast.Constant)
        and node.args[1].value == _CACHE_ATTR
    )


def _is_row_ref(node: ast.AST, cache_names: set[str]) -> bool:
    """A row taken out of the cache: ``<cache>[...]`` or ``<cache>.get(...)``."""

    if isinstance(node, ast.Subscript):
        return _is_cache_ref(node.value, cache_names)
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and _is_cache_ref(node.func.value, cache_names)
    )


def _writes_cache_row(func: ast.AST) -> bool:
    cache_names: set[str] = set()
    row_names: set[str] = set()
    # Two passes, so a name is known as an alias wherever it is used in the function.
    for _ in range(2):
        for node in ast.walk(func):
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name):
                    if _is_cache_ref(node.value, cache_names):
                        cache_names.add(target.id)
                    elif _is_row_ref(node.value, cache_names):
                        row_names.add(target.id)
    for node in ast.walk(func):
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
            targets = [node.target]
        elif isinstance(node, ast.Delete):
            targets = node.targets
        for target in targets:
            if isinstance(target, ast.Attribute) and target.attr == _CACHE_ATTR:
                return True
            if isinstance(target, ast.Subscript) and (
                _is_cache_ref(target.value, cache_names)
                or _is_row_ref(target.value, cache_names)
                or (isinstance(target.value, ast.Name) and target.value.id in row_names)
            ):
                return True
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _MUTATING_METHODS
            and (
                _is_cache_ref(node.func.value, cache_names)
                or _is_row_ref(node.func.value, cache_names)
                or (
                    isinstance(node.func.value, ast.Name)
                    and node.func.value.id in row_names
                )
            )
        ):
            return True
    return False


def _row_writers() -> set[tuple[str, str]]:
    root = Path(__file__).resolve().parents[1] / "custom_components" / "googlefindmy"
    found: set[tuple[str, str]] = set()
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for func in ast.walk(tree):
            if isinstance(
                func, (ast.FunctionDef, ast.AsyncFunctionDef)
            ) and _writes_cache_row(func):
                found.add((path.relative_to(root).as_posix(), func.name))
    return found


def test_every_cache_row_writer_handles_the_fused_report_ring() -> None:
    """A function that changes ``_device_location_data`` must say what it does to the ring.

    ``prime_device_location_cache`` overwrote committed geometry without
    forgetting the remembered reports, and every repeat then kept the primed
    position. This pins the list of writers so the next one is decided, not
    missed.

    The guard is a syntax check with a stated reach, not a proof that no other
    writer exists; a writer in a form it does not see is still a review question.

    Seen, per function: assignment to the attribute itself; item assignment and
    ``del`` on the cache, reached as an attribute, through
    ``getattr(..., "_device_location_data")`` (also inside ``... or {}``) or
    through a local name bound to either by a plain single-target assignment; the
    methods in ``_MUTATING_METHODS`` called on it; and the same writes on a row
    taken out of it with ``[...]`` or ``.get(...)``, directly or through such a
    local name.

    Measured as not seen (not an exhaustive list): a row or the cache handed to another function that writes it
    (``dict.update(row, ...)``, ``operator.setitem``, any helper); the cache
    handed in from outside, such as the keyword ``_device_location_data=`` of the
    standalone stub in ``googlefindmy/main.py``; a local name bound by an
    annotated assignment, a walrus, tuple unpacking, several targets, a
    conditional expression or a loop or comprehension target; a row reached
    through ``.get(...) or {}``; a write more than one subscript below a row
    (``<cache>[d]["k"]["j"] = ...``); an alias kept on an attribute; and anything
    outside a function. The check is per function name, so a further write inside a
    function already listed stays unseen. A local name that is rebound to a copy
    still counts as an alias, which only reports a writer too many.
    """

    assert _row_writers() == set(_ROW_WRITERS)
