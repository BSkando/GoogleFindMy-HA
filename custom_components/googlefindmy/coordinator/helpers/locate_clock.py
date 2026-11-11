# custom_components/googlefindmy/coordinator/helpers/locate_clock.py
"""Per-device clock of location requests, read for ``poll_age_s``.

The EID resolver reports how long before an observation the integration last
sent a location request for the device (``poll_age_s`` in
``FMDN_UWT_TRANSITION``). Both send sites (the poll cycle and the manual locate)
wrap their await of ``api.async_get_device_location`` in ``stamp_on_dispatch``.
The transport calls the observer immediately before each ``session.post``
(``nova_request.observe_request_dispatch``), and only that call writes the
stamp.

The stamp therefore exists only for requests that were about to leave the host,
and it carries that moment, not the start of the attempt: push registration and
the token fetch run first, and together they can take a minute or more. An
attempt that stops earlier writes nothing, whatever the reason (no push token, a
failed push registration, a failure while the payload is built, a failed token
fetch, a time limit or a cancellation before the POST). A reader that runs while
an attempt is still in one of these phases sees the previous value, so nothing
it logs has to be taken back later.

A request that was POSTed counts even if it failed afterwards, for example with
a network or server error: the stamp says that the integration sent a request,
not that Google received or answered it. A retry stamps again.

Two overlapping requests for the same device (a manual locate still in flight
while the poll cycle asks again) each stamp at their own POST, because each
send site sets its own observer in its own context; the later POST wins. The
observer stops writing when its block ends, so a task that was created inside
the block and outlives it (the push receiver's supervisor is started on the
first locate) cannot stamp the device later.

A device purged while its request is still before the POST gets a stamp again
at the POST. The value then refers to a request that was really sent for that
id; it is accepted rather than guarded, because the purge runs only when a
device is deleted.

The same observer can count the requests per device, split by the send site
(``kind="auto"`` for the poll cycle, ``kind="manual"`` for a manual locate), so
the diagnostics show how many requests really left the host (#242). The count
follows the stamp: nothing before the POST counts, a retry after a POST counts
again. The counter lives in memory only; ``LocateCounts.since`` says when it
started, so a restart is visible as a new start instead of a silent reset.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime

from ...NovaApi.nova_request import observe_request_dispatch


def ensure_locate_stamps(existing: object) -> dict[str, float]:
    """Return ``existing`` if it is a ``dict``, else a new empty one.

    Coordinators built without ``__init__`` have no stamp mapping yet; both send
    sites go through this first.
    """
    if isinstance(existing, dict):
        return existing
    return {}


@dataclass
class LocateCounts:
    """Sent location requests per device, split into ``auto`` and ``manual``.

    ``since`` is the UTC time (``YYYY-MM-DDTHH:MM:SSZ``) the counter was created;
    ``by_device`` maps a canonical device id to ``{"auto": n, "manual": m}``.
    """

    since: str
    by_device: dict[str, dict[str, int]] = field(default_factory=dict)


def ensure_locate_counts(existing: object) -> LocateCounts:
    """Return ``existing`` if it is a ``LocateCounts``, else a new empty counter.

    Coordinators built without ``__init__`` have no counter yet; both send sites
    go through this first, so they keep counting into the same object.
    """
    if isinstance(existing, LocateCounts):
        return existing
    return LocateCounts(since=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))


@contextmanager
def stamp_on_dispatch(
    stamps: dict[str, float],
    device_id: str,
    *,
    counts: LocateCounts | None = None,
    kind: str | None = None,
) -> Iterator[None]:
    """Stamp ``device_id`` with ``time.monotonic()`` at each POST inside the block.

    With ``counts`` and ``kind`` the same POST also raises
    ``counts.by_device[device_id][kind]`` by one.
    """
    is_open = True

    def _stamp() -> None:
        if is_open:
            stamps[device_id] = time.monotonic()
            if counts is not None and kind is not None:
                per_device = counts.by_device.setdefault(device_id, {})
                per_device[kind] = per_device.get(kind, 0) + 1

    try:
        with observe_request_dispatch(_stamp):
            yield
    finally:
        is_open = False
