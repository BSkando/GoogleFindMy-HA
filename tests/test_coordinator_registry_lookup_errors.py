# tests/test_coordinator_registry_lookup_errors.py
"""Registry lookup errors in the tracker entity lookup reach the caller.

``_find_tracker_entity_entry`` asks the entity registry for the tracker's
``entity_id``. A ``TypeError`` from that call means the registry API and the
caller disagree on the signature; it used to be swallowed and turned into
"no entry", which hid the mismatch. It now propagates.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator
from custom_components.googlefindmy.coordinator import registry as coordinator_registry
from tests.helpers.config_entries_stub import make_config_entry


class _MismatchedRegistry:
    """Entity registry whose ``async_get_entity_id`` has an incompatible signature."""

    entities: dict[str, SimpleNamespace] = {}

    def async_get_entity_id(self, platform: str, unique_id: str) -> str | None:
        """Two-argument form; the integration calls it with three arguments."""
        return None

    def async_get(self, entity_id: str) -> SimpleNamespace | None:
        return None


def test_registry_type_error_reaches_the_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A signature mismatch of the registry is not answered with "no entry"."""
    registry = _MismatchedRegistry()
    monkeypatch.setattr(coordinator_registry.er, "async_get", lambda hass: registry)

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator.hass = SimpleNamespace()
    coordinator.config_entry = make_config_entry(entry_id="entry-42")
    coordinator.get_device_display_name = lambda device_id: f"Tracker {device_id}"

    with pytest.raises(TypeError):
        coordinator.find_tracker_entity_entry("tracker-42")
