# custom_components/googlefindmy/google_home_filter.py
"""Google Home semantic-location filter (options-first, v2.3+patch).

This module centralizes heuristics to:
- suppress noisy "home"-like detections from Google/Nest/Chromecast speakers,
- substitute obvious Google-Home semantic places with the **Home zone coordinates**,
- debounce repeated "Home" reports within a time window.

Design goals
------------
* **Options-first**: Settings are read from `ConfigEntry.options`, with safe
  fallback to `entry.data` and constants from `const.py`.
* **No I/O in hot paths**: All operations are event-loop friendly.
* **Backward compatible**: Accepts a plain Mapping/dict input for legacy use.

Public API (stable)
-------------------
- `GoogleHomeFilter(hass, config_like)`
- `apply_from_entry(entry)`
- `update_config(config_or_entry)`
- `is_google_home_device(location_name) -> bool`
- `get_home_zone_name() -> str | None`                      # kept for compatibility
- `get_home_zone_attributes() -> dict[str, float] | None`   # latitude/longitude[/radius]
- `is_device_at_home(device_id) -> bool`
- `reset_spam_tracking(device_id) -> None`
- `should_filter_detection(device_id, location_name) -> tuple[bool, dict[str, float] | None]`

Behavioral note
---------------
Historically, this filter could return a *zone name* to replace semantic labels
(e.g. "Nest Hub") with "Home". That forced the device_tracker's state to that
string. As of v2.3 we instead return **GPS coordinates** of `zone.home`
(plus radius when available) so that Home Assistant Core applies the standard
zone engine and sets the tracker state to `home`. This aligns with HA best practices.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from collections.abc import Callable as TypingCallable
from typing import TYPE_CHECKING, Any, cast

from homeassistant.components.zone import DOMAIN as ZONE_DOMAIN
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_LATITUDE, ATTR_LONGITUDE
from homeassistant.core import Event, HomeAssistant, State
from homeassistant.core import callback as ha_callback
from homeassistant.helpers import entity_registry as er

from .const import (
    DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS,
    DOMAIN,
    GOOGLE_HOME_SPAM_THRESHOLD_MINUTES,
    OPT_GOOGLE_HOME_FILTER_ENABLED,
    OPT_GOOGLE_HOME_FILTER_KEYWORDS,
)

_LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from homeassistant.helpers.entity_registry import EntityRegistry
    from homeassistant.helpers.event import CALLBACK_TYPE as _CallbackType
else:
    _CallbackType = Callable[[], None]


def _async_track_state_change_event(*args: Any, **kwargs: Any) -> _CallbackType:
    """Proxy ``async_track_state_change_event`` with lazy import semantics.

    Tests stub the helper by pre-populating ``homeassistant.helpers.event`` with a
    lightweight module. Importing inside this function ensures those stubs remain
    effective and avoids importing Home Assistant's HTTP stack during cold starts.
    """

    from homeassistant.helpers import event as ha_event

    return ha_event.async_track_state_change_event(*args, **kwargs)


def callback(
    func: TypingCallable[[GoogleHomeFilter, Event | None], None],
) -> TypingCallable[[GoogleHomeFilter, Event | None], None]:
    """Typed wrapper around Home Assistant's callback decorator."""

    return cast(
        "TypingCallable[[GoogleHomeFilter, Event | None], None]", ha_callback(func)
    )


# Keep local names for zone attributes to avoid fragile imports.
ZONE_ATTR_RADIUS = "radius"
ZONE_ATTR_PASSIVE = "passive"


class GoogleHomeFilter:
    """Filter Google Home device detections and prevent "home" spam events.

    Responsibilities:
      * Keyword matching for semantic places (e.g., "Nest Hub", "Chromecast").
      * Debounce logic for frequent "Home" detections.
      * Optional substitution of such places with the actual Home zone **coordinates**.

    Configuration (options-first):
      * `OPT_GOOGLE_HOME_FILTER_ENABLED` (bool)
      * `OPT_GOOGLE_HOME_FILTER_KEYWORDS` (str; comma-separated) — also accepts list

    Notes:
      * Debounce timing uses `time.monotonic()` (robust against system clock changes).
      * Entity lookups use the registry shortcut `er.async_get_entity_id(...)`
        for clarity and performance.
    """

    __slots__ = (
        "hass",
        "_enabled",
        "_keywords",
        "_spam_tracking",
        "_spam_threshold",
        "_home_zone_attrs",
        "_home_zone_passive",
        "_unsub_zone_listener",
    )

    def __init__(
        self, hass: HomeAssistant, config_like: Mapping[str, Any] | ConfigEntry
    ) -> None:
        """Initialize the Google Home filter.

        Args:
            hass: Home Assistant instance.
            config_like: Either a `ConfigEntry` (preferred) or a Mapping (legacy).
        """
        self.hass = hass
        self._enabled: bool = True
        self._keywords: list[str] = []
        # Debounce tracking: device_id -> last_seen_monotonic
        self._spam_tracking: dict[str, float] = {}
        self._spam_threshold: float = float(GOOGLE_HOME_SPAM_THRESHOLD_MINUTES) * 60.0

        # Cached Home-zone facts (kept fresh by a state-change listener)
        # Example: {"latitude": float, "longitude": float, "radius": float?}
        self._home_zone_attrs: dict[str, float] | None = None
        self._home_zone_passive: bool = False
        self._unsub_zone_listener: Callable[[], None] | None = None

        self._apply_from_mapping_or_entry(config_like)
        self._refresh_home_zone()

        # Listen specifically for changes to zone.home to refresh cache cheaply.
        self._unsub_zone_listener = _async_track_state_change_event(
            self.hass, ["zone.home"], self._on_home_zone_changed
        )

    # ---------------------------------------------------------------------
    # Configuration helpers
    # ---------------------------------------------------------------------

    def _apply_from_mapping_or_entry(
        self, source: Mapping[str, Any] | ConfigEntry
    ) -> None:
        """Load settings from ConfigEntry (options-first) or from a plain mapping.

        Accepted keyword formats:
          * Comma-separated string
          * Iterable[str] (list/tuple/set)
        """
        enabled: bool = True
        keywords_raw: Any = DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS

        if isinstance(source, ConfigEntry):
            # Options-first, fallback to data
            enabled = bool(
                source.options.get(
                    OPT_GOOGLE_HOME_FILTER_ENABLED,
                    source.data.get(OPT_GOOGLE_HOME_FILTER_ENABLED, True),
                )
            )
            keywords_raw = source.options.get(
                OPT_GOOGLE_HOME_FILTER_KEYWORDS,
                source.data.get(
                    OPT_GOOGLE_HOME_FILTER_KEYWORDS, DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS
                ),
            )
        else:
            enabled = bool(source.get(OPT_GOOGLE_HOME_FILTER_ENABLED, True))
            keywords_raw = source.get(
                OPT_GOOGLE_HOME_FILTER_KEYWORDS, DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS
            )

        self._enabled = bool(enabled)
        self._keywords = self._normalize_keywords(keywords_raw)

        _LOGGER.debug(
            "GoogleHomeFilter loaded (enabled=%s, keywords=%s)",
            self._enabled,
            self._keywords,
        )

    def apply_from_entry(self, entry: ConfigEntry) -> None:
        """(Re)load settings from a ConfigEntry."""
        self._apply_from_mapping_or_entry(entry)

    def update_config(self, config_or_entry: Mapping[str, Any] | ConfigEntry) -> None:
        """Update filter configuration (accepts dict or ConfigEntry)."""
        self._apply_from_mapping_or_entry(config_or_entry)
        _LOGGER.info(
            "Updated Google Home filter config: enabled=%s, keywords=%s",
            self._enabled,
            self._keywords,
        )

    # ---------------------------------------------------------------------
    # Normalization / parsing
    # ---------------------------------------------------------------------

    @staticmethod
    def _normalize_keywords(value: Any) -> list[str]:
        """Normalize keywords to a lowercased, de-duplicated list.

        Accepted formats:
          - str: comma/newline separated
          - list|tuple|set of str: each item is one keyword
        """
        items: list[str] = []

        if isinstance(value, str):
            raw = value.replace("\n", ",")
            items = [p.strip().lower() for p in raw.split(",") if p.strip()]
        elif isinstance(value, (list, tuple, set)):
            for v in value:
                if isinstance(v, str) and v.strip():
                    items.append(v.strip().lower())
        elif value is None:
            items = []
        else:
            # Defensive fallback: stringify unknown types
            items = [str(value).strip().lower()] if str(value).strip() else []

        # De-duplicate while preserving order
        seen = set()
        normed: list[str] = []
        for k in items:
            if k not in seen:
                seen.add(k)
                normed.append(k)
        return normed

    @staticmethod
    def _norm_id(device_id: str) -> str:
        """Create a stable key for per-device bookkeeping."""
        return (device_id or "").strip()

    # ---------------------------------------------------------------------
    # Home zone cache & helpers
    # ---------------------------------------------------------------------

    @callback
    def _on_home_zone_changed(self, _event: Event | None) -> None:
        """Refresh cached Home-zone attributes when `zone.home` changes."""
        self._refresh_home_zone()

    def _refresh_home_zone(self) -> None:
        """(Re)load `zone.home` attributes into the local cache.

        Caches:
          * latitude / longitude (required)
          * radius (optional; may be None)
          * passive (bool; when True, we refrain from substituting coordinates)
        """
        st = self.hass.states.get("zone.home")
        if not st:
            self._home_zone_attrs = None
            self._home_zone_passive = False
            return

        attrs = st.attributes or {}
        self._home_zone_passive = bool(attrs.get(ZONE_ATTR_PASSIVE, False))
        if ATTR_LATITUDE in attrs and ATTR_LONGITUDE in attrs:
            self._home_zone_attrs = {
                "latitude": float(attrs[ATTR_LATITUDE]),
                "longitude": float(attrs[ATTR_LONGITUDE]),
            }
            # 'radius' is optional; keep as float if present and valid
            if ZONE_ATTR_RADIUS in attrs and attrs[ZONE_ATTR_RADIUS] is not None:
                try:
                    radius = float(attrs[ZONE_ATTR_RADIUS])
                except (TypeError, ValueError):
                    # Ignore malformed radius
                    pass
                else:
                    self._home_zone_attrs["radius"] = radius
        else:
            self._home_zone_attrs = None

    def get_home_zone_attributes(self) -> dict[str, float] | None:
        """Return a shallow copy of cached Home-zone attributes (lat/lon[/radius])."""
        if self._home_zone_attrs is None:
            return None
        return dict(self._home_zone_attrs)

    def get_home_zone_name(self) -> str | None:
        """Return the 'Home' zone display name.

        Strategy:
        - Prefer the canonical 'zone.home' if present.
        - Fallback: any zone with 'home' in entity_id or friendly name.
        - Final fallback: literal "Home".

        Note:
        This method is kept for compatibility with older call sites.
        New code should use `get_home_zone_attributes()` and substitute **coordinates**.
        """
        try:
            # Prefer zone.home
            st = self.hass.states.get("zone.home")
            if st:
                friendly_name = st.attributes.get("friendly_name", "Home")
                return str(friendly_name)

            # Fallback: scan all zone.* states
            zone_states: list[State] = self.hass.states.async_all(ZONE_DOMAIN)
            for zst in zone_states:
                fn = str(zst.attributes.get("friendly_name", "")).lower()
                if "home" in zst.entity_id.lower() or "home" in fn:
                    friendly_name = zst.attributes.get("friendly_name", "Home")
                    return str(friendly_name)
            return "Home"
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Failed to resolve Home zone name: %s", err)
            return "Home"

    # ---------------------------------------------------------------------
    # Core logic: keyword match, "am I home?"
    # ---------------------------------------------------------------------

    def is_google_home_device(self, location_name: str | None) -> bool:
        """Return True if the location name matches any configured Google Home keyword."""
        if not self._enabled or not self._keywords or not location_name:
            return False
        location_lower = location_name.lower()
        return any(keyword in location_lower for keyword in self._keywords)

    def _find_tracker_entity_id(self, device_id: str) -> str | None:
        """Resolve the device_tracker entity_id for a given Find My device ID.

        Uses the unique_id shape: f"{DOMAIN}_{device_id}" via registry shortcut.
        """
        try:
            reg: EntityRegistry = er.async_get(self.hass)
            unique_id = f"{DOMAIN}_{device_id}"
            entity_id: str | None = reg.async_get_entity_id(
                "device_tracker", DOMAIN, unique_id
            )
            if entity_id is not None:
                return entity_id
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Entity registry lookup failed for %s: %s", device_id, err)

        # Backward-compatible guesses as a last resort (best effort)
        guess = f"device_tracker.{device_id.lower().replace(' ', '_')}"
        if self.hass.states.get(guess):
            return guess
        guess2 = f"device_tracker.{DOMAIN}_{device_id}"
        if self.hass.states.get(guess2):
            return guess2
        return None

    def is_device_at_home(self, device_id: str) -> bool:
        """Return True if the device is currently in the Home zone (state == 'home')."""
        try:
            entity_id = self._find_tracker_entity_id(device_id)
            if not entity_id:
                _LOGGER.debug("No device_tracker entity found for %s", device_id)
                return False
            st = self.hass.states.get(entity_id)
            if not st:
                return False
            return str(st.state).lower() == "home"
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Error checking if device %s is at home: %s", device_id, err)
            return False

    # ---------------------------------------------------------------------
    # Spam debounce (monotonic)
    # ---------------------------------------------------------------------

    def _should_prevent_spam(self, device_id: str) -> bool:
        """Return True if another detection should be debounced for this device."""
        key = self._norm_id(device_id)
        last = self._spam_tracking.get(key)
        if last is None:
            return False
        return (time.monotonic() - last) < self._spam_threshold

    def _update_spam_tracking(self, device_id: str) -> None:
        """Record now (monotonic) as last detection for this device."""
        self._spam_tracking[self._norm_id(device_id)] = time.monotonic()

    def reset_spam_tracking(self, device_id: str) -> None:
        """Clear spam tracking for a device (e.g., when it leaves Home)."""
        key = self._norm_id(device_id)
        if key in self._spam_tracking:
            del self._spam_tracking[key]
            _LOGGER.debug("Reset spam tracking for %s (left Home zone)", device_id)

    # ---------------------------------------------------------------------
    # Filter decision
    # ---------------------------------------------------------------------

    def should_filter_detection(
        self, device_id: str, location_name: str | None
    ) -> tuple[bool, dict[str, float] | None]:
        """Return (should_filter, replacement_attributes).

        Semantics:
          1) If `location_name` is literally the Home zone ("home" or its friendly name):
             - Apply debounce only (never substitute).
          2) Else, if `location_name` matches Google-Home keywords:
             - If device already 'home' → debounce only.
             - If not 'home' → substitute Home zone **coordinates** (if available & zone not passive).
          3) Otherwise → pass through unchanged.
          4) If `zone.home` is *passive*, never substitute coordinates (warn once per call).

        Returns:
            should_filter: True → suppress; False → pass through (optionally substituted).
            replacement_attributes: Optional dict with {"latitude","longitude",["radius"]}.
        """
        if not self._enabled:
            return False, None

        # 1) Early: literal 'Home' handling (debounce-only, no substitution).
        if location_name:
            loc_cf = str(location_name).strip().casefold()
            home_name_cf = str(self.get_home_zone_name() or "Home").strip().casefold()
            if loc_cf in {"home", home_name_cf}:
                if self._should_prevent_spam(device_id):
                    _LOGGER.debug(
                        "Filtering 'home' spam for %s (location_name=%r)",
                        device_id,
                        location_name,
                    )
                    return True, None
                self._update_spam_tracking(device_id)
                return False, None

        # 2) Google Home semantic device?
        if not self.is_google_home_device(location_name):
            return False, None

        # Already home → debounce-only
        if self.is_device_at_home(device_id):
            if self._should_prevent_spam(device_id):
                _LOGGER.debug(
                    "Filtering Google Home spam for %s at %s", device_id, location_name
                )
                return True, None
            self._update_spam_tracking(device_id)
            return False, None

        # Not home → substitute coordinates (if zone active & known coordinates)
        if self._home_zone_passive:
            _LOGGER.warning(
                "zone.home is passive; cannot substitute coordinates for %s",
                device_id,
            )
            _LOGGER.debug(
                "Google Home detection for %s was at %r", device_id, location_name
            )
            return False, None

        attrs = self.get_home_zone_attributes()
        if attrs:
            _LOGGER.info(
                "Substituting Google Home detection for %s with Home coordinates",
                device_id,
            )
            _LOGGER.debug(
                "Google Home detection for %s was at %r", device_id, location_name
            )
            self._update_spam_tracking(device_id)
            repl = {"latitude": attrs["latitude"], "longitude": attrs["longitude"]}
            if "radius" in attrs and isinstance(attrs.get("radius"), (int, float)):
                repl["radius"] = float(attrs["radius"])
            return False, repl

        # No known Home zone coordinates → pass through unchanged.
        return False, None

    # ---------------------------------------------------------------------
    # Lifecycle (optional)
    # ---------------------------------------------------------------------

    def async_shutdown(self) -> None:
        """Detach listeners (best-effort). Safe to call multiple times."""
        if self._unsub_zone_listener:
            try:
                self._unsub_zone_listener()
            except Exception:  # noqa: BLE001
                pass
            finally:
                self._unsub_zone_listener = None
