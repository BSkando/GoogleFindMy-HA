# custom_components/googlefindmy/map_view.py
"""Map view for Google Find My Device locations."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from aiohttp import web

from homeassistant.components.http import HomeAssistantView
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.entity_registry import async_get as async_get_entity_registry
from homeassistant.util import dt as dt_util

from .const import (
    DOMAIN,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,
    DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
)

_LOGGER = logging.getLogger(__name__)


# ------------------------------- HTML Helpers -------------------------------

def _html_response(title: str, body: str, status: int = 200) -> web.Response:
    """Return a minimal HTML response (no secrets, no stacktraces)."""
    return web.Response(
        text=f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{title}</title></head>
<body>
  <h1>{title}</h1>
  <p>{body}</p>
</body>
</html>""",
        content_type="text/html",
        status=status,
    )


# ------------------------------- Map View -----------------------------------

class GoogleFindMyMapView(HomeAssistantView):
    """View to serve device location maps."""

    url = "/api/googlefindmy/map/{device_id}"
    name = "api:googlefindmy:map"
    requires_auth = False

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the map view."""
        self.hass = hass

    async def get(self, request: web.Request, device_id: str) -> web.Response:
        """Generate and serve a map for the device.

        Security notes:
        - We use a short-lived/static token conveyed via query param. This is a UX helper
          for opening the map from the device page. Do not use it for sensitive data.
        - We never log tokens or full URLs that include tokens.
        """
        # ---- 1) Token check (options-first; 401 on missing/invalid) ----
        auth_token = request.query.get("token")
        if not auth_token:
            return _html_response("Unauthorized", "Missing authentication token.", status=401)

        if auth_token != self._get_simple_token():
            # No token echo in logs; keep message generic.
            _LOGGER.debug("Map token mismatch for device_id=%s", device_id)
            return _html_response("Unauthorized", "Invalid authentication token.", status=401)

        try:
            # ---- 2) Validate device_id against Device Registry; 404 if unknown ----
            dev_reg = dr.async_get(self.hass)
            device_known = False
            for dev in list(dev_reg.devices.values()):
                if any(ident[0] == DOMAIN and ident[1] == device_id for ident in dev.identifiers):
                    device_known = True
                    break

            if not device_known:
                _LOGGER.debug("Map requested for unknown device_id=%s", device_id)
                return _html_response("Not Found", "Device not found.", status=404)

            # ---- 3) Resolve a human-readable device name from coordinator snapshot (best effort) ----
            coordinator_data = self.hass.data.get(DOMAIN, {})
            device_name = "Unknown Device"

            # Extract the raw Google device ID from the full device identifier
            # In multi-account mode, device_id may be formatted as "{entry_id}_{google_device_id}"
            # We need to extract the google_device_id portion for matching against coordinator data
            raw_device_id = device_id
            if "_" in device_id:
                # Try to extract the Google device ID by removing the entry_id prefix
                for entry_id_key in coordinator_data.keys():
                    if entry_id_key == "config_data":
                        continue
                    # Check if device_id starts with this entry_id
                    if device_id.startswith(f"{entry_id_key}_"):
                        raw_device_id = device_id[len(entry_id_key) + 1:]  # +1 for underscore
                        break

            for entry_id_key, coordinator in coordinator_data.items():
                if entry_id_key == "config_data":
                    continue
                data_list = getattr(coordinator, "data", None) or []
                for device in data_list:
                    if device.get("id") == raw_device_id:
                        device_name = device.get("name", device_name)
                        break
                if device_name != "Unknown Device":
                    break

            # ---- 4) Find the device_tracker entity (entity registry) ----
            # Prefer the actual entity id over a guess.
            # Use raw_device_id for entity matching since unique_id uses the raw Google device ID
            entity_id_guess = f"device_tracker.{raw_device_id.replace('-', '_').lower()}"
            entity_registry = async_get_entity_registry(self.hass)
            entity_id: Optional[str] = None

            for entity in entity_registry.entities.values():
                if (
                    entity.unique_id
                    and raw_device_id in entity.unique_id
                    and entity.platform == DOMAIN
                    and entity.entity_id.startswith("device_tracker.")
                ):
                    entity_id = entity.entity_id
                    break

            if not entity_id:
                # Fall back to the guess; the page will render "no history", which is acceptable UX.
                entity_id = entity_id_guess
                _LOGGER.debug("No explicit tracker entity found for %s, using guess %s", device_id, entity_id)

            # ---- 5) Parse time range and accuracy filter from query ----
            end_time = dt_util.utcnow()
            start_time = end_time - timedelta(days=7)  # default: 7 days

            start_param = request.query.get("start")
            end_param = request.query.get("end")
            accuracy_param = request.query.get("accuracy", "0")

            if start_param:
                try:
                    start_time = datetime.fromisoformat(start_param.replace("Z", "+00:00"))
                    if start_time.tzinfo is None:
                        start_time = start_time.replace(tzinfo=dt_util.UTC)
                except ValueError:
                    pass  # keep default

            if end_param:
                try:
                    end_time = datetime.fromisoformat(end_param.replace("Z", "+00:00"))
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=dt_util.UTC)
                except ValueError:
                    pass  # keep default

            # If user swapped values (or end < start), clamp to a sane 7-day window ending at end_time.
            if end_time < start_time:
                start_time = end_time - timedelta(days=7)

            try:
                accuracy_filter = max(0, min(300, int(accuracy_param)))
            except (ValueError, TypeError):
                accuracy_filter = 0

            # ---- 6) Query Recorder history for location points (single entity) ----
            from homeassistant.components.recorder.history import get_significant_states

            history = await self.hass.async_add_executor_job(
                get_significant_states, self.hass, start_time, end_time, [entity_id]
            )

            locations: list[dict[str, Any]] = []
            if entity_id in history:
                last_seen = None
                for state in history[entity_id]:
                    lat_raw = state.attributes.get("latitude")
                    lon_raw = state.attributes.get("longitude")
                    if lat_raw is None or lon_raw is None:
                        continue
                    try:
                        lat = float(lat_raw)
                        lon = float(lon_raw)
                    except (TypeError, ValueError):
                        continue

                    current_last_seen = state.attributes.get("last_seen")
                    if current_last_seen and current_last_seen == last_seen:
                        # de-dupe by identical last_seen
                        continue
                    last_seen = current_last_seen

                    acc_raw = state.attributes.get("gps_accuracy", 0)
                    try:
                        acc = max(0.0, float(acc_raw))
                    except (TypeError, ValueError):
                        acc = 0.0

                    locations.append(
                        {
                            "lat": lat,
                            "lon": lon,
                            "accuracy": acc,
                            "timestamp": state.last_updated.isoformat(),
                            "last_seen": current_last_seen,
                            "entity_id": entity_id,
                            "state": state.state,
                            "is_own_report": state.attributes.get("is_own_report"),
                            "semantic_location": state.attributes.get("semantic_location"),
                        }
                    )

            # ---- 7) Render HTML (no secrets) ----
            html_content = self._generate_map_html(
                device_name, locations, device_id, start_time, end_time, accuracy_filter
            )
            return web.Response(text=html_content, content_type="text/html", charset="utf-8")

        except Exception as err:  # defensive: HTML error page instead of raw tracebacks
            _LOGGER.error("Error generating map for device %s: %s", device_id, err)
            return _html_response("Server Error", "Error generating map.", status=500)

    # ---------------------------- HTML builder ----------------------------

    def _generate_map_html(
        self,
        device_name: str,
        locations: list[dict[str, Any]],
        device_id: str,
        start_time: datetime,
        end_time: datetime,
        accuracy_filter: int = 0,
    ) -> str:
        """Generate HTML content for the map."""
        # Format times for display - convert to Home Assistant's local timezone
        start_local_tz = dt_util.as_local(start_time)
        end_local_tz = dt_util.as_local(end_time)
        start_local = start_local_tz.strftime("%Y-%m-%dT%H:%M")
        end_local = end_local_tz.strftime("%Y-%m-%dT%H:%M")

        if not locations:
            # Empty state page with controls for time range selection
            return f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>{device_name} - Location Map</title>
                <meta charset="utf-8" />
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .controls {{ background: #f5f5f5; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
                    .time-control {{ margin: 10px 0; }}
                    label {{ display: inline-block; width: 120px; font-weight: bold; }}
                    input[type="datetime-local"] {{ padding: 8px; border: 1px solid #ccc; border-radius: 4px; width: 200px; }}
                    button {{ padding: 10px 20px; background: #007cba; color: white; border: none; border-radius: 4px; cursor: pointer; margin: 0 5px; }}
                    button:hover {{ background: #005a8b; }}
                    .quick-buttons {{ margin: 10px 0; }}
                    .quick-buttons button {{ background: #6c757d; }}
                    .quick-buttons button:hover {{ background: #5a6268; }}
                    .message {{ text-align: center; margin-top: 20px; color: #666; }}
                </style>
            </head>
            <body>
                <h1>{device_name}</h1>
                <div class="controls">
                    <h3>Select Time Range</h3>
                    <div class="time-control">
                        <label for="startTime">Start:</label>
                        <input type="datetime-local" id="startTime" value="{start_local}">
                    </div>
                    <div class="time-control">
                        <label for="endTime">End:</label>
                        <input type="datetime-local" id="endTime" value="{end_local}">
                    </div>
                    <div class="quick-buttons">
                        <button onclick="setQuickRange(1)">Last 1 Day</button>
                        <button onclick="setQuickRange(3)">Last 3 Days</button>
                        <button onclick="setQuickRange(7)">Last 7 Days</button>
                        <button onclick="setQuickRange(14)">Last 14 Days</button>
                        <button onclick="setQuickRange(30)">Last 30 Days</button>
                    </div>
                    <button onclick="updateMap()">Update Map</button>
                </div>
                <div class="message">
                    <p>No location history available for the selected time range.</p>
                    <p>Try expanding the date range or check if the device has been active.</p>
                </div>

                <script>
                function setQuickRange(days) {{
                    const end = new Date();
                    const start = new Date(end.getTime() - (days * 24 * 60 * 60 * 1000));

                    document.getElementById('endTime').value = formatDateTime(end);
                    document.getElementById('startTime').value = formatDateTime(start);
                }}

                function formatDateTime(date) {{
                    return date.toISOString().slice(0, 16);
                }}

                function updateMap() {{
                    const startTime = document.getElementById('startTime').value;
                    const endTime = document.getElementById('endTime').value;

                    if (!startTime || !endTime) {{
                        alert('Please select both start and end times');
                        return;
                    }}

                    const url = new URL(window.location.href);
                    url.searchParams.set('start', startTime + ':00Z');
                    url.searchParams.set('end', endTime + ':00Z');
                    window.location.href = url.toString();
                }}
                </script>
            </body>
            </html>
            """

        # Calculate center point
        center_lat = sum(loc["lat"] for loc in locations) / len(locations)
        center_lon = sum(loc["lon"] for loc in locations) / len(locations)

        # Generate markers JavaScript
        markers_js: list[str] = []
        for i, loc in enumerate(locations):
            accuracy = float(loc.get("accuracy", 0.0))

            # Color based on accuracy
            if accuracy <= 5:
                color = "green"
            elif accuracy <= 20:
                color = "orange"
            else:
                color = "red"

            # Convert UTC timestamp to Home Assistant timezone
            timestamp_utc = datetime.fromisoformat(str(loc["timestamp"]).replace("Z", "+00:00"))
            timestamp_local = dt_util.as_local(timestamp_utc)

            # Determine report source
            is_own_report = loc.get("is_own_report")
            if is_own_report is True:
                report_source = "📱 Own Device"
                report_color = "#28a745"  # Green
            elif is_own_report is False:
                report_source = "🌐 Network/Crowd-sourced"
                report_color = "#007cba"  # Blue
            else:
                report_source = "❓ Unknown"
                report_color = "#6c757d"  # Gray

            # Semantic location if available
            semantic_info = ""
            semantic_location = loc.get("semantic_location")
            if semantic_location:
                semantic_info = f"<b>Location Name:</b> {semantic_location}<br>"

            popup_text = f"""
            <b>Location {i+1}</b><br>
            <b>Coordinates:</b> {loc['lat']:.6f}, {loc['lon']:.6f}<br>
            <b>GPS Accuracy:</b> {accuracy:.1f} meters<br>
            <b>Timestamp:</b> {timestamp_local.strftime('%Y-%m-%d %H:%M:%S %Z')}<br>
            <b style="color: {report_color}">Report Source:</b> <span style="color: {report_color}">{report_source}</span><br>
            {semantic_info}<b>Entity ID:</b> {loc.get('entity_id', 'Unknown')}<br>
            <b>Entity State:</b> {loc.get('state', 'Unknown')}<br>
            """

            markers_js.append(
                f"""
                var marker_{i} = L.marker([{loc['lat']}, {loc['lon']}]);
                marker_{i}.accuracy = {accuracy};
                marker_{i}.bindPopup(`{popup_text}`);
                marker_{i}.bindTooltip('Accuracy: {accuracy:.1f}m');
                marker_{i}.addTo(map);

                var circle_{i} = L.circle([{loc['lat']}, {loc['lon']}], {{
                    radius: {accuracy},
                    color: '{color}',
                    fillColor: '{color}',
                    fillOpacity: 0.1
                }});
                circle_{i}.accuracy = {accuracy};
                circle_{i}.addTo(map);
            """
            )

        markers_code = "\n".join(markers_js)

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{device_name} - Location Map</title>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
            <style>
                body {{ margin: 0; padding: 0; font-family: Arial, sans-serif; }}
                #map {{ height: 100vh; width: 100%; }}
                .filter-panel {{
                    position: absolute; top: 10px; right: 10px; z-index: 1000;
                    background: white; padding: 15px; border-radius: 8px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.4);
                    max-width: 380px; font-size: 13px;
                }}
                .filter-panel.collapsed {{ padding: 8px 12px; max-width: 120px; }}
                .filter-panel.collapsed .filter-content {{ display: none; }}
                .filter-content {{ margin-top: 10px; }}
                .filter-section {{ margin: 12px 0; padding: 8px 0; border-bottom: 1px solid #eee; }}
                .filter-section:last-child {{ border-bottom: none; }}
                .filter-control {{ margin: 8px 0; display: flex; align-items: center; }}
                .filter-control label {{
                    display: inline-block; width: 70px; font-size: 12px;
                    font-weight: bold; margin-right: 8px;
                }}
                .filter-control input {{
                    padding: 4px; border: 1px solid #ccc; border-radius: 3px;
                    width: 150px; font-size: 11px;
                }}
                .accuracy-control {{ margin: 10px 0; }}
                .accuracy-control label {{ display: block; margin-bottom: 5px; font-weight: bold; }}
                .slider-container {{ display: flex; align-items: center; gap: 8px; }}
                .accuracy-slider {{
                    flex: 1; height: 6px; background: #ddd; border-radius: 3px;
                    outline: none; cursor: pointer;
                }}
                .accuracy-value {{
                    min-width: 60px; font-size: 11px; font-weight: bold;
                    color: #007cba;
                }}
                .filter-panel button {{
                    padding: 6px 12px; background: #007cba; color: white;
                    border: none; border-radius: 4px; cursor: pointer;
                    margin: 2px; font-size: 11px;
                }}
                .filter-panel button:hover {{ background: #005a8b; }}
                .toggle-btn {{ background: #28a745 !important; }}
                .toggle-btn:hover {{ background: #218838 !important; }}
                .update-btn {{ background: #dc3545 !important; width: 100%; margin-top: 8px; }}
                .update-btn:hover {{ background: #c82333 !important; }}
                h2 {{ margin: 0 0 8px 0; font-size: 16px; }}
                .info {{ margin: 5px 0; font-size: 12px; color: #666; }}
                .current-time {{
                    margin: 8px 0; font-size: 11px; color: #007cba;
                    font-weight: bold; padding: 4px 8px;
                    background: #f8f9fa; border-radius: 4px;
                    border-left: 3px solid #007cba;
                }}
                .leaflet-control-zoom {{ z-index: 1500 !important; }}
            </style>
        </head>
        <body>
            <div class="filter-panel collapsed" id="filterPanel">
                <button class="toggle-btn" onclick="toggleFilters()">📅 Filters</button>

                <div class="filter-content" id="filterContent">
                    <h2>{device_name}</h2>
                    <div class="info">{len(locations)} locations shown</div>
                    <div class="current-time" id="currentTime">🕐 Loading current time...</div>

                    <div class="filter-section">
                        <div class="filter-control">
                            <label for="startTime">Start:</label>
                            <input type="datetime-local" id="startTime" value="{start_local}">
                        </div>
                        <div class="filter-control">
                            <label for="endTime">End:</label>
                            <input type="datetime-local" id="endTime" value="{end_local}">
                        </div>
                    </div>

                    <div class="filter-section">
                        <div class="accuracy-control">
                            <label for="accuracySlider">Accuracy Filter:</label>
                            <div class="slider-container">
                                <input type="range" id="accuracySlider" class="accuracy-slider"
                                       min="0" max="300" value="{accuracy_filter}" oninput="updateAccuracyFilter()">
                                <span class="accuracy-value" id="accuracyValue">Disabled</span>
                            </div>
                        </div>
                    </div>

                    <button class="update-btn" onclick="updateMap()">🔄 Update Map</button>
                </div>
            </div>
            <div id="map"></div>

            <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
            <script>
                var map = L.map('map').setView([{center_lat}, {center_lon}], 13);

                L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
                    attribution: '© OpenStreetMap contributors',
                    referrerPolicy: 'origin'
                }}).addTo(map);

                var allMarkers = [];
                var allCircles = [];

                {markers_code}

                map.eachLayer(function(layer) {{
                    if (layer instanceof L.Marker && layer.accuracy !== undefined) {{
                        allMarkers.push(layer);
                    }} else if (layer instanceof L.Circle && layer.accuracy !== undefined) {{
                        allCircles.push(layer);
                    }}
                }});

                var group = new L.featureGroup();
                allMarkers.forEach(function(marker) {{
                    if (map.hasLayer(marker)) {{
                        group.addLayer(marker);
                    }}
                }});
                if (group.getLayers().length > 0) {{
                    map.fitBounds(group.getBounds().pad(0.1));
                }}

                function toggleFilters() {{
                    const panel = document.getElementById('filterPanel');
                    panel.classList.toggle('collapsed');
                }}

                function updateAccuracyFilter() {{
                    const slider = document.getElementById('accuracySlider');
                    const valueSpan = document.getElementById('accuracyValue');
                    const value = parseInt(slider.value);

                    if (value === 0) {{
                        valueSpan.textContent = 'Disabled';
                        valueSpan.style.color = '#6c757d';
                    }} else {{
                        valueSpan.textContent = value + 'm';
                        valueSpan.style.color = '#007cba';
                    }}

                    filterMarkersByAccuracy(value);
                }}

                function filterMarkersByAccuracy(maxAccuracy) {{
                    if (allMarkers.length === 0 || allCircles.length === 0) {{
                        map.eachLayer(function(layer) {{
                            if (layer instanceof L.Marker && layer.accuracy !== undefined) {{
                                allMarkers.push(layer);
                            }} else if (layer instanceof L.Circle && layer.accuracy !== undefined) {{
                                allCircles.push(layer);
                            }}
                        }});
                    }}

                    allMarkers.forEach(function(marker) {{
                        if (maxAccuracy === 0 || marker.accuracy <= maxAccuracy) {{
                            if (!map.hasLayer(marker)) {{ marker.addTo(map); }}
                        }} else {{
                            if (map.hasLayer(marker)) {{ map.removeLayer(marker); }}
                        }}
                    }});

                    allCircles.forEach(function(circle) {{
                        if (maxAccuracy === 0 || circle.accuracy <= maxAccuracy) {{
                            if (map.hasLayer(circle)) {{ circle.addTo(map); }}
                        }} else {{
                            if (map.hasLayer(circle)) {{ map.removeLayer(circle); }}
                        }}
                    }});

                    var visibleCount = allMarkers.filter(function(m) {{ return map.hasLayer(m); }}).length;
                    var infoElement = document.querySelector('.info');
                    if (infoElement) {{ infoElement.textContent = visibleCount + ' locations shown'; }}
                }}

                function setQuickRange(days) {{
                    const end = new Date();
                    const start = new Date(end.getTime() - (days * 24 * 60 * 60 * 1000));
                    document.getElementById('endTime').value = formatDateTime(end);
                    document.getElementById('startTime').value = formatDateTime(start);
                }}

                function formatDateTime(date) {{ return date.toISOString().slice(0, 16); }}

                function updateMap() {{
                    const startTime = document.getElementById('startTime').value;
                    const endTime = document.getElementById('endTime').value;
                    const accuracyFilter = document.getElementById('accuracySlider').value;

                    if (!startTime || !endTime) {{
                        alert('Please select both start and end times');
                        return;
                    }}

                    const url = new URL(window.location.href);
                    url.searchParams.set('start', startTime + ':00Z');
                    url.searchParams.set('end', endTime + ':00Z');
                    if (accuracyFilter > 0) {{
                        url.searchParams.set('accuracy', accuracyFilter);
                    }} else {{
                        url.searchParams.delete('accuracy');
                    }}
                    window.location.href = url.toString();
                }}

                document.addEventListener('DOMContentLoaded', function() {{
                    updateAccuracyFilter();
                    const initialFilter = {accuracy_filter};
                    if (initialFilter > 0) {{ filterMarkersByAccuracy(initialFilter); }}

                    function updateCurrentTime() {{
                        const now = new Date();
                        const options = {{
                            year: 'numeric', month: 'short', day: 'numeric',
                            hour: '2-digit', minute: '2-digit', second: '2-digit',
                            timeZoneName: 'short'
                        }};
                        document.getElementById('currentTime').textContent = '🕐 ' + now.toLocaleString('en-US', options);
                    }}
                    updateCurrentTime();
                    setInterval(updateCurrentTime, 1000);
                }});
            </script>
        </body>
        </html>
        """

    # ---------------------------- Token helper ----------------------------

    def _get_simple_token(self) -> str:
        """Generate a simple token for basic authentication.

        Notes:
        - Options-first harmony with other platforms (weekly/static).
        - No logging of the token value here or elsewhere.
        """
        import hashlib
        import time

        config_entries = self.hass.config_entries.async_entries(DOMAIN)
        token_expiration_enabled = DEFAULT_MAP_VIEW_TOKEN_EXPIRATION
        if config_entries:
            entry = config_entries[0]
            token_expiration_enabled = entry.options.get(
                OPT_MAP_VIEW_TOKEN_EXPIRATION,
                entry.data.get(OPT_MAP_VIEW_TOKEN_EXPIRATION, DEFAULT_MAP_VIEW_TOKEN_EXPIRATION),
            )

        # Prefer HA's instance UUID if present; fall back to a benign constant.
        ha_uuid = str(self.hass.data.get("core.uuid") or "ha")

        if token_expiration_enabled:
            week = str(int(time.time() // 604800))  # 7-day bucket
            return hashlib.md5(f"{ha_uuid}:{week}".encode()).hexdigest()[:16]
        return hashlib.md5(f"{ha_uuid}:static".encode()).hexdigest()[:16]


# ------------------------------ Redirect View -------------------------------

class GoogleFindMyMapRedirectView(HomeAssistantView):
    """View to redirect to appropriate map URL based on request origin."""

    url = "/api/googlefindmy/redirect_map/{device_id}"
    name = "api:googlefindmy:redirect_map"
    requires_auth = False

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the redirect view."""
        self.hass = hass

    async def get(self, request: web.Request, device_id: str) -> web.Response:
        """Redirect to the map path using a **relative** Location header.

        Why relative?
        - Browser resolves against the current origin (proxy/cloud friendly).
        - Avoids computing or persisting absolute base URLs.
        - RFC 9110 allows a URI reference in Location (relative is valid).
        """
        # Require token but do not echo it back in logs.
        auth_token = request.query.get("token")
        if not auth_token:
            return _html_response("Bad Request", "Missing authentication token.", status=400)

        # Preserve all query parameters (incl. start/end/accuracy/token) in the redirect.
        # Build a relative URL so the browser keeps the current origin automatically.
        from urllib.parse import urlencode

        query_dict = dict(request.query.items())
        redirect_url = f"/api/googlefindmy/map/{device_id}?{urlencode(query_dict)}"
        _LOGGER.debug("Relative redirect prepared for device_id=%s", device_id)

        raise web.HTTPFound(location=redirect_url)
