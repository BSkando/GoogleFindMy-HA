"""Map view for Google Find My Device locations."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from aiohttp import web
import ipaddress  # IPv4/IPv6 checks for private/link-local/loopback

from homeassistant.components.http import HomeAssistantView
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.entity_registry import async_get as async_get_entity_registry
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


def _is_private_host(host: str) -> bool:
    """Return True if host is private/loopback/link-local (IPv4/IPv6) or a known local hostname."""
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_private or ip.is_loopback or ip.is_link_local
    except ValueError:
        # Not an IP literal; treat common local names as private
        return host in ("localhost", "homeassistant.local")


class GoogleFindMyMapView(HomeAssistantView):
    """View to serve device location maps."""

    url = "/api/googlefindmy/map/{device_id}"
    name = "api:googlefindmy:map"
    requires_auth = False

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the map view."""
        self.hass = hass

    async def get(self, request: web.Request, device_id: str) -> web.Response:
        """Generate and serve a map for the device."""
        # Simple authentication check via query parameter
        auth_token = request.query.get('token')
        if not auth_token or auth_token != self._get_simple_token():
            return web.Response(
                text="""
                <html>
                <head><title>Access Denied</title></head>
                <body>
                    <h1>Access Denied</h1>
                    <p>This map requires proper authentication.</p>
                    <p>Please access the map through the Home Assistant device page.</p>
                </body>
                </html>
                """,
                content_type='text/html',
                status=403
            )

        try:
            # Get device name from coordinator
            coordinator_data = self.hass.data.get(DOMAIN, {})
            device_name = "Unknown Device"
            _LOGGER.debug(f"Looking for device_id '{device_id}' in coordinator data")

            for entry_id, coordinator in coordinator_data.items():
                if entry_id == "config_data":
                    continue
                if hasattr(coordinator, 'data') and coordinator.data:
                    _LOGGER.debug(f"Coordinator {entry_id} has {len(coordinator.data)} devices")
                    for device in coordinator.data:
                        device_id_in_data = device.get('id')
                        device_name_in_data = device.get('name')
                        _LOGGER.debug(f"Found device id='{device_id_in_data}' name='{device_name_in_data}'")
                        if device_id_in_data == device_id:
                            device_name = device.get('name', 'Unknown Device')
                            _LOGGER.debug(f"Matched device '{device_name}' for id '{device_id}'")
                            break
                else:
                    _LOGGER.debug(f"Coordinator {entry_id} has no data")

            # Get location history from Home Assistant
            entity_id = f"device_tracker.{device_id.replace('-', '_').lower()}"

            # Try to find the actual entity ID
            entity_registry = async_get_entity_registry(self.hass)
            _LOGGER.debug(f"Looking for device_tracker entity for device {device_id}")
            _LOGGER.debug(f"Initial entity_id guess: {entity_id}")

            found_entity = False
            for entity in entity_registry.entities.values():
                if entity.unique_id and device_id in entity.unique_id and entity.platform == "googlefindmy" and entity.entity_id.startswith("device_tracker."):
                    _LOGGER.debug(f"Found matching entity: {entity.entity_id} with unique_id: {entity.unique_id}")
                    entity_id = entity.entity_id
                    found_entity = True
                    break

            if not found_entity:
                _LOGGER.warning(f"No GoogleFindMy device_tracker entity found for device {device_id}")
            else:
                _LOGGER.debug(f"Using entity_id: {entity_id}")

            # Get time range from URL parameters
            end_time = dt_util.utcnow()
            start_time = end_time - timedelta(days=7)  # default 7 days

            # Parse custom start/end times if provided
            start_param = request.query.get('start')
            end_param = request.query.get('end')
            accuracy_param = request.query.get('accuracy', '0')

            if start_param:
                try:
                    start_time = datetime.fromisoformat(start_param.replace('Z', '+00:00'))
                    if start_time.tzinfo is None:
                        start_time = start_time.replace(tzinfo=dt_util.UTC)
                except ValueError:
                    pass  # Use default if invalid

            if end_param:
                try:
                    end_time = datetime.fromisoformat(end_param.replace('Z', '+00:00'))
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=dt_util.UTC)
                except ValueError:
                    pass  # Use default if invalid

            # Parse accuracy filter
            try:
                accuracy_filter = max(0, min(300, int(accuracy_param)))
            except (ValueError, TypeError):
                accuracy_filter = 0

            # Query state history
            from homeassistant.components.recorder.history import get_significant_states

            history = await self.hass.async_add_executor_job(
                get_significant_states,
                self.hass,
                start_time,
                end_time,
                [entity_id]
            )

            locations: list[dict[str, Any]] = []
            if entity_id in history:
                last_seen = None
                for state in history[entity_id]:
                    if (state.attributes.get("latitude") is not None and
                        state.attributes.get("longitude") is not None):

                        # Skip duplicates based on last_seen attribute
                        current_last_seen = state.attributes.get("last_seen")
                        if current_last_seen and current_last_seen == last_seen:
                            continue
                        last_seen = current_last_seen

                        locations.append({
                            "lat": state.attributes["latitude"],
                            "lon": state.attributes["longitude"],
                            "accuracy": state.attributes.get("gps_accuracy", 0),
                            "timestamp": state.last_updated.isoformat(),
                            "last_seen": current_last_seen,
                            "entity_id": entity_id,
                            "state": state.state,
                            "is_own_report": state.attributes.get("is_own_report"),
                            "semantic_location": state.attributes.get("semantic_location")
                        })

            # Generate HTML map
            html_content = self._generate_map_html(device_name, locations, device_id, start_time, end_time, accuracy_filter)

            return web.Response(
                text=html_content,
                content_type="text/html",
                charset="utf-8"
            )

        except Exception as e:
            _LOGGER.error(f"Error generating map for device {device_id}: {e}")
            return web.Response(
                text=f"Error generating map: {e}",
                status=500
            )

    def _generate_map_html(self, device_name: str, locations: list[dict[str, Any]], device_id: str, start_time: datetime, end_time: datetime, accuracy_filter: int = 0) -> str:
        """Generate HTML content for the map."""
        # Format times for display - convert to Home Assistant's local timezone
        start_local_tz = dt_util.as_local(start_time)
        end_local_tz = dt_util.as_local(end_time)
        start_local = start_local_tz.strftime('%Y-%m-%dT%H:%M')
        end_local = end_local_tz.strftime('%Y-%m-%dT%H:%M')

        if not locations:
            return f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>{device_name} - Location Map</title>
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
            accuracy = loc.get("accuracy", 0)

            # Color based on accuracy
            if accuracy <= 5:
                color = "green"
            elif accuracy <= 20:
                color = "orange"
            else:
                color = "red"

            # Convert UTC timestamp to Home Assistant timezone
            timestamp_utc = datetime.fromisoformat(loc["timestamp"].replace("Z", "+00:00"))
            # Convert to Home Assistant's configured timezone
            timestamp_local = dt_util.as_local(timestamp_utc)

            # Determine report source
            is_own_report = loc.get('is_own_report')
            if is_own_report is True:
                report_source = "📱 Own Device"
                report_color = "#28a745"  # Green
            elif is_own_report is False:
                report_source = "🌐 Network/Crowd-sourced"
                report_color = "#007cba"  # Blue
            else:
                report_source = "❓ Unknown"
                report_color = "#6c757d"  # Gray

            # Add semantic location if available
            semantic_info = ""
            semantic_location = loc.get('semantic_location')
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

            markers_js.append(f"""
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
            """)

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

                /* Filter panel - positioned to not conflict with zoom buttons */
                .filter-panel {{
                    position: absolute; top: 10px; right: 10px; z-index: 1000;
                    background: white; padding: 15px; border-radius: 8px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.4);
                    max-width: 380px; font-size: 13px;
                }}

                /* Collapsed state - just show toggle button */
                .filter-panel.collapsed {{
                    padding: 8px 12px;
                    max-width: 120px;
                }}
                .filter-panel.collapsed .filter-content {{ display: none; }}

                /* Filter content */
                .filter-content {{ margin-top: 10px; }}
                .filter-section {{ margin: 12px 0; padding: 8px 0; border-bottom: 1px solid #eee; }}
                .filter-section:last-child {{ border-bottom: none; }}

                /* Controls styling */
                .filter-control {{ margin: 8px 0; display: flex; align-items: center; }}
                .filter-control label {{
                    display: inline-block; width: 70px; font-size: 12px;
                    font-weight: bold; margin-right: 8px;
                }}
                .filter-control input {{
                    padding: 4px; border: 1px solid #ccc; border-radius: 3px;
                    width: 150px; font-size: 11px;
                }}

                /* Accuracy slider */
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

                /* Buttons */
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

                /* Ensure zoom controls don't conflict */
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

                    <!-- Time Range Section -->
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

                    <!-- Accuracy Filter Section -->
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
                    attribution: '© OpenStreetMap contributors'
                }}).addTo(map);

                // Store all markers and circles globally for filtering
                var allMarkers = [];
                var allCircles = [];

                {markers_code}

                // Collect all markers and circles
                map.eachLayer(function(layer) {{
                    if (layer instanceof L.Marker && layer.accuracy !== undefined) {{
                        allMarkers.push(layer);
                    }} else if (layer instanceof L.Circle && layer.accuracy !== undefined) {{
                        allCircles.push(layer);
                    }}
                }});

                // Fit map to show all markers
                var group = new L.featureGroup();
                allMarkers.forEach(function(marker) {{
                    if (map.hasLayer(marker)) {{
                        group.addLayer(marker);
                    }}
                }});
                if (group.getLayers().length > 0) {{
                    map.fitBounds(group.getBounds().pad(0.1));
                }}

                // Filter panel functions
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

                    // Apply real-time accuracy filtering to existing markers
                    filterMarkersByAccuracy(value);
                }}

                function filterMarkersByAccuracy(maxAccuracy) {{
                    console.log('Filtering by accuracy:', maxAccuracy);

                    // Collect all markers and circles on first run
                    if (allMarkers.length === 0 || allCircles.length === 0) {{
                        map.eachLayer(function(layer) {{
                            if (layer instanceof L.Marker && layer.accuracy !== undefined) {{
                                allMarkers.push(layer);
                            }} else if (layer instanceof L.Circle && layer.accuracy !== undefined) {{
                                allCircles.push(layer);
                            }}
                        }});
                        console.log('Found', allMarkers.length, 'markers and', allCircles.length, 'circles');
                    }}

                    // Show/hide markers based on accuracy
                    allMarkers.forEach(function(marker) {{
                        if (maxAccuracy === 0 || marker.accuracy <= maxAccuracy) {{
                            if (!map.hasLayer(marker)) {{
                                marker.addTo(map);
                            }}
                        }} else {{
                            if (map.hasLayer(marker)) {{
                                map.removeLayer(marker);
                            }}
                        }}
                    }});

                    // Show/hide circles based on accuracy
                    allCircles.forEach(function(circle) {{
                        if (maxAccuracy === 0 || circle.accuracy <= maxAccuracy) {{
                            if (!map.hasLayer(circle)) {{
                                circle.addTo(map);
                            }}
                        }} else {{
                            if (map.hasLayer(circle)) {{
                                map.removeLayer(circle);
                            }}
                        }}
                    }});

                    // Update location count
                    var visibleCount = allMarkers.filter(function(m) {{
                        return map.hasLayer(m);
                    }}).length;

                    var infoElement = document.querySelector('.info');
                    if (infoElement) {{
                        infoElement.textContent = visibleCount + ' locations shown';
                    }}
                }}

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

                // Update current time display
                function updateCurrentTime() {{
                    const now = new Date();
                    const options = {{
                        year: 'numeric',
                        month: 'short',
                        day: 'numeric',
                        hour: '2-digit',
                        minute: '2-digit',
                        second: '2-digit',
                        timeZoneName: 'short'
                    }};
                    const timeString = now.toLocaleString('en-US', options);
                    document.getElementById('currentTime').textContent = '🕐 ' + timeString;
                }}

                // Initialize on page load
                document.addEventListener('DOMContentLoaded', function() {{
                    updateAccuracyFilter(); // Set initial display value
                    const initialFilter = {accuracy_filter};
                    if (initialFilter > 0) {{
                        filterMarkersByAccuracy(initialFilter); // Apply initial filter
                    }}

                    // Start current time updates
                    updateCurrentTime();
                    setInterval(updateCurrentTime, 1000); // Update every second
                }});
            </script>
        </body>
        </html>
        """

    def _get_simple_token(self) -> str:
        """Generate a simple token for basic authentication."""
        import hashlib
        import time
        from .const import DOMAIN, DEFAULT_MAP_VIEW_TOKEN_EXPIRATION

        # Check if token expiration is enabled in config
        config_entries = self.hass.config_entries.async_entries(DOMAIN)
        token_expiration_enabled = DEFAULT_MAP_VIEW_TOKEN_EXPIRATION
        if config_entries:
            token_expiration_enabled = config_entries[0].data.get("map_view_token_expiration", DEFAULT_MAP_VIEW_TOKEN_EXPIRATION)

        ha_uuid = str(self.hass.data.get("core.uuid", "ha"))

        if token_expiration_enabled:
            # Use weekly expiration when enabled
            week = str(int(time.time() // 604800))  # Current week since epoch (7 days)
            return hashlib.md5(f"{ha_uuid}:{week}".encode()).hexdigest()[:16]
        else:
            # No expiration - use static token based on HA UUID only
            return hashlib.md5(f"{ha_uuid}:static".encode()).hexdigest()[:16]


class GoogleFindMyMapRedirectView(HomeAssistantView):
    """View to redirect to appropriate map URL based on request origin."""

    url = "/api/googlefindmy/redirect_map/{device_id}"
    name = "api:googlefindmy:redirect_map"
    requires_auth = False

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the redirect view."""
        self.hass = hass

    async def get(self, request: web.Request, device_id: str) -> web.Response:
        """Redirect to the appropriate map URL based on request origin."""
        import socket
        from homeassistant.helpers.network import get_url

        # Get the auth token
        auth_token = request.query.get('token')
        if not auth_token:
            return web.Response(text="Missing authentication token", status=400)

        # Detect the appropriate base URL based on request headers and origin
        host_header = request.headers.get('Host', '')
        x_forwarded_host = request.headers.get('X-Forwarded-Host', '')
        origin = request.headers.get('Origin', '')

        _LOGGER.debug(f"Request headers - Host: {host_header}, X-Forwarded-Host: {x_forwarded_host}, Origin: {origin}")

        # Determine if this is a cloud request or local request
        is_cloud_request = False
        base_url: str | None = None

        # Check for Nabu Casa cloud indicators - more comprehensive detection
        cloud_indicators = ['nabu', 'ui.nabu.casa', 'www.nabucasa.com', 'nabucasa.com', 'duckdns.org', 'remote.nabucasa.com']
        if any(cloud_indicator in host_header.lower() for cloud_indicator in cloud_indicators):
            is_cloud_request = True
        elif x_forwarded_host and any(cloud_indicator in x_forwarded_host.lower() for cloud_indicator in cloud_indicators):
            is_cloud_request = True
        elif origin and any(cloud_indicator in origin.lower() for cloud_indicator in cloud_indicators):
            is_cloud_request = True

        # Additional checks for cloud requests
        # Check for non-local IP addresses in Host header (could indicate external/cloud access)
        if not is_cloud_request and host_header:
            host_ip = host_header.split(':')[0]  # Remove port if present
            # Check if host is not a local/private IP address
            if not _is_private_host(host_ip):
                is_cloud_request = True
                _LOGGER.debug(f"Detected external IP in host header: {host_ip}, treating as cloud request")

        # Prefer HA's URL helper first (best practice)
        try:
            if is_cloud_request:
                # Use HA's cloud/external URL detection
                base_url = get_url(self.hass, prefer_external=True, allow_cloud=True)
                _LOGGER.info(f"Detected cloud request, using external URL: {base_url}")
            else:
                # Use HA's internal URL for local requests
                base_url = get_url(self.hass, prefer_external=False, allow_cloud=False, allow_external=False, allow_internal=True)
                _LOGGER.info(f"Detected local request, using internal URL: {base_url}")
        except Exception as e:
            _LOGGER.warning(f"URL detection with get_url failed: {e}")

        if not base_url:
            # Use local IP detection for local requests
            try:
                # Use socket connection method to get the actual local network IP
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                local_ip = s.getsockname()[0]
                s.close()

                # Get HA port and SSL settings from config
                port = 8123
                use_ssl = False

                # Try to get actual port from HA configuration
                if hasattr(self.hass, 'http') and hasattr(self.hass.http, 'server_port'):
                    port = self.hass.http.server_port or 8123
                    use_ssl = hasattr(self.hass.http, 'ssl_context') and self.hass.http.ssl_context is not None

                protocol = "https" if use_ssl else "http"
                base_url = f"{protocol}://{local_ip}:{port}"
                _LOGGER.info(f"Detected local request, using local IP URL: {base_url}")

            except Exception as e:
                _LOGGER.warning(f"Local IP detection failed: {e}, falling back to HA network detection")
                # Fallback to HA's network detection
                try:
                    base_url = get_url(self.hass, prefer_external=False, allow_cloud=False, allow_external=False, allow_internal=True)
                    _LOGGER.info(f"Using HA internal URL fallback: {base_url}")
                except Exception as fallback_e:
                    _LOGGER.error(f"All URL detection methods failed: {fallback_e}")
                    base_url = "http://homeassistant.local:8123"

        # Build the redirect URL
        redirect_url = f"{base_url}/api/googlefindmy/map/{device_id}?token={auth_token}"

        _LOGGER.debug(f"Redirecting to: {redirect_url}")

        # Return a 302 redirect response
        return web.Response(status=302, headers={'Location': redirect_url})
