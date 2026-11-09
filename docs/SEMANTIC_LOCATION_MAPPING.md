# Semantic Location Mapping Guide

This guide explains how to configure and use semantic location overrides in the Google Find My integration.

## What the feature does

Google sometimes reports a **semantic name** (for example, "Kitchen Display") without GPS coordinates. Semantic mappings let you substitute those semantic-only reports with your own coordinates and detection radius so the device tracker stays pinned to a stable point you control.

**Key behaviors:**

- Semantic mappings apply before Google Home spam filtering. When a mapping exists, the mapping wins and the spam filter is skipped for that report.
- Unmapped semantic names still pass through the existing replay/debounce protections and the Google Home spam filter.
- Mappings are case-insensitive; "Kitchen" and "kitchen" refer to the same entry.

## Configuring semantic locations

1. In Home Assistant, open **Settings → Devices & Services → Integrations** and select your Google Find My entry.
2. Choose **Configure** to open the options flow.
3. Select **Semantic locations** from the menu.
4. Use the actions inside the menu:
   - **Add semantic location** to create a new mapping. The form pre-fills latitude/longitude/"Detection radius (m)" using your Home zone when available.
   - **Edit semantic location** to update an existing mapping. The form pre-fills the saved coordinates and detection radius for the selected name.
   - **Delete semantic locations** to remove one or more mappings.

### Field guidance

- **Semantic name**: The label reported by Google (case-insensitive). Avoid leading/trailing spaces.
- **Latitude / Longitude**: Coordinates to substitute when this semantic name arrives.
- **Detection radius (m)**: Approximates the device's connection range. The default floor is 50m to mirror how far strong Bluetooth and Wi‑Fi beacons can reach and to ensure GPS fixes with <50m accuracy remain authoritative. Raise or lower this as needed for your environment.

### Save behavior

- Each add/edit/delete action writes a fresh options dictionary via `async_update_entry`; the options menu then reloads so the latest mappings are visible immediately.
- Duplicate names (case-insensitive) are rejected; pick a unique label if you see the duplicate warning.

## How mappings interact with filters

- When a mapping is found, the integration injects your coordinates and marks the report as trusted. The Google Home semantic spam filter is **not** applied to that report.
- When no mapping is found, the spam filter runs as before. Replay/debounce guards still prevent recycled semantic hints from producing repeated updates.
- You do **not** need to disable the spam filter; mappings take precedence automatically.

## Finding semantic names to map

Semantic names currently surface in a few places:

- **Semantic labels diagnostic sensor**: The integration exposes a **Semantic labels** diagnostic sensor on the service device. It lists every semantic label observed so far (case-insensitive) and tracks which device IDs reported each label alongside first/last seen timestamps. Use this as the primary lookup when you are unsure which names to map.
- **Device tracker attributes**: Each Google Find My tracker exposes `semantic_name` (and related metadata) in its state attributes. Inspect the entity in Developer Tools → States to see the latest semantic label reported for that device.
- **Debug logs**: Enabling debug logging for `custom_components.googlefindmy` will print semantic labels when they arrive. This is useful if the attribute shows `None` and you need to wait for the next report.
- **Diagnostics downloads**: The integration's diagnostics (from the integration entry menu) include recent location payloads, which list any `semantic_name` values delivered by the API.

## Tips for reliable mappings

- Start with the Home zone defaults, then refine the coordinates/radius to the actual device position.
- Keep semantic names short and consistent with Google's spelling to avoid near-duplicates.
- Review mappings after moving hardware (for example, relocating a speaker) so the coordinates stay accurate.
