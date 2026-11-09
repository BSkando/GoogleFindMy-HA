# BLE Battery Sensor — Architecture & Lessons Learned

This document describes the **BLE battery sensor** feature: how battery
state flows from a Bluetooth Low Energy advertisement all the way to a
Home Assistant sensor entity.  It also records the lessons learned during
development so that future contributors avoid the same pitfalls.

---

## 1. High-level data flow

```
┌─────────────┐   BLE advert    ┌──────────┐  resolve_eid()  ┌─────────────────────┐
│  Tracker    │ ──────────────► │ Bermuda  │ ──────────────► │  EID Resolver       │
│  (FMDN)    │   0x40|EID|flags│ (scanner) │  raw payload    │  (googlefindmy)     │
└─────────────┘                 └──────────┘                 │                     │
                                                             │  _resolve_eid_…()   │
                                                             │  ↓                  │
                                                             │  _update_ble_battery│
                                                             │  ↓                  │
                                                             │  _ble_battery_state │
                                                             │  [canonical_id]     │
                                                             └────────┬────────────┘
                                                                      │
              ┌───────────────────────────────────────────────────────┘
              │ get_ble_battery_state(canonical_id)
              ▼
┌───────────────────────────┐   _build_entities()    ┌────────────────────────────┐
│  sensor.py                │ ◄────────────────────  │  Coordinator update loop   │
│  GoogleFindMyBLEBattery-  │                        │  (_add_new_devices listener)│
│  Sensor                   │                        └────────────────────────────┘
│  ._device_id = canonical  │
│  .native_value → battery% │
└───────────────────────────┘
```

### Step-by-step

| # | Where | What happens |
|---|-------|-------------|
| 1 | **Tracker** | Broadcasts a BLE advertisement with Frame Type `0x40` (normal operation) or `0x41` (unwanted tracking protection mode active), independent of the EID length, plus an optional 1-byte hashed-flags field. The specification does not couple frame type and EID length ([Find Hub Network Accessory Specification](https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn), retrieved 2026-08-05). A `0x41` frame carrying a 20-byte legacy EID is therefore specification-conformant — a legacy-EID beacon in unwanted tracking protection mode — and not a defensive tolerance. Field data in [`docs/UWT_FIELD_DATA.md`](UWT_FIELD_DATA.md) show frame type and hashed-flags bit 7 agreeing on every logged probe line, with no `FMDN_FLAGS_CONFLICT` entry; only 20-byte EIDs have been observed so far; no 32-byte EID observed. See `docs/FMDN.md` S7 for the history of the older generation reading. |
| 2 | **Bermuda** (`fmdn/extraction.py`) | `extract_raw_fmdn_payloads()` extracts the **unmodified** service-data bytes (including the hashed-flags byte). |
| 3 | **Bermuda** (`fmdn/integration.py`) | `normalize_eid_bytes()` converts the type to `bytes` (no content stripping), then calls `resolver.resolve_eid(payload)` or `resolver.resolve_eid_all(payload)`. Note that this is only the raw-payload path: Bermuda's `auto` extraction mode additionally feeds *frame-and-flags-stripped* EID candidates through the same call as a fallback, and those carry no hashed-flags byte at all (`layout="bare"` on our side). |
| 4 | **Resolver** (`eid_resolver.py`) | `_resolve_eid_internal()` looks up the EID in the precomputed cache. If found, returns `list[EIDMatch]` plus the raw payload and frame metadata. |
| 5 | **Resolver** (`_update_ble_battery()`) | Takes the hashed-flags byte from the offset established by the matched EID candidate (`EidCandidate.offset + len(eid)`), rather than re-deriving that position from the payload — which the specification confirms is octet 28 for 160-bit-curve frames and octet 40 for 256-bit ones, i.e. directly after the EID. When the candidate's geometry says no flags byte is present (`layout="bare"`, or a framed payload that ends with the EID), none is decoded. Sliding-window matches carry no parsed geometry and keep the previous frame-format derivation. XOR-decodes it with `compute_flags_xor_mask()`, extracts 2-bit battery level and UWT mode. The FMDN spec qualifies the hashed-flags byte explicitly: *"bits are referenced from most significant to least significant"* (verbatim, [FHN spec §Hashed Flags](https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn#hashed_flags), retrieved 2026-06-02). So spec bits 5-6 (battery) map to standard LSB-first bits 2-1 and spec bit 7 (UWT) maps to standard bit 0, i.e. `battery = (byte >> 1) & 0x03` and `uwt = byte & 0x01`. The naive LSB-first reading `(byte >> 5) & 0x03` / `byte & 0x80` is wrong and is anti-regression-guarded by `tests/test_ble_battery_sensor.py::TestHashedFlagsMsbFirstConvention` and `::TestMsbFirstNotLsbFirstDemonstratesCodexMisinterpretation`. Stores a `BLEBatteryState` keyed by **`canonical_id`**. |
| 6 | **Sensor** (`sensor.py` → `_build_entities()`) | At setup and on every coordinator update, iterates devices (`dev_id = device["id"]`, the canonical_id). Creates a `GoogleFindMyBLEBatterySensor` when the entity registry already holds its `unique_id`, or when `resolver.get_ble_battery_state(dev_id)` returns non-None (see section 4). |
| 7 | **Sensor** (`native_value` property) | On each HA state poll, reads the latest `BLEBatteryState` from the resolver and returns `battery_pct` while that sighting is at most 7 days old. Without fresh resolver data it falls back to the restored value under the same limit (see section 4). |

---

## 2. Identity model — the three device IDs

Understanding the identity model is **critical** for this feature.
Three identifiers coexist in the system:

| Identifier | Source | Example | Used where |
|---|---|---|---|
| **`canonical_id`** | Google API (`device["id"]` in coordinator snapshot, `DeviceIdentity.canonical_id`) | `01KBBxxx:aaaaaaaa-…-bbbbbbbb` | Coordinator snapshots, sensor `_device_id`, `_ble_battery_state` key |
| **`registry_id`** | HA device registry (`device.id`, `DeviceIdentity.registry_id`) | `11b2838b4bb2ba2eb5f4f4b2c742cbf9` | `EIDMatch.device_id`, internal HA references |
| **`config_entry_id`** | HA config entry | `abcdef1234567890` | `EIDMatch.config_entry_id` |

The mapping lives in `coordinator/identity.py`:

```python
registry_map[canonical_id] = (device.id, None)
#            ^^^^^^^^^^^^     ^^^^^^^^^
#            Google API ID    HA registry ID
```

### Key rule

> **`_ble_battery_state` MUST be keyed by `canonical_id`** because
> the sensor entity queries it with `device["id"]` from the coordinator
> snapshot, which is always the canonical_id.

The storage key is computed as:
```python
storage_key = match.canonical_id or match.device_id
```

The `or match.device_id` fallback handles the (theoretical) case where
`canonical_id` is empty, but in practice it is always set.

---

## 3. Hashed-flags byte — decoding

The FMDN hashed-flags byte is XOR-obfuscated per rotation window.
The resolver computes the XOR mask during EID precomputation
(`compute_flags_xor_mask()` in `FMDNCrypto/eid_generator.py`).

```
Raw flags byte:    0xAB
XOR mask:          0x73   (derived from EIK + time counter)
Decoded:           0xAB ^ 0x73 = 0xD8

Bit layout (decoded):
  Bits 0-4:   Reserved / implementation-specific
  Bits 5-6:   Battery level (2-bit enum)
  Bit  7:     UWT mode (Unwanted Tracking protection active)

Battery mapping:
  0 → UNSUPPORTED → None (unknown)
  1 → NORMAL      → 100%
  2 → LOW         →  25%
  3 → CRITICALLY_LOW → 5%
```

The XOR mask is computed for **all** EID windows (previous, current,
next) so the resolver can decode the flags regardless of which rotation
window the tracker is currently broadcasting.

---

## 4. Sensor creation and restore

The battery sensor is created in one of two ways:

1. **From the entity registry at setup.** `_build_entities()` checks for every
   visible device whether the entity registry already holds the sensor's
   `unique_id` (`async_get_entity_id("sensor", DOMAIN, unique_id)`). If it does,
   the sensor is created at once, even when the resolver has no BLE data yet or
   no resolver exists. Without resolver data this path logs at DEBUG; if the
   resolver already has data (for example on a reload of the integration),
   the INFO record `BLE battery sensor created` is logged instead.
2. **On the first BLE battery state.** For a device without a registry entry,
   `_build_entities()` checks `resolver.get_ble_battery_state(dev_id)`. If the
   result is `None` (no BLE data yet), no sensor is created. Once Bermuda (or
   another BLE scanner) resolves an advertisement and the resolver stores the
   battery state, the next coordinator update creates the sensor and logs
   `BLE battery sensor created` at INFO.

After creation, the sensor is tracked in `known_battery_ids` to avoid
duplicate creation. A new device still gets no battery sensor until it has
been seen over BLE once; a device that had one before a restart gets it back
at setup.

### Restored value and its age limit

`GoogleFindMyBLEBatterySensor` is a `RestoreSensor`. Besides the value it
stores the wall-clock time of the last BLE sighting (`last_observation_wall`
in its extra restore data, `BLEBatteryExtraStoredData`; older states fall
back to the `last_ble_observation` attribute). Without fresh resolver data
the sensor shows the restored value only while that sighting is at most 7
days old (`BLE_BATTERY_RESTORE_MAX_AGE_S`, the span of Home Assistant's
`STATE_EXPIRATION`), at restore time and while running. Resolver data
whose sighting is older than 7 days counts as no data, so a tracker that
stops sending loses its value after 7 days even while Home Assistant keeps
running. A sighting time more than 5 minutes in the future
(`BLE_BATTERY_FUTURE_TOLERANCE_S`) counts as invalid. A restored value without a sighting time is discarded (DEBUG). Without a value the sensor shows
`unknown` while the device is present and `unavailable` while it is absent.

The unwanted tracking mode binary sensor (`binary_sensor.py`,
`_build_uwt_entities()`) follows the same two creation paths but restores
nothing: until the first sighting it shows `unknown` for a present device and
`unavailable` for an absent one. Its registry path logs the same way: DEBUG
without resolver data, otherwise the INFO record `UWT-Mode binary sensor
created`.

---

## 5. Logging strategy

| Log | Level | When | Purpose |
|---|---|---|---|
| `FMDN_FLAGS_PROBE` (first decode) | **INFO** | Once per device (lifetime of resolver instance) | Confirms the BLE battery pipeline works end-to-end |
| `FMDN_FLAGS_PROBE CANNOT_DECODE` | DEBUG | Once per device when decode fails | Diagnostics for missing XOR mask or truncated payload |
| `BLE battery changed` | DEBUG | On every battery level change | Track battery transitions |
| `BLE battery sensor created` | **INFO** | Once per device when the entity is created from a first BLE battery state | Confirms sensor appeared in HA |
| `BLE battery sensor created from entity registry` | DEBUG | Once per device and setup when the entity is created from its registry entry without BLE data (with BLE data the INFO record above is logged) | Shows the sensor came back at setup without BLE data |
| `Discarding restored BLE battery value` | DEBUG | At restore, when the stored state carries no BLE sighting time | Explains why a restored value is not shown |

The first-decode `FMDN_FLAGS_PROBE` is at **INFO** level intentionally.
It fires exactly once per device per HA session, so it adds no noise when
the integration's log level is raised to INFO for troubleshooting. It is not
visible by default: without a `logger:` configuration (and without
`--verbose`) Home Assistant sets the root logger to WARNING, so the probe
appears only once INFO is enabled for the integration, for example with
`custom_components.googlefindmy: info` (step 2 of "Reporting P-256 tracker
results" in the README shows two ways to set it). The log view under **Settings → System → Logs** lists warnings and errors only; the
probe appears in the full log.

---

## 6. Lessons Learned

### 6.1  Device ID mismatch (root cause bug)

**Symptom:** Battery sensor never appeared despite Bermuda correctly
resolving EIDs and `_update_ble_battery()` being called.

**Root cause:** `_ble_battery_state` was keyed by `match.device_id`
(HA device registry ID), but `get_ble_battery_state()` was called with
`device["id"]` from the coordinator snapshot (Google API canonical_id).
These are **always** different identifiers.

```
Resolver stored:  _ble_battery_state["11b2838b4bb2ba2eb5…"]  ← registry_id
Sensor queried:   get_ble_battery_state("01KBBxxx:aaaa…")    ← canonical_id
→ NEVER equal → lookup ALWAYS returned None → sensor NEVER created
```

**Fix:** Key `_ble_battery_state` by `match.canonical_id` (with
fallback to `match.device_id`).

**Lesson:** When multiple identifier namespaces coexist, document and
test the keying contract explicitly.  The `EIDMatch` dataclass carries
both `device_id` (registry) and `canonical_id` (Google API), and it is
easy to pick the wrong one.

### 6.2  Log level demotion hides diagnostics

**Symptom:** User demoted `FMDN_FLAGS_PROBE` from INFO to DEBUG.
Afterward, the message disappeared from the log the user was reading,
and the user concluded the code was broken.

**Root cause:** Not a code bug. Which records reach the log depends on
the `logger:` configuration. Without one (and without `--verbose`), Home
Assistant sets the root logger to WARNING, so INFO and DEBUG are both filtered; with
`custom_components.googlefindmy: info`, INFO passes and DEBUG does not.
The demotion moved the one-time probe below the level enabled for
troubleshooting.

**Fix:** Reverted the first-decode `FMDN_FLAGS_PROBE` to INFO.  It
fires once per device per session, so it is acceptable at INFO.  All
subsequent/repeated logs remain at DEBUG.

**Lesson:** A one-time-per-device diagnostic may stay at INFO: it
confirms end-to-end functionality once the integration is raised to INFO
and creates no log noise. INFO is not visible by default, so a level
choice should not rest on default visibility. Repeated /
per-advertisement logs stay at DEBUG.

### 6.3  `resolve_eid()` is a public API with no internal callers

**Symptom:** During initial debugging, it looked like `resolve_eid()`
was never called because no callers existed within the googlefindmy
codebase.

**Root cause:** `resolve_eid()` is intentionally a **public API** for
external consumers (e.g., Bermuda).  The integration itself never calls
it.  The EID resolution path is driven entirely by the external BLE
scanner.

**Lesson:** When tracing a data path, check external consumers (other
integrations) in addition to internal callers.  The
`Ephemeral_Identifier_Resolver_API.md` documents this contract.

### 6.4  Google API `battery_level` is always None

The `battery_level` field in the coordinator data model (from
`ProtoDecoders/decoder.py`) is always `None` — Google's API does not
populate it for FMDN trackers.  Battery data is only available via the
BLE hashed-flags byte decoded locally.  Do not confuse
`device["battery_level"]` (always None) with
`BLEBatteryState.battery_level` (from BLE).

### 6.5  Bermuda passes full raw payloads

Bermuda's `extract_raw_fmdn_payloads()` returns the **unmodified**
payload bytes including the hashed-flags byte.  The
`normalize_eid_bytes()` helper only normalizes the Python type
(`bytearray`/`memoryview`/`str` → `bytes`) without stripping content.
This is correct and expected — the resolver needs the full payload to
extract the flags byte.

---

## 7. Test coverage

Tests live in `tests/test_ble_battery_sensor.py`,
`tests/test_ble_battery_sensor_setup.py` (creation at setup from the entity
registry and on the first sighting), `tests/test_ble_battery_sensor_max_age.py`
(the 7-day limit) and `tests/test_uwt_mode_binary_sensor_setup.py` (the same
creation paths for the unwanted tracking mode binary sensor). They cover:

- Battery state storage and retrieval via canonical_id
- All battery levels (UNSUPPORTED, NORMAL, LOW, CRITICALLY_LOW)
- UWT mode detection
- Sensor creation, availability, and restore behavior
- Shared-device propagation (multiple matches per advertisement)
- XOR mask computation and flags byte decoding
- Frame format detection (service-data vs raw-header)

The `_match()` test helper defaults `canonical_id` to `device_id` so
that the storage key matches the lookup key in test scenarios.

---

## 8. File reference

| File | Symbol | Role |
|---|---|---|
| `eid_resolver.py` | `GoogleFindMyEIDResolver._update_ble_battery()`, `.get_ble_battery_state()` | Decode and store the battery state; read it back |
| `eid_resolver.py` | `BLEBatteryState`, `FMDN_BATTERY_PCT` | Battery state dataclass and percentage mapping |
| `eid_resolver.py` | `EIDMatch` | Carries both `device_id` and `canonical_id` |
| `sensor.py` | `async_setup_entry` → `_add_tracker_scope` → `_build_entities()` | Battery sensor creation (section 4) |
| `sensor.py` | `GoogleFindMyBLEBatterySensor`, `BLE_BATTERY_RESTORE_MAX_AGE_S`, `BLEBatteryExtraStoredData` | Sensor entity, 7-day limit, restore data with the sighting time |
| `binary_sensor.py` | `_build_uwt_entities()` | Unwanted tracking mode sensor creation (section 4) |
| `coordinator/identity.py` | `IdentityOperations.get_active_device_identities()` | `registry_map[canonical_id] = (device.id, None)` |
| `coordinator/main.py` | `DeviceIdentity` | Device identity dataclass |
| `FMDNCrypto/eid_generator.py` | `compute_flags_xor_mask()` | XOR mask for the hashed-flags byte |
| `ProtoDecoders/decoder.py` | `_build_device_stub()` | `"id": canonic_id` in the device stub |
| `tests/test_ble_battery_sensor.py` | (module) | Decoding, storage and sensor behaviour |
| `tests/test_ble_battery_sensor_setup.py` | (module) | Creation at setup and on the first sighting |
| `tests/test_ble_battery_sensor_max_age.py` | (module) | 7-day limit of the shown value |
| `tests/test_uwt_mode_binary_sensor_setup.py` | (module) | Creation paths of the unwanted tracking mode sensor |
