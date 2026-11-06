# FMDN / Find Hub — Technical Reference for Implementers (RAG-friendly)

Version: **2025-12-19** • Status: **Implementation Verified / Production Reference** (public docs + peer-reviewed research + integration logs from the working resolver)

Anchors: every section carries a stable token `⟦…⟧` for RAG retrieval.
IDs: section IDs like `S0`, `S3.2` are stable for internal linking.

---

## Table of Contents (RAG Index)

* S0. Overview & Roles — `⟦OVERVIEW⟧`
* S1. Cryptographic Primitives & Key Taxonomy — `⟦CRYPTO_PRIMITIVES⟧`
* S2. Provisioning (Fast Pair + EIK lifecycle) — `⟦PROV_FLOW⟧`
* S3. BLE Advertising & EID (timebase, jitter, curves) — `⟦BLE_EID⟧`
* S4. Finder Device Behavior (scan, upload, throttling) — `⟦FINDER_BEHAV⟧`
* S5. Server-side Behavior (aggregation & modes) — `⟦SERVER_BEHAV⟧`
* S6. Owner App / Client (fetch, unwrap, decrypt, aggregate) — `⟦OWNER_BEHAV⟧`
* S7. Unwanted-Tracking Protections (UT / DULT) — `⟦UT_PROTECTION⟧`
* S8. Message Formats & Fields — `⟦DATA_FORMATS⟧`
* S9. State Machines & Failure Models — `⟦STATE_MACHINES⟧`
* S10. Security Notes & Best Practices — `⟦SECURITY_NOTES⟧`
* S11. Open Points / Not Publicly Specified — `⟦OPEN_POINTS⟧`
* S12. Operational Behavior & Logs — `⟦OP_LOGS⟧`
* Appendix A. Evidence-anchored Pseudocode — `⟦APPX_PSEUDOCODE⟧`
* References (URLs)

---

## S0. Overview & Roles `⟦OVERVIEW⟧`

**Roles.** *Provider* (tracker/headphones), *Seeker* (owner’s phone during setup), *Finder* (any participating Android device), *Backend* (Google services), *Owner Client* (Find Hub / Find My Device UI or compatible client).

**Network model.** The Find Hub / FMDN system is an **offline finding network**: Providers broadcast BLE beacons; Finders upload *end-to-end encrypted* location reports; only the Owner Client can decrypt and interpret those reports. (Böttger et al., 2025). ([Pet Symposium][1])

**Evidence note.** The public ecosystem contains both (a) **official partner documentation** (partly summarized publicly) and (b) **peer-reviewed measurements** of deployed behavior. When these differ, this document marks: **Specified** (official), **Measured** (peer-reviewed), **Observed in implementation logs** (your integration), and **Hypothesis** (reasonable but not publicly confirmed).

---

## S1. Cryptographic Primitives & Key Taxonomy `⟦CRYPTO_PRIMITIVES⟧`

### S1.1 Master secret: EIK / EIDK (32 B)

**EIK (Ephemeral Identity Key, 32 bytes).** Generated on the Seeker (phone) and provisioned to the Provider; used as the root secret for rotating identities. (Böttger et al., 2025; Google, n.d.). ([Pet Symposium][1])

> Terminology: some materials use “EIDK”. This document uses **EIK** for the 32-byte root secret.

### S1.2 Purpose-scoped derived keys (short keys)

**Specified (public partner summary):** the Find Hub accessory documentation describes multiple **8-byte keys** derived from the EIK for distinct purposes (e.g., e2ee, recovery, ring). The exact derivation method is not always fully described in public summaries; treat “domain separation” as required design intent. (Google, n.d.). ([Google for Developers][2])

**Implementation guidance:** keep a strict separation between:

* **EIK (32 B)** — root secret for identity/cryptography
* **Derived short keys (8 B)** — purpose-bound keys
* **Wrapped/transport forms** — encrypted containers that must be unwrapped before use

### S1.3 Curves and AEADs (Measured vs. Specified)

**Measured (peer-reviewed):**

* EID / E2EE key agreement uses **NIST P-160R1** (i.e., *secp160r1*) by default in all tested trackers. (Böttger et al., 2025). ([Pet Symposium][1])
* **P-256 EIDs** exist but were observed as rare and device-specific (e.g., Sony WH-1000XM5 in the study). (Böttger et al., 2025). ([Pet Symposium][1]) This integration decrypts crowdsourced reports from P-256 trackers with provisional readings; see "Foreign-report readings" in [`docs/CRYPTOGRAPHY.md`](CRYPTOGRAPHY.md#foreign-report-readings).
* Finder → Owner E2EE location encryption in the measured design uses **HKDF-SHA-256** and **AES-EAX-256** (as described by the paper’s reconstruction). (Böttger et al., 2025). ([Pet Symposium][1])

**Observed in your integration context:**

* Your logs show an **encryptedIdentityKey wrapper of 60 bytes** being **unwrapped to 32 bytes** before EID generation, and your note indicates **AES-GCM** is involved in that unwrapping path. (Observed in your logs + code path).
* Treat that wrapper as *not* the EIK; it is a **transport/container** that must be unwrapped.

---

## S2. Provisioning (Fast Pair + EIK lifecycle) `⟦PROV_FLOW⟧`

### S2.1 Setup flow (Evidence)

**Measured:** The Seeker generates the **EIK** and **sends it to the tracker** during setup; the tracker then uses that EIK for EID rotation. (Böttger et al., 2025). ([Pet Symposium][1])

**Specified (public partner summary):** Find Hub accessories extend Fast Pair with additional characteristics/procedures for Find Hub network participation. (Google, n.d.). ([Google for Developers][2])

### S2.2 Clear separation: EIK vs. account keys vs. wrappers

**Do not conflate:**

* **Account Key** (Fast Pair pairing secret; used to authenticate/protect provisioning operations)
* **EIK (32 B)** (root secret for rotating identity and related crypto)
* **encryptedIdentityKey (wrapper)** (cloud/API transport blob; must be unwrapped to obtain the 32-byte EIK in your integration)

### S2.3 Hypothesis: why a 60-byte wrapper exists

**Hypothesis (integration-driven):** Some backend/API responses deliver *wrapped* secrets (e.g., `encryptedUserSecrets.encryptedIdentityKey`) that embed an EIK encrypted under a device-/account-bound key and authenticated (e.g., AES-GCM). This enables cloud sync without exposing the EIK in clear to the backend.
This is consistent with the general principle that the EIK remains within the owner trust boundary, but the exact wrapper format is not publicly specified in the sources cited here.

---

## S3. BLE Advertising & EID `⟦BLE_EID⟧`

### S3.1 Advertising payload (Implementation verified)

**Verified in production + measured:** Standard BLE advertising includes:

* **Frame Type / UT byte** (`0x40` in normal operation; `0x41` while unwanted tracking protection mode is active, independent of EID length and curve; see S7)
* **EID (20 bytes)** in standard advertising (legacy, secp160r1 x-coordinate width)
* **Hashed flags** (status bits, obfuscated)
  (Böttger et al., 2025; confirmed by `eid_resolver.py` logs using 0x40 frames). ([Pet Symposium][1])

### S3.2 Rotation cadence, jitter, and resolver offsets (Implementation verified)

**Rotation base:** EIDs rotate on a cadence of **2^K seconds**, commonly **K = 10 → 1024 seconds**. (Böttger et al., 2025). ([Pet Symposium][1])

**Jitter (important):** The rotation time is **randomized**: the next rotation time is a multiple of the rotation period **plus a random offset (measured recommendation: 1–204 seconds)**; the study reports all tested trackers implement this. (Böttger et al., 2025). ([Pet Symposium][1])

**Resolver requirement:** any resolver that precomputes expected EIDs must keep **previous / current / next** windows (“rolling window”) to tolerate overlap caused by jitter. The resolver computes an **offset** per hit: `offset = observed_broadcast_time - expected_rotation_anchor`. Negative offsets indicate the advertisement arrived earlier than the nominal anchor (common when jitter advances the beacon); positive offsets capture late/clock-skewed broadcasts. Rolling-window lookups stay tolerant of these offsets by accepting matches in adjacent windows and recording the offset for diagnostics.

### S3.3 Timebase: “seconds since setup”, not Unix epoch (Measured)

**Measured:** EID derivation uses a **time counter since tracker setup/provisioning** (not wall-clock time). (Böttger et al., 2025). ([Pet Symposium][1])

**Measured (operational detail):** Backend results may include a time value aligned to the **last multiple of 1024 seconds since setup** (rounded down). (Böttger et al., 2025). ([Pet Symposium][1])

### S3.4 EID derivation (Measured; Table-10 PRF pattern)

**Measured algorithm (core idea):**

1. Compute `ts = seconds_since_setup`
2. Mask lower `K` bits to align to the rotation window
3. Build a **32-byte block** with fixed padding + `K` + masked timestamp repeated
4. Compute `seed = AES-ECB(EIK, block)`
5. Interpret `seed` as integer, reduce/modulo the curve order, compute `R = r·G`
6. **EID = x(R)**
   (Böttger et al., 2025). ([Pet Symposium][1])

**Curve selection (Measured):**

* Standard advertising corresponds to **20-byte EID** on **P-160R1 / secp160r1** (x-coordinate width).
* **32-byte EID** on **P-256** is possible but measured as rare/device-specific.
  (Böttger et al., 2025). ([Pet Symposium][1])

### S3.5 MAC rotation vs. UT mode (Measured)

**Measured:** During normal operation, the BLE MAC address rotates **with the EID** (~1024 s). If **UT mode** is enabled, MAC rotation is **reduced to one rotation per 24 hours**, while EID rotation remains unchanged. (Böttger et al., 2025). ([Pet Symposium][1])

---

## S4. Finder Device Behavior (scan, upload, throttling) `⟦FINDER_BEHAV⟧`

### S4.1 Scanning and match inputs (Measured)

**Measured:** Finder devices scan for these BLE beacons and can upload reports keyed by a **truncated EID (10 bytes)** (used as an index/selector in the protocol reconstruction). (Böttger et al., 2025). ([Pet Symposium][1])

### S4.2 Upload gating / throttling (Measured where stated; otherwise hypothesis)

**Measured (qualitative):** The paper describes client-side behaviors that reduce noise and abuse risk (e.g., delays and throttling considerations). (Böttger et al., 2025). ([Pet Symposium][1])

**Hypothesis (implementation pattern):** distance/accuracy/time heuristics and battery/screen-off backoffs can be expected in production clients, but exact thresholds may vary by OS release.

### S4.3 Double encryption and integrity (Measured)

**Measured:** Location reports are end-to-end encrypted for the owner, and additionally protected for transport with further encryption/signing mechanisms involving Google keys (e.g., P-256 + AES-GCM in the measured reconstruction), plus platform integrity mechanisms. (Böttger et al., 2025). ([Pet Symposium][1])

---

## S5. Server-side Behavior (aggregation & modes) `⟦SERVER_BEHAV⟧`

### S5.1 High-Traffic mode (Measured)

**Measured:** The backend only delivers “High Traffic” location reports to the owner client if **at least two reports from at least two distinct finder devices** are available; older locations may be purged and caching may keep only a limited number of recent points. (Böttger et al., 2025). ([Pet Symposium][1])

### S5.2 “In All Areas” mode (Measured)

**Measured:** “In All Areas” enables less-aggregated reporting behavior compared to High-Traffic, subject to policy and platform constraints. (Böttger et al., 2025). ([Pet Symposium][1])

---

## S6. Owner App / Client (fetch, unwrap, decrypt, aggregate) `⟦OWNER_BEHAV⟧`

### S6.1 Fetch + decrypt pipeline (Measured)

**Measured:** The owner device performs aggregation locally because the server cannot aggregate E2EE ciphertext. (Böttger et al., 2025). ([Pet Symposium][1])

### S6.2 Critical implementer rule: **unwrap before EID** (Implementation verified)

**Verified (code + resolver logs):**

* `encryptedUserSecrets.encryptedIdentityKey` is **60 bytes** and is an **AES-GCM-wrapped container**.
* `key_derivation.py` unwraps it to the **32-byte EIK**, and resolver logs confirm successful unwraps before EID computation.

**Normative requirement for this codebase:**

> **EID derivation MUST consume the 32-byte raw EIK, never the 60-byte wrapper.** The unwrap step (AES-GCM → 32-byte EIK) is mandatory for every resolver path.

**Documentation requirement:**

* Treat the wrapper as **`EIK_WRAPPED` (60 bytes)** and the output as **`EIK` (32 bytes)**.
* Keep the unwrap function a first-class step in the Owner Client pipeline (before EID generation and any derived short keys).
* Layout note: the container follows the AES-GCM structure `{nonce | ciphertext | tag}`; continue to document nonce/tag sizing as additional test vectors are captured.

### S6.3 Owner-device encryption (Measured; separate from tracker E2EE)

**Measured:** In the paper’s reconstruction, owner-device location report encryption uses **SHA-256(EIK)** to derive a 32-byte key and then **AES-GCM** with a 12-byte IV and 128-bit tag (for owner location reporting). (Böttger et al., 2025). ([Pet Symposium][1])

---

## S7. Unwanted-Tracking Protections (UT / DULT) `⟦UT_PROTECTION⟧`

**Public product/security communications:** Google describes unwanted-tracking protections and cross-platform alerting concepts (with ecosystem coordination). (Google, 2024). ([Google Online Security Blog][3])

**User-facing behavior:** Android provides functionality to detect unknown trackers and take actions (scan, alerts, etc.). (Google, n.d.). ([Pet Symposium][4])

**Measured (protocol behavior):** UT mode affects MAC rotation as described in S3.5. (Böttger et al., 2025). ([Pet Symposium][1])

**Resolved (specification, retrieved 2026-08-05):** the frame type is set to `0x41` while unwanted tracking protection mode is active and back to `0x40` when it is deactivated; hashed-flags spec bit 7 carries the same state redundantly. The specification also fixes the remaining bits of that byte (bits 0-4 reserved and zero, bits 5-6 battery level) and permits omitting the byte entirely only when the beacon does not support battery level indication *and* is not currently in unwanted tracking protection mode (a capability and a state, not two capabilities). ([Find Hub Network Accessory Specification][fhna-spec])

**Field measurement (2026-10-11):** an older reading in this repository tied `0x41` to 32-byte P-256 EIDs regardless of tracking state. It goes back at least to `588b0e02` (the resolver API doc listed `0x41` as the 256-bit frame); `ed340df5` and `f26d6cd4` carried it into the battery decoder docs. None of these commits records an observation behind that tie; `f26d6cd4` even notes `0x41` frames carrying 20-byte legacy EIDs. The specification has tied the frame type to the mode since v1.1 (changelog: "Added a cleartext indication of unwanted tracking protection mode."). Logging both signals side by side on real installations showed frame type and bit 7 agreeing on every probe line, with no `FMDN_FLAGS_CONFLICT` entry ([`docs/UWT_FIELD_DATA.md`](UWT_FIELD_DATA.md)). Coverage limit: only 20-byte EIDs have been observed so far; no 32-byte EID observed; for 32-byte EIDs the equivalence rests on the specification. `FMDN_FLAGS_CONFLICT` log entries remain the evidence channel for a disagreement; `FMDN_FLAGS_PROBE ... CANNOT_DECODE` covers the complementary case of a `0x41` frame arriving without a decodable flags byte, which the specification permits only while a beacon is not in unwanted tracking protection mode -- so a `0x41` frame arriving without one is either a beacon breaking that rule or a payload shape sliced wrongly.

[fhna-spec]: https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn

---

## S8. Message Formats & Fields `⟦DATA_FORMATS⟧`

### S8.1 BLE advert (Measured; conceptual layout)

```text
Service: Fast Pair / Find Hub extension (UUID + service data)
Payload (standard advertising, typical):
  UT_byte (1) | EID_x (20) | hashed_flags (0..1)

Notes:
- EID_x is x-coordinate of R = r·G on secp160r1 (measured default).
- Rotation ~1024s with additional jitter in rotation timing.
- UT mode affects MAC rotation (measured: 24h MAC rotation).
```

### S8.2 Finder upload (Measured elements; some fields remain open)

```text
Indexing:
- truncatedEID (10 bytes) used as selector/index (measured)

Crypto (measured reconstruction):
- ECDH on P-160R1 with keys derived from EID rotation
- HKDF-SHA-256
- AES-EAX-256 for E2EE payload
- additional transport protection using Google keys (e.g., P-256 + AES-GCM)
```

### S8.3 Owner secrets container (Observed in your integration; not a public spec)

```text
encryptedUserSecrets.encryptedIdentityKey:
- observed length: 60 bytes (Moto Tag example)
- MUST be unwrapped to 32 bytes before EID derivation
- unwrapping observed to involve AES-GCM in your codebase
```

---

## S9. State Machines & Failure Models `⟦STATE_MACHINES⟧`

**Provisioning (conceptual):**

```text
Unpaired → FastPair(AccountKey) → Provisionable → SetEIK → Beaconing(EID rotation)
Failure classes:
- wrong wrapper vs. raw EIK (integration error)
- missing setup_epoch / wrong timebase anchor
- clock skew handling if using wall time instead of "seconds since setup"
```

**Finder upload path (conceptual):**

```text
Scan → Observe advert(UT,EID,flags) → Compute truncatedEID → Gate/Throttle → Encrypt → Upload → Retry
```

**Owner retrieval path (conceptual):**

```text
Fetch secrets → Unwrap EIK (if wrapped) → Precompute rolling EIDs → Fetch ciphertext → Decrypt → Aggregate → Display
```

---

## S10. Security Notes & Best Practices `⟦SECURITY_NOTES⟧`

* **Key hygiene:** treat EIK as a root secret; isolate derived keys by purpose; never reuse AEAD nonces across domains. (Google, n.d.; Böttger et al., 2025). ([Google for Developers][2])
* **Implementation pitfall (observed):** do not log raw keys/wrappers outside diagnostics; when diagnosing, prefer *lengths, hashes, and offsets* over full hex dumps.
* **Timebase correctness:** implement “seconds since setup” as the authoritative counter for EID derivation; treat Unix epoch only as an outer reference. (Böttger et al., 2025). ([Pet Symposium][1])
* **Resolver robustness:** rolling window (prev/current/next) is mandatory due to jittered rotations. (Böttger et al., 2025). ([Pet Symposium][1])

---

## S11. Open Points / Not Publicly Specified `⟦OPEN_POINTS⟧`

* Exact semantics and cryptographic definition of **UT byte** and **hashed flags** across accessory generations (unless you hold the applicable partner spec revision).
* Exact format of **60-byte EIK wrapper** (nonce/tag sizing, AAD rules, versioning) beyond the confirmed AES-GCM structure; capture detailed layout once more test vectors are recorded.
* Precise REST endpoints, headers, attestation details, and server retention policies (subject to change).

---

## S12. Operational Behavior & Logs `⟦OP_LOGS⟧`

**Purpose:** Provide anonymized, real-world resolver traces showing the rolling-window logic, offset interpretation, and `0x40` frame format.

### S12.1 Anonymized hit sequence (0x40 legacy frame)

```text
2025-12-21 13:16:32.531 INFO (MainThread) [custom_components.googlefindmy.eid_resolver] HIT: device=device_id_1 canonical=canonical_id_1:beacon_guid_1 reversed=False offset=-1759955231 eid_prefix=561cb584
2025-12-21 13:16:32.532 WARNING (MainThread) [custom_components.bermuda] FMDN RAW SPY: Scanner BT Scanner 6 Lab -> AA:BB:CC:DD:EE:FF | Data: 40a1b2c3d4e5f60718293a4b5c6d7e8090a1b2c3d4e5
2025-12-21 13:16:32.533 INFO (MainThread) [custom_components.googlefindmy.eid_resolver] HIT: device=device_id_2 canonical=canonical_id_1:beacon_guid_2 reversed=False offset=-1748949279 eid_prefix=a1b2c3d4
2025-12-21 13:16:32.533 WARNING (MainThread) [custom_components.bermuda] FMDN RAW SPY: Scanner BT Scanner 6 Lab -> 11:22:33:44:55:66 | Data: 40c03146443e20a49e2a2a6711a6c87641b8e8a68bdc
```

### S12.2 Interpretation

* `Data: 40…` — `0x40` is the **Frame Type / UT byte** for the legacy 160-bit advert. The following **20 bytes** encode the EID, and the final **1 byte** carries hashed/flag bits, for a total of 22 bytes in the service data payload.
* `HIT` — the resolver found the broadcast EID in the **precomputed rolling window (previous/current/next)** derived from the unwrapped 32-byte EIK.
* `offset` — `observed_broadcast_time - expected_rotation_anchor`. Negative values show the beacon arrived earlier than the nominal anchor (jitter advancing the frame); positive values would indicate a late/clock-skewed advert. The resolver records the offset for diagnostics while accepting matches in adjacent windows so jitter and clock skew do not cause false negatives.
* **Anonymization:** Device IDs, canonical IDs, MAC addresses, and EID payloads above are redacted into realistic placeholders while preserving structure (0x40 prefix, 20-byte EID body, 1-byte flag) to illustrate the frame format without exposing production data.

---

## S13. On-demand Beacon Actions Diagnostic `⟦BEACON_DIAGNOSTIC⟧`

`googlefindmy.read_beacon_state` (administrators only, always returns a response) asks a tracker directly what it holds. Nothing runs unless it is called: no timer, no listener, no entity, and the module logs at DEBUG only.

**Operations.** The set is closed in code (`beacon_actions.DataId`): `0x00` read beacon parameters, `0x01` read provisioning state, `0x06` read ringing state. Setting or clearing the EIK, ringing, reading the EIK and unwanted-tracking commands are rejected before any GATT access (`beacon_actions._BeaconSession.run`). Each operation reads a fresh nonce, writes the request, waits for the notification and verifies its authentication segment.

**Finding the tracker.** Connectable `0xFE2C` advertisements whose account key filter holds the device's account key; the newest sighting wins, RSSI only breaks ties, because the cache keeps a tracker's previous address and its older RSSI can be the stronger one. If a connection fails, the selection repeats and the next address gets its turn, up to three tries within `timeout`. Untried addresses share the time, each try is capped at 25 s, and up to 10 s stay reserved for the reads; `connection_attempts` lists the tries with masked addresses. An address, also a lone one, is tried again only when no untried one is left. The filter is probabilistic: another device can match by chance (roughly 1 in 40 000 per frame for a filter holding one key, more for fuller filters), and it would then see a nonce answered with an HMAC, nothing it could derive a key from. A completely full filter never matches. The spec does not promise such frames for every provisioned tracker (a locator tag may stop them after reading the beacon parameters following a power loss), so `address` can name the target; `filter_checked` is then `false`.

**Reading the result.**

| `eik_set` | `eik_matches` | `is_owner` | Meaning |
|---|---|---|---|
| `true` | `true` | any | The tracker holds this EIK. With `eid_check.result: match` the derivation is known; with `no_match` it differs |
| `true` | `false` | `false` | Another EIK and this account key is not the owner key: the tracker likely belongs to another setup |
| `true` | `false` | `true` | Another EIK although this key is the owner: the integration likely reads the wrong key |
| `false` | `false` | any | Not provisioned; the unauthenticated error says nothing about the key here |
| `null` | `true` | `null` | The tracker holds this EIK, but `0x01` was not verified: the account key is missing or not accepted, see `operations` |
| `null` | `null` | `null` | `0x01` was not verified and `0x06` decided nothing, see `operations`. Not a verdict on the EIK |
| any | `null` | any | Undecided (timeout, transport error, malformed or skipped operation), see `operations`. Never read this as a foreign key |

`connection` says how far the run got: `no_matching_frame`, `no_account_key_for_search`, `device_not_connectable`, `timeout`, `connection_failed`, `gatt_setup_failed` (the fields above are then absent) or `connected`.

`eik_matches` is `false` only after a `0x06` write was rejected with `0x80` on a link where `0x01` verified. The spec also returns `0x80` for an unprovisioned Provider, and over an ESP32 proxy the same number can be the local `ESP_GATT_NO_RESOURCES`; a verified `0x01` on the same link makes that less likely but cannot exclude it, so repeat a `false` before acting on it. `eid_check` compares the reported EID with every variant for the windows around the tracker's clock, because rotation happens 1 to 204 s after the calculated boundary.

**Responses the parser does not know.** If `0x01` fails the format check, `operations.read_provisioning_state.response` gives `stage` (`nonce`, `frame`, `length`, `auth`), `frame_length`, `data_length_field`, `additional_length` and `auth_valid`; an authenticated frame adds `flags_byte` and the first five data bytes. `eid_check` then also tries the 20 and 32 bytes after the first byte, or the whole data if it is exactly that long, and carries `inferred_form: true`: the specification does not define that form, so a match proves the EID, not the layout. `include_raw_response: true` adds the whole frame as `raw_frame`, which holds the full current EID.

**Searching the counter.** The specification clears the low 10 bits of the clock, and `eid_check` follows it. With `search_seconds` above 0 the action looks second by second around the clock, with the low bits left in place, for the counter that yields the reported EID, the EID the chosen address advertises, or `search_eid`. `eid_search` gives `result` (`match`, `no_match`, `time_budget_exhausted`, `no_eik`, `no_clock`, `no_target`) and on a match `source`, `variant`, `counter_offset` and `counter_low_bits`. `counter_low_bits: 0` is what the specification prescribes, anything else is a tracker departing from it. The search runs after the link has closed, so it holds no proxy slot, and in an executor, so the event loop keeps running; it loads the CPU, stops after 5 minutes at the latest and keeps the action busy until then. If a calling script stops or Home Assistant shuts down, the search ends within a moment; closing the browser or losing an API connection does not stop it. Measured in a container: 400 to 2 400 counters per second for a 20-byte EID, depending on load, and less when a 20-byte and a 32-byte EID are searched together, so a day on each side takes from about 72 s to the whole budget. A result of `time_budget_exhausted` says the window was not covered, see `counters_checked`.

**Two identities.** Earbuds can run two identities under rotating addresses, both with a Fast Pair frame that matches the account key. Measured on Pixel Buds Pro 2 (firmware 6.144, case open and powered): the `locator` sends the FMDN frame and answers `0x01` with a placeholder (`00ff` and zeros), and the `audio` device announces Common Audio (`0x1853`), sends no FMDN frame and answers `0x01` with a rotating value that was not seen on air. On such a device the table above holds only for the identity whose `0x01` and `0x06` behaviour is known: `eid_check.result: no_match` at `audio` says nothing about the derivation, because that value is not the advertised EID, so compare `advertised_eid_check` at the `locator` instead. The response gives `identity_form` (`locator`, `audio`, `other`) and `advertised_tx_power` of the chosen address (`identity_form` is absent when no address was chosen and `null` without a sighting; `other` includes an FMDN service with a different frame type), and for a locator with an EID `advertised_eid_prefix` and, after a verified clock, `advertised_eid_check`. Home Assistant merges service data per address, so these can stem from an earlier advertisement of the same address. The input `identity_form` restricts the search to one of them and has no effect on a named address; `frames_other_form` counts the matches it skipped.

**The response form with one more byte.** The same earbuds answer `0x01` with 22 bytes instead of the specified 21: flags, 20 bytes, and one byte the authentication covers but the specification does not define. The parser takes that form (also 34 bytes), sets `eik_set` and `is_owner`, gives the byte as `trailing_byte`, and `eid_check` carries `inferred_form: true` when an EID was compared. A placeholder is reported as `eid_placeholder: true` and is neither compared nor searched for, also when it is advertised or supplied as `search_eid`. Only the 22-byte form with a 20-byte placeholder is measured, the 32-byte forms are inferred. `protocol_version` is the version the tracker gave with its nonce.

**Searching derivations.** With `derivation_windows` above 0 the action tries other key, block and scalar forms on secp160r1 at the rotation boundaries around the clock, nearest first, against the reported EID, the advertised EID and `search_eid` (a value both reported and advertised has `source: reported`). These are guesses, the specification names only the EIK and Table 10. 32-byte targets are skipped, so a lone one gives `no_target`. `derivation_search` gives `result` (`match`, `no_match`, `time_budget_exhausted`, `no_key`, `no_clock`, `no_target`), the counts `windows`, `keys`, `blocks`, `scalars`, `targets`, `combinations_checked` and on a match the names `source`, `key`, `block`, `scalar` and `window_offset`, never key bytes. `no_match` says only that none of these guesses holds, and `time_budget_exhausted` that the outer windows were not reached (measured in a test container: about 1600 combinations per second, so some 64 of the at most 256 windows per side fit). It runs after the connection has closed, for at most 5 minutes, and after a counter search if one was asked for, so a call can take 10 minutes more than one without searches; other beacon diagnostics are refused as busy meanwhile.

**Owner account key.** The spec says: "The Provider chooses one of the existing account keys as the owner account key the first time a Seeker accesses the Beacon Actions characteristic. The chosen owner account key can't be changed until the Provider is factory reset." Use the diagnostic on trackers that are already set up.

**Privacy.** Keys never appear in logs, responses or errors. The response carries the masked address and the first four bytes of the EID, unless `include_raw_response` asks for the whole frame. `search_eid` is never echoed, and an EID the chosen address advertises only by its first four bytes.

## Appendix A — Evidence-anchored Pseudocode `⟦APPX_PSEUDOCODE⟧`

### A1. EID derivation (Measured pattern; secp160r1 default)

```text
input:
  EIK (32B), K (e.g., 10), t = seconds_since_setup

1  ts_u32   = t mod 2^32
2  ts_mask  = ts_u32 & ~((1<<K)-1)           # clear lower K bits
3  B        = 0xFF*11 || byte(K) || u32be(ts_mask) || 0x00*11 || byte(K) || u32be(ts_mask)   # 32 bytes
4  seed     = AES-ECB-256(key=EIK, plaintext=B)     # 32 bytes
5  r        = int_be(seed) mod n_curve
6  if r == 0: r = 1
7  R        = r * G
8  EID      = x(R) encoded big-endian, width = 20 bytes (secp160r1) or 32 bytes (p256)
```

### A2. Rolling-window resolver (Measured necessity due to jitter)

```text
for each device:
  for window in {prev, current, next}:
     t_rel = seconds_since_setup_at(window_anchor)
     eid   = EID(EIK, t_rel)
     cache[eid] = device_id
```

### A3. Unwrap before EID (Observed in your integration)

```text
input:
  encryptedIdentityKey (len observed: 60 bytes)
output:
  EIK (32 bytes)

1  EIK = AES-GCM-Unwrap(owner/account-bound key material, encryptedIdentityKey, aad?, nonce?, tag?)
2  assert len(EIK) == 32
3  proceed with A1
```

---

## References (URLs)

```text
Peer-reviewed research:
- Böttger et al. (2025). PoPETs 2025 paper (PETS): https://petsymposium.org/popets/2025/popets-2025-0147.pdf

Official / product communications:
- Google Security Blog (2024). Unwanted tracker alerts / protections: https://security.googleblog.com/2024/05/multi-layered-user-protections-from-unwanted-tracker-alerts.html
- Android Help (n.d.). Find unknown trackers: https://support.google.com/android/answer/13658562?hl=en

Developer documentation (public summaries / partner docs landing):
- Google Developers (n.d.). Find Hub / Fast Pair extensions (Find Hub network specs): https://developers.google.com/nearby/fast-pair/specifications
- Google Developers (n.d.). Find Hub landing page: https://developers.google.com/nearby/fast-pair/landing-page-find-hub

Standards context:
- IETF DULT WG: https://datatracker.ietf.org/group/dult/

Marketing context:
- Android Find Hub overview: https://www.android.com/learn-find-hub/
```

[1]: https://petsymposium.org/popets/2025/popets-2025-0147.pdf "Okay Google, Where’s My Tracker? Security, Privacy, and Performance Evaluation of Google's Find My Device Network"
[2]: https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn "Find Hub Network Accessory Specification  |  Fast Pair  |  Google for Developers"
[3]: https://security.googleblog.com/2024/04/find-my-device-network-security-privacy-protections.html "
Google Online Security Blog: How we built the new Find Hub network with user security and privacy in mind
"
[4]: https://petsymposium.org/2025/paperlist.php "Privacy Enhancing Technologies Symposium 2025"
