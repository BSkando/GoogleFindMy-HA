# Play Sound Architecture

## Overview

The Play Sound feature allows users to ring a tracked device (FMDN tag, headphones,
Android phone) from Home Assistant. This document describes the current cloud-only
implementation, the upstream state (which is identical), and the future architecture
for direct BLE ringing.

**Key fact:** Neither upstream (leonboe1/GoogleFindMyTools) nor this fork parse the
Nova API response. Both implementations are fire-and-forget. The response format is
undocumented by Google and has never been decoded by any known open-source project.

---

## Current Architecture: Cloud-Only (Nova API)

### Request Flow

```
User presses "Play Sound" button in HA
       |
       v
button.py  GoogleFindMyPlaySoundButton.async_press()
       |  calls hass.services.async_call(DOMAIN, SERVICE_PLAY_SOUND, ...)
       v
api.py  async_play_sound()
       |  validates push readiness, resolves FCM token
       v
start_sound_request.py  async_submit_start_sound_request()
       |  builds protobuf payload, submits to Nova
       v
sound_request.py  create_sound_request(should_start=True, ...)
       |  creates ExecuteActionRequest protobuf
       v
nbe_execute_action.py  create_action_request() + serialize_action_request()
       |  sets scope=SPOT_DEVICE, action=startSound, component=UNSPECIFIED
       |  serializes to hex
       v
nova_request.py  async_nova_request(NOVA_ACTION_API_SCOPE, hex_payload)
       |  authenticates (AAS -> ADM token chain)
       |  POST to https://android.googleapis.com/nova/{NOVA_ACTION_API_SCOPE}
       v
Google Cloud Server
       |  routes command via FCM push notification to device
       v
Target Device rings
       |  (device sends FCM push back to confirm — see "Two Confirmation Paths")
       v
fcm_receiver_ha.py  _handle_notification_async()
       |  receives DeviceUpdate protobuf via FCM
       |  BUT: no callback registered for sound events → falls through
       v
(Sound confirmation silently lost)
```

### Key Files

| File | Responsibility |
|------|---------------|
| `button.py` | `GoogleFindMyPlaySoundButton` / `StopSoundButton` — HA button entities |
| `api.py` | `async_play_sound()` / `async_stop_sound()` — entry point, FCM token resolution |
| `sound_request.py` | `create_sound_request()` — pure protobuf builder (no I/O) |
| `nbe_execute_action.py` | `create_action_request()` + `serialize_action_request()` — protobuf envelope |
| `start_sound_request.py` | `async_submit_start_sound_request()` — Nova submission |
| `stop_sound_request.py` | `async_submit_stop_sound_request()` — Nova submission |
| `nova_request.py` | `async_nova_request()` — HTTP transport, auth, retry |
| `fcm_receiver_ha.py` | FCM push receiver — handles ALL incoming notifications |
| `_cli_helpers.py` | CLI FCM token resolution for standalone testing |

### Protobuf Structure (Request Only — No Response Defined)

```protobuf
// DeviceUpdate.proto — only request messages exist
message ExecuteActionRequest {
    ExecuteActionScope scope = 1;              // SPOT_DEVICE + canonicId
    ExecuteActionType action = 2;              // startSound or stopSound
    ExecuteActionRequestMetadata requestMetadata = 3;  // requestUuid, gcmRegistrationId
}

message ExecuteActionType {
    ExecuteActionLocateTrackerType locateTracker = 30;
    ExecuteActionSoundType startSound = 31;    // component = DEVICE_COMPONENT_UNSPECIFIED
    ExecuteActionSoundType stopSound = 32;
}

// NO ExecuteActionResponse message exists — not in upstream, not here, not in
// Google's public proto repositories (googleapis/googleapis).
```

### Two Potential Confirmation Paths (Neither Currently Used)

There are two distinct mechanisms that could confirm a ring command succeeded:

#### Path A: Nova HTTP Response (unknown format)

```
nova_request.py:1407-1408
    if status == HTTP_OK:
        return cast(bytes, content).hex()   # ← raw hex, NEVER PARSED
```

Google returns a protobuf body on HTTP 200. Its format is unknown:
- No `ExecuteActionResponse` proto defined anywhere (upstream, Google, or us)
- `google.internal.spot.v1.SpotService` is deliberately excluded from public APIs
- `_decode_error_response()` only runs for non-200 statuses
- `NovaLogicError` is defined but never raised (dead code)
- Upstream (leonboe1) also ignores this response — return value is discarded

#### Path B: FCM Push Callback (infrastructure exists, not wired for sound)

The LocateTracker flow uses FCM callbacks:
1. Register callback via `fcm_receiver.async_register_for_location_updates(canonic_id, cb)`
2. Submit Nova request
3. Wait for FCM push containing `DeviceUpdate` protobuf with matching `requestUuid`

For sound events, **no callback is registered.** The FCM push arrives via
`_handle_notification_async()`, but with no registered callback, it falls through
to `_process_background_update()` which tries to decode it as a location response.

The `DeviceUpdate` protobuf includes `ExecuteActionRequestMetadata.requestUuid` which
could be matched against the UUID from `start_sound_request()` for correlation.

### What This Means

| What we know | What we don't know |
|---|---|
| HTTP 200 = Google accepted the HTTP request | Whether the command reached the device |
| Response body is non-empty protobuf | The response protobuf schema |
| FCM push arrives after successful commands | Whether sound-specific FCM pushes differ from location pushes |
| `requestUuid` is sent and echoed in FCM | Whether the HTTP response also echoes it |

**IRR-CA-NO-RING-CONFIRMATION (deliberate boundary).** A Nova HTTP 200 proves
that the submission was accepted, nothing more. No `ExecuteActionResponse`
schema exists (Path A) and no FCM callback is registered for sound (Path B),
so on the cloud path neither delivery nor execution is observable. Every
`ok`/`submitted`/`cancelled` produced by `api.py` and by
`coordinator/locate.py` means exactly this and must not be worded as
confirmation.

The boundary is a property of the *cloud* path, not of the protocol. The FMDN
specification does define a ring confirmation: when ringing starts or stops,
the beacon emits a notification on the Beacon Actions characteristic
(`FE2C1238-8366-4814-8EB0-01DE32100BEA`) carrying a ringing state of Started,
Failed to start or stop, Stopped (timeout), Stopped (button press) or Stopped
(GATT request). That signal exists on a channel this integration does not
speak. Saying "no confirmation exists" would therefore be wrong; the accurate
statement is that no confirmation is reachable *over Nova*.

Two consequences follow, and both are already implemented:

- `StopSoundOutcome` is four-valued rather than boolean, so a stop that was
  submitted without a provable cancel key is reported as `UNCORRELATED`
  instead of being collapsed into success (BSkando#195).
- Even `CANCELLED` claims only "submitted with a correlated cancel key", never
  "the device stopped".

**IRR-CA-SOUND-FAILURE-CLASS (who refused, and who may be blamed).** A sound
command can fail in ways that have nothing to do with each other, and the layer
that knows which one happened is `api.py`. `SoundDispatchOutcome` carries that
knowledge across the boundary instead of collapsing it into a bool:

| Outcome | The server answered | May a caller arm a push cooldown |
|---|---|---|
| `ACCEPTED` | yes, HTTP 200 | no |
| `REJECTED_AUTH` | yes, on credentials (HTTP 401/403, or a permanent auth error) | no |
| `REJECTED_RATE_LIMIT` | yes, HTTP 429 | no |
| `REJECTED_SERVER` | yes, for any other reason (5xx, a Nova logic error, and the non-credential 4xx such as 400/404) | no |
| `TRANSPORT_FAILED` | no usable answer was obtained | **yes** (see the stop-path qualifier below) |
| `NOT_SENT` | no transport was used at all | no |
| `INTERNAL_ERROR` | our own defect or a broken contract | no |

Only `TRANSPORT_FAILED` describes a broken push transport. Reporting anything
else as one is what let an expired sign-in, a rate limit and a bug of our own
each produce the same 90-second cooldown and the same `FcmStatus.DEGRADED`, and
`can_play_sound()` reported the button as unavailable for the duration. This is
the same failure class as `StopSoundOutcome` one layer up: a bool cannot carry
the state space.

`PlaySoundResult` pairs that outcome with the cancel key, and the two are
deliberately independent. The key answers one question only, "may the device be
ringing" (see IRR-CA-CANCEL-KEY-ON-SUCCESS-ONLY); the cause is read from the
outcome and never inferred from the presence or absence of a key.

The rule has exactly one consumer, and stating it in a type is not the same as
enforcing it. `coordinator/locate.py` holds every call to
`_note_push_transport_problem()` on the sound paths, and `async_play_sound` and
`async_stop_sound` each arm it on `TRANSPORT_FAILED` alone. On the stop path the
call is indirect and carries one further condition: it goes through
`_note_stop_transport_problem_without_extending()`, which reports the failure in
full but restores a window that was already running instead of restarting it
(IRR-CA-STOP-BREAKS-SELF-INFLICTED-COOLDOWN below). Their blanket
`except Exception` handlers do not arm it at all: `api.py` classifies every
unexpected exception in band and returns `INTERNAL_ERROR` rather than raising
(`api.py`, the final handler of both sound methods), so what still reaches those
coordinator handlers is a failure of the coordinator's own bookkeeping around
the call, and none of that is the push transport. The two typed handlers for
`TimeoutError`/`ClientConnectionError`/`ClientError` do keep the cooldown: `api`
is a Protocol, and an aiohttp error surfacing from an implementation that does
not wrap it really is a transport failure. The `pytest.mark.parametrize` tables
in `tests/test_coordinator_locate_basics.py` carry one row per enum member and
are guarded against a member being added without a decision, so the "may a
caller arm a push cooldown" column above is a measured claim, not a wish.

**IRR-CA-PLAY-REMEDY-SPLIT (what the user is supposed to do about it).**
`SoundDispatchOutcome` says who refused. It does not say what the person who
pressed the button should do next, and that is a different question with a
different answer set. `coordinator.async_play_sound` therefore returns its own
type, `PlaySoundOutcome`, and `services.py` turns it into one of two messages:

| `PlaySoundOutcome` | Reached by | User message | Remedy |
|---|---|---|---|
| `ACCEPTED` | `SoundDispatchOutcome.ACCEPTED` | none (the call succeeds) | none -- though "accepted" is still a statement about the submission, not about the device (IRR-CA-NO-RING-CONFIRMATION) |
| `SUPPRESSED` | the readiness gate, on an active push cooldown while the ring capability is still unknown | `play_sound_suppressed` | wait; the window clears itself within 90 seconds |
| `FAILED` | everything else: `can_ring is False`, all six non-accepting `SoundDispatchOutcome` members, `ConfigEntryAuthFailed`, a connection error, an unexpected exception, and any value from outside the enum | `play_sound_rejected` | read the log entry, then act on what it names |

This is the split the stop path has carried since `StopSoundOutcome` landed, and
the dividing line is the same: the REMEDY, not how far the request travelled.
Two members of the readiness gate's single `False` end on opposite sides of it.
A device whose cached capability says `can_ring is False` is `FAILED`, because
no amount of waiting makes it ring; an active push cooldown is `SUPPRESSED`,
because waiting is the entire fix. Those are the gate's only two `False` paths
-- an unknown device is waved through optimistically -- so `SUPPRESSED` has
exactly one source, and no answer of `api.py` can produce it.

Two deliberate blunt edges. `REJECTED_RATE_LIMIT` does pass with time and is
still filed under `FAILED`, matching `StopSoundOutcome.FAILED` rather than
sorting the same condition two ways across sibling paths; the message names the
rate limit. And `NOT_SENT` never reaches the wire and is `FAILED` all the same,
for the reason its own docstring gives.

**Why there is no third message for authentication.** `REJECTED_AUTH` looks like
the obvious candidate for "your sign-in expired, sign in again", and it is not
one -- not because the expired sign-in is absent from it, but because it is
indistinguishable inside it.

Three different situations arrive under that one member. A 401 that survived the
refresh sequence is raised as `NovaAuthError(..., is_permanent=True)`
(`nova_request.py`), and `is_credential_rejection()` answers True on the
permanence flag before it ever looks at the status. A plain HTTP 403 answers
True on the status. And an error with no readable status answers True as the
conservative default. `PlaySoundResult` carries the outcome, not the permanence
flag, so by the time the coordinator sees `REJECTED_AUTH` the three are one.

Only the first of them means "sign in again". The second may not: 403 is an
authorization verdict, and Google's own API guidance (AIP-193) requires
`PERMISSION_DENIED` (HTTP 403) even for a resource that does not exist, with the
permission check running *before* the existence check. A message that read
`REJECTED_AUTH` as an expired sign-in would therefore tell the owner of a deleted
or unshared device to check their credentials -- precisely the misreading
`is_credential_rejection` removed once already, and its docstring says so.

The fix is not a third message on an ambiguous member; it is to stop the
ambiguity at its source by surfacing `is_permanent` in the result, which is a
change to the `api.py` contract and is tracked separately. Until then,
`play_sound_rejected` names the expired sign-in among the possible causes and
points at the log, exactly as `stop_sound_rejected` does -- and the log entry
does distinguish the three, because `nova_request.py` logs the permanent 401 in
its own words.

**IRR-CA-STOP-BREAKS-SELF-INFLICTED-COOLDOWN (the window must not silence its
own remedy).** A play that reaches the wire and then loses the answer does two
things in one breath: it stores a cancel key, and it correctly arms the
90-second push cooldown. `_api_push_ready()` short-circuits on that cooldown, so
the stop the key exists for used to be suppressed for the first 90 seconds after
that play, which is exactly when a user reaches for the Stop button. (How long
the ring itself lasts is a different timer on a different layer, and nothing
here claims to know it -- see the note on the aged cached key in
`async_stop_sound`.) `async_stop_sound` therefore passes the readiness
gate in exactly one case: a window is running **and** the stop would be
correlated, that is, it carries our own cached key and that key is still fresh.

The polarity is deliberately the opposite of `can_play_sound()` and the manual
locate guard, which treat a running window as the hard block and an unconfirmed
transport as passable. Two limits keep the exception honest:

- **What it can distinguish.** While a window runs, `_api_push_ready()` never
  asks the transport, so the transport state is unknown. The guard separates
  "not ready because a window is running" from "not ready for another reason"
  and nothing more; a transport failure is the only thing that arms the window,
  so the two overlap by construction.
- **It cannot feed itself.** A broken-through stop that fails on the transport
  must not restart the window. `_note_push_transport_problem()` sets
  `_push_cooldown_until` absolutely, and the stop button has no availability
  guard, so an unguarded call would let repeated presses keep Play Sound and
  manual locate disabled indefinitely. `async_stop_sound` routes both of its
  cooldown calls through
  `_note_stop_transport_problem_without_extending()`, which arms a window only
  when none is running.

`_stop_would_be_correlated()` is the single definition of "correlated" behind
this gate, the `CANCELLED` outcome and the stop delay
(IRR-CA-STOP-NOT-INTO-RING-START); a second inline derivation at any site is
the drift the extraction exists to prevent, and
`tests/test_coordinator_locate_basics.py` pins the sites to it. It used to
decide whether an accepted stop may spend the cached key
(IRR-CA-POP-ON-CORRELATED-CANCEL-ONLY); that rule is superseded by
IRR-CA-STOP-KEEPS-CANCEL-KEY.

The exception is not free, and the price is a changed outcome class rather than
a changed guarantee: a stop that used to end as `SUPPRESSED` now reaches the
transport and can end as `FAILED`, which `services.py` reports with a different
translation key (`stop_sound_rejected` instead of `stop_sound_suppressed`). It
also removes, for the duration of a window, the only rate brake that sat in
front of the stop dispatch -- outside a window there never was one, and a
`REJECTED_RATE_LIMIT` answer deliberately arms no cooldown.

**IRR-CA-STOP-KEEPS-CANCEL-KEY (a stop spends no key).** A correlated stop
used to pop the cached key once Nova answered HTTP 200. HTTP 200 means Nova
took the stop, not that the tracker executed it, so a second press then went
out without any key and was reported as "could not be matched" -- for a ring
this integration had started itself. The key is now kept. It leaves the cache
the way every key does: a newer accepted play replaces it, it ages past
`SOUND_UUID_MAX_AGE_S` and the reload filter drops it, or the device is
removed. A repeated stop carries the same key and is `CANCELLED` again; the
cooldown exception applies to it unchanged and still cannot extend a running
window.

**IRR-CA-STOP-NOT-INTO-RING-START (hold a stop back until the ring has
started).** Measured on a moto tag 2 (2026-10-17/18, ten rings): a stop that
reached the tracker around the moment its ring was about to start left the
tracker silent -- no BLE advertisements until a long press on its button -- in
two of two runs (stops 11.8 s and 9.4 s after Nova accepted the play). Stops
11.6 s or later after acceptance, with the ring already audible, went through
in four of four. Where the ring was heard it started 5.6 to 10.3 s after
acceptance. `async_stop_sound` therefore holds a stop back until
`STOP_SOUND_MIN_DELAY_AFTER_PLAY_S` (12 s) after the acceptance of the play it
cancels, and logs the wait on INFO. Only our own accepted play arms the delay,
and only for its own key: a stop without a key, with an aged or foreign key, or
after an ambiguous play is sent at once, because nothing tells us when that
ring began. A stop the readiness gate declines is declined at once rather than
after the wait. Limits: six runs on one tracker model; in one failed run the
ring had still not started 11.8 s after acceptance, so 12 s is a boundary, not
a guarantee. Waiting for a confirmed start would be the stronger rule (see the
measurement below); no such signal is decoded yet.

#### Follow-up (not in this change)

The play path arms the same cooldown without this guard
(`coordinator/locate.py`, `async_play_sound`), and `can_play_sound()` returns
early on a cached `can_ring` capability before it ever looks at the window, so a
play can reach the arming call while a window runs. That behaviour predates the
stop-path exception and is unchanged by it; guarding it is a separate decision
with its own tests.

Closing the boundary on the cloud path means wiring **Path B**: registering an
FCM callback for sound events and matching the incoming
`ExecuteActionRequestMetadata.requestUuid` against the key held by the
coordinator. That is tracked separately and carries its own risks: an
unmatched push currently falls through to the location decoder, and a
callback that never fires would need a timeout policy of its own rather than
an indefinite wait.

*Measured (2026-10-17/18, live diagnostic logging on one installation, 19
play and stop actions).* The sound push **does** carry a `DeviceUpdate`: every
action produced three pushes, each with `fcmMetadata.requestUuid` set to the
key that was sent (empty after a stop sent without a key). The pushes carry the
device's secrets bundle (`encryptedUserSecrets`) and a
`deviceMetadata.information.locationInformation` without any reports; it is
this shape that makes the location decoder log `No locations found`. The
third push of each group carries a field outside the schema under
`locationInformation` -- field 1 after a start, field 2 after a stop -- whose
**length** separated the outcomes without exception in this series: 5 after a
ring that was heard, 6 after a start that was cancelled or failed, 2 after a
stop that was executed, 6 roughly 20 s after a stop that failed. The values
inside the field are not decoded, and the series covers one tracker model, so
the lengths are an observation, not a protocol contract; nothing in the code
relies on them.

Two consequences are wired in. The receiver recognises such a push as a Play/Stop
Sound acknowledgement when its `requestUuid` is one of the coordinator's own
Play Sound keys (`is_own_sound_request_uuid`); a set `requestUuid` alone is not
the criterion, because locate requests set one too. The push still runs through
the decryptor, since its secrets bundle feeds the device cache as a
metadata-only record; only `No locations found` drops from INFO to DEBUG, next
to a DEBUG line naming the acknowledgement. An empty locate answer keeps its
INFO line. And an uncorrelated stop now names its case to the user
(`StopSoundUncorrelatedReason`: no key, aged key, foreign key) instead of one
message listing every cause; the outcome itself stays `UNCORRELATED` and stays
an error for the caller (BSkando#195).

Closing it on the local path means speaking GATT directly (`0x05` with ring
operation `0x00`), which additionally yields a stop that is independent of who
started the ring. That is a separate design with its own plan; see the BLE
sections below for the protocol details.

### Upstream Parity

**Our code and upstream are functionally identical for PlaySound:**

| Aspect | Upstream (leonboe1) | This Fork |
|--------|---------------------|-----------|
| Nova HTTP response | `return response.content.hex()` — caller discards return value | `return cast(bytes, content).hex()` — caller stores but ignores `_response_hex` |
| Response parsing | None | None |
| FCM callback for sound | `lambda x: print(x)` (prints raw hex to stdout) | Not registered |
| BLE GATT ring code | **None** — only ring key derivation + registration | None |
| `ExecuteActionResponse` proto | Not defined | Not defined |

**Upstream has no BLE ring implementation.** The ring key derivation in
`key_derivation.py` and the `ringKey` field in `RegisterBleDeviceRequest` are used
during device registration to tell Google the ring key. No code exists to use that
key for direct BLE GATT writes. The BLE ring protocol was independently attempted by
community members in GitHub Issue #66.

### Authentication Chain

```
AAS Token (Google Account Sign-In)
    |
    v  async_nova_request() exchanges AAS -> ADM
ADM Token (Android Device Management)
    |
    v  Authorization: Bearer {ADM_TOKEN}
Nova API Endpoint (NOVA_ACTION_API_SCOPE)
    |
    v  FCM Push (device confirms back via FCM)
Device
```

Token management is handled by `AsyncTTLPolicy` in `nova_request.py` with:
- Proactive refresh before expiry
- Entry-scoped caching (multi-account safe)
- Automatic 401 recovery with multi-step retry (ADM refresh → AAS+ADM refresh → cooldown)

---

## BLE Ring Protocol (FMDN Specification)

### FMDN Beacon Actions Characteristic

The FMDN specification at `developers.google.com/nearby/fast-pair/specifications/extensions/fmdn`
defines a GATT characteristic for direct device control:

- **UUID:** `FE2C1238-8366-4814-8EB0-01DE32100BEA` (Beacon Actions)
- **Protocol:** Read nonce → compute auth → write command → read notification

| Data ID | Operation | Description |
|---------|-----------|-------------|
| `0x05` | Ring | Start ringing the tracker |
| `0x06` | Read ringing state | Check if currently ringing |

**Important:** The FMDN spec documents only the BLE-level protocol. It says nothing
about server-side APIs, Nova endpoints, or cloud ring commands.

### BLE Ring Protocol Steps

```
Step 1: Read Beacon Actions characteristic
        → Receive 8-byte random nonce from tracker

Step 2: Compute auth key
        ring_key = SHA256(EIK || 0x02)[:8]     # 8-byte truncated
        auth_data = HMAC-SHA256(ring_key, nonce || data_id=0x05 || addl_data)[:8]

Step 3: Write to Beacon Actions characteristic
        Payload: [data_id=0x05] [data_len] [8-byte auth_key] [op_mask(1B)] [timeout(2B BE)] [volume(1B)]
        Where: addl_data = op_mask + timeout + volume = 4 bytes
               data_len = len(auth_key) + len(addl_data) = 8 + 4 = 12

Step 4: Read notification (Table 6 in FMDN spec)
        → Ring state byte:
            0x00 = Started successfully
            0x01 = Failed (auth or hardware)
            0x02 = Stopped (timeout)
            0x03 = Stopped (button press)
            0x04 = Stopped (GATT command)
        → Components bitmask + remaining time
```

### Community BLE Ring Attempt — Detailed Analysis (Issue #66)

Source: https://gist.github.com/mik-laj/4c1c363391115ccb14ee856a9c1c12a1

mik-laj published a standalone `bleak`-based ring script (`ring_nearby.py`). The
script scans for FMDN advertisements, connects via GATT, and writes ring commands.
DefenestratingWizard identified **two bugs** (both in `data_len` handling):

#### Bug 1: Payload `data_len` field

```python
# BUG (mik-laj):
payload = bytes([DATA_ID_RING, len(addl)]) + auth8 + addl
#                               ^^^^^^^^  = 4 (only addl, missing auth key!)

# CORRECT:
payload = bytes([DATA_ID_RING, len(auth8) + len(addl)]) + auth8 + addl
#                               ^^^^^^^^^^^^^^^^^^^^^^^^  = 8 + 4 = 12
```

#### Bug 2: HMAC input `data_len` (causes wrong auth key!)

```python
# BUG (mik-laj):
data_len = len(addl)       # = 4, but HMAC sees wrong length → wrong auth

# CORRECT:
data_len = len(addl) + 8   # auth key (8B) IS counted in data_len for HMAC too
```

Both bugs together cause ATT Error `0x81`. Fixing only one is insufficient because
`data_len` appears in both the wire format AND the HMAC input — a wrong value
produces both a malformed payload and an incorrect authentication.

#### Verified Wire Format (from Wireshark capture of real Find Hub app)

```
Ring command (successful, captured from official Google Find Hub app):

  05 0c a7 25 03 f0 6a 9d d4 2a ff 02 58 00
  ── ── ──────────────────────── ── ───── ──
  │  │           │                │    │    │
  │  │           │                │    │    └── volume (0x00 = default)
  │  │           │                │    └── timeout (0x0258 = 600 deciseconds = 60s)
  │  │           │                └── op_mask (0xFF = ring all components)
  │  │           └── 8-byte HMAC-SHA256 one-time auth key
  │  └── data_len = 0x0c = 12 = 8 (auth) + 4 (addl)
  └── data_id = 0x05 (Ring)

Nonce/challenge read (from Beacon Actions characteristic):

  01 f3 be eb 39 9d 61 cf a0
  ── ────────────────────────
  │           │
  │           └── 8-byte random nonce
  └── proto_major = 0x01
```

#### Corrected HMAC Computation

```python
def make_auth(ring_key, proto_major, nonce8, data_id, addl):
    data_len = len(addl) + 8     # MUST include auth key length
    msg = bytes([proto_major]) + nonce8 + bytes([data_id, data_len]) + addl
    return hmac.new(ring_key, msg, hashlib.sha256).digest()[:8]
```

#### Corrected Payload Construction

```python
def build_ring_message(ring_key, nonce8, proto_major,
                       op_mask=0xFF, timeout_s=60.0, volume=0x00):
    t_ds = min(int(timeout_s * 10), 6000)
    addl = bytes([op_mask]) + struct.pack(">H", t_ds) + bytes([volume])
    auth8 = make_auth(ring_key, proto_major, nonce8, 0x05, addl)
    return bytes([0x05, len(auth8) + len(addl)]) + auth8 + addl
```

#### Open Question: Ring Key Derivation

mik-laj reported that keys from `FMDNOwnerOperations.generate_keys()` did not match
the keys observed in the Wireshark capture. He was uncertain whether the EIK (after
AES decryption with the owner key) or the raw encrypted identity key should be used.

Our code derives: `ring_key = SHA256(decrypted_EIK || 0x02)[:8]`, which matches the
FMDN spec. mik-laj may have used the encrypted key by mistake (he logged both).
DefenestratingWizard's fix resolved the `data_len` bug but did not confirm whether
the ring key derivation was also corrected — the issue remains open.

### Why Neither Codebase Has BLE Ringing

1. **Upstream is CLI-focused** — designed for OAuth + Nova API interactions, not BLE
2. **This fork is HA-focused** — HA servers are typically not BLE-adjacent to trackers
3. **Ring key is registered, not used** — `key_derivation.py` derives the ring key
   and `create_ble_device.py` sends it to Google during registration, but no code
   uses it for local BLE commands
4. **Community attempt unfinished** — mik-laj's script has bugs, no confirmed success

---

## DULT Non-Owner Sound Protocol (AirGuard)

### Discovery: AirGuard Uses a Completely Different Protocol

leonboe1's anti-stalking app **AirGuard** (`seemoo-lab/AirGuard`) implements BLE
ringing for Google FMDN trackers, but it does NOT use the FMDN Beacon Actions
characteristic. Instead, it uses the **DULT (Detecting Unwanted Location Trackers)**
protocol, defined in [IETF draft-ietf-dult-accessory-protocol-00](https://datatracker.ietf.org/doc/html/draft-ietf-dult-accessory-protocol-00).

This is a separate GATT service with no authentication — designed for the anti-stalking
use case where the caller does NOT own the tracker.

### DULT ANOS (Accessory Non-Owner Service) Details

| Attribute | Value |
|-----------|-------|
| Service UUID | `15190001-12F4-C226-88ED-2AC5579F2A85` |
| Characteristic UUID | `8E0C0001-1D68-FB92-BF61-48377421680E` |
| CCCD Descriptor | `00002902-0000-1000-8000-00805F9B34FB` |
| Byte Order | **Little endian** (opposite of FMDN Beacon Actions!) |
| Authentication | **None** |
| Availability | **Separated state only**; with Find Hub the service can turn the mode on (Data ID `0x07`, relayed through the Find Hub network) and only a holder of the identity key turns it off (`0x08`); DULT's 30 minutes bound the separation criterion, and Google's server criteria are not published. The accessory enables the unauthenticated motion-gated sound 8-24 hours after the state begins (DULT `T_(SEPARATED_UT_TIMEOUT)`, a randomised value from a uniform distribution: `draft-detecting-unwanted-location-trackers-01` sections 3.4.4 and 3.12.2.1, table 16; see `docs/TRIGGER_MECHANISMS.md` sections 4.1 and 8) |

### DULT Opcodes (Little-Endian Wire Format)

| Opcode Name | Logical Value | Wire Bytes (LE) | Direction | Required |
|-------------|--------------|-----------------|-----------|----------|
| Sound_Start | `0x0300` | `[0x00, 0x03]` | → Accessory (Write) | Yes |
| Sound_Stop | `0x0301` | `[0x01, 0x03]` | → Accessory (Write) | Yes |
| Command_Response | `0x0302` | `[0x02, 0x03]` | ← Accessory (Indication) | Yes |
| Sound_Completed | `0x0303` | `[0x03, 0x03]` | ← Accessory (Indication) | Yes |
| Get_Identifier | `0x0404` | `[0x04, 0x04]` | → Accessory (Write) | Optional |
| Get_Model_Name | `0x0005` | `[0x05, 0x00]` | → Accessory (Write) | Optional |

### DULT Command_Response Format

```
Byte 0-1: Response Opcode 0x0302 → wire [0x02, 0x03]
Byte 2-3: Echoed CommandOpcode (LE) — which command this responds to
Byte 4-5: ResponseStatus (LE):
          0x0000 = Success
          0x0001 = Invalid_state (already ringing / wrong state)
          0x0002 = Invalid_configuration
          0x0003 = Invalid_length
          0x0004 = Invalid_param
          0xFFFF = Invalid_command (not in separated state, or unsupported)
```

### DULT Sound Requirements

- Minimum duration: 5 seconds
- Maximum duration: 30 seconds (auto-stop by accessory)
- Recommended: 12 seconds
- Minimum loudness: 60 Phon peak at 25cm (ISO 532-1:2017)

### AirGuard's GATT Flow (Kotlin)

Source: `seemoo-lab/AirGuard` — `GoogleFindMyNetwork.kt`

```
1. connectGatt(context, false, callback)
2. onConnectionStateChange(GATT_SUCCESS, STATE_CONNECTED)
   → gatt.discoverServices()
3. onServicesDiscovered()
   → Find service containing "12F4" (substring match)
   → Get characteristic 8E0C0001-...
   → setCharacteristicNotification(true)     // no CCCD descriptor write!
   → writeCharacteristic([0x00, 0x03])       // DULT Sound_Start
   → broadcast ACTION_EVENT_RUNNING
4. onCharacteristicWrite(GATT_SUCCESS)
   → Handler.postDelayed(5000ms):            // hardcoded 5-second timer
     → writeCharacteristic([0x01, 0x03])     // DULT Sound_Stop
5. onCharacteristicWrite(GATT_SUCCESS) for stop
   → disconnect() + broadcast ACTION_EVENT_COMPLETED
```

**Weaknesses observed in AirGuard:**
- No timeout on connection or service discovery
- CCCD descriptor not written (no actual BLE indications received)
- Sound_Completed notification from tracker is never read
- Hardcoded 5-second duration (DULT recommends 12s)
- No handling if connection drops during the 5s timer

### Why DULT Is NOT Suitable For Us

**We are the owner.** Our trackers will typically be near HA (i.e., near the owner's
account). In near-owner state, the tracker responds to DULT Sound_Start with
`Invalid_command (0xFFFF)`.

DULT only works when the tracker is in "separated state". With Find Hub the service can put
the tracker into that state (Data ID `0x07`, relayed through the Find Hub network), and
only a holder of the identity key takes it out (`0x08`). DULT's 30 minutes bound the
separation criterion; Google's server criteria are not published. The unauthenticated
motion-gated sound becomes available only 8-24 hours after the state begins.
This is the opposite of our use case.

> **Provenance of both figures, corrected 2026-09-03:** they are normative DULT, not
> field reports. The 30 minutes are the near-owner → separated transition, `SHALL` in
> `draft-detecting-unwanted-location-trackers-01` section 3.4.4 with table 2 (section
> 3.4.5 governs the way back, on reunion); the 8-24
> hours are `T_(SEPARATED_UT_TIMEOUT)`, a "random value between 8-24 hours chosen from a
> uniform distribution", after which the accessory "MUST enable the motion detector"
> (section 3.12.2.1, table 16).
>
> **The FMDN mode is that same state, not a separate command-only concept.** The Find
> Hub Network Accessory Specification says so in its section "Unwanted tracking
> prevention": `"Unwanted tracking protection mode" defined in this document maps to the
> "separated state" defined by the DULT spec`, and certified devices "must also meet the
> requirements" of DULT (<https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn>, retrieved 2026-09-03). Data ID `0x07` / `0x08`
> on the Beacon Actions characteristic are an **additional**, seeker-driven path into and
> out of the mode, not the only one. An earlier revision of this block claimed the
> opposite; that claim is withdrawn.
>
> What survives unchanged is the operational point: the 8-24 hours gate the autonomous
> chime, not the flag bit, so the `uwt_mode` binary sensor must not be used as a
> separation timer ([BSkando#210](https://github.com/BSkando/GoogleFindMy-HA/issues/210)).
>
> **Third revision, 2026-10-11: who switches the mode.** The text above calls `0x07` /
> `0x08` an additional path next to an autonomous transition. The Find Hub specification
> says more about the mode itself: "The Find Hub service can relay an unwanted tracking
> protection mode activation request through the Find Hub network" (section "Unwanted
> tracking protection mode", retrieved 2026-10-11). Activation is authenticated with the
> unwanted tracking protection key; deactivation additionally requires that "The hashed
> ephemeral identity key matches the current ephemeral identity key". With Find Hub the
> service can therefore turn the mode on (`0x07`, relayed through the Find Hub network),
> and only a holder of the identity key can turn it off (`0x08`); in practice this is the
> owner's phone (an inference, not specified). DULT's 30 minutes bound the separation
> criterion; Google's server criteria are not published, and other vendors' firmware is
> not public. Field data: `docs/UWT_FIELD_DATA.md`.

**We must use FMDN Beacon Actions (authenticated ring)** because:
1. We have the EIK → can derive the ring key
2. It works regardless of separated state
3. It provides proper ring state notifications (started/failed/stopped + reason)

### Three Ring Sources (Confirmed by Nordic SDK)

The Nordic nRF Connect SDK (`nrfconnect/sdk-nrf`) confirms FMDN trackers have
three independent ring trigger sources:

| Source | SDK Constant | Protocol | Auth |
|--------|-------------|----------|------|
| Owner BLE ring | `FMDN_BT_GATT` | FMDN Beacon Actions (`FE2C1238`) | HMAC-SHA256 |
| Non-owner BLE sound | `DULT_BT_GATT` | DULT ANOS (`15190001-12F4`) | None |
| Motion auto-ring | `DULT_MOTION_DETECTOR` | Internal (separated state) | N/A |

> **The `HMAC-SHA256` in row 1 is conditional on how UTP mode was activated.**
> Activating unwanted tracking protection mode (Data ID `0x07`) takes an optional
> control flag, `0x01` "Skip ringing authentication", specified as "When set, ringing
> requests aren't authenticated while in unwanted tracking protection mode" (Find Hub
> Network Accessory Specification, section "Beacon Actions",
> <https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn>,
> retrieved 2026-08-05). A beacon activated with that flag set still expects
> authentication data on a ring request but no longer verifies it, so while the mode
> is active the owner ring path is reachable by any party in Bluetooth range without
> the ring key.
>
> Three consequences, in decreasing order of confidence:
>
> 1. An implementation of the owner ring must not infer from a successful ring that
>    the ring was authenticated, and must not treat the ring key as a capability that
>    only the owner holds while the mode is active.
> 2. A chime with no cloud request behind it is specification-conformant and does not
>    imply a defect in this integration
>    ([BSkando#195](https://github.com/BSkando/GoogleFindMy-HA/issues/195),
>    [BSkando#108](https://github.com/BSkando/GoogleFindMy-HA/issues/108)).
> 3. This flag is **not** the likeliest explanation for those reports. Row 2 of the
>    table above, the DULT non-owner sound, is unauthenticated by design and needs no
>    flag at all; the reporter in
>    [BSkando#210](https://github.com/BSkando/GoogleFindMy-HA/issues/210) attributes
>    the observed chirps to that path, having instrumented the event bus to rule this
>    integration out as the source. The `0x07` flag is documented here because it is
>    the only mechanism that also removes authentication from the **owner** path,
>    which the DULT explanation does not.
>
> The advertisement reports the mode (`uwt_mode` binary sensor), never the flag, and
> the mode itself is observed to flap on timescales of under a minute
> ([BSkando#210](https://github.com/BSkando/GoogleFindMy-HA/issues/210)). It is
> context for a report, not evidence of a cause.

---

## Comparison: All Three Ring Paths

| Aspect | Cloud (Nova API) | BLE Owner Ring (FMDN) | BLE Non-Owner Sound (DULT) |
|--------|------------------|-----------------------|---------------------------|
| **Latency** | 2-15 seconds (FCM) | < 1 second | < 1 second |
| **Range** | Global | ~30m BLE | ~30m BLE |
| **Auth** | Google OAuth + FCM | HMAC-SHA256 (ring key), but see the UTP control-flag note above: not verified while the tracker is in unwanted tracking protection mode that was activated with flag `0x01` | None |
| **Availability** | Always | Always (owner has key) | Separated state only |
| **Confirmation** | None (fire-and-forget) | Ring state notification | Command_Response indication |
| **Reliability** | FCM delivery dependent | Direct, deterministic | Direct, deterministic |
| **HA compat** | Works everywhere | Requires `bluetooth` | Requires `bluetooth` |
| **ESPHome proxy** | N/A | Supported (active mode) | Supported (active mode) |
| **MAC rotation** | N/A (server routing) | Must know current MAC | Must know current MAC |
| **Our use case** | **Primary path** | **BLE fallback** | Not applicable (owner ≠ separated) |

---

## HA Bluetooth Stack for Future BLE Ringing

### Architecture Layers

```
+-------------------------------------------------+
|  GoogleFindMy-HA (this integration)             |
|  Uses: establish_connection + write_gatt_char   |
+-------------------------------------------------+
|  bleak-retry-connector                          |
|  Handles: retries, backoff, service caching     |
+-------------------------------------------------+
|  Bleak (BleakClient)                            |
|  Handles: GATT protocol, platform abstraction   |
+-------------------------------------------------+
|  homeassistant.components.bluetooth             |
|  Handles: adapter discovery, scanner sharing,   |
|  ESPHome proxy routing, adapter failover        |
+-------------------------------------------------+
|  BlueZ (local USB) OR ESPHome BLE Proxy         |
+-------------------------------------------------+
```

### Standard Pattern for GATT Writes in HA

```python
from homeassistant.components.bluetooth import async_ble_device_from_address
from bleak_retry_connector import establish_connection, BleakClientWithServiceCache

# 1. Obtain BLEDevice from HA's bluetooth component
ble_device = async_ble_device_from_address(hass, current_mac)

# 2. Connect with retry logic
client = await establish_connection(
    BleakClientWithServiceCache,
    ble_device,
    name="fmdn_tracker",
    max_attempts=3,
)

# 3. Read nonce, compute auth, write ring command
nonce = await client.read_gatt_char(BEACON_ACTIONS_UUID)
payload = build_ring_payload(ring_key, nonce)
await client.write_gatt_char(BEACON_ACTIONS_UUID, payload)

# 4. Read response notification for confirmation
# ...

await client.disconnect()
```

### Bermuda vs. HA Bluetooth for BLE Ringing

| Aspect | Bermuda | HA Bluetooth (`homeassistant.components.bluetooth`) |
|--------|---------|------------------------------------------------------|
| **Purpose** | Passive room presence / trilateration | Full BLE stack (scan + GATT) |
| **GATT writes** | No | Yes |
| **Active connections** | No | Yes (via bleak-retry-connector) |
| **ESPHome proxy** | Reads RSSI only | Full GATT proxy (active mode) |
| **Role in this project** | Location signal source, EID advertisement relay | Required for future BLE ringing |

**Bermuda is not needed for BLE ringing.** The HA bluetooth integration provides
everything required. Bermuda's role remains passive location tracking.

### MAC Address Rotation Challenge

FMDN trackers rotate their BLE MAC address for privacy. To connect via GATT, the
current MAC must be known. Two resolution paths exist:

1. **From HA scanner data:** `async_ble_device_from_address(hass, current_mac)` — but
   this requires knowing the rotated MAC, which changes every ~15 minutes.
2. **From EID resolution:** The `eid_resolver.py` already maps EIDs to device
   identities. The BLE advertisement that contained the matched EID also carries the
   current MAC address (in `BluetoothServiceInfoBleak.address`). This address can be
   captured during EID resolution and stored for the connection window.

### Required Manifest Changes for BLE Support

```json
{
    "dependencies": ["http", "bluetooth"],
    "requirements": [
        "bleak>=0.21.0",
        "bleak-retry-connector>=3.4.0",
        ...existing requirements...
    ]
}
```

---

## Key Derivation for Ringing

The ring authentication key is derived from the Ephemeral Identity Key (EIK):

```
EIK (32 bytes, from device registration)
    |
    v  SHA256(EIK || 0x02)[:8]
Ring Key (8 bytes)
    |
    v  HMAC-SHA256(ring_key, nonce || 0x05 || addl_data)[:8]
One-Time Auth Key (8 bytes, sent in GATT write)
```

This derivation is already implemented in `key_derivation.py:58`:
```python
self.ringing_key = calculate_truncated_sha256(identity_key_bytes, 0x02)
```

The ring key is currently only used during device registration
(`create_ble_device.py:110`), but will be reused for direct BLE ring commands.

---

## Glossary

| Term | Definition |
|------|-----------|
| **Nova API** | Google's server-side API for Find My Device actions |
| **FCM** | Firebase Cloud Messaging — push notification transport |
| **FMDN** | Find My Device Network — Google's crowdsource tracker protocol |
| **EIK** | Ephemeral Identity Key — 32-byte root key for tracker crypto |
| **EID** | Ephemeral Identifier — rotating BLE address derived from EIK |
| **Beacon Actions** | FMDN GATT characteristic (`FE2C1238`) for owner-authenticated commands (ring, UTP); ring authentication can be waived for the duration of UTP mode by control flag `0x01` at activation |
| **DULT** | Detecting Unwanted Location Trackers — IETF specification for anti-stalking |
| **ANOS** | Accessory Non-Owner Service — DULT GATT service (`15190001-12F4`) for unauthenticated commands |
| **Separated State** | DULT accessory state; DULT bounds the separation criterion at 30 minutes and ends the state on reunion (DULT sections 3.4.4 / 3.4.5). With Find Hub the service can turn the mode on (`0x07`, relayed through the Find Hub network) and only a holder of the identity key turns it off (`0x08`); Google's server criteria are not published; the unauthenticated DULT sound is enabled only after a further 8-24 hours (`T_(SEPARATED_UT_TIMEOUT)`, section 3.12.2.1 and table 16). Both figures are normative DULT. The Find Hub specification **maps** its unwanted tracking protection mode onto this state, so the `uwt_mode` binary sensor reports it. The 8-24 hours gate the motion-triggered chime, not the flag bit |
| **GATT** | Generic Attribute Profile — BLE protocol for read/write operations |
| **CCCD** | Client Characteristic Configuration Descriptor — enables BLE notifications/indications |
| **ADM** | Android Device Management — Google auth token type |
| **AAS** | Android Account Sign-In — Google auth token type |
| **Bermuda** | Third-party HA integration for BLE room presence |
| **bleak** | Python BLE library used by HA's bluetooth integration |
| **AirGuard** | Anti-stalking app by seemoo-lab/leonboe1 — uses DULT protocol (not FMDN Beacon Actions) |
| **Nordic SDK** | nRF Connect SDK — reference implementation for FMDN+DULT tracker firmware |
