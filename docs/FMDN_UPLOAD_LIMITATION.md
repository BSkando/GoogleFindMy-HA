# FMDN Finder Upload Limitation

## TL;DR

**The FMDN Finder upload functionality is disabled because it requires DroidGuard attestation** - a device integrity verification system that only real Android devices can provide.

This is not a bug or missing feature - it's a deliberate security measure by Google.

The DroidGuard requirement is confirmed for **crowdsourced reports about other people's devices**. For **location reports about your own devices** a separate owner path exists (`SpotReportingService/UploadOwnerScans`, see [Owner location reports](#owner-location-reports-uploadownerscans-unverified)); whether Google accepts it from Home Assistant is **unmeasured**.

---

## Background

The FMDN (Find My Device Network), now also called "Find Hub", is a crowdsourced location network similar to Apple's Find My network. It consists of two types of operations:

### 1. OWNER Operations (✅ Working)

These manage **your own devices**:
- Register trackers (`CreateBleDevice`)
- Get encryption keys (`GetEidInfoForE2eeDevices`)
- Upload precomputed key IDs (`UploadPrecomputedPublicKeyIds`)

**These work** because they only require standard OAuth authentication.

### 2. FINDER Operations (❌ Blocked)

These report sightings of **other people's devices**:
- Upload location reports (`UploadLocationReports` or similar)
- Contribute sightings to the crowdsourced network

**These are blocked** because they require DroidGuard attestation.

---

## Owner location reports (`UploadOwnerScans`, unverified)

The two categories above leave out a third one: reporting the location of **your own** devices. The disabled finder sits exactly there, because it only reports a device whose recent sighting the EID resolver matched against one of the account's identity keys (`fmdn_finder/bermuda_listener.py`). It nevertheless builds and sends the report in a shape that matches neither the decrypt path of this integration nor the owner upload of the Spot client.

### What is read, and what is not measured

* **Read:** the source of the iOS client [leonboe1/Spot](https://github.com/leonboe1/Spot) at commit `55f87f62302fae24150188124e8e07d5285568d8` (GPL-3.0, by the GoogleFindMyTools maintainer). Only names, message structure and file positions are taken from it here, no code.
* **Not measured:** whether Google accepts an owner report sent from a non-Android client. Spot shows the request, not its success, and its ordinary sighting uploads are disabled by default (`uploadInterval`, Spot `README.md`).
* **Scope of the DroidGuard statement:** [Issue #19](https://github.com/leonboe1/GoogleFindMyTools/issues/19) is about finder reports. Spot never sets `UploadScansRequest.droidGuardResults` on the owner path, and `ProtoDecoders/DeviceUpdate.proto` already marks `UploadScansResponse` with the comment `Only for UploadScans, not UploadOwnerScans.` Whether the owner path needs attestation is untested; the 192 combinations listed below do not include `SpotReportingService`.

### How Spot sends an owner report

1. Service and method: `spot-pa.googleapis.com/google.internal.spot.v1.SpotReportingService/UploadOwnerScans` (Spot `SpotApiManager.swift`, the `isUpload` branch).
2. Inner payload: a protobuf `Location` with latitude and longitude multiplied by 1e7 and the altitude (Spot `OwnerReportGenerator` in `UploadOwnerScans.swift`).
3. Location encryption: AES-GCM with `SHA256(identity key)` (Spot `LocationCryptor.encryptSymmetric`), `isOwnReport = true`, no ephemeral public key.
4. Identifier: `advertisement.identifier.canonicDeviceID`.
5. Envelope: the whole `LocationReportsUpload` is encrypted with Google's service key (`GetServicePublicKey`, Spot `ServerPublicKeyEncryptor.swift`) and placed in `UploadScansRequest.reportsEncryptedWithServerKey`.
6. Metadata: `clientMetadata.version.playServicesVersion` is set; there are no random fields.

### Where the disabled finder code differs

| # | Aspect | Spot owner path | This integration (`fmdn_finder/`) |
|---|--------|-----------------|-----------------------------------|
| 1 | Endpoint | `SpotReportingService/UploadOwnerScans` | `SpotService/UploadLocationReports` (`FMDN_UPLOAD_SERVICE`, `FMDN_UPLOAD_METHOD` in `google_uploader.py`) |
| 2 | Inner payload | protobuf `Location` | text `"lat,lon"` (`gps_data` in `location_uploader.py`) |
| 3 | Location encryption | AES-GCM with `SHA256(EIK)`, own report | ECDH against the EID, yet `isOwnReport=True` |
| 4 | Identifier | `canonicDeviceID` | `truncatedEid` |
| 5 | Envelope | `LocationReportsUpload` encrypted with the service key inside `UploadScansRequest` | serialized `LocationReportsUpload` sent as is, without `UploadScansRequest` |
| 6 | Metadata | `clientMetadata.version.playServicesVersion` | `random1` and `random2`, no `clientMetadata` |

Row 2 also breaks the read-back inside this integration: `NovaApi/ExecuteAction/LocateTracker/decrypt_locations.py` parses every decrypted location payload as `device_update_pb2.Location`, so a text payload fails there even if Google accepted the upload. A comment in the same file already describes the contradiction of row 3: this uploader stamps reports with `isOwnReport=True` while carrying a non-empty `publicKeyRandom`.

### Rule for changes

Do not fix single rows of the table. A partial fix (for example only the payload format) produces untested code on a path that may be the wrong one. Change the upload path only together with a live probe: one `UploadOwnerScans` call for one own device, then a fetch through the integration that returns an own report with the expected timestamp. The finder stays disabled (`FEATURE_FMDN_FINDER_ENABLED = False` in `const.py`) until then.

---

## What is DroidGuard?

DroidGuard is Google's device integrity verification system (the foundation of SafetyNet and Play Integrity API). It:

1. Executes obfuscated bytecode on the device to collect hardware fingerprints
2. Sends fingerprint data to Google's servers
3. Returns a cryptographically signed attestation token
4. This token proves the request comes from a genuine, unmodified Android device

**Home Assistant cannot generate valid DroidGuard tokens** because it's not an Android device running Google Play Services.

---

## Confirmed by GoogleFindMyTools Maintainer

This limitation was confirmed by Leon Böttger, the maintainer of GoogleFindMyTools:

> **"uploading finder reports requires a valid DroidGuard report. If you want, you can try to trick DroidGuard, but I was not successful doing so."**
>
> — [GitHub Issue #19](https://github.com/leonboe1/GoogleFindMyTools/issues/19), February 17, 2025

The issue was closed as "COMPLETED" with no solution.

---

## Research Conducted

We systematically tested **192 endpoint combinations**:

### Servers (4)
- `spot-pa.googleapis.com` (default, works for owner ops)
- `nearbyfinder-pa.googleapis.com`
- `findmydevice.googleapis.com`
- `find-my.googleapis.com`

### Services (6)
- `google.internal.spot.v1.SpotService`
- `google.internal.nearby.v1.FinderService`
- `google.internal.fmdn.v1.FinderService`
- `google.internal.findmydevice.v1.FinderService`
- `google.internal.findhub.v1.FindHubService`
- `google.internal.findhub.v1.ContributorService`

### Methods (8)
- `ReportDeviceSighting`
- `UploadDeviceSightings`
- `ContributeLocationReport`
- `SubmitLocationReport`
- `ReportSighting`
- `UploadSighting`
- `ReportLocation`
- `UploadLocationReports`

### Result

**All 192 combinations returned `UNIMPLEMENTED`.**

The error is misleading - the endpoint likely exists but rejects requests without valid DroidGuard attestation. The server returns `UNIMPLEMENTED` rather than `PERMISSION_DENIED` or `UNAUTHENTICATED` to avoid leaking information about the API structure.

---

## Most Likely Correct Endpoint

Based on our research, the endpoint is **most likely**:

```
Server:  spot-pa.googleapis.com
Service: google.internal.spot.v1.SpotService
Method:  UploadLocationReports
```

With required headers:

```http
Authorization: Bearer {OAUTH_TOKEN}
Content-Type: application/grpc
Te: trailers
User-Agent: com.google.android.gms/244433022 grpc-java-cronet/1.69.0-SNAPSHOT

# MISSING - cannot be generated without Android device:
X-DroidGuard-Result: {BASE64_ATTESTATION_TOKEN}
```

---

## Why Google Requires This

1. **Prevent Abuse**: Without attestation, malicious actors could flood the network with fake location reports, polluting the crowdsourced data.

2. **Prevent Tracking**: Attackers could upload false locations for target devices to mislead owners.

3. **Maintain Network Quality**: Only real Android devices with Google Play Services contribute meaningful, accurate location data.

4. **Privacy Protection**: Ensures location data comes from legitimate sources that comply with Google's privacy policies.

---

## Alternative Approaches

### Option 1: Accept the Limitation (Recommended)

Your integration can already:
- ✅ Register custom trackers (ESP32, etc.)
- ✅ Query device locations
- ✅ Receive and decrypt location reports
- ✅ Use Bermuda for local area detection

The upload limitation means Home Assistant cannot contribute location sightings to Google's network - but your Android phone already does this automatically via Google Play Services.

### Option 2: Android Companion App

Build a companion app that:
1. Runs on a real Android device (has valid DroidGuard)
2. Receives location data from Home Assistant
3. Uploads to Google with proper attestation
4. Reports success/failure back to HA

**Complexity**: High
**Maintenance**: Ongoing (Play Services changes)

### Option 3: ADB/Shizuku Proxy

Use a dedicated Android device as a proxy:
1. Home Assistant prepares the upload payload
2. Sends to Android device via ADB/Shizuku
3. Android device adds DroidGuard token and uploads
4. Returns result to Home Assistant

**Complexity**: Very High
**Reliability**: Low (depends on phone availability)

### Option 4: Wait for Official API

Google may eventually provide a public API for third-party finder devices. However, given the security implications, this seems unlikely.

---

## Enabling Upload (Future)

If someone successfully obtains DroidGuard attestation (e.g., through a companion app), the upload can be enabled:

1. **Edit `google_uploader.py`**:
   ```python
   FMDN_UPLOAD_ENABLED = True
   ```

2. **Add DroidGuard header** in `_try_grpc_upload()`:
   ```python
   metadata = (
       ("authorization", f"Bearer {token}"),
       ("x-droidguard-result", droidguard_token),  # Add this
   )
   ```

3. **Implement DroidGuard token generation** (requires Android device integration)

---

## References

- [GoogleFindMyTools Issue #19](https://github.com/leonboe1/GoogleFindMyTools/issues/19) - Upload limitation confirmed
- [Find Hub Network Specification](https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn) - Official docs
- [Google Security Blog](https://security.googleblog.com/2024/04/find-my-device-network-security-privacy-protections.html) - FMDN privacy design
- [PoPETS 2025 Paper](https://petsymposium.org/popets/2025/popets-2025-0147.pdf) - Security analysis of FMDN
- [SSTIC 2022: DroidGuard Deep Dive](https://www.sstic.org/media/SSTIC2022/SSTIC-actes/droidguard_a_deep_dive_into_safetynet/SSTIC2022-Article-droidguard_a_deep_dive_into_safetynet-thomas.pdf) - Attestation internals

---

## Summary

| Feature | Status | Reason |
|---------|--------|--------|
| Register custom trackers | ✅ Works | Owner operation |
| Query device locations | ✅ Works | Owner operation |
| Decrypt location reports | ✅ Works | Owner operation |
| Upload finder sightings | ❌ Blocked | Requires DroidGuard |
| Upload own-device locations (`UploadOwnerScans`) | ❓ Unmeasured | Owner path read in Spot, not tested from HA |

**The FMDN Finder upload is disabled by design. This is Google's security measure, not a bug in this integration.**
