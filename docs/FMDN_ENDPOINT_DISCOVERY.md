# FMDN Endpoint Discovery Guide - With Shizuku

> ⚠️ **IMPORTANT UPDATE (January 2026)**
>
> **The FMDN Finder upload endpoint has been identified but is protected by DroidGuard attestation.**
>
> We tested 192 endpoint combinations - all returned `UNIMPLEMENTED` because they require
> an `X-DroidGuard-Result` header that only real Android devices can provide.
>
> **See: [FMDN_UPLOAD_LIMITATION.md](FMDN_UPLOAD_LIMITATION.md) for full details.**
>
> The information below is kept for reference and future research.

---

This guide shows how to identify the Google FMDN upload endpoint using Shizuku.

## Prerequisites

- ✅ **Shizuku** installed and active
- ✅ Android device with **Google Play Services** (FMDN-capable)
- ✅ **FMDN Beacon** nearby (Pixel Buds, ESP32 tracker, etc.)
- Optional: **PCAPdroid** or **HTTP Toolkit** for network capture

---

## Method 1: Logcat Analysis (RECOMMENDED)

### Step 1: Start Shizuku Logcat

```bash
# On PC (via ADB)
adb shell

# In the shell (with Shizuku permissions)
logcat -c  # Clear logs

# Filter FMDN-related logs
logcat | grep -i "fmdn\|findmy\|locationreport\|spot-pa\|beacon"
```

### Step 2: Trigger FMDN Upload

1. **Disable "Find My Device"** in settings
2. **Enable it again**
3. **Walk near an FMDN beacon**
4. **Wait 2-5 minutes** (Play Services uploads in background)

### Step 3: Analyze Logs

**Look for:**

```
# Endpoint URLs
E/GmsClient: spot-pa.googleapis.com
E/NetworkRequest: POST /v1/...

# Protobuf messages
D/FMDN: LocationReportsUpload
D/Spot: Uploading location report

# HTTP requests
I/NetworkSecurityConfig: https://spot-pa.googleapis.com/...
```

**Important Log Tags:**

| Tag | Description |
|-----|-------------|
| `GmsClient` | Google Mobile Services Client |
| `Spot` | FMDN/Spot API |
| `FMDN` | Find My Device Network |
| `NetworkRequest` | HTTP Requests |
| `Chimera` | Play Services Module Loader |

---

## Method 2: Network Capture with PCAPdroid

### Installation

1. Install **PCAPdroid** from F-Droid or GitHub
2. Enable **Shizuku mode** in PCAPdroid
   - Settings → Root Capture → **Shizuku**

### Start Capture

```bash
# 1. Open PCAPdroid
# 2. Set filters:
#    - App: "Google Play Services"
#    - Host: "spot-pa.googleapis.com" OR "findmydevice.googleapis.com"

# 3. Start capture
# 4. Trigger FMDN upload (see Method 1, Step 2)
# 5. Stop capture
```

### Analysis

```bash
# Export PCAP file
# Copy to PC:
adb pull /sdcard/Download/pcapdroid_*.pcap

# Open with Wireshark
wireshark pcapdroid_*.pcap

# Filter:
http.request.method == "POST" && (http.host contains "spot-pa" || http.host contains "findmy")
```

**Search in Wireshark:**

1. **POST request** to Google
2. **Request URI** (e.g., `/v1/uploadLocationReports`)
3. **Request headers**:
   - `X-Goog-Api-Key`
   - `Authorization: Bearer ...`
   - `Content-Type: application/x-protobuf`

---

## Method 3: APK Decompilation (Advanced)

### Step 1: Extract Google Play Services

```bash
# With Shizuku + SAI (Split APKs Installer)
# OR manually via ADB:

# Find package name
adb shell pm list packages | grep google

# Output:
# com.google.android.gms (← Google Play Services)

# Find APK path
adb shell pm path com.google.android.gms

# Output (example):
# package:/system/priv-app/PrebuiltGmsCore/PrebuiltGmsCore.apk

# Extract APK
adb pull /system/priv-app/PrebuiltGmsCore/PrebuiltGmsCore.apk play_services.apk
```

### Step 2: Decompile with JADX

```bash
# Install JADX (https://github.com/skylot/jadx)
# On Linux:
wget https://github.com/skylot/jadx/releases/download/v1.5.0/jadx-1.5.0.zip
unzip jadx-1.5.0.zip
cd jadx-1.5.0

# Decompile APK
./bin/jadx-gui ../play_services.apk
```

### Step 3: Code Search

**Search in JADX GUI:**

1. **Text search**: `LocationReportsUpload`
   - Finds protobuf classes and API calls

2. **Text search**: `uploadLocationReports` or `UploadLocationReports`
   - Finds endpoint strings

3. **Text search**: `spot-pa.googleapis.com`
   - Finds base URL constants

4. **Class search**: `.*FMDN.*` or `.*Spot.*`
   - Finds FMDN modules

**Typical code structure:**

```java
// Example (hypothetical)
public class SpotApiClient {
    private static final String BASE_URL = "https://spot-pa.googleapis.com";
    private static final String UPLOAD_ENDPOINT = "/v1/uploadLocationReports";

    public void uploadLocationReport(LocationReportsUpload request) {
        HttpRequest req = new HttpRequest.Builder()
            .url(BASE_URL + UPLOAD_ENDPOINT)
            .method("POST")
            .header("Content-Type", "application/x-protobuf")
            .body(request.toByteArray())
            .build();
        // ...
    }
}
```

---

## Method 4: HTTP Toolkit (GUI-based)

### Installation

1. Install **HTTP Toolkit** on PC (https://httptoolkit.com)
2. Install **Android app**
3. Enable **Shizuku integration**

### Capture

```bash
# 1. Start HTTP Toolkit on PC
# 2. Connect Android device via ADB
# 3. Select "Android Device via ADB"
# 4. Install certificate on Android (follow instructions)
# 5. Filter: "Google Play Services"
# 6. Trigger FMDN upload
# 7. Analyze POST requests
```

**Advantage:** Live decoding of protobuf payloads!

---

## Expected Endpoint Candidates

Based on existing Google APIs and reverse engineering research:

| Endpoint | Probability | Notes |
|----------|------------|-------|
| `/google.internal.spot.v1.SpotService/UploadLocationReports` | 🟢 **Very High** | gRPC pattern matching `CreateBleDevice` |
| `/v1/uploadLocationReports` | 🟢 High | Standard REST pattern |
| `/v1/UploadLocationReports` | 🟢 High | PascalCase variant |
| `/v1/finder/upload` | 🟡 Medium | Finder-specific |
| `/v1/spot/uploadReports` | 🟡 Medium | Spot API prefix |
| `/rpc/UploadLocationReports` | 🟠 Low | Generic gRPC-style |

**Base URLs:**

- `https://spot-pa.googleapis.com` ← **Primary (confirmed active)**
- `https://findmydevice.googleapis.com` ← Alternative
- `https://nearbyfinder-pa.googleapis.com` ← Alternative

---

## Known API Patterns (From Codebase Research)

### Existing Spot API Endpoints (Confirmed)

This integration already uses the following **confirmed working** Spot API endpoints:

1. **`CreateBleDevice`**
   - Full URL: `https://spot-pa.googleapis.com/google.internal.spot.v1.SpotService/CreateBleDevice`
   - Purpose: Register ESP32/custom trackers in FMDN network
   - Protocol: gRPC over HTTP/2
   - Source: `SpotApi/CreateBleDevice/create_ble_device.py:83`

2. **`GetEidInfoForE2eeDevices`**
   - Full URL: `https://spot-pa.googleapis.com/google.internal.spot.v1.SpotService/GetEidInfoForE2eeDevices`
   - Purpose: Retrieve E2EE encryption keys for device EIDs
   - Protocol: gRPC over HTTP/2
   - Source: `SpotApi/GetEidInfoForE2eeDevices/get_eid_info_request.py`

3. **`UploadPrecomputedPublicKeyIds`**
   - Full URL: `https://spot-pa.googleapis.com/google.internal.spot.v1.SpotService/UploadPrecomputedPublicKeyIds`
   - Purpose: Upload precomputed public key IDs
   - Protocol: gRPC over HTTP/2
   - Source: `SpotApi/UploadPrecomputedPublicKeyIds/upload_precomputed_public_key_ids.py:43`

### Spot API Endpoint Known From `leonboe1/Spot` (Not Used by This Integration)

[`leonboe1/Spot`](https://github.com/leonboe1/Spot) is an unofficial iOS client by the author of GoogleFindMyTools, published on 2026-10-23 under GPL-3.0. Its field names are reverse engineered and not confirmed by Google. Its `DeviceUpdate.proto` defines 96 messages including nested ones, the copy in this repository 42.

**`ListEidsForBleDevices`**
- Full URL: `https://spot-pa.googleapis.com/google.internal.spot.v1.SpotService/ListEidsForBleDevices`
- Purpose: Returns, per device, `rotationPeriod` and a list of `timestamp`, the first 10 bytes of the EID, and `trackableComponent`
- Protocol: gRPC over HTTP/2, empty request message

Measured once on 2026-11-06 for one account with 11 devices:
- Every device reported `rotationPeriod` 1024 and, per series, 87 or 88 entries spaced 1024 s apart, starting 43 to 59 minutes before the call and reaching about 24 hours ahead.
- All 22 EIDs seen over the air in a 21-minute capture (17 in frame type 0x40, 5 in 0x41) were in the list, including those of the Pixel Buds Pro 2.
- Single-component trackers omit `trackableComponent`. Pixel Buds Pro 2 (firmware 6.144) list five parallel series with the values 0, 1, 2, 254 and 255, not only the right = 1, left = 2, case = 3 that Spot names. With the earbuds in the closed case the EIDs heard from them belonged to the series 255; a second capture with the earbuds out of the case also heard the series 2 and 254. What the values mean is not established.

### API Pattern Analysis

**Common Pattern:**
```
https://spot-pa.googleapis.com/google.internal.spot.v1.SpotService/{MethodName}
```

**Key Characteristics:**
- ✅ Uses **gRPC over HTTP/2** (not standard REST)
- ✅ Requires `Content-Type: application/grpc` header
- ✅ Requires `Te: trailers` header (gRPC specification)
- ✅ Uses **Protocol Buffers** for request/response serialization
- ✅ Requires **Bearer token** authentication (ADM or SPOT token)
- ✅ User-Agent: `com.google.android.gms/244433022 grpc-java-cronet/1.69.0-SNAPSHOT`

**Based on this pattern, the FMDN Finder upload endpoint is most likely:**

```
https://spot-pa.googleapis.com/google.internal.spot.v1.SpotService/UploadLocationReports
```

### Protobuf Message Structure (Known)

From `ProtoDecoders/LocationReportsUpload.proto`:

```protobuf
message LocationReportsUpload {
    repeated Report reports = 1;
    ClientMetadata clientMetadata = 2;
    uint64 random1 = 3;
    uint64 random2 = 4;
}

message Report {
    Advertisement advertisement = 1;
    Time time = 4;
    LocationReport location = 6;
}

message Advertisement {
    Identifier identifier = 5;
    uint32 unwantedTrackingModeEnabled = 6;
}

message Identifier {
    bytes truncatedEid = 6;
    bytes canonicDeviceId = 7;
}
```

This protobuf structure is **already implemented** in the codebase and matches the FMDN specification (FMDN.md Appendix A2).

---

## Research Sources & References

### Official Documentation

1. **Google FMDN Specification**
   - URL: https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn
   - Status: Public, but endpoints not documented
   - Key info: BLE advertising format, encryption spec, provisioning flow

2. **Google Security Blog - FMDN Privacy**
   - URL: https://security.googleblog.com/2024/04/find-my-device-network-security-privacy-protections.html
   - Key info: E2EE architecture, aggregation modes, throttling

3. **PoPETs 2025 Research Paper**
   - URL: https://petsymposium.org/popets/2025/popets-2025-0147.pdf
   - Title: "Okay Google, Where's My Tracker? Security, Privacy, and Performance Evaluation of Google's Find My Device Network"
   - Key info: Security analysis, upload behavior patterns

### Reverse Engineering Research

4. **GoogleFindMyTools Repository** (by Leon Böttger)
   - URL: https://github.com/leonboe1/GoogleFindMyTools
   - Key contributions:
     * Identified protobuf structures for Spot API
     * Confirmed `CreateBleDevice` endpoint
     * ESP32 firmware implementation for custom trackers
     * ~900 stars, active community

5. **GitHub Issue #45 - Spot API Manual Location**
   - URL: https://github.com/leonboe1/GoogleFindMyTools/issues/45
   - Discussion about using Spot API to manually set device location
   - Community insights on API behavior

### Technical Architecture Notes

From `custom_components/googlefindmy/FMDN.md` (S11 - Open Points):

> **Exact REST endpoints and headers** are not fully public; reverse-engineered names may change. Treat client attestation headers as implementation-defined.

This confirms:
- ✅ Endpoints must be discovered via reverse engineering
- ⚠️ Endpoint names may change in future Play Services updates
- ⚠️ Additional headers (like Play Integrity attestation) may be required

### Integration Architecture

The integration reuses existing infrastructure:

1. **Authentication:**
   - Same ADM/SPOT tokens as existing device queries
   - Token refresh handled by `nova_request.py` TTL policy
   - Scope: May require `NOVA_ACTION_API_SCOPE` or new `FMDN_FINDER_API_SCOPE`

2. **HTTP Transport:**
   - Uses `httpx` with HTTP/2 support (for gRPC)
   - Same retry logic as `spot_request.py` (401/403 auto-retry)
   - Integrated with Home Assistant's `aiohttp` session

3. **Encryption:**
   - Reuses `FMDNCrypto/foreign_tracker_cryptor.py`
   - ECDH + HKDF-SHA-256 + AES-EAX
   - SECP160r1 curve (FMDN spec)

---

## Most Likely Endpoint (Summary)

Based on all research, the **highest probability endpoint** is:

```
URL: https://spot-pa.googleapis.com/google.internal.spot.v1.SpotService/UploadLocationReports
Method: POST
Protocol: gRPC over HTTP/2
Content-Type: application/grpc
Payload: LocationReportsUpload protobuf (binary serialized)
Auth: Bearer {ADM_TOKEN} or Bearer {SPOT_TOKEN}
Headers:
  - Te: trailers
  - Grpc-Accept-Encoding: gzip
  - User-Agent: com.google.android.gms/244433022 grpc-java-cronet/1.69.0-SNAPSHOT
  - Authorization: Bearer {token}
  (Possibly: X-Android-Package, X-Android-Cert for Play Integrity)
```

**Alternative endpoints to test if primary fails:**
1. `https://spot-pa.googleapis.com/v1/uploadLocationReports` (REST-style)
2. `https://spot-pa.googleapis.com/v1/UploadLocationReports` (REST PascalCase)
3. `https://findmydevice.googleapis.com/v1/finder/upload` (Alternative service)

---

## After Successful Identification

### 1. Enter Endpoint

```python
# custom_components/googlefindmy/fmdn_finder/google_uploader.py

# Change lines 32-38:
FMDN_UPLOAD_ENDPOINT = "UploadLocationReports"  # ← YOUR FOUND ENDPOINT
FMDN_UPLOAD_ENABLED = True  # ← ENABLE
```

### 2. Add Request Headers

If additional headers are needed:

```python
# In _upload_via_nova_request():
response = await async_nova_request(
    hass=hass,
    endpoint=FMDN_UPLOAD_ENDPOINT,
    data=payload,
    headers={
        "X-Goog-Api-Key": "...",  # If required
        "X-Android-Package": "com.google.android.gms",
        # Additional headers from Wireshark
    }
)
```

### 3. Test

```bash
# Restart HA
systemctl restart home-assistant

# Monitor logs
tail -f home-assistant.log | grep -i fmdn

# Expected output:
# INFO: FMDN location report uploaded successfully
```

---

## Troubleshooting

### No FMDN Logs Visible

```bash
# Increase log level
logcat -v time | grep -E "FMDN|Spot|findmy" -i

# All Google Play Services logs
logcat -v time --pid=$(pidof com.google.android.gms)
```

### PCAPdroid Shows No HTTPS Data

- ✅ Ensure **system certificate** is installed
- ✅ Shizuku mode active (not root mode)
- ✅ "Decrypt HTTPS" enabled in settings

### APK Extraction Fails

```bash
# Alternative: Use APKPure or APKMirror
# Search for "Google Play Services" (latest version)
```

---

## Quick Reference: Shizuku Commands

```bash
# Start Shizuku (via Wireless ADB)
adb tcpip 5555
adb connect 192.168.1.X:5555

# Logcat with filter
adb shell logcat | grep -iE "fmdn|spot|locationreport"

# Find process ID
adb shell pidof com.google.android.gms

# Network statistics
adb shell dumpsys netstats | grep -i google

# FMDN service status
adb shell dumpsys activity services | grep -i fmdn
```

---

## Helpful Tools

| Tool | Purpose | Link |
|------|---------|------|
| **Shizuku** | ADB Permission Management | [GitHub](https://github.com/RikkaApps/Shizuku) |
| **PCAPdroid** | Network Capture | [F-Droid](https://f-droid.org/packages/com.emanuelef.remote_capture/) |
| **HTTP Toolkit** | HTTP/HTTPS Proxy | [Website](https://httptoolkit.com) |
| **JADX** | APK Decompiler | [GitHub](https://github.com/skylot/jadx) |
| **SAI** | Split APK Installer | [F-Droid](https://f-droid.org/packages/com.aefyr.sai.fdroid/) |

---

## Privacy Notice

⚠️ **Network capture exposes sensitive data:**
- OAuth tokens
- Location data
- Google account information

**Recommendations:**
- Use a **test account**
- **Delete** captures after analysis
- **Never share** unfiltered PCAP files

---

## Next Steps After Finding Endpoint

1. ✅ Enter endpoint in `google_uploader.py`
2. ✅ Enable uploads (`FMDN_UPLOAD_ENABLED = True`)
3. ✅ Test with real FMDN beacon
4. ✅ Create pull request
5. ✅ Update documentation

**On success:** Home Assistant becomes a full FMDN Finder! 🎉

---

## Support

If you found the endpoint, please document it here:

```python
# custom_components/googlefindmy/fmdn_finder/google_uploader.py

FMDN_UPLOAD_ENDPOINT = "YOUR_FOUND_ENDPOINT"
FMDN_UPLOAD_ENABLED = True

# Optional: Document the source
# Found via: [Method] on [Date]
# Base URL: [URL]
# Headers: [List]
```

**Good luck! 🚀**
