# custom_components/googlefindmy/Auth/firebase_messaging/const.py
#
# firebase-messaging
# https://github.com/sdb9696/firebase-messaging
#
# MIT License
#
# Copyright (c) 2017 Matthieu Lemoine
# Copyright (c) 2023 Steven Beth
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Constants module for the FCM/GC(M) transport layer."""

# --- GCM/legacy endpoints -------------------------------------------------------------

GCM_BASE_URL = "https://android.clients.google.com"
GCM_REGISTER_URL = f"{GCM_BASE_URL}/c2dm/register"
# Secondary variant used by the single-toggle fallback in the registration routine.
GCM_REGISTER3_URL = f"{GCM_BASE_URL}/c2dm/register3"

GCM_CHECKIN_URL = f"{GCM_BASE_URL}/checkin"

# Public key used by the C2DM/FCM registration legacy flow (binary and urlsafe base64)
GCM_SERVER_KEY_BIN = (
    b"\x04\x33\x94\xf7\xdf\xa1\xeb\xb1\xdc\x03\xa2\x5e\x15\x71\xdb\x48\xd3"
    + b"\x2e\xed\xed\xb2\x34\xdb\xb7\x47\x3a\x0c\x8f\xc4\xcc\xe1\x6f\x3c"
    + b"\x8c\x84\xdf\xab\xb6\x66\x3e\xf2\x0c\xd4\x8b\xfe\xe3\xf9\x76\x2f"
    + b"\x14\x1c\x63\x08\x6a\x6f\x2d\xb1\x1a\x95\xb0\xce\x37\xc0\x9c\x6e"
)
# urlsafe b64 encoding of the binary key with = padding removed
GCM_SERVER_KEY_B64 = (
    "BDOU99-h67HcA6JeFXHbSNMu7e2yNNu3RzoM"
    + "j8TM4W88jITfq7ZmPvIM1Iv-4_l2LxQcYwhqby2xGpWwzjfAnG4"
)

# --- FCM HTTP APIs --------------------------------------------------------------------

FCM_SUBSCRIBE_URL = "https://fcm.googleapis.com/fcm/connect/subscribe/"
FCM_SEND_URL = "https://fcm.googleapis.com/fcm/send/"

FCM_API = "https://fcm.googleapis.com/v1/"
FCM_REGISTRATION = "https://fcmregistrations.googleapis.com/v1/"
FCM_INSTALLATION = "https://firebaseinstallations.googleapis.com/v1/"
AUTH_VERSION = "FIS_v2"
SDK_VERSION = "w:0.6.6"

# --- Misc (kept from upstream for compatibility) --------------------------------------

DOORBELLS_ENDPOINT = "/clients_api/doorbots/{0}"

# --- MCS (Mobile Client-Server) socket constants -------------------------------------

MCS_VERSION = 41
MCS_HOST = "mtalk.google.com"
MCS_PORT = 5228
# RFC 8305 section 5 recommended delay; matches aiohttp's default in Home Assistant.
MCS_HAPPY_EYEBALLS_DELAY_S = 0.25
# Limit per attempt for DNS, TCP and TLS; the SSL context is built before it starts.
# 5 attempts x 12 s + at most 18.6 s backoff = 78.6 s, below FCM_IDLE_RESET_AFTER_S (90 s).
MCS_CONNECT_TIMEOUT_S = 12.0
MCS_SELECTIVE_ACK_ID = 12
MCS_STREAM_ACK_ID = 13
