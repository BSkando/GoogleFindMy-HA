# custom_components/googlefindmy/ProtoDecoders/decoder.py
#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#

from __future__ import annotations

import binascii
import datetime
import logging
import math
import os
import subprocess
from collections.abc import Iterable
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol

# Imported at module load (integration setup runs in the executor): the probe
# runs on the event loop, where a lazy ``import_module`` would be blocking
# filesystem work (AGENTS.md 11.3).
from google.protobuf.unknown_fields import UnknownFieldSet

from google.protobuf.message import DecodeError, Message

try:
    from zoneinfo import ZoneInfo  # stdlib, Python 3.9+
except ImportError:
    ZoneInfo = None  # type: ignore

if TYPE_CHECKING:
    from custom_components.googlefindmy.Auth.token_cache import TokenCache

from custom_components.googlefindmy.NovaApi.nova_request import (
    NovaProtobufDecodeError,
)
from custom_components.googlefindmy.ProtoDecoders import (
    Common_pb2,
    DeviceUpdate_pb2,
    LocationReportsUpload_pb2,
)

_LOGGER = logging.getLogger(__name__)

# Process-wide guard so the aggregate canonicless-device WARNING (how many devices
# Google returned without a canonical ID) is announced only once per config entry.
# get_devices_with_location still runs more than once per poll (a capability probe pass
# plus the main poll), but only the main poll passes emit_canonicless_diagnostics=True
# and touches this guard, so the diagnostics are emitted exactly once per poll (CQS). The
# key is the pair (entry scope, count): entry scope (cache.entry_id) keeps two config
# entries
# from silencing each other, and the count re-surfaces the WARNING only when the
# number of affected devices changes (new information). The per-device name is logged
# at DEBUG instead (not guarded), so enabling debug logging always reveals which
# devices are affected.
#
# The guard maps an entry scope (cache.entry_id, or "<no-entry>" in stub mode) to the
# LAST affected-device count that entry warned about. Storing the last count — rather
# than a set of every count ever seen — is the correctness point: a count that returns
# to an earlier value (1 -> 2 -> 1) still re-surfaces, because the comparison is against
# the previous state, not membership in an all-time history that never forgets. A
# recovery (count 0) drops the entry, so a later drop re-warns even if the count returns
# to a pre-recovery value, and the map stays scoped to entries that are actually
# affected. It is cleared per entry on config-entry unload/reload (see
# async_unload_entry) so a reload re-arms the warning for a still-missing device; the
# argument-free reset is the test-isolation hook.
_last_canonicless_count_by_entry: dict[str, int] = {}

# Process-wide map of the device names that were emitted (visible) in the PREVIOUS run of
# get_devices_with_location for each entry scope. It powers the "was visible, now gone"
# transition discriminator: a benign-class device (phone or reduced-listing accessory)
# normally drops silently, but if it was visible last run and disappears now, that is a
# real regression and is treated as warn-worthy. The set is rebuilt from the names
# actually emitted, but only on the main poll (emit_canonicless_diagnostics=True); the
# capability probe pass leaves it untouched so the main poll reads the genuine previous
# poll, not a set the probe already overwrote. A transition therefore warns at most once
# before falling back to silence. It is cleared per entry on the same unload/reset hook
# as the count guard.
_visible_device_names_by_entry: dict[str, set[str]] = {}

# Process-wide, entry-scoped tally of canonicless drops observed on the LAST main
# poll, for the anonymized diagnostics aggregate (P1-2). Unlike the WARNING guard
# above, this is transition-independent: each entry maps to {"total", "benign",
# "warn"} where total == benign + warn and warn counts the tracker-shaped
# (not-benign) drops only, with no "was visible" component. Only the main poll
# (emit_canonicless_diagnostics=True) rewrites it; the capability probe leaves it
# untouched (CQS). The coordinator reads it per poll via get_canonicless_counts.
_canonicless_counts_by_entry: dict[str, dict[str, int]] = {}

# Names of the devices dropped for lack of a canonical id on the LAST main poll,
# per entry scope and regardless of class, each with the number of drops under it
# (two devices can share a name). The stale-device removal reads them: a tracker
# that Google still lists, only without a canonical id, is still in the account and
# must not be removed as missing, and the count tells a new drop from one that was
# already there. Each drop is keyed by ``(name, kind)``, the kind being the class of
# the device Google reports (CANONICLESS_KIND_*), so a phone and a tracker of one
# name that swap roles in one listing do not cancel out. Written and cleared together
# with the tally above; read via get_canonicless_name_counts.
_canonicless_name_counts_by_entry: dict[str, dict[tuple[str, str], int]] = {}

#: Kinds of a canonicless drop, from the device type Google reports for the device
#: (``identifierInformation.type``) and whether it carries an ``information`` block.
CANONICLESS_KIND_ANDROID = "android"
CANONICLESS_KIND_NO_INFORMATION = "no_information"
CANONICLESS_KIND_TRACKER = "tracker"


def get_canonicless_counts(entry_id: str) -> dict[str, int]:
    """Return the last main-poll canonicless drop tally for an entry scope.

    Args:
        entry_id: The entry scope (``cache.entry_id`` or ``"<no-entry>"``).

    Returns:
        A ``{"total", "benign", "warn"}`` dict; all-zero for an entry that has
        not yet recorded a main-poll drop tally.
    """
    counts = _canonicless_counts_by_entry.get(entry_id)
    if counts is None:
        return {"total": 0, "benign": 0, "warn": 0}
    return dict(counts)


def get_canonicless_name_counts(entry_id: str) -> dict[tuple[str, str], int]:
    """Return the names dropped for lack of a canonical id on the last main poll.

    Args:
        entry_id: The entry scope (``cache.entry_id`` or ``"<no-entry>"``).

    Returns:
        Each ``(raw device name, kind)`` with the number of drops under it,
        the kind being one of the ``CANONICLESS_KIND_*`` values; empty for an
        entry without a recorded main poll. A copy, so callers cannot change
        the recorded tally.
    """
    return dict(_canonicless_name_counts_by_entry.get(entry_id, {}))


def _reset_canonicless_warning_state(entry_id: str | None = None) -> None:
    """Clear the canonicless-device warning guard and the visibility-transition map.

    Args:
        entry_id: When ``None`` (test isolation), both module-level maps are cleared
            entirely. When an entry id is given, only that entry's keys are dropped from
            both maps, so unloading or reloading one config entry re-arms its warning on
            the next poll without disturbing other entries' guards. A whole-map clear per
            unload would re-introduce the cross-entry coupling the keyed guard fixed.
    """
    if entry_id is None:
        _last_canonicless_count_by_entry.clear()
        _visible_device_names_by_entry.clear()
        _canonicless_counts_by_entry.clear()
        _canonicless_name_counts_by_entry.clear()
        return
    _last_canonicless_count_by_entry.pop(entry_id, None)
    _visible_device_names_by_entry.pop(entry_id, None)
    _canonicless_counts_by_entry.pop(entry_id, None)
    _canonicless_name_counts_by_entry.pop(entry_id, None)


_text_format_module: Any | None = None


def _get_text_format() -> Any:
    """Lazily import ``google.protobuf.text_format`` outside the event loop."""

    global _text_format_module
    if _text_format_module is None:
        _text_format_module = import_module("google.protobuf.text_format")
    return _text_format_module


def _unknown_field_numbers(message: Message) -> list[int]:
    """Field numbers of the unknown fields a decoded message carries.

    Read from the runtime's unknown-field set of the message and of every
    known sub-message it holds (``str(message)`` omits unknown fields on
    current protobuf runtimes, so the old probe never fired). The numbers are
    schema drift diagnostics, the values behind them are wire content and are
    not returned. No text serialisation: this runs on the event loop inside
    the DEBUG branch of the device-list walk, twice per poll, and a
    ``text_format`` pass over every device message would stall the loop for
    large lists (``AGENTS.md`` 11.3, async-first).
    """
    if not isinstance(message, Message):
        # Test doubles stand in for the message in the debug branch.
        return []
    numbers: set[int] = set()
    pending: list[Message] = [message]
    while pending:
        current = pending.pop()
        numbers.update(field.field_number for field in UnknownFieldSet(current))
        for descriptor, value in current.ListFields():
            # `message_type` is set for message-typed fields on every runtime;
            # the upb descriptor has no `label`, so repeated is read from the
            # value (map fields yield message values via `.values()`).
            if descriptor.message_type is None:
                continue
            if isinstance(value, Message):
                pending.append(value)
            else:
                items = value.values() if hasattr(value, "values") else value
                pending.extend(item for item in items if isinstance(item, Message))
    return sorted(numbers)


def _usable_id_count(entries: Iterable[object] | None) -> int:
    """Count the entries with a non-empty string ``id``, the row loop's entry test.

    Takes the container as the row loop does: iterates it, without asking for its
    truth value or length.
    """
    if entries is None:
        return 0
    count = 0
    for entry in entries:
        value = getattr(entry, "id", None)
        if isinstance(value, str) and value:
            count += 1
    return count


def _identifier_structure(device: object) -> tuple[str, bool, int, int]:
    """Shape of a device's identifier block.

    Returns the enum name of ``identifierInformation.type``, whether
    ``phoneInformation`` is set, and the usable ID count of both branches (phone
    branch, generic branch). Type name, presence and counts only, never the IDs,
    so the line can go into a public issue. Never raises an ``Exception``: test
    doubles lack fields, and an enum value newer than the vendored schema has no
    name.
    ``BaseException`` (cancellation, shutdown) propagates.
    """
    try:
        info = getattr(device, "identifierInformation", None)
        type_value = getattr(info, "type", None)
        if isinstance(type_value, int) and not isinstance(type_value, bool):
            try:
                type_name = DeviceUpdate_pb2.IdentifierInformationType.Name(type_value)
            except ValueError:
                type_name = f"UNRECOGNIZED({int.__repr__(type_value)})"
        else:
            type_name = "<absent>"
        phone = getattr(info, "phoneInformation", None)
        # On a real message, reading an unset sub-message returns a default
        # instance, so presence comes from HasField; doubles signal it by the
        # attribute itself.
        has_phone = bool(
            info.HasField("phoneInformation")
            if isinstance(info, Message)
            else phone is not None
        )
        phone_ids = _usable_id_count(
            getattr(getattr(phone, "canonicIds", None), "canonicId", None)
        )
        generic_ids = _usable_id_count(
            getattr(getattr(info, "canonicIds", None), "canonicId", None)
        )
    except Exception:
        return ("<unreadable>", False, -1, -1)
    return (type_name, has_phone, phone_ids, generic_ids)


class _DecryptLocationsCallable(Protocol):
    """Runtime signature for the decrypt helper imported lazily."""

    def __call__(
        self,
        device_update_protobuf: DeviceUpdate_pb2.DeviceUpdate,
        *,
        cache: TokenCache,
    ) -> list[dict[str, Any]] | None: ...


# --------------------------------------------------------------------------------------
# Pretty printer helpers (dev tooling)
# --------------------------------------------------------------------------------------


# Fields that hold or wrap key material, tracker secrets or personal data of the
# phone or the account. Developer output shows at most their length; the
# integration never logs them. `tests/test_decoder_spot_schema.py` fails on a new
# field whose name reads like a secret or like personal data and that is neither
# listed here nor explained there. Only scalar fields count: `phoneInformation`
# on `IdentitfierInformation` is a message that holds the canonical ids, and a
# message of a listed name shows its fields one by one.
_SECRET_PROTO_FIELDS: frozenset[str] = frozenset(
    {
        "contactInfo",
        "cookieValue",
        "droidGuardResults",
        "email",
        "encryptedEIK",
        "infoMessage",
        "encryptedPrivateKeyWrapper",
        "imei",
        "inviteeKey",
        "inviterKey",
        "keyRotationInformation",
        "phoneInformation",
        "phoneNumber",
        "privateKey",
        "recoveryKey",
        "reportsEncryptedWithServerKey",
        "ringKey",
        "sharingKeys",
        "unwantedTrackingKey",
        "updateForAccount",
    }
)


_SECRET_MESSAGE_FIELD = "gcmRegistrationId"  # The FCM push token of this client.


def _clear_secret_fields(message: Message) -> None:
    """Remove the scalar fields of ``_SECRET_PROTO_FIELDS``, also in sub-messages.

    ``gcmRegistrationId`` is removed as a whole: its only field is a plain ``id``.
    """
    for field, value in message.ListFields():
        if field.message_type is None:
            if field.name in _SECRET_PROTO_FIELDS:
                message.ClearField(field.name)
        elif field.name == _SECRET_MESSAGE_FIELD:
            message.ClearField(field.name)
        elif isinstance(value, Message):
            _clear_secret_fields(value)
        else:
            items = value.values() if hasattr(value, "values") else value
            for item in items:
                _clear_secret_fields(item)


def _redacted_copy(message: Message) -> Message:
    """Return a copy of ``message`` without secret fields for developer output."""
    copy = type(message)()
    copy.CopyFrom(message)
    _clear_secret_fields(copy)
    return copy


def custom_message_formatter(
    message: Message,
    indent: str,
    _as_one_line: bool,
) -> str:
    """Format protobuf messages with bytes fields as hex strings (dev convenience).

    Note:
        This is a developer-facing utility and is intentionally tolerant to schema changes.
        Time fields (named 'Time') are rendered as ISO-8601 UTC (Z). Optionally, when the
        environment variable GOOGLEFINDMY_DEV_TZ is set to a valid IANA zone (e.g.,
        "Europe/Berlin"), a second line with that display timezone is printed.
        This output is for human readability only and must not be used for program logic.
    """
    lines = []
    indent = f"{indent}"
    indent = indent.removeprefix("0")

    # Resolve optional display timezone from env; default to UTC.
    display_tz_name = os.environ.get("GOOGLEFINDMY_DEV_TZ", "UTC")
    display_tz: datetime.tzinfo = datetime.UTC
    if display_tz_name == "UTC" or ZoneInfo is None:
        display_tz_name = "UTC"
    else:
        try:
            display_tz = ZoneInfo(display_tz_name)
        except Exception:
            display_tz = datetime.UTC
            display_tz_name = "UTC"

    for field, value in message.ListFields():
        if field.name == _SECRET_MESSAGE_FIELD:
            lines.append(f"{indent}{field.name}: <redacted>")
        elif field.message_type is None and field.name in _SECRET_PROTO_FIELDS:
            size = len(value) if hasattr(value, "__len__") else 1
            lines.append(f"{indent}{field.name}: <redacted len={size}>")
        elif field.type == field.TYPE_BYTES:
            # Repeated bytes fields hold a container of values.
            for item in [value] if isinstance(value, bytes) else value:
                hex_value = binascii.hexlify(item).decode("utf-8")
                lines.append(f'{indent}{field.name}: "{hex_value}"')
        elif field.type == field.TYPE_MESSAGE:
            # The upb descriptor of current runtimes has no `label`, so repeated
            # is read from the value (see `_unknown_field_numbers`).
            if not isinstance(value, Message):
                for sub_message in value:
                    if field.message_type.name == "Time":
                        # seconds (+ optional nanos) -> float seconds
                        secs = getattr(sub_message, "seconds", 0)
                        nanos = getattr(sub_message, "nanos", 0)
                        unix_time = float(secs) + float(nanos) / 1e9

                        dt_utc = datetime.datetime.fromtimestamp(
                            unix_time, tz=datetime.UTC
                        )
                        utc_str = dt_utc.isoformat().replace("+00:00", "Z")
                        if display_tz_name == "UTC":
                            lines.append(
                                f"{indent}{field.name} {{\n{indent}  utc: {utc_str}\n{indent}}}"
                            )
                        else:
                            dt_disp = dt_utc.astimezone(display_tz)
                            disp_str = dt_disp.isoformat()
                            lines.append(
                                f"{indent}{field.name} {{\n{indent}  utc: {utc_str}\n{indent}  {display_tz_name}: {disp_str}\n{indent}}}"
                            )
                    else:
                        nested_message = custom_message_formatter(
                            sub_message, f"{indent}  ", _as_one_line
                        )
                        lines.append(
                            f"{indent}{field.name} {{\n{nested_message}\n{indent}}}"
                        )
            elif field.message_type.name == "Time":
                # seconds (+ optional nanos) -> float seconds
                secs = getattr(value, "seconds", 0)
                nanos = getattr(value, "nanos", 0)
                unix_time = float(secs) + float(nanos) / 1e9

                dt_utc = datetime.datetime.fromtimestamp(unix_time, tz=datetime.UTC)
                utc_str = dt_utc.isoformat().replace("+00:00", "Z")
                if display_tz_name == "UTC":
                    lines.append(
                        f"{indent}{field.name} {{\n{indent}  utc: {utc_str}\n{indent}}}"
                    )
                else:
                    dt_disp = dt_utc.astimezone(display_tz)
                    disp_str = dt_disp.isoformat()
                    lines.append(
                        f"{indent}{field.name} {{\n{indent}  utc: {utc_str}\n{indent}  {display_tz_name}: {disp_str}\n{indent}}}"
                    )
            else:
                nested_message = custom_message_formatter(
                    value, f"{indent}  ", _as_one_line
                )
                lines.append(f"{indent}{field.name} {{\n{nested_message}\n{indent}}}")
        else:
            lines.append(f"{indent}{field.name}: {value}")
    return "\n".join(lines)


# --------------------------------------------------------------------------------------
# Protobuf parse helpers (stable API)
# --------------------------------------------------------------------------------------


def parse_location_report_upload_protobuf(
    hex_string: str,
) -> LocationReportsUpload_pb2.LocationReportsUpload:
    """Parse LocationReportsUpload from a hex string.

    Raises:
        NovaProtobufDecodeError: If the response cannot be decoded as a valid Protobuf.
    """
    location_reports = LocationReportsUpload_pb2.LocationReportsUpload()
    try:
        location_reports.ParseFromString(bytes.fromhex(hex_string))
    except (DecodeError, ValueError, binascii.Error) as exc:
        _LOGGER.error(
            "Failed to decode Google Protobuf response (LocationReportsUpload): %s",
            exc,
        )
        raise NovaProtobufDecodeError(
            f"LocationReportsUpload decode failed: {exc}"
        ) from exc
    return location_reports


def parse_device_update_protobuf(
    hex_string: str,
) -> DeviceUpdate_pb2.DeviceUpdate:
    """Parse DeviceUpdate from a hex string.

    Raises:
        NovaProtobufDecodeError: If the response cannot be decoded as a valid Protobuf.
    """
    device_update = DeviceUpdate_pb2.DeviceUpdate()
    try:
        device_update.ParseFromString(bytes.fromhex(hex_string))
    except (DecodeError, ValueError, binascii.Error) as exc:
        _LOGGER.error(
            "Failed to decode Google Protobuf response (DeviceUpdate): %s",
            exc,
        )
        raise NovaProtobufDecodeError(f"DeviceUpdate decode failed: {exc}") from exc
    return device_update


def parse_device_list_protobuf(
    hex_string: str,
) -> DeviceUpdate_pb2.DevicesList:
    """Parse DevicesList from a hex string.

    Raises:
        NovaProtobufDecodeError: If the response cannot be decoded as a valid Protobuf.
    """
    device_list = DeviceUpdate_pb2.DevicesList()
    try:
        device_list.ParseFromString(bytes.fromhex(hex_string))
    except (DecodeError, ValueError, binascii.Error) as exc:
        _LOGGER.error(
            "Failed to decode Google Protobuf response (DevicesList): %s",
            exc,
        )
        raise NovaProtobufDecodeError(f"DevicesList decode failed: {exc}") from exc
    return device_list


# --------------------------------------------------------------------------------------
# Canonical ID extraction
# --------------------------------------------------------------------------------------


def trackable_components_of(registration: object) -> int | None:
    """Return ``DeviceCapabilities.trackableComponents`` of a registration.

    ``capabilities`` is declared ``bytes`` because foreign content must not fail
    the device list (see ``ProtoDecoders/AGENTS.md``). This is the only reader:
    it returns ``None`` for anything but a positive count and logs no content.
    """
    raw = getattr(registration, "capabilities", None)
    if not isinstance(raw, bytes) or not raw:
        return None
    capabilities = DeviceUpdate_pb2.DeviceCapabilities()
    try:
        capabilities.ParseFromString(raw)
    except DecodeError:
        return None
    count = capabilities.trackableComponents
    return count if count > 0 else None


def get_canonic_ids(
    device_list: DeviceUpdate_pb2.DevicesList,
) -> list[tuple[str, str]]:
    """Return (device_name, canonic_id) for devices in the list.

    Only returns the PRIMARY (first) canonical ID for each device.
    Android devices can have multiple canonical IDs (historical IDs from
    updates/resets), but we only want the current primary identifier.

    Defensive policy:
        * Handle Android and non-Android identifier shapes.
        * Skip non-string/empty IDs to avoid downstream surprises.
        * Use only first valid ID per device to prevent duplicates.
    """
    result: list[tuple[str, str]] = []
    for device in getattr(device_list, "deviceMetadata", []):
        try:
            if device.identifierInformation.type == DeviceUpdate_pb2.IDENTIFIER_ANDROID:
                canonic_ids = (
                    device.identifierInformation.phoneInformation.canonicIds.canonicId
                )
            else:
                canonic_ids = device.identifierInformation.canonicIds.canonicId
        except Exception:
            # Fallback: no canonic IDs available for this device
            canonic_ids = []

        device_name = getattr(device, "userDefinedDeviceName", None) or ""

        for canonic_id in canonic_ids:
            cid = getattr(canonic_id, "id", None)
            if isinstance(cid, str) and cid:
                _LOGGER.debug(
                    "ID extraction: Using primary ID '%s' for device '%s'",
                    cid,
                    device_name,
                )
                result.append((device_name, cid))
                break  # Only use first (primary) canonical ID per device
    return result


# --------------------------------------------------------------------------------------
# Location extraction with contamination shielding
# --------------------------------------------------------------------------------------

# Tunables to keep behavior explicit and easily auditable
_NEAR_TS_TOLERANCE_S: float = 5.0  # semantic merge tolerance (seconds)

_ANCHOR_METADATA_KEYS: tuple[str, ...] = (
    "pair_date",
    "secrets_creation_date",
    "device_registration",
    "device_type_information",
    "encrypted_user_secrets",
    "identity_key",
    "identity_key_candidates",
    "encrypted_identity_key_candidates",
    "encrypted_identity_key",
    "owner_key_version",
    "time_anchors_debug",
    "metadata_only",
)

_DEVICE_STUB_KEYS: tuple[str, ...] = (
    "name",
    "id",
    "device_id",
    "encrypted_identity_key",
    "encrypted_account_key",
    "public_key_address",
    "owner_key_version",
    "device_type",
    "fast_pair_model_id",
    "manufacturer",
    "model",
    "trackable_components",
    "latitude",
    "longitude",
    "altitude",
    "accuracy",
    "last_seen",
    "status",
    "status_code",
    "_report_hint",
    "is_own_report",
    "semantic_name",
    "battery_level",
    *_ANCHOR_METADATA_KEYS,
)


def _build_device_stub(device_name: str, canonic_id: str) -> dict[str, Any]:
    """Return a normalized, predictable stub for a device row.

    The stub ensures consistent keys across call sites and prevents
    accidental overwrites caused by missing keys.
    """
    return {
        "name": device_name,
        "id": canonic_id,
        "device_id": canonic_id,
        "encrypted_identity_key": None,
        "encrypted_account_key": None,
        "public_key_address": None,
        "owner_key_version": None,
        "device_type": None,
        "fast_pair_model_id": None,
        "manufacturer": None,
        "model": None,
        "trackable_components": None,
        "latitude": None,
        "longitude": None,
        "altitude": None,
        "accuracy": None,
        "last_seen": None,
        "status": None,
        "status_code": None,
        "_report_hint": None,
        "is_own_report": None,
        "semantic_name": None,
        "battery_level": None,
        "pair_date": None,
        "secrets_creation_date": None,
        "device_registration": None,
        "device_type_information": None,
        "encrypted_user_secrets": None,
        "identity_key": None,
        "identity_key_candidates": None,
        "encrypted_identity_key_candidates": None,
        "time_anchors_debug": None,
        "metadata_only": None,
    }


def _normalize_location_dict(loc: dict[str, Any]) -> dict[str, Any]:
    """Coerce numeric fields to floats (when present) and drop invalid values.

    Only mutates a shallow copy. Unknown keys are preserved (e.g., `_report_hint`).

    Validation rules:
        - NaN/Inf values are dropped for all numeric fields.
        - accuracy < 0.001m is dropped: The Android Location API uses 0.0 as an error
          code meaning "no accuracy available". Modern dual-frequency GNSS can achieve
          sub-meter accuracy, so only the error code (0.0) is filtered. The REPORT is
          kept, but accuracy is treated as unmeasured (acc_rank = -inf in ranking).
        - "Null Island" coordinates (0.0, 0.0) are dropped: This location in the
          Atlantic Ocean is a common API default when no real location is available.
    """
    # Minimum valid accuracy threshold (1mm).
    # Only the error code 0.0 (and negative values) are filtered.
    # Modern dual-frequency GNSS can achieve sub-meter accuracy.
    _MIN_VALID_ACCURACY = 0.001

    out = dict(loc)
    for num_key in ("latitude", "longitude", "accuracy", "last_seen", "altitude"):
        val = out.get(num_key)
        if val is None:
            continue
        try:
            f = float(val)
            if not math.isfinite(f):
                out.pop(num_key, None)
            # accuracy < 0.001m is the error code (0.0 = "no accuracy").
            # We KEEP the report but REMOVE the accuracy key so it doesn't
            # corrupt ranking (acc_rank = -inf when accuracy is None).
            # Valid sub-meter values like 0.5m are preserved!
            elif num_key == "accuracy" and f < _MIN_VALID_ACCURACY:
                out.pop(num_key, None)
            else:
                out[num_key] = f
        except (TypeError, ValueError):
            out.pop(num_key, None)

    # "Null Island" filter: Coordinates at (0.0, 0.0) are in the Atlantic Ocean
    # and indicate a default/missing value from the API, not a real location.
    lat = out.get("latitude")
    lon = out.get("longitude")
    if lat is not None and lon is not None:
        # Use small epsilon for floating point comparison (covers 0.0 defaults)
        if abs(lat) < 0.0001 and abs(lon) < 0.0001:
            out.pop("latitude", None)
            out.pop("longitude", None)
            out.pop("accuracy", None)  # Without coordinates, accuracy is meaningless

    return out


def _normalize_timestamp(value: Any) -> int | None:
    """Return epoch seconds from primitive or Timestamp-like values.

    For *anchor* timestamps, treat unset/default values (0 / non-positive) as missing.
    """

    if value is None:
        return None

    # Protobuf Timestamp-like object
    if hasattr(value, "seconds"):
        try:
            seconds = int(getattr(value, "seconds"))
        except (TypeError, ValueError):
            return None
        return seconds if seconds > 0 else None

    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None

    return seconds if seconds > 0 else None


def _merge_dict_preserve_left(
    left: dict[str, Any], right: dict[str, Any]
) -> dict[str, Any]:
    """Merge two dicts without overwriting keys from the left dict."""

    merged = dict(left)
    for key, value in right.items():
        if key not in merged:
            merged[key] = value
    return merged


def _collect_anchor_metadata(
    location_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    """Union anchor/metadata keys across *all* candidates.

    This protects metadata-only candidates from being dropped when a different
    candidate wins the location ranking.
    """

    union: dict[str, Any] = {}
    for key in _ANCHOR_METADATA_KEYS:
        union[key] = None

    # Union strategy:
    # - timestamps: keep the first non-null positive value
    # - identity_key: keep the first non-null bytes value
    # - *_candidates: union lists (dedup, stable order)
    # - dict blobs: keep first; for time_anchors_debug merge (preserve-left)
    # - metadata_only: OR
    for cand in location_candidates:
        if not isinstance(cand, dict):
            continue

        # boolean
        if "metadata_only" in cand:
            union["metadata_only"] = bool(union.get("metadata_only")) or bool(
                cand.get("metadata_only")
            )

        for ts_key in ("pair_date", "secrets_creation_date"):
            if ts_key in cand:
                ts_val = _normalize_timestamp(cand.get(ts_key))
                if ts_val is not None and union.get(ts_key) is None:
                    union[ts_key] = ts_val

        # identity key material (convert bytes to hex string for consistency)
        if union.get("identity_key") is None:
            ik_val = cand.get("identity_key")
            if isinstance(ik_val, (bytes, bytearray)) and ik_val:
                union["identity_key"] = bytes(ik_val).hex()
            elif isinstance(ik_val, str) and ik_val:
                union["identity_key"] = ik_val

        # encrypted_identity_key (convert bytes to hex string for consistency)
        if union.get("encrypted_identity_key") is None:
            eik_val = cand.get("encrypted_identity_key")
            if isinstance(eik_val, (bytes, bytearray)) and eik_val:
                union["encrypted_identity_key"] = bytes(eik_val).hex()
            elif isinstance(eik_val, str) and eik_val:
                union["encrypted_identity_key"] = eik_val

        # owner_key_version (int)
        if union.get("owner_key_version") is None:
            okv = cand.get("owner_key_version")
            if isinstance(okv, int) and okv > 0:
                union["owner_key_version"] = okv

        # list unions
        for list_key in (
            "identity_key_candidates",
            "encrypted_identity_key_candidates",
        ):
            lst = cand.get(list_key)
            if not isinstance(lst, list) or not lst:
                continue
            existing = union.get(list_key)
            if not isinstance(existing, list):
                existing = []
            for item in lst:
                if isinstance(item, (bytes, bytearray)) and item:
                    b = bytes(item)
                    if b not in existing:
                        existing.append(b)
                # allow non-bytes items (diagnostics) but still dedup
                elif item not in existing:
                    existing.append(item)
            union[list_key] = existing

        # dict blobs (diagnostics)
        for dict_key in (
            "device_registration",
            "device_type_information",
            "encrypted_user_secrets",
        ):
            if union.get(dict_key) is None:
                d_val = cand.get(dict_key)
                if isinstance(d_val, dict) and d_val:
                    union[dict_key] = d_val

        # debug blob: merge without overwriting left keys
        dbg = cand.get("time_anchors_debug")
        if isinstance(dbg, dict) and dbg:
            existing_dbg = union.get("time_anchors_debug")
            if isinstance(existing_dbg, dict) and existing_dbg:
                union["time_anchors_debug"] = _merge_dict_preserve_left(
                    existing_dbg, dbg
                )
            else:
                union["time_anchors_debug"] = dbg

    return union


def _get_rank_tuple(n: dict[str, Any]) -> tuple[float, int, int, float, str]:
    """Create a sort key tuple prioritizing the freshest timestamp.

    Priority (high to low):
      1. Newer ``last_seen`` timestamp
      2. Source/Status: Owner > Crowdsourced > Aggregated > Unknown
         (BUT: is_own_report only trusted if accuracy is valid)
      3. Presence of coordinates (tie-breaker when timestamps/status match)
      4. Better accuracy (smaller is better)
      5. Deterministic tie-breaker string
    """
    # Pre-compute accuracy validity for status ranking decisions.
    # A report claiming is_own_report=True but with invalid/missing accuracy
    # (after normalization strips accuracy <= 0) is suspicious and should NOT
    # get the "own report" bonus. This prevents the January 2025 phantom bug.
    acc = n.get("accuracy")
    has_valid_accuracy = (
        isinstance(acc, (int, float)) and math.isfinite(float(acc)) and float(acc) > 0
    )

    # 1) Owner-Reports take precedence ONLY if accuracy is trustworthy
    is_own_flag = bool(n.get("is_own_report"))
    # Own report bonus requires valid accuracy (or no coordinates = semantic only)
    has_coords = isinstance(n.get("latitude"), (int, float)) and isinstance(
        n.get("longitude"), (int, float)
    )
    # Trust is_own_report if: (1) accuracy is valid, OR (2) no coordinates (semantic)
    is_own_trusted = is_own_flag and (has_valid_accuracy or not has_coords)

    # 2) Robustly determine status rank (String, Int, or via Hint)
    status_code = n.get("status_code")
    try:
        status_code_int = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        status_code_int = None
    raw_status = n.get("status")
    if isinstance(raw_status, str):
        status_name = raw_status.strip().lower()
    elif isinstance(raw_status, (int, float)):
        try:
            status_name = Common_pb2.Status.Name(int(raw_status)).lower()
        except Exception:
            status_name = str(int(raw_status))
    else:
        status_name = ""

    hint = str(n.get("_report_hint") or "").strip().lower()

    # 3) Derive rank (multiple paths for robustness)
    # Use a sentinel object for robust enum comparisons
    _MISSING = object()
    cs = getattr(Common_pb2.Status, "CROWDSOURCED", _MISSING)
    ag = getattr(Common_pb2.Status, "AGGREGATED", _MISSING)
    if is_own_trusted:
        status_rank = 3
    elif (
        (cs is not _MISSING and status_code_int == cs)
        or "crowdsourced" in status_name
        or "in_all_areas" in status_name
        or hint == "in_all_areas"
    ):
        status_rank = 2
    elif (
        (ag is not _MISSING and status_code_int == ag)
        or "aggregated" in status_name
        or "high_traffic" in status_name
        or hint == "high_traffic"
    ):
        status_rank = 1
    else:
        status_rank = 0  # SEMANTIC/Unknown/Default

    # has_coords already computed above as boolean; convert to int for tuple
    has_coords_rank = 1 if has_coords else 0

    seen = n.get("last_seen")
    seen_rank = (
        float(seen)
        if isinstance(seen, (int, float)) and math.isfinite(float(seen))
        else float("-inf")
    )

    # accuracy <= 0 is physically impossible for GPS; treat as missing/worst rank.
    # Defense in depth: even if _normalize_location_dict didn't filter it.
    # (has_valid_accuracy already computed above; assert helps mypy)
    if has_valid_accuracy:
        assert acc is not None  # validated in has_valid_accuracy
        acc_rank = -float(acc)
    else:
        acc_rank = float("-inf")

    # Deterministic final tiebreaker: canonical content key (string).
    stable_key = "|".join(
        str(x)
        for x in (
            n.get("status_code", ""),
            n.get("status", ""),
            int(bool(n.get("is_own_report"))),
            n.get("last_seen", ""),
            n.get("latitude", ""),
            n.get("longitude", ""),
            n.get("accuracy", ""),
            n.get("semantic_name", ""),
        )
    )
    return (seen_rank, status_rank, has_coords_rank, acc_rank, stable_key)


def _select_best_location(
    cands: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Choose the most useful location from a list of candidates.

    This function normalizes all candidates once, then sorts them based on a
    clear priority hierarchy to find the single most relevant location report.

    Priority (high to low):
        1) Newer `last_seen` timestamp
        2) Status/Source (Owner > Crowdsourced > Aggregated)
        3) Presence of coordinates (tie-breaker when timestamps/status match)
        4) Better accuracy (smaller is better)
        5) Deterministic tie-breaker (canonical stable key)

    Returns:
        A tuple containing:
        - The single best location as a normalized dictionary, or None.
        - The complete list of all normalized candidates, which can be reused
          by downstream functions like semantic merging without re-processing.
    """
    if not cands:
        return None, []

    # Normalize once up front
    normed_cands: list[dict[str, Any]] = [
        _normalize_location_dict(c or {}) for c in cands
    ]

    # Sort using the new rank tuple which prioritizes recency over status
    normed_cands.sort(key=_get_rank_tuple, reverse=True)

    best_candidate = normed_cands[0]

    return dict(best_candidate), normed_cands


def _merge_semantics_if_near_ts(
    best: dict[str, Any],
    normed_cands: list[dict[str, Any]],
    *,
    tolerance_s: float = _NEAR_TS_TOLERANCE_S,
) -> dict[str, Any]:
    """Attach semantic labels and freshest timestamps to the best fix.

    This keeps the most useful coordinate payload while still promoting
    fresher semantic-only reports so downstream consumers perceive the update
    as new. When a semantic report outranks a coordinate fix, the latest
    available coordinates are borrowed back after the merge so spatial data
    remains populated.
    """

    def _extract_ts(raw_ts: Any) -> float:
        try:
            ts = float(raw_ts)
        except (TypeError, ValueError):
            return float("-inf")
        if not math.isfinite(ts) or ts <= 0:
            return float("-inf")
        return ts

    out = dict(best)

    # Extract best candidate's timestamp FIRST - used for identity protection.
    t_best = _extract_ts(out.get("last_seen"))

    # Track the freshest coordinate-bearing candidate so semantic-only entries can
    # still expose stable position data after the merge step.
    #
    # IDENTITY PROTECTION: Initialize to t_best (not -inf) so that lower-ranked
    # candidates with the SAME timestamp cannot "steal" coordinates from the best.
    # A lower-ranked entry must be STRICTLY newer to update coordinates.
    best_coordinate: dict[str, Any] | None = None
    best_coordinate_ts = t_best  # Protects against timestamp collision identity theft

    # Track the semantic label currently attached to the outgoing payload.
    semantic_label: str | None = None
    semantic_ts = float("-inf")
    if out.get("semantic_name"):
        semantic_label = str(out["semantic_name"])
        semantic_ts = _extract_ts(out.get("last_seen"))

    # Historical behaviour: borrow a semantic label very close to the coordinate
    # fix timestamp when none is present yet.
    if semantic_label is None and t_best > float("-inf"):
        best_label: str | None = None
        best_label_ts = float("-inf")
        min_delta = float("inf")
        for n in normed_cands:
            label = n.get("semantic_name")
            if not label:
                continue
            ts = _extract_ts(n.get("last_seen"))
            if ts == float("-inf"):
                continue
            delta = abs(ts - t_best)
            if delta <= tolerance_s and delta < min_delta:
                best_label = str(label)
                best_label_ts = ts
                min_delta = delta
        if best_label is not None:
            semantic_label = best_label
            semantic_ts = best_label_ts

    latest_seen = t_best
    latest_semantic_label = semantic_label
    latest_semantic_ts = semantic_ts

    for n in normed_cands:
        ts = _extract_ts(n.get("last_seen"))
        latest_seen = max(latest_seen, ts)

        # Use strict > to preserve sort order: when timestamps are equal,
        # the first entry (already ranked higher by _get_rank_tuple) wins.
        # Using >= would cause "last-write-wins" and prefer worse entries.
        if (
            isinstance(n.get("latitude"), (int, float))
            and isinstance(n.get("longitude"), (int, float))
            and ts > best_coordinate_ts
        ):
            best_coordinate = n
            best_coordinate_ts = ts

        label = n.get("semantic_name")
        if label:
            if ts > latest_semantic_ts:
                latest_semantic_label = str(label)
                latest_semantic_ts = ts
            elif latest_semantic_label is None and ts == latest_semantic_ts:
                latest_semantic_label = str(label)

    if latest_seen > float("-inf"):
        out["last_seen"] = latest_seen

    if latest_semantic_label:
        out["semantic_name"] = latest_semantic_label

    if best_coordinate is not None:
        for coord_field in ("latitude", "longitude", "accuracy", "altitude"):
            value = best_coordinate.get(coord_field)
            if value is not None:
                out[coord_field] = value

    return out


def get_devices_with_location(
    device_list: DeviceUpdate_pb2.DevicesList,
    *,
    cache: TokenCache | None = None,
    emit_canonicless_diagnostics: bool = False,
) -> list[dict[str, Any]]:
    """Extract one consolidated row per canonic device ID from a device list.

    This function serves as a robust barrier against data contamination by
    ensuring its output is always clean, consistent, and predictable. When a
    real TokenCache instance is provided, encrypted location payloads are
    decrypted; otherwise the function returns sanitized stubs without
    attempting the decrypt workflow.

    Guarantees:
        * **One Row Per ID**: Returns exactly one dictionary per unique canonic ID,
          preventing duplicate entries from overwriting valid data downstream.
        * **Deterministic Selection**: If multiple location reports are embedded
          for a single device, it deterministically selects the single best one.
        * **Consistent Shape**: Returned dictionaries always contain the same set of
          keys (defined in `_DEVICE_STUB_KEYS`), preventing `KeyError` exceptions
          in consumer code.
        * **Data Hygiene**: All numeric fields are coerced to `float`, validated
          for finiteness (no `NaN`/`Inf`), and sanitized before being returned.

    Args:
        device_list: The decoded ``DevicesList`` to extract rows from.
        cache: Optional ``TokenCache``; when provided, encrypted payloads are
            decrypted, otherwise sanitized stubs are returned.
        emit_canonicless_diagnostics: Command-Query-Separation gate. The function
            runs more than once per poll (a capability probe at ``api.py:361`` and
            the main poll at ``api.py:702``). Only the main poll passes ``True`` and
            is allowed to emit the canonicless drop diagnostics (DEBUG lines, the
            aggregate count WARNING) and to mutate the module-level visibility/count
            guards. The probe pass keeps the default ``False`` and stays silent and
            side-effect free, so the main poll reads the genuine previous-poll
            visibility set instead of one the probe already overwrote. Pure row
            extraction (the query) is identical for both values.

    Returns:
        A list of device data dictionaries. Fields may be `None` if no valid
        data was found, but the key structure is always consistent.
    """
    # Lazy import keeps module import-time light and avoids heavy dependencies if unused.
    try:
        from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (  # noqa: E501
            decrypt_location_response_locations as _decrypt_locations,
        )
    except Exception:
        # If the decrypt layer is unavailable, return stubs only.
        decrypt_location_response_locations: _DecryptLocationsCallable | None = None
    else:
        decrypt_location_response_locations = _decrypt_locations

    results: list[dict[str, Any]] = []

    # Entry scope for the canonicless-device guard: cache.entry_id keeps two config
    # entries from silencing each other's diagnostics. None in stub mode (cache absent).
    entry_scope = getattr(cache, "entry_id", None) or "<no-entry>"

    # Count devices Google returned without a usable canonical ID in this call, so the
    # default-visible WARNING can report an aggregate count instead of per-device names.
    canonicless_count = 0

    # Transition-independent canonicless tally for the diagnostics aggregate (P1-2),
    # kept separate from canonicless_count (which is warn-worthy AND transition-aware).
    # total == benign + warn; warn counts the tracker-shaped (not-benign) drops only.
    canonicless_total = 0
    canonicless_benign = 0
    canonicless_warn = 0
    # Names and kinds behind those drops, for the stale-device removal (S-R3d).
    canonicless_names: dict[tuple[str, str], int] = {}

    # Names emitted (visible) in THIS run, captured to rebuild the visibility-transition
    # map after the loop. Compared against the previous run's set, never the running one.
    emitted_names_this_run: set[str] = set()
    # Only the main poll reads (and below, rewrites) the cross-poll visibility set. The
    # capability probe pass (emit_canonicless_diagnostics=False) short-circuits to an empty
    # set so it neither sees nor mutates the transition state (CQS).
    previously_visible_names = (
        _visible_device_names_by_entry.get(entry_scope, set())
        if emit_canonicless_diagnostics
        else set()
    )

    for device in getattr(device_list, "deviceMetadata", []):
        # Resolve canonic IDs for this device (Android vs. generic path)
        # FIX: For phones (IDENTIFIER_ANDROID), use only the FIRST canonical ID.
        # Phones can have multiple canonical IDs in the array (e.g., after re-pairing),
        # but only the first/primary ID should be used to avoid creating duplicate
        # device entries. This matches fcm_receiver_ha._extract_canonic_id_from_response().
        is_android_device = False
        try:
            is_android_device = (
                device.identifierInformation.type == DeviceUpdate_pb2.IDENTIFIER_ANDROID
            )
            if is_android_device:
                all_ids = (
                    device.identifierInformation.phoneInformation.canonicIds.canonicId
                )
                # Use only the first canonical ID for phones to prevent duplicates
                canonic_ids = all_ids[:1] if all_ids else []
            else:
                canonic_ids = device.identifierInformation.canonicIds.canonicId
        except Exception:
            canonic_ids = []

        device_name = getattr(device, "userDefinedDeviceName", None) or ""

        if _LOGGER.isEnabledFor(logging.DEBUG):
            debug_fields = [f.name for f, _ in device.ListFields()]
            _LOGGER.debug("Device '%s' raw fields: %s", device_name, debug_fields)

            if device.HasField("information"):
                info_fields = [f.name for f, _ in device.information.ListFields()]
                _LOGGER.debug("  -> information fields: %s", info_fields)

                if device.information.HasField("locationInformation"):
                    loc_fields = [
                        f.name
                        for f, _ in device.information.locationInformation.ListFields()
                    ]
                    _LOGGER.debug("    -> locationInformation fields: %s", loc_fields)

            unknown_numbers = _unknown_field_numbers(device)
            if unknown_numbers:
                # Field numbers only: the text-format lines carry the
                # server-supplied values of those fields, which are raw API
                # payload and never reach the log (AGENTS.md section 5).
                _LOGGER.debug(
                    "Device '%s' has UNKNOWN FIELDS: numbers=%s, count=%d",
                    device_name,
                    unknown_numbers,
                    len(unknown_numbers),
                )

            if emit_canonicless_diagnostics and not _usable_id_count(canonic_ids):
                # Structure only, never ID values; the device name stays at
                # DEBUG. Which identifier branch Google filled for a device
                # whose read entries yield no row in the loop below
                # (BSkando/GoogleFindMy-HA#224, hypothesis "ID in the other
                # branch"). Not on the capability probe pass, like the other
                # canonicless output.
                id_type, has_phone_info, phone_ids, generic_ids = _identifier_structure(
                    device
                )
                _LOGGER.debug(
                    "Device '%s' IDENTIFIER STRUCTURE: type=%s, "
                    "has_phoneInformation=%s, phoneInformation.canonicIds=%d, "
                    "canonicIds=%d",
                    device_name,
                    id_type,
                    has_phone_info,
                    phone_ids,
                    generic_ids,
                )

        # Try decryption ONCE per device; share across all its canonic IDs
        location_candidates: list[dict[str, Any]] = []
        encrypted_identity_key = None
        owner_key_version = None
        device_type = None
        fast_pair_model_id: str | None = None
        manufacturer: str | None = None
        model: str | None = None
        trackable_components: int | None = None
        pair_date: int | None = None
        secrets_creation_date: int | None = None
        encrypted_account_key: str | None = None
        public_key_address: str | None = None
        if decrypt_location_response_locations is not None and cache is not None:
            try:
                if device.HasField("information") and device.information.HasField(
                    "locationInformation"
                ):
                    locinfo = device.information.locationInformation
                    has_reports = False
                    if locinfo.HasField("reports"):
                        r = locinfo.reports
                        # Either network reports exist, or a 'recentLocation' is set
                        if r.HasField("recentLocationAndNetworkLocations"):
                            rn = r.recentLocationAndNetworkLocations
                            has_reports = (
                                rn.HasField("recentLocation")
                                or len(getattr(rn, "networkLocations", [])) > 0
                            )

                    if has_reports:
                        mock_device_update = DeviceUpdate_pb2.DeviceUpdate()
                        mock_device_update.deviceMetadata.CopyFrom(device)
                        location_candidates = (
                            decrypt_location_response_locations(
                                mock_device_update,
                                cache=cache,
                            )
                            or []
                        )
            except Exception as err:
                # Defensive: decryption issues must not break the whole list, but log
                # the root cause so users can debug key or parsing mismatches.
                # The device list is refreshed without a user action (every
                # DEVICE_LIST_POLL_INTERVAL), so records at INFO and above name the
                # device by its canonical IDs and the display name goes to a DEBUG
                # companion (AGENTS.md section 5 (b)).
                ids_text = (
                    ", ".join(
                        str(getattr(canonic_id, "id", "")) for canonic_id in canonic_ids
                    )
                    or "<none>"
                )
                _LOGGER.warning(
                    "Failed to decrypt location for device with IDs %s: %s",
                    ids_text,
                    err,
                    exc_info=err,
                )
                _LOGGER.debug(
                    "Decrypt failure above concerns '%s' (IDs: %s)",
                    device_name or "<unknown>",
                    ids_text,
                )
                location_candidates = []

        if device.HasField("information") and device.information.HasField(
            "deviceRegistration"
        ):
            registration = device.information.deviceRegistration
            encrypted_user_secrets = registration.encryptedUserSecrets

            if encrypted_user_secrets.encryptedIdentityKey:
                encrypted_identity_key = (
                    encrypted_user_secrets.encryptedIdentityKey.hex()
                )

            # NOTE: In Proto3, non-optional scalar fields (int32) do not support
            # HasField(). Accessing them directly returns 0 if unset.
            owner_key_version = encrypted_user_secrets.ownerKeyVersion

            if registration.HasField("deviceTypeInformation"):
                device_type = registration.deviceTypeInformation.deviceType

            raw_fast_pair_model_id = getattr(registration, "fastPairModelId", None)
            if isinstance(raw_fast_pair_model_id, str):
                fast_pair_model_id = raw_fast_pair_model_id or None
            elif isinstance(raw_fast_pair_model_id, (bytes, bytearray)):
                try:
                    fast_pair_model_id = raw_fast_pair_model_id.decode() or None
                except UnicodeDecodeError:
                    fast_pair_model_id = raw_fast_pair_model_id.hex()

            raw_manufacturer = getattr(registration, "manufacturer", None)
            if isinstance(raw_manufacturer, str):
                raw_manufacturer = raw_manufacturer.strip()
                manufacturer = raw_manufacturer or None

            raw_model = getattr(registration, "model", None)
            if isinstance(raw_model, str):
                raw_model = raw_model.strip()
                model = raw_model or None

            trackable_components = trackable_components_of(registration)

            # Anchor timestamps used for relative EID timebases
            # - pairDate is a scalar proto3 field: default 0 means "unset"
            raw_pair_date: Any = getattr(registration, "pairDate", None)
            try:
                if isinstance(raw_pair_date, (int, float)) and raw_pair_date > 0:
                    pair_date = int(raw_pair_date)
            except (TypeError, ValueError):
                pair_date = None

            # - creationDate is a Timestamp message: avoid treating an unset/default Timestamp as epoch (0)
            raw_creation_date_seconds: Any = None
            try:
                if encrypted_user_secrets.HasField("creationDate"):
                    creation_date_obj: Any | None = getattr(
                        encrypted_user_secrets, "creationDate", None
                    )
                    raw_creation_date_seconds = getattr(
                        creation_date_obj, "seconds", None
                    )
            except (ValueError, AttributeError):
                creation_date_obj = getattr(
                    encrypted_user_secrets, "creationDate", None
                )
                raw_creation_date_seconds = (
                    getattr(creation_date_obj, "seconds", None)
                    if creation_date_obj is not None
                    else None
                )

            try:
                if (
                    isinstance(raw_creation_date_seconds, (int, float))
                    and raw_creation_date_seconds > 0
                ):
                    secrets_creation_date = int(raw_creation_date_seconds)
            except (TypeError, ValueError):
                secrets_creation_date = None

            raw_encrypted_account_key = getattr(
                encrypted_user_secrets, "encryptedAccountKey", None
            )
            if isinstance(raw_encrypted_account_key, (bytes, bytearray)) and (
                raw_encrypted_account_key
            ):
                encrypted_account_key = bytes(raw_encrypted_account_key).hex()

            raw_public_key_address = getattr(
                encrypted_user_secrets, "encryptedSha256AccountKeyPublicAddress", None
            )
            if isinstance(raw_public_key_address, (bytes, bytearray)) and (
                raw_public_key_address
            ):
                public_key_address = bytes(raw_public_key_address).hex()

        # --- DIAGNOSTIC: FIND HIDDEN KEYS ---
        # Phones (IDENTIFIER_ANDROID) and similar devices may not have keys in the
        # device listing payload, but keys ARE available via the Locate flow (FCM).
        # Only warn for tracker-like devices that unexpectedly lack keys.
        #
        # This whole block is a diagnostic (logging only, no row/return effect), so it is
        # gated to the main poll (emit_canonicless_diagnostics=True) for the same CQS
        # reason as the canonicless drop diagnostics below: get_devices_with_location runs
        # twice per poll (capability probe + main poll), and an ungated dump here would
        # duplicate the "Missing Key" WARNING once per poll for every keyless tracker on
        # the probe pass. The probe pass stays diagnostically silent.
        if emit_canonicless_diagnostics and not encrypted_identity_key:
            ids_str = ", ".join(
                [str(getattr(canonic_id, "id", "")) for canonic_id in canonic_ids]
            )

            # Check if device has reduced fields (no 'information' block)
            # Note: is_android_device is already set at the top of the device loop
            has_information_block = device.HasField("information")

            # Phone devices legitimately don't have keys in list_devices - this is expected.
            # The keys are provided via the Locate flow (FCM response) instead.
            if is_android_device or not has_information_block:
                _LOGGER.debug(
                    "Device '%s' has no key in listing (expected for phones/Android devices); "
                    "keys will be obtained via Locate flow. (IDs: %s)",
                    device_name,
                    ids_str,
                )
            else:
                # For tracker devices that should have keys but don't, log at WARNING.
                # This runs on every unattended device list refresh, so the WARNING
                # names the device by its canonical IDs and the display name goes to
                # a DEBUG companion (AGENTS.md section 5 (b)).
                _LOGGER.warning(
                    "DEBUG STRUCTURE: Missing Key for device (IDs: %s)",
                    ids_str or "<none>",
                )
                _LOGGER.debug(
                    "Missing-key device above is '%s' (IDs: %s)",
                    device_name,
                    ids_str or "<none>",
                )

                # Level 1
                fields = [f.name for f, _ in device.ListFields()]
                _LOGGER.warning(" -> Device fields: %s", fields)

                if device.HasField("information"):
                    info = device.information
                    _LOGGER.warning(
                        " -> Info fields: %s", [f.name for f, _ in info.ListFields()]
                    )

                    if info.HasField("deviceRegistration"):
                        reg = info.deviceRegistration
                        _LOGGER.warning(
                            " -> Registration fields: %s",
                            [f.name for f, _ in reg.ListFields()],
                        )

                        if reg.HasField("encryptedUserSecrets"):
                            sec = reg.encryptedUserSecrets
                            _LOGGER.warning(
                                " -> Secrets fields: %s",
                                [f.name for f, _ in sec.ListFields()],
                            )
        # ------------------------------------

        # If decryption yielded results, select the best one and keep normalized list.
        if location_candidates:
            best, normed = _select_best_location(location_candidates)
            if best:
                best = _merge_semantics_if_near_ts(best, normed)
        else:
            best, normed = None, []

        # Collect anchor/metadata keys across all candidates to avoid dropping metadata-only payloads.
        anchor_union = _collect_anchor_metadata(location_candidates)
        if pair_date is not None:
            anchor_union["pair_date"] = pair_date
        if secrets_creation_date is not None:
            anchor_union["secrets_creation_date"] = secrets_creation_date

        # Record provenance hints for later refactoring-robust testing/debugging.
        if pair_date is not None or secrets_creation_date is not None:
            dbg = anchor_union.get("time_anchors_debug")
            if not isinstance(dbg, dict):
                dbg = {}
            if pair_date is not None:
                dbg.setdefault("pair_date_source", "device_registration.proto")
            if secrets_creation_date is not None:
                dbg.setdefault(
                    "secrets_creation_date_source", "encrypted_user_secrets.proto"
                )
            anchor_union["time_anchors_debug"] = dbg

        # Emit **exactly one** row per canonic ID.
        emitted_for_device = 0
        for canonic in canonic_ids:
            cid = getattr(canonic, "id", None)
            if not (isinstance(cid, str) and cid):
                continue

            row = _build_device_stub(device_name, cid)
            row["encrypted_identity_key"] = encrypted_identity_key
            row["owner_key_version"] = owner_key_version
            row["device_type"] = device_type
            row["fast_pair_model_id"] = fast_pair_model_id
            row["manufacturer"] = manufacturer
            row["model"] = model
            row["trackable_components"] = trackable_components
            row["pair_date"] = pair_date
            row["secrets_creation_date"] = secrets_creation_date
            row["encrypted_account_key"] = encrypted_account_key
            row["public_key_address"] = public_key_address
            # Apply anchor/metadata union after identity fields are set (and after location merge below)

            if best:
                # best already normalized by selection; merge only known keys
                for k in _DEVICE_STUB_KEYS:
                    if k in best and best[k] is not None:
                        row[k] = best[k]
                # Ensure device identity fields are not overwritten by nested payloads
                row["name"] = device_name
                row["id"] = cid
                row["device_id"] = cid

            # Ensure anchor/metadata keys survive location ranking and do not get overwritten by defaults.
            for k in _ANCHOR_METADATA_KEYS:
                v = anchor_union.get(k)
                if v is not None:
                    row[k] = v

            results.append(row)
            emitted_for_device += 1

        if emitted_for_device > 0:
            # Remember this device as visible this run so a later disappearance can be
            # told apart from a device that was never shown (the transition discriminator).
            emitted_names_this_run.add(device_name)
        elif emit_canonicless_diagnostics:
            # Google returned this device without a usable canonical ID, so no row was
            # emitted and it silently vanishes from Home Assistant. Emitting the drop
            # diagnostics is a query-side concern gated to the main poll: the capability
            # probe pass (emit_canonicless_diagnostics=False) skips this whole branch, so
            # it neither logs nor advances the count. Severity tracks actionability: a
            # benign class (an Android phone, or a reduced listing with no 'information'
            # block such as a Bluetooth accessory) drops as expected for its class and is
            # located -- if at all -- via the Locate flow, so it only logs at DEBUG. It
            # becomes warn-worthy only when it is tracker-shaped (has an 'information'
            # block and is not Android) OR when it was visible in the previous run and is
            # now gone (a real regression). The per-device name stays at DEBUG (AGENTS.md
            # privacy guidance); the default-visible tier is the count WARNING below.
            # has_information_block is recomputed locally because the binding above lives
            # inside the `if not encrypted_identity_key:` block and is not in scope when a
            # key was present.
            has_information_block = device.HasField("information")
            benign = is_android_device or not has_information_block

            # Transition-independent diagnostics tally (P1-2): every canonicless
            # drop counts once into total, then into benign or warn purely by
            # class (no was_visible component). Deliberately decoupled from the
            # warn_worthy logic below so the aggregate is deterministic per poll.
            canonicless_total += 1
            if device_name:
                if is_android_device:
                    kind = CANONICLESS_KIND_ANDROID
                elif not has_information_block:
                    kind = CANONICLESS_KIND_NO_INFORMATION
                else:
                    kind = CANONICLESS_KIND_TRACKER
                key = (device_name, kind)
                canonicless_names[key] = canonicless_names.get(key, 0) + 1
            if benign:
                canonicless_benign += 1
            else:
                canonicless_warn += 1

            was_visible = device_name in previously_visible_names
            warn_worthy = (not benign) or was_visible

            if warn_worthy:
                canonicless_count += 1
                _LOGGER.debug(
                    "Device '%s' was returned without a canonical ID and is not shown "
                    "in Home Assistant. Make it findable again in the Find My Device app "
                    "(or your Google account), then reload the integration.",
                    device_name or "<unnamed>",
                )
            else:
                # Benign class, expected for this device type and not actionable: log at
                # DEBUG with no remediation hint (no "reload"/"findable again") so the line
                # is not mistaken for a problem the user can fix.
                _LOGGER.debug(
                    "Device '%s' was returned without a canonical ID in this listing and "
                    "is not shown as its own entity. This is expected for this device "
                    "class (phones and Bluetooth accessories are located via the owner's "
                    "Locate flow, not as standalone Find My Device entries); no action is "
                    "required.",
                    device_name or "<unnamed>",
                )

    # Diagnostics emission and the cross-poll guard mutations are gated to the main poll
    # (emit_canonicless_diagnostics=True). The capability probe pass returns here without
    # touching either module-level map, so the main poll always reads a previous-poll
    # visibility set that the probe has not overwritten (CQS).
    if emit_canonicless_diagnostics:
        # Rebuild the per-entry visibility set from the names emitted in THIS run, strictly
        # after the device loop so the transition check inside the loop compared against
        # the PREVIOUS run's set. Dropped devices (no row emitted) are intentionally
        # excluded, so a device that disappears next run is recognised as a "was visible,
        # now gone" transition exactly once.
        _visible_device_names_by_entry[entry_scope] = emitted_names_this_run

        # Publish the transition-independent diagnostics tally for this entry,
        # set absolutely from this poll (the coordinator reads it per poll). The
        # probe pass skips this block, so only the main poll writes the counts.
        _canonicless_counts_by_entry[entry_scope] = {
            "total": canonicless_total,
            "benign": canonicless_benign,
            "warn": canonicless_warn,
        }
        _canonicless_name_counts_by_entry[entry_scope] = canonicless_names

        if canonicless_count:
            # Default-visible aggregate: report only how many devices are affected, never
            # their names. Re-arm per entry whenever the affected-device COUNT changes:
            # compare against this entry's last warned count, so a stable count warns once
            # while any change (a device dropped or recovered) re-surfaces it, including a
            # count that returns to an earlier value (1 -> 2 -> 1), which a lifetime set of
            # every count ever seen would wrongly suppress.
            last_count = _last_canonicless_count_by_entry.get(entry_scope)
            if last_count != canonicless_count:
                _last_canonicless_count_by_entry[entry_scope] = canonicless_count
                _LOGGER.warning(
                    "%d device(s) returned by Google without a canonical ID are not shown "
                    "in Home Assistant. Enable debug logging for "
                    "custom_components.googlefindmy to see which devices are affected, then "
                    "make them findable again in the Find My Device app (or your Google "
                    "account) and reload the integration.",
                    canonicless_count,
                )
        else:
            # This entry currently has no affected devices: forget its last count so a
            # later drop re-surfaces the warning even if the count returns to a
            # pre-recovery value. Keeps the guard scoped to entries that are affected.
            _last_canonicless_count_by_entry.pop(entry_scope, None)

    return results


# --------------------------------------------------------------------------------------
# Dev print helpers
# --------------------------------------------------------------------------------------


def print_location_report_upload_protobuf(hex_string: str) -> None:
    msg = _redacted_copy(parse_location_report_upload_protobuf(hex_string))
    try:
        s = _get_text_format().MessageToString(
            msg, message_formatter=custom_message_formatter
        )
    except TypeError:
        s = _get_text_format().MessageToString(msg)
    print(s)


def print_device_update_protobuf(hex_string: str) -> None:
    msg = _redacted_copy(parse_device_update_protobuf(hex_string))
    try:
        s = _get_text_format().MessageToString(
            msg, message_formatter=custom_message_formatter
        )
    except TypeError:
        s = _get_text_format().MessageToString(msg)
    print(s)


def print_device_list_protobuf(hex_string: str) -> None:
    msg = _redacted_copy(parse_device_list_protobuf(hex_string))
    try:
        s = _get_text_format().MessageToString(
            msg, message_formatter=custom_message_formatter
        )
    except TypeError:
        s = _get_text_format().MessageToString(msg)
    print(s)


# --------------------------------------------------------------------------------------
# Developer entry point (protobuf regen + sample dumps)
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    # dev-only import to avoid hard dependency at runtime
    from custom_components.googlefindmy.example_data_provider import (
        get_example_data,
    )

    # Recompile (developer convenience)
    try:
        subprocess.run(
            ["protoc", "--python_out=.", "ProtoDecoders/Common.proto"],
            cwd="../",
            check=True,
        )
        subprocess.run(
            ["protoc", "--python_out=.", "ProtoDecoders/DeviceUpdate.proto"],
            cwd="../",
            check=True,
        )
        subprocess.run(
            ["protoc", "--python_out=.", "ProtoDecoders/LocationReportsUpload.proto"],
            cwd="../",
            check=True,
        )
        subprocess.run(
            ["protoc", "--pyi_out=.", "ProtoDecoders/Common.proto"],
            cwd="../",
            check=True,
        )
        subprocess.run(
            ["protoc", "--pyi_out=.", "ProtoDecoders/DeviceUpdate.proto"],
            cwd="../",
            check=True,
        )
        subprocess.run(
            ["protoc", "--pyi_out=.", "ProtoDecoders/LocationReportsUpload.proto"],
            cwd="../",
            check=True,
        )
    except FileNotFoundError:
        print("protoc not found. Skipping proto regeneration.")
    except subprocess.CalledProcessError as e:
        print(f"protoc failed: {e}")

    print("\n ------------------- \n")

    print("Device List: ")
    print_device_list_protobuf(get_example_data("sample_nbe_list_devices_response"))

    print("Own Report: ")
    print_location_report_upload_protobuf(get_example_data("sample_own_report"))

    print("\n ------------------- \n")

    print("Not Own Report: ")
    print_location_report_upload_protobuf(get_example_data("sample_foreign_report"))

    print("\n ------------------- \n")

    print("Device Update: ")
    print_device_update_protobuf(get_example_data("sample_device_update"))
