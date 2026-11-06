# tests/test_architecture_contracts.py
from __future__ import annotations

import inspect
from dataclasses import fields

import pytest

from custom_components.googlefindmy import eid_resolver
from custom_components.googlefindmy.coordinator import DeviceIdentity
from custom_components.googlefindmy.ProtoDecoders.decoder import _DEVICE_STUB_KEYS


def test_decoder_exports_required_keys() -> None:
    """Ensure the decoder promises to emit required anchor keys."""

    required_keys = {
        "pair_date",
        "secrets_creation_date",
        "manufacturer",
        "model",
        "encrypted_account_key",
        "public_key_address",
        "trackable_components",
    }
    missing_keys = required_keys.difference(_DEVICE_STUB_KEYS)

    assert not missing_keys, (
        "CRITICAL CONTRACT VIOLATION: The decoder is no longer exporting "
        "`pair_date`, `secrets_creation_date`, or device metadata keys. The "
        "EID resolver relies on these keys. Please add them back to "
        "`_DEVICE_STUB_KEYS` in "
        "`decoder.py`."
    )


def test_device_identity_definition() -> None:
    """Ensure DeviceIdentity can carry required anchor metadata."""

    required_fields = {
        "pair_date",
        "secrets_creation_date",
        "manufacturer",
        "model",
        "encrypted_account_key",
        "public_key_address",
        "trackable_components",
    }
    available_fields = tuple(field.name for field in fields(DeviceIdentity))
    missing = required_fields.difference(available_fields)

    assert not missing, (
        "CRITICAL SCHEMA MISMATCH: `DeviceIdentity` in `coordinator.py` is "
        "missing metadata fields. The Resolver cannot access "
        "`pair_date`, `secrets_creation_date`, `manufacturer`, `model`, or "
        "encrypted keys without them. Update the NamedTuple definition."
    )


def test_decoder_output_fits_identity() -> None:
    """Ensure decoder output keys align with DeviceIdentity constructor."""

    stub_payload = {key: None for key in _DEVICE_STUB_KEYS}
    identity_fields = {field.name for field in fields(DeviceIdentity)}
    shared_fields = identity_fields.intersection(stub_payload)

    base_kwargs = {
        "registry_id": "registry-id",
        "canonical_id": "canonical-id",
        "identity_key": b"",
    }
    identity_kwargs = {
        key: stub_payload[key] for key in shared_fields if key not in base_kwargs
    }
    kwargs = {**base_kwargs, **identity_kwargs}

    signature = inspect.signature(DeviceIdentity)
    unexpected = set(identity_kwargs).difference(signature.parameters)
    assert not unexpected, (
        "INTEGRATION BREAKAGE: The keys provided by `decoder.py` do not match "
        "the arguments expected by `DeviceIdentity` in `coordinator.py`. You "
        "renamed a key in one place but not the other."
    )

    try:
        DeviceIdentity(**kwargs)
    except TypeError:  # pragma: no cover - loud fail path
        pytest.fail(
            "INTEGRATION BREAKAGE: The keys provided by `decoder.py` do not "
            "match the arguments expected by `DeviceIdentity` in "
            "`coordinator.py`. You renamed a key in one place but not the "
            "other."
        )


def test_resolver_safety_constants_exist() -> None:
    """Ensure Deep Scan safety constants remain present and conservative."""

    value = getattr(eid_resolver, "MIN_UNIX_WINDOW_SIZE", None)
    assert isinstance(value, int) and value >= 128, (
        "CRITICAL SAFETY: The Deep Scan constant `MIN_UNIX_WINDOW_SIZE` is "
        "missing or dangerously small."
    )


def test_readme_options_table_mirrors_option_keys() -> None:
    """The README says its options table mirrors ``OPTION_KEYS``. Hold it to it.

    Without this pin the claim decays silently: three toggles
    (``speed_gate_enabled``, ``roundtrip_confirm_enabled`` and, when it was
    added, ``accuracy_gate_enabled``) had reached ``OPTION_KEYS`` while the
    table still listed neither, so a user-facing setting existed with no
    documented default or behaviour.
    """
    from pathlib import Path

    from custom_components.googlefindmy.const import OPTION_KEYS

    readme = Path(__file__).resolve().parents[1] / "README.md"
    text = readme.read_text(encoding="utf-8")
    start = text.index("## Configuration Options")
    # The table ends at the first subsection that follows it.
    end = text.index("\n### ", start)
    table = text[start:end]

    missing = [key for key in OPTION_KEYS if f"`{key}`" not in table]
    assert not missing, (
        "README's Configuration Options table claims to mirror OPTION_KEYS but "
        f"does not list: {missing}. Add a row (option, default, units, "
        "description) for each, in OPTION_KEYS order."
    )


def test_auth_does_not_import_the_coordinator_package() -> None:
    """The push receiver must not pull the coordinator package into its imports.

    ``coordinator/__init__`` imports ``main`` and ``polling``, and ``polling``
    imports ``CRASH_LOOP_FATAL_PREFIX`` from ``Auth.fcm_receiver_ha``. An import
    of any coordinator module from ``Auth`` therefore closes a cycle whose
    outcome depends on which module is imported first - it works until the order
    changes. ``Auth/AGENTS.md`` asks for the remedy this pins: put the shared
    utility in a dependency-light module and import that.
    """
    from pathlib import Path

    auth_dir = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "googlefindmy"
        / "Auth"
    )
    offenders: list[str] = []
    for path in sorted(auth_dir.rglob("*.py")):
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            stripped = line.strip()
            if not stripped.startswith(("import ", "from ")):
                continue
            if "TYPE_CHECKING" in stripped:
                continue
            if "googlefindmy.coordinator" in stripped or stripped.startswith(
                "from ..coordinator"
            ):
                offenders.append(f"{path.name}:{lineno}: {stripped}")

    assert not offenders, (
        "Auth modules import the coordinator package, which imports polling, "
        "which imports a constant back from Auth.fcm_receiver_ha:\n"
        + "\n".join(offenders)
    )
