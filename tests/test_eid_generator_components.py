# tests/test_eid_generator_components.py
"""Known-answer tests for the per-component EID derivation.

The expected values come from a separate implementation that imports nothing
from this repository (its own secp160r1 arithmetic and AES-256-ECB over the
Table 10 block), with the synthetic key ``bytes(range(32))``::

    cd <measurement dir> && python -B -c '
    import hashlib, krypt as K
    E = bytes(range(32))
    for c in (0, 2, 255):
        kc = hashlib.sha256(E + bytes((4, c))).digest()
        for z in (1024, 150528, 249856):
            r = int.from_bytes(K.aes(kc, K.block(z)), "big") % K.N
            print(c, z, kc.hex(), K.spec_eid(kc, z).hex(),
                  hashlib.sha256(r.to_bytes(20, "big")).digest()[-1])
    '
"""

from __future__ import annotations

import pytest

from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
    COMPONENT_KEY_LABEL,
    MEASURED_COMPONENT_ROWS,
    EidVariant,
    compute_flags_xor_mask,
    derive_component_key,
    generate_eid_variant,
)

SYNTHETIC_EIK = bytes(range(32))

COMPONENT_KEYS = {
    0: "6e7117dcec48caa9aeb944df33cadcc4b3166c80e2c48943e6eeee723532425a",
    2: "2a71bbcd65b2f6cb941707bc4a62202e61c96053453df361e630ee19305296d1",
    255: "4ba9b6edf2f9b3e802c28c54f0c89a2c9e64debbe664f578c028fb7d61d823f3",
}

KNOWN_ANSWERS = [
    (0, 1024, "c7f08d73faf713b523d45211e2bfd205c15c1cc9", 219),
    (0, 150528, "006d60601e1bf24827e5a72a89a837866d2a2de1", 156),
    (0, 249856, "5fc5e89fcbfa880fa5f72155994fa41d4bc92b80", 28),
    (2, 1024, "43b2f8a71a1cf43d7236aa874b2fe6d8366e796b", 205),
    (2, 150528, "841b27e816bc4edbf23ea6db754d8a3b61bebe32", 102),
    (2, 249856, "5f33666d30579d38347b48b3106ab49fcb9969a2", 130),
    (255, 1024, "52ac763292a9687a175c2e3a1e9629c94bdc4a74", 188),
    (255, 150528, "79b8497c94159e0121d470fc6df88911de63ac77", 183),
    (255, 249856, "1516726c0f42463a8e8194b5f28e36731f4f39e6", 106),
]


def test_measured_constants() -> None:
    assert COMPONENT_KEY_LABEL == 0x04
    assert MEASURED_COMPONENT_ROWS == (0, 1, 2, 254, 255)


@pytest.mark.parametrize(("component", "expected"), sorted(COMPONENT_KEYS.items()))
def test_component_key_known_answer(component: int, expected: str) -> None:
    assert derive_component_key(SYNTHETIC_EIK, component).hex() == expected


@pytest.mark.parametrize(("component", "counter", "eid", "mask"), KNOWN_ANSWERS)
def test_eid_and_flags_mask_known_answer(
    component: int, counter: int, eid: str, mask: int
) -> None:
    key = derive_component_key(SYNTHETIC_EIK, component)
    derived = generate_eid_variant(key, counter, EidVariant.LEGACY_SECP160R1_X20_BE)
    assert derived.hex() == eid
    assert compute_flags_xor_mask(key, counter) == mask


def test_component_key_differs_from_eik_and_between_rows() -> None:
    keys = {derive_component_key(SYNTHETIC_EIK, c) for c in MEASURED_COMPONENT_ROWS}
    assert len(keys) == len(MEASURED_COMPONENT_ROWS)
    assert SYNTHETIC_EIK not in keys


@pytest.mark.parametrize("component", [True, False, -1, 256, "2", 2.0, None])
def test_component_key_rejects_invalid_component(component: object) -> None:
    with pytest.raises(ValueError, match="component"):
        derive_component_key(SYNTHETIC_EIK, component)  # type: ignore[arg-type]


@pytest.mark.parametrize("length", [0, 31, 33])
def test_component_key_rejects_wrong_key_length(length: int) -> None:
    with pytest.raises(ValueError, match="Ephemeral Identity Key"):
        derive_component_key(bytes(length), 2)
