# tests/test_brand_assets.py
"""Guard the local brand images in ``custom_components/googlefindmy/brand/``.

Since Home Assistant 2026.3 the ``brands`` integration serves the images of a
custom integration from its own ``brand/`` directory first and only falls back
to the brands CDN when a file is missing there. HACS validates the same path
(``<domain>/brand/icon.png``) before it falls back to ``domains.json``.

Nothing reports a broken local image: Home Assistant ignores an unknown file
name, and a wrongly sized image is served as is. These tests pin what the
brands repository requires
(https://github.com/home-assistant/brands#image-specification), require a
PNG form that can carry the transparency it only prefers, and check that the
release ZIP carries the directory to users.

Not covered: the visual content of the images, and whether a ``dark_*``
variant is needed for legibility on dark themes. Both are review decisions.
"""

from __future__ import annotations

import ast
import importlib.metadata
import json
import struct
from pathlib import Path

import pytest

BRAND_DIR = Path("custom_components/googlefindmy/brand")
RELEASE_WORKFLOW = Path(".github/workflows/release-stamp.yml")

#: Mirror of ``homeassistant.components.brands.const.ALLOWED_IMAGES``. Any
#: other name in ``brand/`` is never served. Kept as a literal because the
#: test stubs may pin a Home Assistant release older than 2026.3;
#: ``test_allowed_names_match_core`` compares it when the module is available.
ALLOWED_IMAGES = frozenset(
    {
        "icon.png",
        "logo.png",
        "icon@2x.png",
        "logo@2x.png",
        "dark_icon.png",
        "dark_logo.png",
        "dark_icon@2x.png",
        "dark_logo@2x.png",
    }
)

REQUIRED_IMAGES = ("icon.png", "icon@2x.png")

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
#: PNG colour types with an alpha channel: grey + alpha (4), RGBA (6).
_PNG_ALPHA_COLOR_TYPES = frozenset({4, 6})


def _png_header(path: Path) -> tuple[int, int, int, int]:
    """Return ``(width, height, bit_depth, color_type)`` from the IHDR chunk.

    Reads the header directly so the test needs no image library.
    """

    data = path.read_bytes()[:33]
    assert data[:8] == _PNG_SIGNATURE, f"{path} is not a PNG file"
    length, chunk_type = struct.unpack(">I4s", data[8:16])
    assert chunk_type == b"IHDR" and length == 13, f"{path} has no leading IHDR chunk"
    width, height, bit_depth, color_type = struct.unpack(">IIBB", data[16:26])
    return width, height, bit_depth, color_type


def _png_has_transparency(path: Path) -> bool:
    """Return whether the PNG format can carry transparency.

    Either the colour type has an alpha channel (any bit depth), or a
    ``tRNS`` chunk precedes the image data, as in palette images written
    by optimisers such as pngquant. Whether any pixel is actually
    transparent is not checked; that would need the image data decoded.
    """

    data = path.read_bytes()
    _, _, _, color_type = _png_header(path)
    if color_type in _PNG_ALPHA_COLOR_TYPES:
        return True
    offset = len(_PNG_SIGNATURE)
    while offset + 8 <= len(data):
        length, chunk_type = struct.unpack(">I4s", data[offset : offset + 8])
        if chunk_type == b"tRNS":
            return True
        if chunk_type == b"IDAT":
            return False
        offset += 12 + length
    return False


def _brand_files() -> list[Path]:
    # Runs at collection time; a missing directory is reported by
    # test_required_brand_images_exist instead of a collection error.
    if not BRAND_DIR.is_dir():
        return []
    return sorted(p for p in BRAND_DIR.iterdir() if p.is_file())


@pytest.mark.parametrize("name", REQUIRED_IMAGES)
def test_required_brand_images_exist(name: str) -> None:
    """``icon.png`` is the root of every fallback chain; ``@2x`` serves hDPI."""

    assert (BRAND_DIR / name).is_file(), f"missing {BRAND_DIR / name}"


def test_brand_directory_holds_only_served_names() -> None:
    """A misspelled file (``Icon.png``, ``icon_2x.png``) would be ignored silently."""

    unknown = [p.name for p in _brand_files() if p.name not in ALLOWED_IMAGES]
    assert not unknown, (
        f"files in {BRAND_DIR} that Home Assistant never serves: {unknown}; "
        f"allowed names: {sorted(ALLOWED_IMAGES)}"
    )


def _core_allowed_images() -> frozenset[str] | None:
    """Read ``ALLOWED_IMAGES`` from the installed Home Assistant source.

    The module is parsed, not imported: the test suite replaces
    ``homeassistant.components.http`` with a stub, and importing
    ``homeassistant.components.brands.const`` fails against that stub.
    Returns ``None`` when Home Assistant is missing or predates the
    ``brands`` integration (before 2026.3).
    """

    try:
        dist = importlib.metadata.distribution("homeassistant")
    except importlib.metadata.PackageNotFoundError:
        return None
    const_py = Path(str(dist.locate_file("homeassistant/components/brands/const.py")))
    if not const_py.is_file():
        return None
    tree = ast.parse(const_py.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AnnAssign | ast.Assign):
            targets = [node.target] if isinstance(node, ast.AnnAssign) else node.targets
            if any(
                isinstance(t, ast.Name) and t.id == "ALLOWED_IMAGES" for t in targets
            ):
                assert node.value is not None
                return frozenset(
                    n.value
                    for n in ast.walk(node.value)
                    if isinstance(n, ast.Constant) and isinstance(n.value, str)
                )
    pytest.fail(f"ALLOWED_IMAGES not found in {const_py}")


def test_allowed_names_match_core() -> None:
    """Catch drift between the mirror above and Home Assistant itself."""

    core = _core_allowed_images()
    if core is None:
        pytest.skip("Home Assistant with the brands integration is not installed")
    assert core == ALLOWED_IMAGES


@pytest.mark.parametrize("path", _brand_files(), ids=lambda p: p.name)
def test_brand_image_geometry(path: Path) -> None:
    """Sizes as required by the brands image specification, plus an alpha form.

    The specification only prefers transparency; this repository requires a
    PNG form that can carry it (alpha channel or tRNS). A fully opaque image
    in such a form still passes; that remains a review decision.
    """

    width, height, _, _ = _png_header(path)
    assert _png_has_transparency(path), (
        f"{path.name}: expected a PNG that can carry transparency "
        "(alpha channel or tRNS)"
    )

    hidpi = path.name.endswith("@2x.png")
    if "icon" in path.name:
        side = 512 if hidpi else 256
        assert (width, height) == (side, side), (
            f"{path.name}: icons must be {side}x{side}, got {width}x{height}"
        )
    else:
        low, high = (256, 512) if hidpi else (128, 256)
        shortest = min(width, height)
        assert low <= shortest <= high, (
            f"{path.name}: shortest logo side must be {low}..{high} px, got {shortest}"
        )


def test_hidpi_images_pair_with_a_normal_image() -> None:
    """An ``@2x`` file without its 1x partner points at a naming mistake."""

    names = {p.name for p in _brand_files()}
    orphans = sorted(
        n for n in names if n.endswith("@2x.png") and n.replace("@2x", "") not in names
    )
    assert not orphans, f"@2x images without a normal-size partner: {orphans}"


def test_release_zip_includes_brand_directory() -> None:
    """The HACS asset is built from the domain directory, so ``brand/`` rides along.

    ``tests/test_hacs_validation.py`` locks the ``zip -r ../../<asset> .`` form.
    This test adds the two conditions under which that form would still drop
    the images: ``brand/`` outside the domain directory, or an exclusion on the
    ZIP command line.
    """

    domain_dir = Path("custom_components/googlefindmy")
    assert BRAND_DIR.parent == domain_dir

    filename = json.loads(Path("hacs.json").read_text(encoding="utf-8"))["filename"]
    zip_lines = [
        line.strip()
        for line in RELEASE_WORKFLOW.read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("zip ")
    ]
    assert zip_lines == [f"zip -r ../../{filename} ."], (
        f"unexpected ZIP command(s) in {RELEASE_WORKFLOW}: {zip_lines}; "
        "an exclusion (-x/--exclude) or a second ZIP step could drop brand/"
    )
