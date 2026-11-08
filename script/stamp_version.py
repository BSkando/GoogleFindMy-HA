#!/usr/bin/env python3
# script/stamp_version.py
"""Deterministically stamp a released version into the three version literals.

This is the code-writing half of the "manual release -> code stamp" handshake
(see .github/workflows/release-stamp.yml). It takes the *exact* version the
maintainer published (``--version``, e.g. ``1.7.14`` or a beta ``1.7.14b1``)
and writes it verbatim into the three places that must move together:

* ``pyproject.toml``            -> ``[tool.poetry] version``
* ``custom_components/googlefindmy/const.py`` -> ``INTEGRATION_VERSION`` (no
  type annotation on purpose; ``version_variables`` and the guard tests both
  require the annotationless ``NAME = "x"`` form)
* ``custom_components/googlefindmy/manifest.json`` -> top-level ``"version"``

It never *computes* a version (that is semantic-release's job in the proposal
step) -- it only mirrors the tag the maintainer chose, so a hand-picked or beta
tag is honoured exactly. Each substitution must hit exactly one literal; zero or
multiple hits abort with a non-zero exit so a mis-anchored write can never land.

Usage (preview the effect on a clean worktree, then inspect ``git diff``)::

    python script/stamp_version.py --version 1.7.14
    python script/stamp_version.py --version 1.7.14b1 --repo-root /path/to/repo

The ``--version`` argument is a *version* and never carries a ``v`` prefix:
``X.Y.Z`` with an optional ``.N`` fourth segment followed by an optional
``bN``/``aN`` prerelease (covers 1.7.14, 1.7.14b1, 1.7.13.1, 1.7.13.1b1). The
segment order is PEP 440: the numeric release tuple precedes the prerelease, so
``1.7.14.1b1`` is valid but ``1.7.14b1.1`` is not. The *published git tag* may
carry a leading ``v`` (BSkando tags ``vX.Y.Z``, jleinenbach ``X.Y.Z[.N]``);
release-stamp.yml strips that optional ``v`` before calling this script, so both
fork tag conventions produce the same v-less version literals.

``--check-against-tags FILE`` checks the version against the existing tags
instead of stamping (release-stamp.yml runs it before the stamp)::

    python script/stamp_version.py --version 1.15.34 --tag 1.15.34 \
        --check-against-tags tags.txt --line 1.15

It refuses a version that does not have exactly three numeric segments (an
``aN``/``bN`` prerelease suffix is allowed, a ``v`` is not), that is not
greater than every tag of the same repository, that leaves the release line
given by ``--line``, or that equals a tag listed in ``--reserved-tags`` (the
other repository's line). Limit: the workflow runs after the release is
published, so a refusal stops the stamp, not the release; the maintainer then
deletes the release and its tag.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

# Canonical VERSION shape (SSOT). This matches a *version* (what goes into the
# code literals), which never carries a `v`. A `v`-prefixed git tag (BSkando
# `vX.Y.Z`) is normalised by release-stamp.yml -- it strips the optional leading
# `v` before validating/stamping -- so this regex stays strictly v-less. Mirrors
# the real tag set on 1.7, which is PEP 440 (`bN`/`aN` betas, four-segment `.N`)
# -- deliberately broader than SemVer so hand-picked maintenance tags stamp
# cleanly. Consequence:
# semantic-release (SemVer-only) cannot compute a trustworthy *proposal* for such
# lines, so release.yml (Workflow A) opens an empty hand-tag draft there instead
# of proposing a stale number; the stamp path here still honours the exact chosen
# tag verbatim. The shell guard in release-stamp.yml MUST stay byte-equal to this
# pattern (locked by tests/test_stamp_version.py).
VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?([ab][0-9]+)?$")

# Shape of a new release (``--check-against-tags``): three numeric segments,
# optionally a PEP 440 alpha/beta suffix, no ``v`` and no fourth segment.
RELEASE_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+([ab][0-9]+)?$")
# Leading version of an existing tag: an optional ``v``/``V``, the numeric
# release segments and an optional alpha/beta suffix. Anything after that (such
# as the ``-6`` of upstream's historic ``V1.7.0-6``) is ignored.
_TAG_VERSION_RE = re.compile(
    r"^[vV]?(?P<release>[0-9]+(?:\.[0-9]+)*)(?:(?P<pre>[ab])(?P<pre_n>[0-9]+))?"
)
_RELEASE_WIDTH = 6
_LINE_RE = re.compile(r"^[0-9]+\.[0-9]+$")

CONST_REL = "custom_components/googlefindmy/const.py"
MANIFEST_REL = "custom_components/googlefindmy/manifest.json"
PYPROJECT_REL = "pyproject.toml"


class StampError(RuntimeError):
    """Raised when a literal cannot be stamped exactly once."""


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def stamp_pyproject(text: str, version: str) -> str:
    """Replace ``version = "..."`` inside the ``[tool.poetry]`` table only.

    Table-scoped so a dependency spec or another table's ``version`` can never
    be hit. Exactly one match is required.
    """
    lines = text.splitlines(keepends=True)
    in_poetry = False
    hits = 0
    version_line = re.compile(r'^(version\s*=\s*)"[^"]*"(.*)$')
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_poetry = stripped == "[tool.poetry]"
            continue
        if in_poetry:
            m = version_line.match(line)
            if m:
                lines[i] = f'{m.group(1)}"{version}"{m.group(2)}\n'
                hits += 1
    if hits != 1:
        raise StampError(
            f"{PYPROJECT_REL}: expected exactly 1 [tool.poetry] version line, "
            f"found {hits}"
        )
    return "".join(lines)


def stamp_const(text: str, version: str) -> str:
    """Replace the annotationless ``INTEGRATION_VERSION`` literal, canonicalising.

    The match is deliberately as permissive as the loosest guard test
    (test_hypothesis_properties.py: flexible whitespace, single or double
    quotes), so a valid-per-test const.py never makes the stamp fail. The
    written form is always the canonical ``INTEGRATION_VERSION = "x"`` (single
    spaces, double quotes) that test_hacs_validation.py requires -- so stamping
    also normalises the literal and never re-introduces a type annotation.

    The trailing match is ``[ \\t]*$`` (horizontal whitespace only), not
    ``\\s*$``: a greedy ``\\s*$`` would also swallow the newline(s) after the
    literal and delete the blank line that follows it in const.py on every
    stamp. Restricting to spaces/tabs keeps the surrounding layout intact.
    """
    pattern = re.compile(
        r"""^INTEGRATION_VERSION\s*=\s*["'][^"']*["'][ \t]*$""", re.MULTILINE
    )
    new_text, hits = pattern.subn(f'INTEGRATION_VERSION = "{version}"', text)
    if hits != 1:
        raise StampError(
            f"{CONST_REL}: expected exactly 1 INTEGRATION_VERSION literal "
            f'(form INTEGRATION_VERSION = "x"), found {hits}'
        )
    return new_text


def stamp_manifest(text: str, version: str) -> str:
    """Replace the top-level ``"version"`` value with a minimal one-line diff.

    JSON is parsed to confirm a single top-level ``version`` key exists, then a
    targeted regex rewrites only that value (avoids reordering/reformatting the
    manifest, which hassfest is sensitive to). Exactly one match is required.
    """
    data = json.loads(text)
    if not isinstance(data, dict) or "version" not in data:
        raise StampError(f"{MANIFEST_REL}: no top-level 'version' key")
    pattern = re.compile(r'("version"\s*:\s*)"[^"]*"')
    new_text, hits = pattern.subn(rf'\g<1>"{version}"', text)
    if hits != 1:
        raise StampError(
            f'{MANIFEST_REL}: expected exactly 1 "version" field, found {hits}'
        )
    # Cross-check: the rewritten value round-trips and equals the target.
    if json.loads(new_text).get("version") != version:
        raise StampError(f"{MANIFEST_REL}: post-stamp version mismatch")
    return new_text


def stamp_all(repo_root: Path, version: str) -> list[Path]:
    """Stamp all three files. Returns the list of changed paths (sorted)."""
    targets = {
        repo_root / PYPROJECT_REL: stamp_pyproject,
        repo_root / CONST_REL: stamp_const,
        repo_root / MANIFEST_REL: stamp_manifest,
    }
    changed: list[Path] = []
    for path, stamper in targets.items():
        if not path.is_file():
            raise StampError(f"{path}: file not found")
        original = _read(path)
        stamped = stamper(original, version)
        if stamped != original:
            _write(path, stamped)
            changed.append(path)
    return sorted(changed)


def version_key(tag: str) -> tuple[int, ...] | None:
    """Return a sort key for ``tag``, or ``None`` when it carries no version.

    The numeric segments are padded with zeros, so ``1.7.15.20`` sorts below
    ``1.15.21`` as HACS does; a prerelease sorts below its release, an alpha
    below a beta.
    """
    match = _TAG_VERSION_RE.match(tag.strip())
    if match is None:
        return None
    release = [int(part) for part in match.group("release").split(".")]
    if len(release) > _RELEASE_WIDTH:
        return None
    release += [0] * (_RELEASE_WIDTH - len(release))
    pre = match.group("pre")
    rank = 2 if pre is None else (0 if pre == "a" else 1)
    pre_n = int(match.group("pre_n")) if pre is not None else 0
    return (*release, rank, pre_n)


def check_against_tags(
    version: str,
    *,
    tags: list[str],
    tag: str | None = None,
    line: str | None = None,
    reserved: list[str] | None = None,
) -> list[str]:
    """Return the reasons to refuse ``version``; an empty list accepts it.

    ``tags`` are the tags of the repository that publishes ``version``; the
    published ``tag`` itself (default: ``version``) is skipped because the
    workflow runs after publishing. ``line`` is ``MAJOR.MINOR``; ``reserved``
    are the tags of the other repository's line, which must not be reused.
    """
    reasons: list[str] = []
    if not RELEASE_RE.match(version):
        reasons.append(
            f"version {version} does not have exactly three numeric segments "
            "(X.Y.Z, optionally aN/bN, no 'v')"
        )
    key = version_key(version)
    if key is None:
        reasons.append(f"version {version} is not a version")
        return reasons
    own = version if tag is None else tag
    others = [
        (other_key, other)
        for other in (t.strip() for t in tags)
        if other and other != own
        if (other_key := version_key(other)) is not None
    ]
    if others:
        highest_key, highest = max(others)
        if key <= highest_key:
            reasons.append(f"version {version} is not greater than {highest}")
    if line is not None:
        if not _LINE_RE.match(line):
            raise ValueError(f"line {line!r} is not MAJOR.MINOR")
        major, minor = (int(part) for part in line.split("."))
        if key[:2] != (major, minor):
            reasons.append(
                f"version {version} leaves the release line {line} (expected {line}.x)"
            )
    for other in (t.strip() for t in reserved or []):
        if other and version_key(other) == key:
            reasons.append(
                f"version {version} is already used by the fork as tag {other}"
            )
            break
    return reasons


def _read_tags(path: Path) -> list[str]:
    return [line for line in _read(path).splitlines() if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Stamp the exact released version into pyproject.toml, const.py and "
            "manifest.json. Does not compute a version; mirrors --version verbatim."
        )
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Exact version to stamp, e.g. 1.7.14 or 1.7.14b1 (no 'v' prefix).",
    )
    parser.add_argument(
        "--check-against-tags",
        type=Path,
        metavar="FILE",
        help=(
            "Check the version against the tags listed in FILE (one per line) "
            "and exit; nothing is stamped."
        ),
    )
    parser.add_argument(
        "--tag",
        help="Published tag name, skipped in the comparison (default: --version).",
    )
    parser.add_argument(
        "--line",
        help="Release line MAJOR.MINOR the version must stay in, e.g. 1.15.",
    )
    parser.add_argument(
        "--reserved-tags",
        type=Path,
        metavar="FILE",
        help="Tags of the other repository's line; their numbers are refused.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root (default: current working directory).",
    )
    args = parser.parse_args()

    version = args.version.strip()
    if not VERSION_RE.match(version):
        parser.error(
            f"version {version!r} violates tag_format='{{version}}'; expected "
            "X.Y.Z[.N][bN] (optional .N fourth segment then optional bN/aN, "
            "no 'v' prefix)"
        )

    if args.check_against_tags is not None:
        reserved = (
            _read_tags(args.reserved_tags) if args.reserved_tags is not None else None
        )
        try:
            reasons = check_against_tags(
                version,
                tags=_read_tags(args.check_against_tags),
                tag=args.tag,
                line=args.line,
                reserved=reserved,
            )
        except ValueError as exc:
            parser.error(str(exc))
        for reason in reasons:
            print(f"stamp_version: REFUSED: {reason}")
        if reasons:
            return 1
        print(f"stamp_version: OK, {version} passes the tag check")
        return 0

    try:
        changed = stamp_all(args.repo_root, version)
    except StampError as exc:
        print(f"stamp_version: ERROR: {exc}")
        return 1

    for path in changed:
        print(f"stamped {path} -> {version}")
    print(f"stamp_version: OK, {len(changed)} file(s) set to {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
