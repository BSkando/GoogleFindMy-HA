# tests/test_hacs_validation.py
"""Validate HACS metadata alignment and guard against unsupported characters."""

from __future__ import annotations

import importlib.metadata
import json
import re
from pathlib import Path

import pytest
import yaml
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from custom_components.googlefindmy.const import INTEGRATION_VERSION


@pytest.fixture(name="hacs_metadata")
def fixture_hacs_metadata() -> dict[str, object]:
    """Load the hacs.json metadata file."""

    hacs_path = Path("hacs.json")
    metadata = json.loads(hacs_path.read_text(encoding="utf-8"))
    assert isinstance(metadata, dict)
    return metadata


def test_hacs_metadata_matches_manifest(
    hacs_metadata: dict[str, object],
    manifest: dict[str, object],
    integration_root: Path,
) -> None:
    """Ensure HACS metadata mirrors manifest declarations and const values."""

    allowed_keys = {
        "name",
        "homeassistant",
        "content_in_root",
        "render_readme",
        "filename",
        "zip_release",
        "hide_default_branch",
    }
    assert set(hacs_metadata).issubset(allowed_keys)
    assert hacs_metadata["name"] == manifest["name"]

    const_text = (integration_root / "const.py").read_text(encoding="utf-8")
    # No type annotation: semantic-release's version_variables regex only matches
    # `NAME = "x"`, not `NAME: str = "x"` (see const.py / AGENTS.md). This pattern
    # therefore doubles as a guard against re-introducing the annotation.
    match = re.search(r'INTEGRATION_VERSION = "([^"]+)"', const_text)
    assert match, "INTEGRATION_VERSION constant missing (or has a type annotation)"
    assert manifest["version"] == INTEGRATION_VERSION == match.group(1)
    assert "homeassistant" not in manifest


def test_hacs_metadata_field_types(hacs_metadata: dict[str, object]) -> None:
    """hacs.json fields must carry the types HACS's ``hacsjson`` validator requires.

    The ci.yml transient guard (see test_ci_tolerates_transient_hacs_regression)
    may classify a HACS run ``transient`` when only ``hacsjson`` +
    ``integration_manifest`` fail, on the premise that a *genuine* hacs.json schema
    error is caught locally instead. That premise only holds if this job rejects the
    same class HACS would: HACS documents ``content_in_root``/``zip_release`` and the
    other switches as booleans, so a non-boolean value (e.g. the string ``"true"``)
    fails HACS ``hacsjson`` but would otherwise slip through the fallback green.
    ``test_hacs_metadata_matches_manifest`` only checks the allowed *keys*, not their
    types; validate the types here so the fallback tolerance stays safe.
    """

    # ``isinstance(5, bool)`` is False and ``isinstance("true", bool)`` is False, so
    # this rejects exactly the non-boolean values HACS's hacsjson validator rejects.
    for field in (
        "content_in_root",
        "zip_release",
        "render_readme",
        "hide_default_branch",
    ):
        if field in hacs_metadata:
            assert isinstance(hacs_metadata[field], bool), (
                f"hacs.json '{field}' must be a boolean; HACS rejects other types "
                f"(got {type(hacs_metadata[field]).__name__})"
            )
    for field in ("name", "filename", "homeassistant"):
        if field in hacs_metadata:
            assert isinstance(hacs_metadata[field], str), (
                f"hacs.json '{field}' must be a string "
                f"(got {type(hacs_metadata[field]).__name__})"
            )


def test_hacs_metadata_requires_name(hacs_metadata: dict[str, object]) -> None:
    """hacs.json must declare a non-empty ``name`` (HACS's ``hacsjson`` validator).

    The ci.yml transient guard (see test_ci_tolerates_transient_hacs_regression)
    tolerates a HACS run when only ``hacsjson`` + ``integration_manifest`` fail, on the
    premise that a *genuine* metadata regression is caught locally instead.
    ``test_hacs_metadata_field_types`` covers a wrong field *type*; this covers a
    *missing* required key -- which HACS also rejects, but the fallback would otherwise
    wave through green. Source: hacs.xyz/docs/publish/integration.
    """

    name = hacs_metadata.get("name")
    assert isinstance(name, str) and name.strip(), (
        "hacs.json must declare a non-empty 'name' string; HACS's hacsjson validator "
        "requires it"
    )


def test_manifest_satisfies_hacs_integration_contract(
    manifest: dict[str, object],
) -> None:
    """manifest.json must carry the keys HACS's ``integration_manifest`` validator needs.

    HACS requires an integration manifest to declare ``domain``, ``documentation``,
    ``issue_tracker``, ``codeowners``, ``name`` and ``version``
    (hacs.xyz/docs/publish/integration). The ci.yml transient guard ignores
    ``integration_manifest`` in its fallback and marks the job ``transient`` when the
    remaining checks pass; that is only safe if a *genuine* manifest regression is
    caught locally. hassfest overlaps but does not mirror HACS's exact set, so
    reproduce the HACS contract here: a missing or wrongly-typed required key fails
    this job deterministically, independent of the HACS tolerance.
    """

    for field in ("domain", "documentation", "issue_tracker", "name", "version"):
        value = manifest.get(field)
        assert isinstance(value, str) and value.strip(), (
            f"manifest.json must declare a non-empty '{field}' string "
            "(HACS integration_manifest requirement)"
        )
    codeowners = manifest.get("codeowners")
    assert isinstance(codeowners, list) and codeowners, (
        "manifest.json 'codeowners' must be a non-empty list "
        "(HACS integration_manifest requirement)"
    )
    assert all(isinstance(owner, str) and owner.strip() for owner in codeowners), (
        "manifest.json 'codeowners' entries must all be non-empty strings"
    )


def test_no_micro_sign_in_integration_files(
    integration_python_files: list[Path], integration_root: Path
) -> None:
    """Integration Python files must not contain the micro sign character."""

    offenders: list[str] = []
    for path in integration_python_files:
        text = path.read_text(encoding="utf-8")
        if "\u00b5" in text:
            offenders.append(str(path.relative_to(integration_root)))
    assert not offenders, f"micro sign detected in: {offenders}"


def test_hacs_zip_release_declares_filename(
    hacs_metadata: dict[str, object],
) -> None:
    """When zip_release is enabled, a ``.zip`` release asset must be declared.

    HACS then installs that asset instead of the git source archive, so the
    version-stamped ZIP built by release-stamp.yml actually reaches users. The
    two only line up if hacs.json names the asset; guard that here.
    """

    if hacs_metadata.get("zip_release"):
        filename = hacs_metadata.get("filename")
        assert isinstance(filename, str) and filename.endswith(".zip"), (
            "zip_release=true requires a '.zip' 'filename' entry in hacs.json"
        )


def test_release_zip_places_manifest_at_root() -> None:
    """release-stamp.yml must zip the domain *contents*, not the domain dir.

    With zip_release=true HACS extracts the asset straight into
    ``custom_components/googlefindmy/`` (verified against HACS ``extractall``
    semantics), so ``manifest.json`` has to sit at the ZIP root. Building the ZIP
    from the parent (``zip -r ../googlefindmy.zip googlefindmy``) nests it one
    level too deep and hides the integration -- a broken install for everyone.
    This guard locks the fixed ``cd <domain> && zip . `` form so the regression
    cannot silently return.
    """

    workflow = Path(".github/workflows/release-stamp.yml").read_text(encoding="utf-8")
    filename = json.loads(Path("hacs.json").read_text(encoding="utf-8")).get("filename")
    assert filename, "hacs.json must declare the release asset filename"
    assert "cd custom_components/googlefindmy" in workflow, (
        "ZIP must be built from inside the integration domain directory"
    )
    assert f"zip -r ../../{filename} ." in workflow, (
        f"ZIP step must run 'zip -r ../../{filename} .' from the domain dir "
        "so the asset root is the domain root"
    )
    # Regression guard: the pre-fix form nested the domain dir inside the asset.
    assert "zip -r ../googlefindmy.zip googlefindmy" not in workflow, (
        "found the pre-fix ZIP form that nests custom_components/googlefindmy/ "
        "inside the asset (breaks the HACS install)"
    )
    assert f'gh release upload "$TAG_NAME" {filename} --clobber' in workflow, (
        f"workflow must upload the declared hacs.json asset name {filename!r}"
    )


def test_ci_tolerates_transient_hacs_regression() -> None:
    """ci.yml must guard the HACS job against the transient hacs/integration#5252
    regression without permanently disabling the two affected validators.

    The upstream action intermittently fetches the raw hacs.json / manifest.json
    as None and reports them invalid on byte-identical, valid files. The guard
    runs HACS first (continue-on-error), and only on failure re-runs it with
    exactly ``hacsjson`` and ``integration_manifest`` isolated. If everything
    else passes the failure was the known transient signature (tolerated); a
    failure of any other check still exits non-zero (real defect). The isolation
    must NOT leak into the primary run, otherwise those two checks would be off
    permanently. This guard locks the shape so the resilience cannot silently
    regress into either a blanket-ignore or a hard block.
    """

    ci = yaml.safe_load(Path(".github/workflows/ci.yml").read_text(encoding="utf-8"))
    steps = ci["jobs"]["hacs"]["steps"]
    by_id = {step["id"]: step for step in steps if "id" in step}

    primary = by_id["hacs"]
    surgical = by_id["hacs_surgical"]
    verdict = by_id["hacs_verdict"]

    # Primary run tolerates failure (so the classifier can decide) and ignores
    # nothing: the isolation must not be permanent, and the HACS default
    # repository submission requires a run without any ignore.
    assert primary.get("continue-on-error") is True, (
        "primary HACS run must set continue-on-error so the guard can classify"
    )
    assert "ignore" not in primary["with"], (
        "primary HACS run must not ignore any check (isolation not permanent)"
    )

    # Fallback isolates exactly the two transient-prone validators and only runs
    # when the primary run failed (never when HACS is healthy).
    assert surgical.get("continue-on-error") is True, (
        "fallback HACS run must set continue-on-error"
    )
    assert set(surgical["with"]["ignore"].split()) == {
        "hacsjson",
        "integration_manifest",
    }, "fallback HACS run must isolate exactly hacsjson + integration_manifest"
    assert "steps.hacs.outcome == 'failure'" in surgical["if"], (
        "fallback HACS run must be gated on the primary run failing"
    )

    # The classifier must be able to fail the job on a real defect: it must NOT
    # be continue-on-error, and must exit non-zero outside the tolerated paths.
    assert not verdict.get("continue-on-error", False), (
        "classifier must not be continue-on-error, or a real defect passes silently"
    )
    assert "exit 1" in verdict["run"], (
        "classifier must exit non-zero when the failure is not the transient one"
    )


def test_manifest_lists_no_home_assistant_core_dependency(
    manifest: dict[str, object],
) -> None:
    """manifest.json must not list a package Home Assistant itself depends on.

    hassfest rejects such entries for custom integrations ("is a dependency of
    Home Assistant itself and must not be listed in the manifest"); Home
    Assistant pins them already. They stay in ``requirements.txt`` and
    ``pyproject.toml`` for the standalone tooling (see ``AGENTS.md``).

    This is a local early warning, not a replacement for hassfest: it reads only
    the installed ``homeassistant`` distribution's own requirements, while
    hassfest also reads core's ``requirements.txt``, ``requirements_all.txt`` and
    ``package_constraints.txt``. hassfest in CI stays authoritative.
    """
    try:
        core_requires = importlib.metadata.requires("homeassistant") or []
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("homeassistant is not installed; hassfest covers this in CI")
    core = {canonicalize_name(Requirement(entry).name) for entry in core_requires}
    requirements = manifest.get("requirements")
    assert isinstance(requirements, list)
    listed = {canonicalize_name(Requirement(str(entry)).name) for entry in requirements}
    assert sorted(listed & core) == []
