# tests/test_stamp_version.py
"""Regression tests for ``script/stamp_version.py`` (the release-stamp writer).

Covers three things the code-architekt review flagged as unguarded (Z1/Z2):

* ``VERSION_RE`` against the *real* 1.7 tag matrix -- plain SemVer releases plus
  the PEP 440 forms the maintainer actually uses (beta ``bN``, four-segment
  ``.N``) -- and the invalid forms that must be rejected (``v`` prefix, the
  Workflow-A hand-tag placeholder, ``rc`` prereleases, etc.).
* the three per-file stampers: exact-one-hit accounting, table/key scoping so a
  dependency spec or sibling key is never touched, and const canonicalisation.
* a byte-parity guard between ``VERSION_RE`` and the shell regex mirrored in
  ``.github/workflows/release-stamp.yml`` -- so the two guards can never drift
  apart silently.

The suite is CI-authoritative: ``tests/conftest.py`` hard-requires the real
``homeassistant`` package at collection time, so it is not locally collectable
without it. The stamp logic itself is pure stdlib and imported directly.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from script.stamp_version import (  # noqa: E402  (path bootstrap must precede import)
    VERSION_RE,
    StampError,
    stamp_const,
    stamp_manifest,
    stamp_pyproject,
)

# --- VERSION_RE: the real tag matrix on the 1.7 line -----------------------

VALID_TAGS = [
    "1.7.14",  # plain SemVer patch release
    "1.7.9",  # plain SemVer
    "1.8.0",  # plain SemVer minor
    "1.7.14b1",  # PEP 440 beta (maintainer's hand-tag scheme)
    "1.7.9b1",  # PEP 440 beta
    "1.7.13b2",  # PEP 440 beta, real tag on 1.7
    "1.7.14a1",  # PEP 440 alpha
    "1.7.13.1",  # four-segment maintenance tag, real tag on 1.7
    "1.7.13.0",  # four-segment maintenance tag
    "1.7.14.1b1",  # PEP 440 order: four-segment release THEN prerelease
]

INVALID_TAGS = [
    "v1.7.14",  # a v-prefix is not a VERSION; release-stamp.yml strips it off
    #             the TAG first (see test_tag_to_version_v_strip_semantics)
    "1.7",  # too few segments
    "1.7.14-rc1",  # SemVer-style prerelease, not this scheme
    "1.7.14rc1",  # rc is not in the [ab] prerelease set
    "1.7.14.1.2",  # five segments
    "1.7.14b1.1",  # prerelease before the fourth segment (not PEP 440 order)
    "1.7.14 ",  # trailing whitespace
    " 1.7.14",  # leading whitespace
    "",  # empty
    "set-release-tag",  # Workflow A hand-mode placeholder MUST be rejected
]


@pytest.mark.parametrize("tag", VALID_TAGS)
def test_version_re_accepts_real_tags(tag: str) -> None:
    assert VERSION_RE.match(tag), f"{tag!r} should be accepted by VERSION_RE"


@pytest.mark.parametrize("tag", INVALID_TAGS)
def test_version_re_rejects_bad_tags(tag: str) -> None:
    assert not VERSION_RE.match(tag), f"{tag!r} should be rejected by VERSION_RE"


# --- stamp_pyproject: [tool.poetry] table scoping --------------------------


def test_stamp_pyproject_hits_only_tool_poetry() -> None:
    text = (
        "[tool.poetry]\n"
        'version = "1.7.13"\n'
        'name = "x"\n'
        "\n"
        "[tool.poetry.dependencies]\n"
        'somelib = { version = "2.0.0" }\n'
        "\n"
        "[tool.other]\n"
        'version = "9.9.9"\n'
    )
    out = stamp_pyproject(text, "1.7.14b1")
    assert 'version = "1.7.14b1"' in out
    # Dependency spec and the unrelated table are untouched.
    assert 'somelib = { version = "2.0.0" }' in out
    assert 'version = "9.9.9"' in out
    # Exactly one literal changed.
    assert out.count('"1.7.14b1"') == 1


def test_stamp_pyproject_zero_hits_raises() -> None:
    with pytest.raises(StampError):
        stamp_pyproject('[tool.other]\nversion = "1.0.0"\n', "1.7.14")


# --- stamp_const: annotationless literal, canonicalisation -----------------


def test_stamp_const_canonicalises_loose_form() -> None:
    # Loosest guard-valid form: single quotes and extra whitespace.
    text = "INTEGRATION_VERSION  =  '1.7.13'\n"
    out = stamp_const(text, "1.7.14b1")
    # Written form is always the canonical double-quote single-space shape.
    assert out == 'INTEGRATION_VERSION = "1.7.14b1"\n'


def test_stamp_const_double_quote_form() -> None:
    text = 'INTEGRATION_VERSION = "1.7.13"\n'
    assert stamp_const(text, "1.7.14") == 'INTEGRATION_VERSION = "1.7.14"\n'


def test_stamp_const_preserves_following_blank_line() -> None:
    # Mirrors the real const.py layout: the literal is followed by a blank line.
    # A greedy `\s*$` match would swallow that newline and delete the blank line
    # on every stamp; the horizontal-whitespace match must leave it intact.
    text = 'INTEGRATION_VERSION = "1.7.13"\n\n# next section\n'
    out = stamp_const(text, "1.7.14")
    assert out == 'INTEGRATION_VERSION = "1.7.14"\n\n# next section\n'


def test_stamp_const_zero_hits_raises() -> None:
    with pytest.raises(StampError):
        stamp_const("OTHER = 1\n", "1.7.14")


# --- stamp_manifest: single top-level "version" ----------------------------


def test_stamp_manifest_replaces_only_version() -> None:
    text = '{\n  "domain": "googlefindmy",\n  "version": "1.7.13",\n  "name": "x"\n}\n'
    out = stamp_manifest(text, "1.7.14b1")
    parsed = json.loads(out)
    assert parsed["version"] == "1.7.14b1"
    assert parsed["domain"] == "googlefindmy"  # untouched


def test_stamp_manifest_no_version_key_raises() -> None:
    with pytest.raises(StampError):
        stamp_manifest('{"domain": "x"}', "1.7.14")


# --- Z2: byte-parity between VERSION_RE and the shell guard -----------------


def test_workflow_guard_regex_matches_version_re() -> None:
    """Both release workflows' tag guards must mirror VERSION_RE byte-for-byte.

    The guards live in different languages (Python ``re`` vs POSIX ERE via
    ``grep -qE``) but are kept textually identical across ``release-stamp.yml``
    (the tag guard) and ``release.yml`` (the REAL_TIP line-tip gate) so a change
    to one without the others is caught here instead of at release time.
    """
    for wf_name in (
        ".github/workflows/release-stamp.yml",
        ".github/workflows/release.yml",
    ):
        wf = (_REPO_ROOT / wf_name).read_text(encoding="utf-8")
        # Other steps also grep to classify errors, but only with ``-qiE``/``-qxF``;
        # pick the sole ``grep -qE`` carrying a ``^[0-9]`` version-shaped pattern.
        patterns = re.findall(r"grep -qE '([^']+)'", wf)
        tag_patterns = [p for p in patterns if p.startswith(r"^[0-9]+\.")]
        assert len(tag_patterns) == 1, (
            f"expected exactly one tag guard in {wf_name}, found {tag_patterns}"
        )
        assert tag_patterns[0] == VERSION_RE.pattern, (
            f"guard drift in {wf_name}: {tag_patterns[0]!r} != "
            f"VERSION_RE {VERSION_RE.pattern!r}"
        )


def test_real_tip_gate_blanks_foreign_tag_keeps_pep440() -> None:
    """release.yml REAL_TIP gate: foreign tips blanked, PEP 440 tips kept.

    The gate first strips an optional leading ``v`` (BSkando tags ``vX.Y.Z``),
    then re-uses VERSION_RE (the SSOT) to decide whether the topological line tip
    is a valid version. A foreign, non-version tag (Workflow-A hand-tag
    placeholder, ``nightly``, ``v2`` -> ``2`` after the strip) must be blanked so
    it cannot spuriously route PSR to the hand-tag draft; a genuine PEP 440
    beta/four-segment tag -- and a genuine ``v``-prefixed release tag -- must be
    kept so a real stale-base mismatch still fires ``MODE=hand``. This mirrors the
    shell steps
    ``REAL_TIP="${REAL_TIP#v}"`` then
    ``[ -n "$REAL_TIP" ] && ! printf '%s' "$REAL_TIP" | grep -qE '<VERSION_RE>'``.

    Mutation cross-check (re-runnable): (1) replace ``gate``'s tail with a bare
    ``return stripped`` (no gating) and the three foreign-tag assertions turn red;
    (2) drop the ``#v`` strip line and ``gate("v1.7.14")`` turns red.
    """

    def gate(real_tip: str) -> str:
        # Faithful Python mirror of the release.yml shell gate, incl. the
        # ``REAL_TIP="${REAL_TIP#v}"`` strip that precedes the VERSION_RE check.
        stripped = real_tip[1:] if real_tip.startswith("v") else real_tip
        return stripped if VERSION_RE.match(stripped) else ""

    # foreign / non-version tips are blanked -> no spurious MODE=hand
    assert gate("set-release-tag") == ""
    assert gate("nightly") == ""
    assert gate("v2") == ""  # v-strip -> "2", still not a full version
    # genuine PEP 440 line tips are kept -> MODE=hand still fires on a mismatch
    assert gate("1.7.14b1") == "1.7.14b1"
    assert gate("1.7.14.0") == "1.7.14.0"
    assert gate("1.7.13") == "1.7.13"
    # a genuine v-prefixed release tag (BSkando) is kept as its v-less version
    assert gate("v1.7.14") == "1.7.14"

    # Lock the strip into release.yml itself: the mirror above only models the
    # intended behaviour, so without this the strip could be dropped from the
    # workflow with no red test.
    wf = (_REPO_ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    assert 'REAL_TIP="${REAL_TIP#v}"' in wf, (
        "release.yml must strip an optional leading v from REAL_TIP"
    )


# --- v-prefix tag -> v-less version mapping (both fork conventions) ---------


def test_release_stamp_strips_v_for_version_keeps_tag_for_release() -> None:
    """release-stamp.yml maps a (possibly v-prefixed) TAG to a v-less VERSION.

    The version stamped into the code literals is the v-less value
    (``VERSION="${TAG_NAME#v}"``), while git/release object addressing keeps the
    full published tag (checkout ref, ``rev-list refs/tags``, ``gh release
    upload``). Guards against a mutation that strips the wrong variable, which the
    byte-parity test above cannot catch (it only checks the regex pattern).
    """
    wf = (_REPO_ROOT / ".github/workflows/release-stamp.yml").read_text(
        encoding="utf-8"
    )
    # The version passed to the stamp script is the v-stripped value.
    assert 'VERSION="${TAG_NAME#v}"' in wf, "must derive a v-less VERSION"
    assert '--version "$VERSION"' in wf, "stamp must use the v-less VERSION"
    # The guard validates the stripped VERSION, not the raw TAG_NAME.
    assert "printf '%s' \"$VERSION\" | grep -qE" in wf, (
        "guard must validate the stripped VERSION"
    )
    # Git/release object addressing keeps the full published tag (with any `v`).
    assert "ref: ${{ github.event.release.tag_name }}" in wf
    assert 'git rev-list -n1 "refs/tags/${TAG_NAME}"' in wf
    assert 'gh release upload "$TAG_NAME"' in wf, (
        "release upload must address the real tag, not the stripped version "
        "(locked by test_hacs_validation.py)"
    )


@pytest.mark.parametrize(
    ("tag", "version"),
    [
        ("v1.7.14", "1.7.14"),  # BSkando v-prefix -> v-less version
        ("1.7.14", "1.7.14"),  # jleinenbach no-prefix -> unchanged (no-op)
        ("v1.7.14b1", "1.7.14b1"),  # v-prefix beta
        ("1.7.13.1", "1.7.13.1"),  # four-segment, no prefix
        ("vv1", "v1"),  # POSIX ${VAR#v} strips exactly ONE leading v
    ],
)
def test_tag_to_version_v_strip_semantics(tag: str, version: str) -> None:
    """POSIX ``${TAG_NAME#v}`` strips at most one leading ``v``.

    ``vv1 -> v1`` (not ``1``) proves a single-strip, so the guard would still
    reject a malformed double-v tag. A real ``v``-prefixed tag maps to a version
    that VERSION_RE accepts; the raw ``v``-tag itself does not.
    """
    stripped = tag[1:] if tag.startswith("v") else tag
    assert stripped == version
    if version in {"1.7.14", "1.7.14b1", "1.7.13.1"}:
        assert VERSION_RE.match(stripped), f"{stripped!r} must be a valid version"
        if tag.startswith("v"):
            assert not VERSION_RE.match(tag), f"raw tag {tag!r} must not match"


# --- Tag-vs-branch: checkout must pin the published tag ---------------------


def test_workflow_checks_out_published_tag() -> None:
    """The stamp workflow must check out the published TAG, not target_commitish.

    GitHub documents ``release.target_commitish`` as unused once the git tag
    already exists -- it then defaults to the repo default branch, which only
    coincidentally equals the maintenance line on some forks. Checking that out
    would stamp and package the wrong branch. Pinning ``tag_name`` guarantees the
    stamped tree and the uploaded HACS ZIP always match the exact published
    commit. Regression guard for the tag-vs-branch confusion
    (CA-WORKFLOW-EVENT-PAYLOAD-CONTRACT-001).
    """
    wf = (_REPO_ROOT / ".github/workflows/release-stamp.yml").read_text(
        encoding="utf-8"
    )
    # The checkout ref must reference the published tag ...
    assert "ref: ${{ github.event.release.tag_name }}" in wf, (
        "checkout step must pin the published tag as ref"
    )
    # ... and never the stale target_commitish.
    assert "ref: ${{ github.event.release.target_commitish }}" not in wf, (
        "checkout must not use target_commitish (stale for pre-existing tags)"
    )
    # The stamp push must resolve the owning branch from the tag commit, so a
    # stale/wrong target_commitish cannot send the stamp to the wrong branch.
    assert "git branch -r --contains" in wf, (
        "stamp push must resolve the owning branch from the tag commit, "
        "not trust the stale target_commitish"
    )
    # The resolution must NOT fall back to the stale event hint as a tiebreaker:
    # when the tag sits on several branches (e.g. main + 1.7 on a non-diverged
    # maintenance line) the stale target_commitish could steer the stamp to the
    # wrong branch. Only an unambiguous single owner is stamped; zero or several
    # owners skip safely (CA-WORKFLOW-EVENT-PAYLOAD-CONTRACT-001).
    assert "TARGET_BRANCH" not in wf, (
        "branch resolution must not use the stale target_commitish hint at all; "
        "the tiebreaker could pick the wrong branch when the tag has several owners"
    )


# --- Step order: stamp, then upload, then push -------------------------------


def test_workflow_uploads_hacs_zip_before_branch_push() -> None:
    """The HACS ZIP upload must run AFTER the stamp and BEFORE the branch push.

    The push step may end the job (a branch-rules rejection with an
    unclassified message hits ``exit 1``; a failing fallback ``gh pr create``
    aborts under ``set -e``). The upload step carries no ``if:``, so an
    upload placed after the push would be lost whenever the push fails, and
    the release would ship without its HACS asset. The working tree is already
    stamped once ``script/stamp_version.py`` has run, so the upload depends on
    the stamp step but not on the commit or the push. Regression guard for the
    step order; the existing tests only pin the upload command and the branch
    resolution, not their order.
    """
    wf = (_REPO_ROOT / ".github/workflows/release-stamp.yml").read_text(
        encoding="utf-8"
    )
    stamp_name = "- name: Stamp version into the three literals"
    upload_name = "- name: Rebuild and upload the HACS ZIP from the stamped tree"
    push_name = "- name: Resolve the owning branch and push the stamp"
    for name in (stamp_name, upload_name, push_name):
        assert name in wf, f"step renamed or removed; update the anchor: {name!r}"
    stamp, upload, push = (wf.index(n) for n in (stamp_name, upload_name, push_name))
    assert stamp < upload < push, (
        "step order must be stamp -> HACS ZIP upload -> branch-stamp push; "
        "an upload before the stamp ships an unstamped ZIP, an upload after "
        "the push is lost when branch rules reject the push"
    )
    # The upload step itself must not be conditional: limit the window to this
    # one step (up to the next step marker) and look for the YAML `if:` key.
    next_step = wf.find("\n      - name:", upload + 1)
    upload_block = wf[upload:next_step]
    assert re.search(r"^\s*if:", upload_block, re.MULTILINE) is None, (
        "the upload step must run unconditionally (no `if:`), independent of "
        "whether the stamp commit or the branch push happened"
    )


# --- Tag check before the stamp: schema, monotonicity, version line ---------

# Fork tags as published, measured on 2026-11-08: four-segment tags up to
# 1.7.15.20, then 1.15.21 to 1.15.33 (the ".7." was dropped by hand).
_FORK_TAGS = [
    "v1.7.14",
    "1.7.15.13",
    "1.7.15.20",
    *(f"1.15.{patch}" for patch in range(21, 34)),
]


def _check(version: str, tags: list[str], **kwargs: Any) -> list[str]:
    from script import stamp_version

    return stamp_version.check_against_tags(version, tags=tags, **kwargs)


def test_next_fork_release_in_its_line_passes() -> None:
    """``1.15.34`` follows ``1.15.33`` in the fork line and is accepted."""

    assert _check("1.15.34", _FORK_TAGS, line="1.15") == []


def test_return_to_the_four_segment_scheme_is_refused() -> None:
    """Occasion: ``1.7.15.34`` after ``1.15.33``.

    HACS compares ``1.7.15.34 > 1.15.33`` as false, so fork users would never
    be offered the release. It also leaves the three-segment scheme and the
    fork line.
    """

    reasons = _check("1.7.15.34", _FORK_TAGS, line="1.15")

    assert any("three numeric segments" in reason for reason in reasons)
    assert any("not greater than 1.15.33" in reason for reason in reasons)
    assert any("line 1.15" in reason for reason in reasons)


def test_leaving_the_line_in_force_is_refused() -> None:
    """Occasion: ``1.15.21`` after ``1.7.15.20`` while ``1.7.15.x`` was meant.

    The number is greater and has three segments, so only the line rule, with
    the line that was in force then, catches the dropped ``.7.``.
    """

    tags = ["1.7.15.13", "1.7.15.20"]

    assert _check("1.15.21", tags, line="1.7") == [
        "version 1.15.21 leaves the release line 1.7 (expected 1.7.x)"
    ]


@pytest.mark.parametrize(
    ("version", "fragment"),
    [
        ("v1.15.34", "three numeric segments"),
        ("1.15.34.1", "three numeric segments"),
        ("1.15.33b1", "not greater than 1.15.33"),
        ("1.15.32", "not greater than 1.15.33"),
        ("1.8.0", "not greater than 1.15.33"),
    ],
)
def test_schema_and_monotonicity_violations_are_refused(
    version: str, fragment: str
) -> None:
    """A ``v``, a fourth segment, a repeated or lower number are each refused."""

    assert any(fragment in reason for reason in _check(version, _FORK_TAGS))


def test_the_published_tag_itself_is_not_compared() -> None:
    """The workflow runs after publishing, so the new tag is already listed."""

    assert _check("1.15.34", [*_FORK_TAGS, "1.15.34"], tag="1.15.34") == []


def test_the_same_number_under_another_tag_name_is_refused() -> None:
    """``v1.15.34`` and ``1.15.34`` name the same version."""

    reasons = _check("1.15.34", [*_FORK_TAGS, "v1.15.34", "1.15.34"], tag="1.15.34")

    assert reasons == ["version 1.15.34 is not greater than v1.15.34"]


@pytest.mark.parametrize(
    ("version", "tags", "passes"),
    [
        ("1.15.34", ["1.15.34b1"], True),  # a final release follows its beta
        ("1.15.34b2", ["1.15.34b1"], True),
        ("1.15.34b1", ["1.15.34"], False),  # a beta after its final release
        ("1.15.34b1", ["1.15.34a3"], True),  # beta after alpha
        ("1.15.34", ["nightly", "latest"], True),  # tags without a number
        ("1.7.15", ["V1.7.0-6", "V1.5.4"], True),  # upstream's historic tags
    ],
)
def test_prerelease_and_foreign_tags_are_ordered(
    version: str, tags: list[str], passes: bool
) -> None:
    """Prereleases sort below their release; tags without a number are skipped."""

    assert (_check(version, tags) == []) is passes


def test_upstream_refuses_a_number_the_fork_already_used() -> None:
    """Upstream must not publish a number the fork line already carries."""

    upstream_tags = ["v1.7.14"]

    assert _check("1.8.0", upstream_tags, reserved=_FORK_TAGS) == []
    assert _check("1.15.21", upstream_tags, reserved=_FORK_TAGS) == [
        "version 1.15.21 is already used by the fork as tag 1.15.21"
    ]


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_REPO_ROOT / "script" / "stamp_version.py"), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_check_refuses_without_stamping(tmp_path: Path) -> None:
    """``--check-against-tags`` only checks; it exits 1 and names the reason."""

    tags = tmp_path / "tags.txt"
    tags.write_text("\n".join(_FORK_TAGS) + "\n", encoding="utf-8")

    refused = _run_cli(
        "--version",
        "1.7.15.34",
        "--check-against-tags",
        str(tags),
        "--line",
        "1.15",
        "--repo-root",
        str(tmp_path),
    )
    accepted = _run_cli(
        "--version",
        "1.15.34",
        "--check-against-tags",
        str(tags),
        "--line",
        "1.15",
        "--repo-root",
        str(tmp_path),
    )

    assert refused.returncode == 1, refused.stdout + refused.stderr
    assert "not greater than 1.15.33" in refused.stdout
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    # No stamp target exists under tmp_path; a stamp attempt would have failed.
    assert "stamped" not in accepted.stdout


_CHECK_STEP = "Check the version against the existing tags"


def _workflow_steps() -> list[dict[str, Any]]:
    wf = yaml.safe_load(
        (_REPO_ROOT / ".github/workflows/release-stamp.yml").read_text(encoding="utf-8")
    )
    steps: list[dict[str, Any]] = wf["jobs"]["stamp"]["steps"]
    return steps


def test_tag_check_runs_after_validation_and_before_the_stamp() -> None:
    """The check must stop the job before any literal is written or pushed."""

    names = [step.get("name") for step in _workflow_steps()]

    assert names.count(_CHECK_STEP) == 1
    check = names.index(_CHECK_STEP)
    assert names.index("Validate published tag") < check
    assert check < names.index("Stamp version into the three literals")


def _run_check_step(
    tmp_path: Path, *, repository: str, tag: str, tags: list[str], fork_tags: str
) -> subprocess.CompletedProcess[str]:
    """Run the real ``run`` block as ``bash -e`` in a repository with ``tags``."""

    (step,) = [s for s in _workflow_steps() if s.get("name") == _CHECK_STEP]
    run = str(step["run"])
    assert "${{" not in run and "env" not in step and "shell" not in step

    repo = tmp_path / "repo"
    (repo / "script").mkdir(parents=True)
    shutil.copy(_REPO_ROOT / "script" / "stamp_version.py", repo / "script")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "python").symlink_to(sys.executable)
    gh = bin_dir / "gh"
    gh.write_text(
        f"#!/bin/sh\necho \"$*\" >> {tmp_path / 'gh.log'}\nprintf '%s' '{fork_tags}'\n",
        encoding="utf-8",
    )
    gh.chmod(0o755)
    git = shutil.which("git")
    bash = shutil.which("bash")
    assert git and bash, "git and bash are required"
    env = {
        "PATH": f"{bin_dir}:{Path(git).parent}:/usr/bin:/bin",
        "HOME": str(tmp_path),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": str(tmp_path / "gitconfig"),
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.invalid",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.invalid",
        "TAG_NAME": tag,
        "VERSION": tag.removeprefix("v"),
        "GITHUB_REPOSITORY": repository,
        "RUNNER_TEMP": str(tmp_path),
        "GH_TOKEN": "x",
    }
    (tmp_path / "gitconfig").write_text("", encoding="utf-8")
    subprocess.run([git, "init", "-q", str(repo)], check=True, env=env)
    subprocess.run(
        [git, "-C", str(repo), "commit", "-q", "--allow-empty", "-m", "c"],
        check=True,
        env=env,
    )
    for name in tags:
        subprocess.run([git, "-C", str(repo), "tag", name], check=True, env=env)
    return subprocess.run(
        [bash, "-e", "-c", run],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_fork_step_refuses_a_number_below_the_line(tmp_path: Path) -> None:
    """In the fork the step checks the line and tells to delete the release."""

    result = _run_check_step(
        tmp_path,
        repository="jleinenbach/GoogleFindMy-HA",
        tag="1.7.15.34",
        tags=[*_FORK_TAGS, "1.7.15.34"],
        fork_tags="",
    )

    assert result.returncode != 0
    assert "line 1.15" in result.stdout
    assert "delete the release" in result.stdout
    assert not (tmp_path / "gh.log").exists()


def test_fork_step_accepts_the_next_release(tmp_path: Path) -> None:
    """The next fork release passes and the job goes on to the stamp."""

    result = _run_check_step(
        tmp_path,
        repository="jleinenbach/GoogleFindMy-HA",
        tag="1.15.34",
        tags=[*_FORK_TAGS, "1.15.34"],
        fork_tags="",
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_upstream_step_reads_the_fork_tags(tmp_path: Path) -> None:
    """Upstream compares with its own tags and refuses a fork number."""

    fork_tags = "\n".join(_FORK_TAGS) + "\n"
    ok = _run_check_step(
        tmp_path / "ok",
        repository="BSkando/GoogleFindMy-HA",
        tag="1.8.0",
        tags=["v1.7.14", "1.8.0"],
        fork_tags=fork_tags,
    )
    taken = _run_check_step(
        tmp_path / "taken",
        repository="BSkando/GoogleFindMy-HA",
        tag="1.15.21",
        tags=["v1.7.14", "1.15.21"],
        fork_tags=fork_tags,
    )

    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "repos/jleinenbach/GoogleFindMy-HA/tags" in (
        tmp_path / "ok" / "gh.log"
    ).read_text(encoding="utf-8")
    assert taken.returncode != 0
    assert "already used by the fork" in taken.stdout
