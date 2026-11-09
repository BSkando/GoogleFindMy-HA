# tests/test_diff_coverage.py
"""Unit tests for the patch-coverage measurement helper.

A measuring instrument that is itself unmeasured is not evidence, so the module
that answers "are the changed lines covered?" carries its own red probe: a
constructed diff whose changed lines are demonstrably uncovered. If the helper
reported full coverage there, it would not be measuring the thing it names.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from script import diff_coverage

REPO = Path("/repo")

_XML = """<?xml version="1.0" ?>
<coverage line-rate="0.8" branch-rate="0.7">
  <sources><source>/repo/custom_components/googlefindmy</source></sources>
  <packages>
    <package name=".">
      <classes>
        <class name="api.py" filename="api.py">
          <lines>
            <line number="10" hits="1"/>
            <line number="11" hits="0"/>
            <line number="12" hits="3"/>
          </lines>
        </class>
      </classes>
    </package>
  </packages>
</coverage>
"""


def _coverage() -> dict[str, dict[int, int]]:
    """Return the parsed fixture report."""

    return diff_coverage.parse_coverage_xml(_XML, REPO)


def test_coverage_paths_are_repository_relative() -> None:
    """The Cobertura filename is resolved through the declared source root."""

    parsed = _coverage()

    assert set(parsed) == {"custom_components/googlefindmy/api.py"}
    assert parsed["custom_components/googlefindmy/api.py"][11] == 0


def test_diff_hunks_yield_the_added_line_numbers() -> None:
    """A unified=0 hunk header carries the post-image range directly."""

    diff = (
        "diff --git a/custom_components/googlefindmy/api.py b/custom_components/googlefindmy/api.py\n"
        "--- a/custom_components/googlefindmy/api.py\n"
        "+++ b/custom_components/googlefindmy/api.py\n@@ -3 +10,3 @@\n@@ -20 +40 @@\n"
    )

    changed = diff_coverage.parse_diff(diff)

    assert changed == {"custom_components/googlefindmy/api.py": {10, 11, 12, 40}}


def test_deleted_files_contribute_no_lines() -> None:
    """A file whose post-image is /dev/null must not enter the measurement."""

    diff = "diff --git a/gone.py b/gone.py\n--- a/gone.py\n+++ /dev/null\n@@ -1,5 +0,0 @@\n"

    assert diff_coverage.parse_diff(diff) == {}


def test_uninstrumented_files_are_listed_and_not_counted() -> None:
    """Files outside the coverage source are reported separately, never as misses."""

    changed = {
        "custom_components/googlefindmy/api.py": {10, 12},
        "tests/test_api.py": {1, 2, 3},
    }

    result = diff_coverage.evaluate(changed, _coverage())

    assert result.total == 2
    assert result.covered == 2
    assert result.uninstrumented == (("tests/test_api.py", 3),)
    assert result.uninstrumented_lines == 3
    assert result.percent == pytest.approx(100.0)


def test_red_probe_uncovered_changes_are_not_full_coverage() -> None:
    """Red probe: a diff that only touches an uncovered line must not read as 100%."""

    changed = {"custom_components/googlefindmy/api.py": {11}}

    result = diff_coverage.evaluate(changed, _coverage())

    assert result.covered == 0
    assert result.total == 1
    assert result.percent == pytest.approx(0.0)
    assert result.files[0].missing == (11,)
    report = diff_coverage.format_report(result, 82.21)
    assert "uncovered lines: 11" in report
    assert "BELOW THRESHOLD" in report


def test_lines_without_a_statement_are_excluded() -> None:
    """Blank lines and comments have no XML entry and belong to neither side."""

    changed = {"custom_components/googlefindmy/api.py": {10, 99}}

    result = diff_coverage.evaluate(changed, _coverage())

    assert result.total == 1
    assert result.files[0].missing == ()


def test_an_empty_measurement_is_not_a_full_one() -> None:
    """No instrumented changed statement means no percentage, not 100%."""

    result = diff_coverage.evaluate({"docs/x.md": {1}}, _coverage())

    assert result.percent is None
    report = diff_coverage.format_report(result, 82.21)
    assert "This is not 100%." in report
    assert "BELOW THRESHOLD" not in report


def _patch_git(
    monkeypatch: pytest.MonkeyPatch, recorded: list[list[str]], diff: str
) -> None:
    """Replace the git bridge so the tests never touch a real repository."""

    def _fake(args, repo_root):  # type: ignore[no-untyped-def]
        recorded.append(list(args))
        if args[0] == "merge-base":
            return "cafe1234\n"
        if args[0] == "ls-files":
            return ""
        return diff

    monkeypatch.setattr(diff_coverage, "_run_git", _fake)


def _write_xml(tmp_path: Path) -> Path:
    """Write the fixture report into a temporary file."""

    target = tmp_path / "coverage.xml"
    target.write_text(_XML, encoding="utf-8")
    return target


def test_main_resolves_the_base_through_merge_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The diff base is the merge base of the given ref, not the ref itself."""

    recorded: list[list[str]] = []
    _patch_git(
        monkeypatch,
        recorded,
        "diff --git a/custom_components/googlefindmy/api.py b/custom_components/googlefindmy/api.py\n"
        "--- a/custom_components/googlefindmy/api.py\n"
        "+++ b/custom_components/googlefindmy/api.py\n@@ -1 +10,1 @@\n",
    )

    status = diff_coverage.main(
        [
            "--coverage-xml",
            str(_write_xml(tmp_path)),
            "--repo-root",
            "/repo",
            "--base",
            "origin/main",
        ]
    )

    assert status == diff_coverage.EXIT_OK
    assert recorded[0] == ["merge-base", "origin/main", "HEAD"]
    # The working tree, not cafe1234..HEAD: the coverage report is taken over
    # the working tree, and both sides of the intersection must mean the same
    # revision.
    assert recorded[1] == [
        "diff",
        "--unified=0",
        "--no-color",
        "--no-ext-diff",
        "--inter-hunk-context=0",
        "--src-prefix=a/",
        "--dst-prefix=b/",
        "cafe1234",
    ]
    # Untracked files are part of that same working tree and are asked for
    # after the tracked diff, never instead of it.
    assert recorded[2] == ["ls-files", "--others", "--exclude-standard", "-z"]
    assert "base: cafe1234" in capsys.readouterr().out


def _untracked_repo(tmp_path: Path, body: str) -> Path:
    """Create a repository root holding one untracked, instrumented module."""

    package = tmp_path / "custom_components" / "googlefindmy"
    package.mkdir(parents=True)
    (package / "fresh.py").write_text(body, encoding="utf-8")
    (tmp_path / "coverage.xml").write_text(
        _XML.replace("/repo/", f"{tmp_path.as_posix()}/")
        .replace(
            'name="api.py" filename="api.py"', 'name="fresh.py" filename="fresh.py"'
        )
        .replace('<line number="10" hits="1"/>', '<line number="1" hits="1"/>')
        .replace('<line number="11" hits="0"/>', '<line number="2" hits="0"/>')
        .replace('<line number="12" hits="3"/>', ""),
        encoding="utf-8",
    )
    return tmp_path


def test_an_untracked_file_enters_the_measurement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A new, unstaged module is part of the tree the suite ran on.

    Codex finding on PR #1274: ``git diff <base>`` omits untracked files, so
    their statements were listed by the coverage report but never counted as
    changed. Here the tracked diff is empty and the only change is untracked,
    with one covered and one uncovered statement; the result has to be 1/2,
    not "nothing changed".
    """

    repo = _untracked_repo(tmp_path, "a = 1\nb = 2\n")
    recorded: list[list[str]] = []

    def _fake(args, repo_root):  # type: ignore[no-untyped-def]
        recorded.append(list(args))
        if args[0] == "merge-base":
            return "cafe1234\n"
        if args[0] == "ls-files":
            return "custom_components/googlefindmy/fresh.py\0"
        return ""

    monkeypatch.setattr(diff_coverage, "_run_git", _fake)

    status = diff_coverage.main(
        ["--coverage-xml", "coverage.xml", "--repo-root", str(repo), "--threshold", "0"]
    )

    out = capsys.readouterr().out
    assert status == diff_coverage.EXIT_OK
    assert "RESULT: 1/2 (50.00%)" in out


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file names")
def test_an_untracked_file_with_a_newline_in_its_name_enters_the_measurement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A control character in an untracked name does not split the diff header.

    Codex finding on PR #1274: the synthetic header wrote the name raw, so a
    newline in it cut the ``+++`` line in two and the hunk was booked against
    the first half. The XML writer coverage.py uses escapes such a name as
    ``&#10;`` (measured with coverage 7.15.2 on Python 3.13 and 3.14), and
    only the git-quoted header decodes to the same string; the result has to
    be 1/2 for this file, not "uninstrumented".
    """

    awkward = "fresh\nline.py"
    repo = _untracked_repo(tmp_path, "a = 1\nb = 2\n")
    (repo / "custom_components" / "googlefindmy" / "fresh.py").rename(
        repo / "custom_components" / "googlefindmy" / awkward
    )
    xml = repo / "coverage.xml"
    xml.write_text(
        xml.read_text(encoding="utf-8").replace(
            'name="fresh.py" filename="fresh.py"',
            'name="fresh&#10;line.py" filename="fresh&#10;line.py"',
        ),
        encoding="utf-8",
    )

    def _fake(args, repo_root):  # type: ignore[no-untyped-def]
        if args[0] == "merge-base":
            return "cafe1234\n"
        if args[0] == "ls-files":
            return f"custom_components/googlefindmy/{awkward}\0"
        return ""

    monkeypatch.setattr(diff_coverage, "_run_git", _fake)

    status = diff_coverage.main(
        ["--coverage-xml", "coverage.xml", "--repo-root", str(repo), "--threshold", "0"]
    )

    out = capsys.readouterr().out
    assert status == diff_coverage.EXIT_OK
    assert "RESULT: 1/2 (50.00%)" in out
    assert "uninstrumented" not in out


def test_an_untracked_file_that_vanished_is_a_measurement_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A listed file that cannot be read is reported, not silently skipped."""

    repo = _untracked_repo(tmp_path, "a = 1\n")

    def _fake(args, repo_root):  # type: ignore[no-untyped-def]
        if args[0] == "merge-base":
            return "cafe1234\n"
        if args[0] == "ls-files":
            return "custom_components/googlefindmy/gone.py\0"
        return ""

    monkeypatch.setattr(diff_coverage, "_run_git", _fake)

    status = diff_coverage.main(
        ["--coverage-xml", "coverage.xml", "--repo-root", str(repo)]
    )

    assert status == diff_coverage.EXIT_ERROR
    assert (
        "untracked file 'custom_components/googlefindmy/gone.py'"
        in capsys.readouterr().err
    )


def test_main_signals_a_shortfall_with_its_own_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Falling below the threshold is status 2, distinct from a broken measurement."""

    _patch_git(
        monkeypatch,
        [],
        "diff --git a/custom_components/googlefindmy/api.py b/custom_components/googlefindmy/api.py\n"
        "--- a/custom_components/googlefindmy/api.py\n"
        "+++ b/custom_components/googlefindmy/api.py\n@@ -1 +11,1 @@\n",
    )

    status = diff_coverage.main(
        ["--coverage-xml", str(_write_xml(tmp_path)), "--repo-root", "/repo"]
    )

    assert status == diff_coverage.EXIT_BELOW_THRESHOLD
    assert "BELOW THRESHOLD" in capsys.readouterr().out


def test_main_reports_a_missing_report_as_an_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A missing coverage.xml is an error, not a coverage verdict."""

    status = diff_coverage.main(
        ["--coverage-xml", str(tmp_path / "absent.xml"), "--repo-root", "/repo"]
    )

    assert status == diff_coverage.EXIT_ERROR
    assert "could not be measured" in capsys.readouterr().err


def test_main_reports_a_git_failure_as_an_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A git failure must not be reported as zero patch coverage."""

    def _fail(args, repo_root):  # type: ignore[no-untyped-def]
        raise diff_coverage.MeasurementError("no merge base")

    monkeypatch.setattr(diff_coverage, "_run_git", _fail)

    status = diff_coverage.main(
        ["--coverage-xml", str(_write_xml(tmp_path)), "--repo-root", "/repo"]
    )

    assert status == diff_coverage.EXIT_ERROR
    assert "no merge base" in capsys.readouterr().err


def test_main_reports_an_absent_measurement_with_its_own_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A diff without an instrumented statement gets its own exit status.

    Sharing status 0 with "measured and fine" is how a docs-only diff would be
    reported as a passing patch coverage by every caller that reads the status
    instead of the text.
    """

    _patch_git(monkeypatch, [], "--- a/docs/x.md\n+++ b/docs/x.md\n@@ -1 +1,2 @@\n")

    status = diff_coverage.main(
        ["--coverage-xml", str(_write_xml(tmp_path)), "--repo-root", "/repo"]
    )

    assert status == diff_coverage.EXIT_NOT_MEASURABLE
    assert "This is not 100%." in capsys.readouterr().out


def test_a_report_from_another_checkout_is_an_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Sources that resolve nowhere must not silently empty the measurement."""

    status = diff_coverage.main(
        ["--coverage-xml", str(_write_xml(tmp_path)), "--repo-root", str(tmp_path)]
    )

    assert status == diff_coverage.EXIT_ERROR
    assert "another checkout" in capsys.readouterr().err


def test_a_malformed_hunk_header_is_a_measurement_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A broken header must fail the measurement, not the whole preflight process."""

    _patch_git(
        monkeypatch,
        [],
        "diff --git a/custom_components/googlefindmy/api.py b/custom_components/googlefindmy/api.py\n"
        "--- a/custom_components/googlefindmy/api.py\n"
        "+++ b/custom_components/googlefindmy/api.py\n@@ broken @@\n",
    )

    status = diff_coverage.main(
        ["--coverage-xml", str(_write_xml(tmp_path)), "--repo-root", "/repo"]
    )

    assert status == diff_coverage.EXIT_ERROR
    assert "hunk header" in capsys.readouterr().err


def test_a_plus_line_without_a_source_header_is_not_a_file() -> None:
    """Only a "+++" that follows its "---" partner starts a file section."""

    diff = "--- a/x\n+++ not-a-header\n@@ -1 +5,2 @@\n"

    assert diff_coverage.parse_diff(diff) == {}


def test_an_existing_file_wins_over_a_merely_resolvable_path(tmp_path: Path) -> None:
    """Two sources can both resolve; only one of them names a real file.

    Cobertura allows several <source> elements, and a source that already ends
    in the package prefix doubles it into a path that resolves cleanly and does
    not exist. Without the file check the parser would pick that one, and every
    entry would land under "not instrumented" while looking measured.
    """

    package = tmp_path / "custom_components" / "googlefindmy"
    package.mkdir(parents=True)
    (package / "api.py").write_text("x = 1\n", encoding="utf-8")
    xml = _XML.replace(
        "<sources><source>/repo/custom_components/googlefindmy</source></sources>",
        "<sources>"
        f"<source>{package / 'custom_components' / 'googlefindmy'}</source>"
        f"<source>{package}</source>"
        "</sources>",
    )

    parsed = diff_coverage.parse_coverage_xml(xml, tmp_path)

    assert set(parsed) == {"custom_components/googlefindmy/api.py"}


def test_a_non_numeric_line_record_is_a_measurement_error() -> None:
    """A truncated or foreign report must not raise through the whole preflight."""

    broken = _XML.replace('hits="1"', 'hits=""', 1)

    with pytest.raises(diff_coverage.MeasurementError) as raised:
        diff_coverage.parse_coverage_xml(broken, REPO)

    assert "non-numeric" in str(raised.value)


@pytest.mark.parametrize(
    "doctype",
    [
        '<!DOCTYPE coverage [<!ENTITY src "/repo/custom_components/googlefindmy">]>',
        '<!DOCTYPE coverage [<!ENTITY ext SYSTEM "file:///etc/passwd">]>',
    ],
)
def test_a_report_declaring_entities_is_a_measurement_error(doctype: str) -> None:
    """The report is parsed with defusedxml: entity declarations are refused.

    The standard library expands internal entities, which is the entry point of
    the exponential-expansion attack; a coverage report never needs one.
    """

    declaring = _XML.replace(
        '<?xml version="1.0" ?>', '<?xml version="1.0" ?>\n' + doctype, 1
    ).replace(
        "<source>/repo/custom_components/googlefindmy</source>",
        "<source>&src;</source>"
        if "ENTITY src" in doctype
        else "<source>&ext;</source>",
        1,
    )

    with pytest.raises(diff_coverage.MeasurementError) as raised:
        diff_coverage.parse_coverage_xml(declaring, REPO)

    assert "declares an entity" in str(raised.value)


def test_a_content_line_is_not_mistaken_for_a_file_header() -> None:
    """A removed "-- note" and an added "++ note" must not start a file section."""

    diff = (
        "diff --git a/custom_components/googlefindmy/api.py b/custom_components/googlefindmy/api.py\n"
        "--- a/custom_components/googlefindmy/api.py\n"
        "+++ b/custom_components/googlefindmy/api.py\n"
        "@@ -1 +10,1 @@\n"
        "--- note\n"
        "+++ note\n"
        "@@ -2 +11,1 @@\n"
    )

    changed = diff_coverage.parse_diff(diff)

    assert changed == {"custom_components/googlefindmy/api.py": {10, 11}}


def test_a_quoted_path_is_unquoted() -> None:
    """A quoted header is stripped of its quotes; they never match a coverage entry."""

    diff = (
        'diff --git a/x "b/custom_components/googlefindmy/a b.py"\n'
        '--- a/x\n+++ "b/custom_components/googlefindmy/a b.py"\n@@ -1 +3,1 @@\n'
    )

    assert diff_coverage.parse_diff(diff) == {
        "custom_components/googlefindmy/a b.py": {3}
    }


def test_a_quoted_path_has_its_escapes_decoded() -> None:
    """git's C-style quoting escapes non-ASCII bytes; the name must be decoded.

    Codex finding on PR #1274: with the default ``core.quotePath`` a path such
    as ``tür.py`` arrives as ``"t\\303\\274r.py"``. Only the decoded name
    matches the coverage report; the raw one would be listed as uninstrumented.
    """

    diff = (
        'diff --git "a/custom_components/googlefindmy/t\\303\\274r \\"x\\".py"'
        ' "b/custom_components/googlefindmy/t\\303\\274r \\"x\\".py"\n'
        '--- "a/custom_components/googlefindmy/t\\303\\274r \\"x\\".py"\n'
        '+++ "b/custom_components/googlefindmy/t\\303\\274r \\"x\\".py"\n'
        "@@ -1 +3,2 @@\n"
    )

    assert diff_coverage.parse_diff(diff) == {
        'custom_components/googlefindmy/tür "x".py': {3, 4}
    }


def test_a_malformed_quoted_path_is_a_measurement_error() -> None:
    """A truncated or unknown escape is reported, not guessed at."""

    with pytest.raises(diff_coverage.MeasurementError):
        diff_coverage._unquote_git_path('"a/x\\"')
    with pytest.raises(diff_coverage.MeasurementError):
        diff_coverage._unquote_git_path('"a/x\\q"')


_AWKWARD_NAMES = (
    "new\nline.py",
    "carriage\rreturn.py",
    "tab\there.py",
    'double"quote.py',
    "back\\slash.py",
    "tür.py",
    "bell\aring.py",
    "escape\x1bkey.py",
    "delete\x7fkey.py",
)


@pytest.mark.parametrize("name", _AWKWARD_NAMES, ids=repr)
def test_quoting_a_path_round_trips_through_unquoting(name: str) -> None:
    """Every byte class git escapes comes back as the original name."""

    quoted = diff_coverage._quote_git_path(f"b/{name}")

    assert quoted.startswith('"') and quoted.endswith('"')
    # Nothing but printable ASCII may remain: a raw control byte would still
    # decode correctly and still split or corrupt the header line.
    assert all(" " <= char <= "~" for char in quoted), repr(quoted)
    assert diff_coverage._unquote_git_path(quoted) == f"b/{name}"


def test_a_plain_or_spaced_path_is_not_quoted() -> None:
    """git leaves a name without awkward bytes alone, a space included.

    Measured against git 2.x with ``core.quotePath`` at its default: ``a b.py``
    arrives as ``+++ b/a b.py`` followed by a tab, never inside quotes. Quoting
    it here would be harmless to the parser but would not be git's form.
    """

    assert diff_coverage._quote_git_path("b/plain.py") == "b/plain.py"
    assert diff_coverage._quote_git_path("b/a b.py") == "b/a b.py"


def _git(repo: Path, *arguments: str) -> str:
    """Run one git command inside ``repo`` and return its stdout."""

    completed = subprocess.run(
        ["git", *arguments],
        check=True,
        capture_output=True,
        text=True,
        cwd=str(repo),
    )
    return completed.stdout


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file names")
def test_the_synthetic_diff_names_files_as_git_does(tmp_path: Path) -> None:
    """The untracked-file diff and a real ``git diff`` parse to the same map.

    The quoting is git's, not ours, so a fake would only pin what we believe
    git does. Here the same awkward names go through both paths: once as
    untracked files through ``_untracked_diff``, once staged and printed by
    git itself. The oracle is called with the pins ``collect_diff`` uses, over
    a repository configured the opposite way (quoting off, mnemonic prefixes
    on), so what is compared is git's form under the settings the tool
    enforces, not under whatever the host happens to carry.
    """

    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "core.quotePath", "false")
    _git(tmp_path, "config", "diff.mnemonicPrefix", "true")
    names = [*_AWKWARD_NAMES, "a b.py", "plain.py"]
    for name in names:
        (tmp_path / name).write_text("y = 1\ny = 2\n", encoding="utf-8")

    synthetic = diff_coverage._untracked_diff(tmp_path)

    _git(tmp_path, "add", "-A")
    real = _git(
        tmp_path,
        "-c",
        "core.quotePath=true",
        "diff",
        "--cached",
        "--unified=0",
        "--no-color",
        "--no-ext-diff",
        "--src-prefix=a/",
        "--dst-prefix=b/",
    )

    # Positive control: git really did quote here, so the comparison is doing work.
    assert '+++ "b/' in real
    assert diff_coverage.parse_diff(synthetic) == diff_coverage.parse_diff(real)
    assert set(diff_coverage.parse_diff(synthetic)) == set(names)
    # The headers themselves, not only what they decode to: a raw non-ASCII
    # byte inside the quotes decodes to the same name and would pass above.
    # git ends an unquoted header holding a space with a tab, which the parser
    # strips and the synthetic diff does not write.
    headers = [
        line.rstrip("\t") for line in real.splitlines() if line.startswith("+++ ")
    ]
    assert sorted(headers) == sorted(
        line for line in synthetic.splitlines() if line.startswith("+++ ")
    )


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file names")
def test_collect_diff_is_immune_to_the_host_prefix_and_quoting_settings(
    tmp_path: Path,
) -> None:
    """A developer's diff-shaping git configuration changes nothing.

    ``_diff_path`` strips ``a/`` and ``b/`` and decodes C-quoting. With the
    prefixes renamed by configuration (``w/`` for the working tree) every
    changed file was reported as uninstrumented; measured red before the pin.
    ``diff.noprefix`` is harmless by comparison, an unprefixed name is taken
    as it is, and ``core.quotePath=false`` only stops the quoting of
    non-ASCII bytes, never of control bytes. ``color.diff=always`` puts
    escape bytes in front of every header, so no line parsed as one; and
    ``diff.interHunkContext`` merges hunks that lie close together, so the
    untouched lines between them counted as changed (measured: one hunk of
    485 lines where eight hunks were meant). All four settings are pinned on
    the command line, and this repository carries the opposite of each.
    """

    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "t@example.invalid")
    _git(tmp_path, "config", "user.name", "t")
    _git(tmp_path, "config", "diff.mnemonicPrefix", "true")
    _git(tmp_path, "config", "core.quotePath", "false")
    _git(tmp_path, "config", "color.diff", "always")
    _git(tmp_path, "config", "diff.interHunkContext", "1000")
    awkward = "tür.py"
    # Two edits with untouched lines between them: only the hunk-context pin
    # keeps them apart, without it lines 2 to 4 are reported as changed too.
    (tmp_path / awkward).write_text(
        "y = 1\na = 0\nb = 0\nc = 0\nz = 1\n", encoding="utf-8"
    )
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-q", "-m", "base")
    (tmp_path / awkward).write_text(
        "y = 2\na = 0\nb = 0\nc = 0\nz = 2\n", encoding="utf-8"
    )
    (tmp_path / "new\nline.py").write_text("z = 3\n", encoding="utf-8")

    _base, diff = diff_coverage.collect_diff("HEAD", tmp_path)

    assert diff_coverage.parse_diff(diff) == {
        awkward: {1, 5},
        "new\nline.py": {1},
    }
