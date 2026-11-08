# tests/test_local_verify_script.py
"""Unit tests for the consolidated local verification helper script."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace

import pytest

from script import local_verify


@pytest.fixture
def _capture_run(monkeypatch: pytest.MonkeyPatch) -> list[Sequence[str]]:
    """Intercept subprocess.run calls and capture the invoked commands."""

    executed: list[Sequence[str]] = []

    def _fake_run(
        command: Sequence[str], *, check: bool = False, **_kwargs: object
    ) -> SimpleNamespace:
        executed.append(tuple(command))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(local_verify.subprocess, "run", _fake_run)
    return executed


def test_default_commands_include_ruff_and_pytest(
    _capture_run: list[Sequence[str]],
) -> None:
    """Ensure the helper runs Ruff format --check before pytest."""

    exit_code = local_verify.main([])

    assert exit_code == 0
    assert len(_capture_run) == 2

    ruff_command = list(_capture_run[0])
    assert ruff_command[0] == local_verify.sys.executable
    assert str(local_verify.RUFF_FORMAT_SCRIPT) in ruff_command
    assert "--check" in ruff_command
    assert not any(arg.startswith("--exclude=") for arg in ruff_command)
    pytest_command = list(_capture_run[1])
    assert pytest_command[:3] == [
        local_verify.sys.executable,
        "-m",
        "pytest_homeassistant_custom_component",
    ]


def test_pytest_still_runs_when_ruff_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ruff failures should not prevent pytest from executing."""

    recorded: list[Sequence[str]] = []

    def _sequenced_run(
        command: Sequence[str], *, check: bool = False, **_kwargs: object
    ) -> SimpleNamespace:
        recorded.append(tuple(command))
        return SimpleNamespace(returncode=1 if not recorded[:-1] else 0)

    monkeypatch.setattr(local_verify.subprocess, "run", _sequenced_run)

    exit_code = local_verify.main([])

    assert exit_code == 1
    assert len(recorded) == 2
    assert list(recorded[1])[:3] == [
        local_verify.sys.executable,
        "-m",
        "pytest_homeassistant_custom_component",
    ]


def test_preflight_names_the_stages_that_did_not_run(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """S7 and S8 must appear as NOT CHECKED with a reason, never as passing."""

    monkeypatch.setattr(
        local_verify, "_static_stages", lambda _interp, _base, skip_format=False: []
    )

    status = local_verify.main(["--all", "--skip-pytest"])

    output = capsys.readouterr().out
    # S5 and S6 were supposed to run and did not, so the status says so; S7 and
    # S8 cannot run here at all and do not move it.
    assert status == local_verify.EXIT_STAGE_ABSENT
    assert "S7 security scans" in output
    assert "S8 manifest" in output
    assert output.count(local_verify.STATUS_SKIPPED) >= 4
    assert "is not a passing stage" in output


def test_suite_stage_requires_every_track_to_be_green(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """One green track and one red track must not add up to a green S5."""

    outcomes = iter([(0, "clamped"), (1, "clamped")])

    def _fake_run(interpreter: str, xml_path: Path) -> tuple[int, str]:
        xml_path.write_text("<coverage/>", encoding="utf-8")
        return next(outcomes)

    monkeypatch.setattr(local_verify, "_run_suite", _fake_run)

    result, reference = local_verify._stage_suite(["/a/python", "/b/python"], tmp_path)

    assert result.status == local_verify.STATUS_FAILED
    assert reference is not None
    assert "/a/python: exit 0" in result.detail
    assert "/b/python: exit 1" in result.detail


def test_suite_stage_is_green_when_both_tracks_are(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Two green tracks report OK and hand the first report to S6."""

    def _fake_run(interpreter: str, xml_path: Path) -> tuple[int, str]:
        xml_path.write_text("<coverage/>", encoding="utf-8")
        return 0, "clamped"

    monkeypatch.setattr(local_verify, "_run_suite", _fake_run)

    result, reference = local_verify._stage_suite(["/a/python", "/b/python"], tmp_path)

    assert result.status == local_verify.STATUS_OK
    assert reference == tmp_path / "coverage-0.xml"
    assert "single track" not in result.detail


def test_a_single_interpreter_is_declared_as_a_single_track(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """One interpreter covers one track, and the report has to say so."""

    def _fake_run(interpreter: str, xml_path: Path) -> tuple[int, str]:
        xml_path.write_text("<coverage/>", encoding="utf-8")
        return 0, "clamped"

    monkeypatch.setattr(local_verify, "_run_suite", _fake_run)

    result, _reference = local_verify._stage_suite(["/a/python"], tmp_path)

    assert result.status == local_verify.STATUS_OK
    assert "single track" in result.detail


def test_a_source_change_during_the_run_discards_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The digest clamp discards a run whose sources moved, it does not interpret it."""

    digests = iter([{"a.py": "before"}, {"a.py": "after"}])
    monkeypatch.setattr(local_verify, "_changed_paths", lambda: ["a.py"])
    monkeypatch.setattr(local_verify, "_digest_of", lambda _paths: next(digests))
    monkeypatch.setattr(local_verify, "_run_command", lambda _command: 0)

    returncode, note = local_verify._run_suite("/a/python", tmp_path / "c.xml")

    assert returncode == 1
    assert "discarded" in note


def test_an_unchanged_clamp_keeps_the_exit_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An intact clamp passes the suite status through untouched."""

    monkeypatch.setattr(local_verify, "_changed_paths", lambda: ["a.py"])
    monkeypatch.setattr(local_verify, "_digest_of", lambda _paths: {"a.py": "same"})
    monkeypatch.setattr(local_verify, "_run_command", lambda _command: 0)

    returncode, note = local_verify._run_suite("/a/python", tmp_path / "c.xml")

    assert returncode == 0
    assert "discarded" not in note


def test_patch_coverage_shortfall_is_a_note_not_a_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Status 2 of the measurement means "below threshold", which never blocks."""

    report = tmp_path / "coverage.xml"
    report.write_text("<coverage/>", encoding="utf-8")
    monkeypatch.setattr(local_verify.diff_coverage, "main", lambda _argv: 2)

    result = local_verify._stage_patch_coverage(report, "origin/main")

    assert result.status == local_verify.STATUS_NOTE


def test_patch_coverage_error_is_a_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Status 1 means the measurement broke, and a broken measurement is no verdict."""

    report = tmp_path / "coverage.xml"
    report.write_text("<coverage/>", encoding="utf-8")
    monkeypatch.setattr(local_verify.diff_coverage, "main", lambda _argv: 1)

    result = local_verify._stage_patch_coverage(report, "origin/main")

    assert result.status == local_verify.STATUS_FAILED


def test_the_verdict_names_every_failing_stage() -> None:
    """The verdict line must enumerate the failures instead of summarising them."""

    results = [
        local_verify.StageResult("S1", "format", local_verify.STATUS_OK, ""),
        local_verify.StageResult("S3", "types", local_verify.STATUS_FAILED, ""),
        local_verify.StageResult("S5", "suite", local_verify.STATUS_FAILED, ""),
    ]

    report = local_verify._format_report(results)

    assert "VERDICT: FAILED (S3, S5)" in report


def test_branch_paths_unite_the_working_tree_with_the_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Committed and uncommitted work are both part of "what this branch touched"."""

    def _fake_capture(command: Sequence[str]) -> SimpleNamespace:
        assert command[1] == "merge-base"
        return SimpleNamespace(returncode=0, stdout="cafe1234\n", stderr="")

    def _fake_capture_raw(command: Sequence[str]) -> SimpleNamespace:
        assert "-z" in command
        return SimpleNamespace(returncode=0, stdout=b"committed.py\0", stderr=b"")

    monkeypatch.setattr(local_verify, "_capture", _fake_capture)
    monkeypatch.setattr(local_verify, "_capture_raw", _fake_capture_raw)
    monkeypatch.setattr(local_verify, "_changed_paths", lambda: ["uncommitted.py"])

    assert local_verify._branch_paths("origin/main") == {
        "committed.py",
        "uncommitted.py",
    }


def test_changed_paths_include_untracked_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The digest clamp holds a new, unstaged file as well as an edited one.

    ``git diff HEAD`` names tracked files only; a module created during the
    branch and not yet staged would otherwise be free to change while the
    suite runs, and the clamp would count one file fewer than the tree changed.
    """

    def _fake_capture_raw(command: Sequence[str]) -> SimpleNamespace:
        if command[1] == "ls-files":
            return SimpleNamespace(returncode=0, stdout=b"new.py\0", stderr=b"")
        return SimpleNamespace(returncode=0, stdout=b"edited.py\0", stderr=b"")

    monkeypatch.setattr(local_verify, "_capture_raw", _fake_capture_raw)

    assert local_verify._changed_paths() == ["edited.py", "new.py"]


def test_branch_paths_survive_a_missing_merge_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a merge base the working tree is still reported, not an empty set."""

    monkeypatch.setattr(
        local_verify,
        "_capture",
        lambda _command: SimpleNamespace(returncode=1, stdout="", stderr="boom"),
    )
    monkeypatch.setattr(local_verify, "_changed_paths", lambda: ["uncommitted.py"])

    assert local_verify._branch_paths("origin/main") == {"uncommitted.py"}


def _git(repo: Path, *arguments: str) -> str:
    """Run one git command inside ``repo`` and return its stripped stdout."""

    completed = subprocess.run(
        ["git", *arguments],
        check=True,
        capture_output=True,
        text=True,
        cwd=str(repo),
    )
    return completed.stdout.strip()


def test_git_paths_arrive_verbatim_under_the_default_quoting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A name git would C-quote reaches the clamp and the branch set as itself.

    Codex finding on PR #1274: with the default ``core.quotePath`` a changed
    or untracked ``tür.py`` came back as ``"t\\303\\274r.py"``; the digest
    helper then looked up that literal, found nothing, and the file dropped out
    of both snapshots. The probe runs real git rather than a fake, because the
    quoting is git's, not ours, and a fake would only pin what we believe git
    does. ``core.quotePath`` is set explicitly so a host configuration that
    switched it off cannot make this test pass for the wrong reason.
    """

    awkward = 'tür "x".py'
    _git(tmp_path, "init", "-q", "-b", "base")
    _git(tmp_path, "config", "core.quotePath", "true")
    _git(tmp_path, "config", "user.email", "t@example.invalid")
    _git(tmp_path, "config", "user.name", "t")
    (tmp_path / "plain.py").write_text("x = 1\n", encoding="utf-8")
    _git(tmp_path, "add", "plain.py")
    _git(tmp_path, "commit", "-q", "-m", "base")
    _git(tmp_path, "checkout", "-q", "-b", "work")
    (tmp_path / awkward).write_text("y = 2\n", encoding="utf-8")
    _git(tmp_path, "add", "--", awkward)
    _git(tmp_path, "commit", "-q", "-m", "awkward")
    (tmp_path / awkward).write_text("y = 3\n", encoding="utf-8")
    (tmp_path / "größe.py").write_text("z = 4\n", encoding="utf-8")
    monkeypatch.setattr(local_verify, "REPO_ROOT", tmp_path)

    # Positive control: git really does quote here, so the -z form is doing work.
    assert _git(tmp_path, "diff", "HEAD", "--name-only").startswith('"')

    assert local_verify._changed_paths() == sorted([awkward, "größe.py"])
    assert local_verify._branch_paths("base") == {awkward, "größe.py"}


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file names")
def test_a_carriage_return_in_a_path_survives_the_capture(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A name holding ``\\r`` reaches the clamp as itself, not as ``\\n``.

    ``subprocess.run(text=True)`` reads stdout through the universal-newline
    layer, which turns a lone carriage return into a newline. ``-z`` hands
    the name over verbatim, and that byte is the name; translated, the clamp
    hashed ``cr\\nx.py`` and recorded a file that does not exist. Real git
    again, because the listing is git's and the translation is the text
    layer's; a fake with a string would sit on the wrong side of it.
    """

    awkward = "cr\rx.py"
    _git(tmp_path, "init", "-q")
    (tmp_path / awkward).write_text("y = 1\n", encoding="utf-8")
    monkeypatch.setattr(local_verify, "REPO_ROOT", tmp_path)

    assert local_verify._changed_paths() == [awkward]
    assert local_verify._digest_of([awkward])[awkward] != local_verify.MISSING_DIGEST


def test_a_missing_path_is_recorded_not_dropped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The snapshot keeps a path that has no file behind it, marked as missing."""

    (tmp_path / "present.py").write_text("x = 1\n", encoding="utf-8")
    monkeypatch.setattr(local_verify, "REPO_ROOT", tmp_path)

    digests = local_verify._digest_of(["present.py", "gone.py"])

    assert set(digests) == {"present.py", "gone.py"}
    assert digests["gone.py"] == local_verify.MISSING_DIGEST
    assert digests["present.py"] != local_verify.MISSING_DIGEST


def test_a_deletion_during_the_run_discards_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A file clean at the start and deleted during the suite trips the clamp.

    Codex finding on PR #1274: the first snapshot does not know the file (it
    was clean), the second path list names it (git sees the deletion), and a
    digest helper that skipped missing files produced an empty second snapshot
    too, so ``before == after`` read as "unchanged" for a run whose tree had
    lost a source file.
    """

    listings = iter([[], ["gone.py"]])
    monkeypatch.setattr(local_verify, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(local_verify, "_changed_paths", lambda: next(listings))
    monkeypatch.setattr(local_verify, "_run_command", lambda _command: 0)

    returncode, note = local_verify._run_suite("/a/python", tmp_path / "c.xml")

    assert returncode == 1
    assert "discarded" in note


def test_the_documented_direct_invocation_starts() -> None:
    """`python script/local_verify.py` is the documented call and must import.

    Running the file directly puts ``script/`` on ``sys.path`` rather than the
    repository root, so the package imports at the top of the module fail
    without the bootstrap. README.md and AGENTS.md both point at exactly this
    form, and it used to end in ModuleNotFoundError.
    """

    repo_root = Path(local_verify.__file__).resolve().parents[1]
    # PYTHONPATH is stripped on purpose: inherited from a pytest run it would
    # put the repository root on sys.path anyway, and the test would then be
    # green without the bootstrap it exists to guard.
    environment = {
        key: value for key, value in os.environ.items() if key != "PYTHONPATH"
    }
    completed = subprocess.run(
        [sys.executable, "script/local_verify.py", "--help"],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        env=environment,
    )

    assert completed.returncode == 0, completed.stderr
    assert "--all" in completed.stdout


def test_a_missing_spell_checker_is_not_clean_spelling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """codespell that never ran must report NOT CHECKED, not zero findings."""

    monkeypatch.setattr(
        local_verify,
        "_capture",
        lambda _command: SimpleNamespace(
            returncode=1, stdout="", stderr="ModuleNotFoundError: codespell_lib"
        ),
    )

    result = local_verify._stage_spelling("/a/python", "origin/main")

    assert result.status == local_verify.STATUS_SKIPPED
    assert "did not run" in result.detail


def test_spelling_findings_are_matched_against_repository_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """codespell prints "./file", git prints "file"; the counter must bridge that."""

    # The word is invented on purpose: a real typo here would make codespell
    # find its own fixture and pollute the very count this test measures.
    finding = "./script/diff_coverage.py:12: xyzzq ==> xyzzy"
    monkeypatch.setattr(
        local_verify,
        "_capture",
        lambda _command: SimpleNamespace(
            returncode=local_verify.CODESPELL_FINDINGS_STATUS,
            stdout=f"{finding}\n",
            stderr="",
        ),
    )
    monkeypatch.setattr(
        local_verify, "_branch_paths", lambda _base: {"script/diff_coverage.py"}
    )

    result = local_verify._stage_spelling("/a/python", "origin/main")

    assert result.status == local_verify.STATUS_NOTE
    assert "1 findings, 1 of them" in result.detail


def test_patch_coverage_without_a_measurable_diff_is_not_checked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No instrumented changed statement is an absent measurement, not a pass."""

    report = tmp_path / "coverage.xml"
    report.write_text("<coverage/>", encoding="utf-8")
    monkeypatch.setattr(
        local_verify.diff_coverage,
        "main",
        lambda _argv: local_verify.diff_coverage.EXIT_NOT_MEASURABLE,
    )

    result = local_verify._stage_patch_coverage(report, "origin/main")

    assert result.status == local_verify.STATUS_SKIPPED
    assert "not full coverage" in result.detail


def test_stages_run_inside_the_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stage must not inspect whatever directory the caller happened to be in."""

    recorded: dict[str, object] = {}

    def _fake_run(command, **kwargs):  # type: ignore[no-untyped-def]
        recorded.update(kwargs)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(local_verify.subprocess, "run", _fake_run)

    local_verify._run_command(["/a/python", "-m", "ruff", "check", "."])

    assert recorded["cwd"] == str(local_verify.REPO_ROOT)


def test_the_clamp_covers_staged_files(monkeypatch: pytest.MonkeyPatch) -> None:
    """Staged and unstaged new files alike are part of the clamp.

    ``git diff HEAD`` (not the index) sees a staged addition; the ``ls-files``
    call sees an addition that was never staged. Both are pinned here, because
    dropping either one silently shrinks the set the clamp holds.
    """

    recorded: list[Sequence[str]] = []

    def _fake_capture_raw(command: Sequence[str]) -> SimpleNamespace:
        recorded.append(tuple(command))
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(local_verify, "_capture_raw", _fake_capture_raw)

    local_verify._changed_paths()

    assert recorded == [
        ("git", "diff", "HEAD", "--name-only", "-z"),
        ("git", "ls-files", "--others", "--exclude-standard", "-z"),
    ]


def test_the_reference_report_is_the_first_track(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A red first track hands no report to S6, because the docs name the first."""

    outcomes = iter([(1, "clamped"), (0, "clamped")])

    def _fake_run(interpreter: str, xml_path: Path) -> tuple[int, str]:
        xml_path.write_text("<coverage/>", encoding="utf-8")
        return next(outcomes)

    monkeypatch.setattr(local_verify, "_run_suite", _fake_run)

    result, reference = local_verify._stage_suite(["/a/python", "/b/python"], tmp_path)

    assert result.status == local_verify.STATUS_FAILED
    assert reference is None


def test_pytest_args_are_rejected_under_all() -> None:
    """S5 has a fixed argument set, so a silent override must not be possible."""

    # The form matters: with a leading "--" argparse itself rejects the call,
    # and the test would then be green through the parser rather than through
    # the check it exists for.
    with pytest.raises(SystemExit) as raised:
        local_verify.main(["--all", "--skip-pytest", "--pytest-args", "-k", "nothing"])

    assert raised.value.code == 2


def test_skip_ruff_is_honoured_under_all(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A flag that is accepted must have an effect, or it lies about the run."""

    monkeypatch.setattr(
        local_verify,
        "_stage_static",
        lambda key, title, *_rest: local_verify.StageResult(
            key, title, local_verify.STATUS_OK, "stubbed"
        ),
    )
    monkeypatch.setattr(
        local_verify,
        "_stage_spelling",
        lambda _interp, _base: local_verify.StageResult(
            "S4", "spelling", local_verify.STATUS_OK, ""
        ),
    )

    local_verify.main(["--all", "--skip-pytest", "--skip-ruff"])

    output = capsys.readouterr().out
    assert "S1 format" in output
    assert "--skip-ruff was given" in output


def test_the_verdict_counts_the_stages_that_did_not_run() -> None:
    """A green verdict must say how much of the preflight never happened."""

    results = [
        local_verify.StageResult("S1", "format", local_verify.STATUS_OK, ""),
        local_verify.StageResult("S7", "security", local_verify.STATUS_SKIPPED, ""),
        local_verify.StageResult("S8", "manifest", local_verify.STATUS_SKIPPED, ""),
    ]

    report = local_verify._format_report(results)

    assert "2 of 3 stages did not run (S7, S8)" in report


def test_the_suite_command_duplicates_no_configured_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Two flags are pinned to exactly one source each, for two different reasons.

    ``--cov-fail-under``: the floor lives in ``[tool.coverage.report]`` and is
    clamped against CI there; a copy here would sit outside that clamp.

    ``-q``: it counts up. The suite command supplies exactly one, and
    ``addopts`` carries none, because a second one makes pytest drop its
    summary line, and the report this stage hands a reviewer would name
    neither the number of tests nor the number of failures. The verdict
    would stay correct (it reads the exit status) while the evidence quietly
    disappeared. Measured on 2026-09-10 and again on 2026-09-12: with one
    ``-q`` the summary line is printed, with two it is not.
    """

    recorded: list[Sequence[str]] = []
    monkeypatch.setattr(local_verify, "_changed_paths", lambda: [])
    monkeypatch.setattr(local_verify, "_digest_of", lambda _paths: {})
    monkeypatch.setattr(
        local_verify, "_run_command", lambda command: recorded.append(command) or 0
    )

    local_verify._run_suite("/a/python", tmp_path / "c.xml")

    assert not any("--cov-fail-under" in argument for argument in recorded[0])
    assert list(recorded[0]).count("-q") == 1
    assert "--quiet" not in recorded[0]

    # The premise of the single -q, pinned: if addopts ever grows one again, the
    # stage runs with -qq and loses its summary line, and this test should say
    # so rather than pass on.
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - py<3.11 path
        import tomli as tomllib  # type: ignore[no-redef]

    repo_root = Path(__file__).resolve().parent.parent
    pyproject_data = tomllib.loads(
        (repo_root / "pyproject.toml").read_text(encoding="utf-8")
    )
    addopts = (
        pyproject_data.get("tool", {})
        .get("pytest", {})
        .get("ini_options", {})
        .get("addopts", [])
    )
    assert "-q" not in addopts
    assert "--quiet" not in addopts


def test_the_stages_that_cannot_run_here_do_not_move_the_status(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """S7 and S8 are absent by design; a red run for them would be ignored."""

    monkeypatch.setattr(
        local_verify,
        "_static_stages",
        lambda _interp, _base, skip_format=False: [
            local_verify.StageResult("S1", "format", local_verify.STATUS_OK, "")
        ],
    )
    monkeypatch.setattr(
        local_verify,
        "_stage_suite",
        lambda _interps, _dir: (
            local_verify.StageResult("S5", "suite", local_verify.STATUS_OK, ""),
            None,
        ),
    )
    monkeypatch.setattr(
        local_verify,
        "_stage_patch_coverage",
        lambda _ref, _base: local_verify.StageResult(
            "S6", "patch coverage", local_verify.STATUS_OK, ""
        ),
    )
    monkeypatch.setattr(local_verify, "clean_pycache", lambda: (0, 0))

    status = local_verify.main(["--all"])

    assert status == 0
    assert local_verify.STATUS_SKIPPED in capsys.readouterr().out


def test_absent_stages_state_a_measured_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    """bandit and pip-audit are dev dependencies; "not installed" must be measured."""

    monkeypatch.setattr(
        local_verify, "_module_present", lambda _interp, name: name == "bandit"
    )
    monkeypatch.setattr(local_verify.shutil, "which", lambda _name: None)

    scans, manifest = local_verify._unavailable_stages("/a/python")

    assert scans.status == local_verify.STATUS_SKIPPED
    assert scans.expected_absent is True
    assert "present but not run" in scans.detail
    assert "bandit" in scans.detail
    assert "not on PATH" in manifest.detail


def test_a_stage_without_its_tool_did_not_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing tool is a stage that did not run, not a stage that failed."""

    monkeypatch.setattr(
        local_verify, "_tool_version", lambda _i, _m: local_verify.UNKNOWN_VERSION
    )
    ran: list[object] = []
    monkeypatch.setattr(
        local_verify, "_run_command", lambda command: ran.append(command) or 0
    )

    result = local_verify._stage_static(
        "S2", "lint", "/a/python", ["/a/python"], "ruff"
    )

    assert result.status == local_verify.STATUS_SKIPPED
    assert "not available" in result.detail
    assert ran == []


def test_the_type_stage_uses_the_prescribed_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AGENTS.md prescribes --install-types --non-interactive; a fresh env needs them."""

    recorded: list[Sequence[str]] = []

    def _fake_stage(key, title, interpreter, command, module):  # type: ignore[no-untyped-def]
        recorded.append(tuple(command))
        return local_verify.StageResult(key, title, local_verify.STATUS_OK, "")

    monkeypatch.setattr(local_verify, "_stage_static", _fake_stage)
    monkeypatch.setattr(
        local_verify,
        "_stage_spelling",
        lambda _i, _b: local_verify.StageResult(
            "S4", "spelling", local_verify.STATUS_OK, ""
        ),
    )

    local_verify._static_stages("/a/python", "origin/main")

    types_command = next(command for command in recorded if "mypy" in command)
    assert "--install-types" in types_command
    assert "--non-interactive" in types_command


def test_the_static_stages_use_the_first_named_track(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """make preflight has no poetry run, so sys.executable is not one of the tracks."""

    seen: list[str] = []
    monkeypatch.setattr(
        local_verify,
        "_static_stages",
        lambda interpreter, _base, skip_format=False: seen.append(interpreter) or [],
    )
    monkeypatch.setattr(local_verify, "_unavailable_stages", lambda _interp: [])

    local_verify.main(["--all", "--skip-pytest", "--python", "/track-a/bin/python"])

    assert seen == ["/track-a/bin/python"]


def test_an_empty_pytest_args_is_rejected_too() -> None:
    """`--all --pytest-args` without values is still an override that cannot work."""

    with pytest.raises(SystemExit):
        local_verify.main(["--all", "--skip-pytest", "--pytest-args"])


def test_the_pin_is_printed_in_the_tools_own_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A "v" on one side and not the other hides a mismatch the line promises to show.

    The pin is read from the module, so a version bump does not touch this test;
    the tool answers without the "v" that the hook rev carries.
    """

    assert local_verify.RUFF_PIN.startswith("v")
    version = local_verify.RUFF_PIN.removeprefix("v")
    monkeypatch.setattr(local_verify, "_tool_version", lambda _i, _m: f"ruff {version}")
    monkeypatch.setattr(local_verify, "_run_command", lambda _command: 0)

    result = local_verify._stage_static("S2", "lint", "/a/python", ["/a"], "ruff")

    assert f"ruff {version} (pin {version})" == result.detail
