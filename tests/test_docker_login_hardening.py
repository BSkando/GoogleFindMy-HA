# tests/test_docker_login_hardening.py
"""Regression guards for the docker-login helper hardening.

Most guards here are text guards (in the spirit of ``test_hacs_validation.py``)
that lock in security/usability fixes for the containerised login helper so they
cannot silently regress; the final guard
(``test_backgrounded_login_child_behaviourally_keeps_stdin_and_default_sigint``)
is a BEHAVIOURAL test that runs the real backgrounding construct from
``entrypoint.sh`` and asserts stdin/SIGINT at runtime, so the launch fix is proven
by behaviour rather than by matching script text. Each guard maps to a concrete
Codex review finding on PR #1208:

* Secrets file must stay owner-only ``0600`` and be handed to the host user via an
  ownership handoff, never relaxed to world-readable ``0644`` (findings A + C).
* The launchers must start the container with ``docker compose run`` (interactive,
  stdin attached) rather than ``docker compose up``, or the account-e-mail prompt
  blocks forever (finding B).
* noVNC must stay bound to loopback by default (earlier finding), so the
  fixed-password session is not exposed on the LAN during sign-in.
* A context-level ``.dockerignore`` must be an ALLOWLIST (ignore the whole
  build context, re-include only the two paths the Dockerfile copies) so that
  EVERY secret location -- ``docker-login/data`` AND the legacy
  ``Auth/secrets.json`` AND any future token/cache path -- stays out of the
  ``--build`` context, not just the one path a denylist happens to name. A bare
  ``!docker-login`` re-includes the WHOLE directory, so it is re-excluded again
  (``docker-login/*``) down to just the one re-included file
  (``!docker-login/entrypoint.sh``); a file dropped directly under
  ``docker-login/`` must NOT reach the context (Codex P2 on PR #1210).
* The QNAP "Container Station" README guidance must NOT present importing the
  compose file as an *application* (``docker compose up`` semantics, no stdin) as
  a first-login path: the ``input("Enter your Google account email:")`` prompt can
  never be answered without a terminal, so the login must go through the stdin-attaching
  ``docker compose run`` (SSH) path (Codex P2 on PR #1210).
* The ownership handoff must run from an ``EXIT`` trap, so a nonzero/interrupted
  ``main.py`` run still returns the produced secrets.json to the host user.
* The CLI must run as a BACKGROUND child that the entrypoint waits on, and the
  signal traps must FORWARD SIGTERM/SIGINT to it. A foreground child makes bash
  defer the trap until the child exits, so ``docker stop`` escalates to SIGKILL and
  the EXIT cleanup never runs (finding: forward termination signals to the child).
* Backgrounding that child must PRESERVE its terminal stdin (``<&0`` at the async
  boundary) and RESET SIGINT/SIGQUIT to default (``trap - INT QUIT`` before exec):
  a non-interactive script otherwise gives an async child ``/dev/null`` stdin (the
  ``input()`` login prompt hits EOF) and hard-ignores SIGINT (Python drops Ctrl-C),
  both regressions of the backgrounding fix above.
* Only the noVNC viewer (7900) may be published. The launchers always pass
  ``--service-ports``, so anything declared in ``docker-compose.yml`` is published
  on EVERY run; a second port entry made the container fail to start on a host
  where that port was already taken, which also broke the file handoff (``./data``)
  and the ``GFMY_CLEARTEXT=1`` output, neither of which uses a port. The token
  endpoint that once carried a second publish was removed with PR #1218, so the
  compose file declares exactly one port and no overlay exists.
* The login image installs ``setuptools`` alongside ``requirements.txt`` in its
  build step. undetected-chromedriver 3.5.5 (the current PyPI top) still imports
  the stdlib ``distutils`` that Python 3.12+ removed (PEP 632); the
  ``selenium/standalone-chrome:latest`` base image is on Python 3.14, so without
  ``setuptools`` re-providing ``distutils`` via its ``_distutils_hack`` the bare
  import fails (``ModuleNotFoundError: distutils``) and Chrome never starts.
  Dropping ``setuptools`` from the install step would restore that failure while
  every other guard here stays green (Codex P1 on PR #1215).
"""

from __future__ import annotations

import logging
import os
import pty
import re
import selectors
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

DOCKER_LOGIN = Path("custom_components/googlefindmy/docker-login")
# Build context root for the login image (docker-compose.yml `build.context: ..`).
BUILD_CONTEXT = DOCKER_LOGIN.parent
LOGIN_SERVICE = "googlefindmy-login"


def _read(name: str) -> str:
    """Return the text of a docker-login helper file."""

    return (DOCKER_LOGIN / name).read_text(encoding="utf-8")


def test_entrypoint_never_makes_secrets_world_readable() -> None:
    """The entrypoint must not relax secrets.json to a group/world-readable mode."""

    entrypoint = _read("entrypoint.sh")
    for forbidden in ("chmod 0644", "chmod 644", "chmod 0640", "chmod 0o644"):
        assert forbidden not in entrypoint, (
            f"entrypoint.sh must not widen secrets.json permissions ({forbidden!r}); "
            "keep owner-only 0600 and hand ownership to the host user instead."
        )


def test_entrypoint_hands_ownership_to_host_user() -> None:
    """The entrypoint must chown the produced secrets to the host UID and keep 0600."""

    entrypoint = _read("entrypoint.sh")
    assert "sudo chown" in entrypoint, (
        "entrypoint.sh must perform the ownership handoff"
    )
    assert "GFMY_HOST_UID" in entrypoint, "entrypoint.sh must honour the host UID"
    assert "chmod 0600" in entrypoint, (
        "entrypoint.sh must keep the produced secrets.json owner-only (0600)"
    )


@pytest.mark.parametrize("launcher", ["login.sh", "login.cmd"])
def test_launcher_uses_compose_run_not_up(launcher: str) -> None:
    """Launchers must use interactive ``compose run`` so stdin reaches Python."""

    text = _read(launcher)
    # `docker compose [-f ...] run`: the launchers now select their compose files
    # explicitly (the base file always, a user override when present), so the
    # invocation carries flags between `compose` and `run`. The subcommand must
    # still be `run` -- match it as a word, not the bare literal string.
    assert re.search(r"docker compose\s+(?:\S+\s+)*run\b", text), (
        f"{launcher} must start the container with `docker compose [-f ...] run` "
        "(stdin attached), not `docker compose up`."
    )
    assert "--rm" in text, (
        f"{launcher} must use --rm so repeated logins do not stack containers"
    )
    assert "docker compose up" not in text, (
        f"{launcher} must not use `docker compose up`: it does not forward stdin, "
        "so the interactive login prompt blocks forever."
    )


def test_launcher_sh_exports_host_ids_without_world_writable_chmod() -> None:
    """login.sh must pass the host UID/GID and must not open ./data to the world."""

    text = _read("login.sh")
    assert "GFMY_HOST_UID" in text and "GFMY_HOST_GID" in text, (
        "login.sh must export the host UID/GID for the ownership handoff"
    )
    assert "chmod 0777" not in text, (
        "login.sh must not make the token directory world-writable; the container "
        "takes ownership of ./data for the run instead."
    )


def test_compose_passes_host_ids_for_handoff() -> None:
    """docker-compose.yml must forward the host UID/GID to the container."""

    compose = _read("docker-compose.yml")
    assert "GFMY_HOST_UID" in compose and "GFMY_HOST_GID" in compose, (
        "docker-compose.yml must forward GFMY_HOST_UID/GFMY_HOST_GID for the handoff"
    )


def test_compose_binds_novnc_to_loopback_by_default() -> None:
    """noVNC must default to a loopback bind, not be exposed on the LAN."""

    compose = _read("docker-compose.yml")
    assert "127.0.0.1" in compose, (
        "docker-compose.yml must bind the fixed-password noVNC port to loopback by default"
    )


def _compose_service_ports(compose_file: str) -> list[str]:
    """Return the login service's declared port publishes from ``compose_file``.

    Parsed from YAML (not grepped) so the extensive *comments* around the ports
    block cannot make a guard pass or fail by accident: only real ``ports:``
    entries count.
    """

    parsed = yaml.safe_load(_read(compose_file))
    service = parsed["services"][LOGIN_SERVICE]
    return [str(entry) for entry in service.get("ports", [])]


@pytest.mark.parametrize("launcher", ["login.sh", "login.cmd"])
def test_launcher_selects_base_compose_explicitly(launcher: str) -> None:
    """Both launchers must name the base compose file when they select files.

    Once any ``-f`` is passed, Compose stops auto-discovering, so the base file has
    to be listed too; the launchers re-add a user override file for the same
    reason, and that override alone would be an incomplete project.
    """

    text = _read(launcher)
    assert "-f docker-compose.yml" in text, (
        f"{launcher} must pass `-f docker-compose.yml` explicitly, because passing "
        "any -f at all disables Compose's implicit file discovery."
    )


def test_login_sh_behaviourally_selects_compose_files(tmp_path: Path) -> None:
    """Behavioural proof: run the REAL login.sh against a stub ``docker``.

    Text guards can be fooled by a stale string elsewhere in the file; this test
    executes ``login.sh`` with a fake ``docker`` on ``PATH`` that only echoes its
    arguments, and asserts the ACTUAL command line. It therefore proves the
    property that matters: exactly one compose file is selected and no second one
    sneaks in, so a busy host port cannot block the file-handoff/clear-text
    tracks.
    """

    bash = shutil.which("bash")
    if bash is None:  # pragma: no cover - CI images all ship bash
        pytest.skip("bash is required to run the launcher")

    work = tmp_path / "docker-login"
    work.mkdir()
    for name in ("login.sh", "docker-compose.yml"):
        shutil.copy(DOCKER_LOGIN / name, work / name)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text('#!/usr/bin/env bash\nprintf "DOCKER_ARGS: %s\\n" "$*"\n', "utf-8")
    stub.chmod(0o755)

    env = {"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path)}

    proc = subprocess.run(
        [bash, str(work / "login.sh")],
        capture_output=True,
        timeout=60,
        check=False,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr.decode()
    argline = next(
        line
        for line in proc.stdout.decode().splitlines()
        if line.startswith("DOCKER_ARGS: ")
    )

    assert "-f docker-compose.yml" in argline, (
        f"login.sh must always select the base compose file; got {argline!r}"
    )
    assert "--service-ports" in argline and " run " in argline
    assert argline.count("-f ") == 1, (
        "login.sh must select exactly the base compose file: a second -f would "
        f"publish a port the removed token endpoint once needed. got {argline!r}"
    )
    assert "How should the finished credentials reach" not in proc.stderr.decode(), (
        "a non-interactive run must never print the handoff menu: CI, a pipe and "
        "`login.sh < /dev/null` have to behave exactly as they did before it existed."
    )


def _launcher_sandbox(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    """Copy the launcher next to its compose file and stub out ``docker``.

    The stub echoes the compose invocation, which is what makes the launcher's
    file selection and its interactive menus testable without a Docker daemon.
    """

    work = tmp_path / "docker-login"
    work.mkdir()
    for name in ("login.sh", "docker-compose.yml"):
        shutil.copy(DOCKER_LOGIN / name, work / name)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text(
        '#!/usr/bin/env bash\nprintf "DOCKER_ARGS: %s\\n" "$*"\n',
        "utf-8",
    )
    stub.chmod(0o755)
    return work, {"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path)}


def _run_launcher_on_tty(
    work: Path, env: dict[str, str], answers: str, timeout: float = 60.0
) -> tuple[int, str]:
    """Run ``login.sh`` with a real terminal on stdin and feed it ``answers``.

    A pty is the only way to exercise the interactive branches at all: both menus
    are gated on ``[ -t 0 ]``, so a pipe silently takes the non-interactive path
    and would make an "the menu did the right thing" assertion vacuously true.
    All answers are written up front (the shell buffers them) and stdout/stderr
    share the pty, so the returned text is what the operator would have seen.
    """

    bash = shutil.which("bash")
    assert bash is not None, "bash is required to run the launcher"

    master, slave = pty.openpty()
    try:
        proc = subprocess.Popen(  # noqa: S603 - fixed argv, test-local stub PATH
            [bash, str(work / "login.sh")],
            stdin=slave,
            stdout=slave,
            stderr=slave,
            env=env,
            close_fds=True,
        )
        os.close(slave)
        slave = -1
        os.write(master, answers.encode())

        chunks: list[bytes] = []
        selector = selectors.DefaultSelector()
        selector.register(master, selectors.EVENT_READ)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not selector.select(0.5):
                if proc.poll() is not None:
                    break
                continue
            try:
                data = os.read(master, 65536)
            except OSError:  # the child closed the pty: normal end of run
                break
            if not data:
                break
            chunks.append(data)
        selector.close()
        try:
            returncode = proc.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            proc.kill()
            raise
    finally:
        if slave != -1:
            os.close(slave)
        os.close(master)
    return returncode, b"".join(chunks).decode("utf-8", "replace")


def test_track_menu_defaults_to_the_file_handoff_on_a_bare_enter(
    tmp_path: Path,
) -> None:
    """Enter, Enter must keep the historical behaviour: the file handoff only.

    Track A is the only handoff that needs no terminal output and no manual copy,
    and it is the one that leaves ``data/secrets.json`` in place (track B prints
    the bundle and the container then deletes that file). Making A the default
    answer is what keeps the question from changing anything for the people who
    just press Enter -- and it is the property that silently breaks if the menu
    ever grows a "no default" or reorders its options.
    """

    work, env = _launcher_sandbox(tmp_path)
    # Two answers: the noVNC address menu, then the handoff menu.
    returncode, out = _run_launcher_on_tty(work, env, "\n\n")

    assert returncode == 0, out
    assert "How should the finished credentials reach" in out, (
        "an interactive run must ASK; that is the whole point of the change."
    )
    assert "GFMY_CLEARTEXT=1" not in out, (
        "a bare Enter must select track A, so the clear-text switch stays off"
    )


@pytest.mark.parametrize("track", ["a", "b"])
def test_track_flag_suppresses_the_menu(tmp_path: Path, track: str) -> None:
    """``--track`` is the scriptable spelling of the menu and must skip it."""

    work, env = _launcher_sandbox(tmp_path)
    bash = shutil.which("bash")
    assert bash is not None
    proc = subprocess.run(
        [bash, str(work / "login.sh"), "--track", track],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr
    assert "How should the finished credentials reach" not in proc.stderr
    argline = next(
        line for line in proc.stdout.splitlines() if line.startswith("DOCKER_ARGS: ")
    )
    assert argline.count("-f ") == 1, (
        f"--track {track} must not change the compose file selection; got {argline!r}"
    )


# The letter of the removed third track, built rather than spelled: the repo-wide
# removal sweep in tests/test_track_b_removal_guard.py searches for exactly that
# spelling, and a literal here would make this file a false positive.
_REMOVED_TRACK_LETTER = chr(ord("a") + 2)


def test_the_removed_third_track_is_refused_loudly(tmp_path: Path) -> None:
    """The third track was the token endpoint and must now fail, not remap.

    The endpoint was removed with PR #1218 and the former third track was
    renumbered to B. Quietly mapping the old letter onto the new B would send a
    script that pinned that spelling to a DIFFERENT handoff than it asked for,
    which is exactly the deprecation shim this removal was meant to avoid. Run for
    real against the stub docker, so a launcher that merely PRINTS a refusal while
    continuing cannot pass.
    """

    work, env = _launcher_sandbox(tmp_path)
    bash = shutil.which("bash")
    assert bash is not None
    proc = subprocess.run(
        [bash, str(work / "login.sh"), "--track", _REMOVED_TRACK_LETTER],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env=env,
    )
    assert proc.returncode == 2, (
        f"login.sh must exit 2 on the removed track letter; got {proc.returncode} "
        f"with {proc.stderr!r}"
    )
    assert "DOCKER_ARGS: " not in proc.stdout, (
        "the refusal must happen BEFORE compose starts, not alongside it"
    )
    assert "needs a or b" in proc.stderr, (
        "the error must name exactly the two remaining tracks"
    )
    cmd = _read("login.cmd")
    assert f'if /i "%~1"=="{_REMOVED_TRACK_LETTER}"' not in cmd, (
        "login.cmd must not accept the removed track letter either"
    )
    assert "--track needs a or b." in cmd, (
        "login.cmd must name exactly the two remaining tracks"
    )


def test_login_cmd_mirrors_the_track_menu_and_the_bind_guard() -> None:
    """``login.cmd`` cannot be executed here (no Windows), so pin it by text.

    Every property the bash tests above prove behaviourally has a counterpart
    here, because a Windows user gets the same two tracks -- a launcher that
    silently offered less would be the quietest way for this to go missing.
    """

    cmd = _read("login.cmd")
    assert "How should the finished credentials reach Home Assistant?" in cmd, (
        "login.cmd must offer the same handoff menu as login.sh."
    )
    assert 'set /p "TRACK_CHOICE=[login] Choice [Enter = A]: "' in cmd, (
        "the menu must default to track A on a bare Enter, like login.sh."
    )
    for guard in ("CLEARTEXT_ENV_SET", "TRACK_FROM_CLI"):
        assert f"if defined {guard} goto :track_done" in cmd, (
            f"a preset {guard} must skip the menu, mirroring the bash precedence."
        )
    assert "GFMY_CLEARTEXT" in cmd.split("call :trim_trailing_blanks")[0], (
        "GFMY_CLEARTEXT must be in the trailing-blank trim list: `set VAR=1 && "
        "login.cmd` stores the blank, and the container compares against a bare "
        '"1".'
    )


@pytest.mark.parametrize(
    ("name", "sink"),
    [
        ("login.sh", "./data/secrets.json"),
        ("login.cmd", r".\data\secrets.json"),
        ("README.md", "data/secrets.json"),
    ],
)
def test_track_b_is_described_as_replacing_the_file_handoff(
    name: str, sink: str
) -> None:
    """Track B deletes the file, so no user-facing text may call it additive.

    ``entrypoint.sh`` prints the bundle and then removes ``secrets.json``. Text
    that describes B as an addition ("also print", "this always happens", "never
    a replacement") sends users to Home Assistant's watched-file import for a
    file that is no longer on disk -- exactly the contradiction Codex flagged on
    PR #1218. The menus, the ``--track`` help and the README paragraphs must all
    say "instead", and each must name the deletion at least once, so a future
    edit cannot quietly restore the additive wording in one place while the
    cost table in the README still says the opposite.
    """

    text = _read(name)
    assert sink in text, f"{name} must still document the file handoff sink."

    lowered = text.lower()
    for additive in ("also print", "this always happens", "never a replacement"):
        assert additive not in lowered, (
            f"{name} still describes track B as additive ({additive!r}); it "
            "replaces the file handoff, because the file is deleted after it "
            "is printed."
        )
    assert "instead" in lowered, (
        f"{name} must present track B as printing the bundle INSTEAD of keeping "
        "the file."
    )
    assert "delete" in lowered, (
        f"{name} must name the deletion of the file, which is what makes track "
        "B a replacement rather than an addition."
    )


def test_readme_scopes_the_unchanged_non_interactive_claim_to_login_sh() -> None:
    """The "non-interactive run is unchanged" promise holds for bash only.

    ``login.sh`` gates the handoff menu on ``[ -t 0 ]``, so CI, a pipe and
    ``< /dev/null`` really do reach the old code path. Batch has no ``[ -t 0 ]``
    equivalent, so ``login.cmd`` always runs ``set /p``: at EOF that is a no-op
    (the A default, historical behaviour), but a redirect that still has CONTENT
    loses its first line to this prompt -- the very line the container's
    account-e-mail question needed, which can then hit EOF and abort the login,
    and a ``b``/``c`` line silently switches track. A README that promised the
    historical behaviour for "a non-interactive run" without naming that
    exception walked Windows automation straight into it (Codex on PR #1218).

    The guard is a COUPLING, not a spell-check: it holds the README to the code
    asymmetry that exists today, and fires if either side moves -- if
    ``login.cmd`` ever gains a real stdin check, this paragraph has to be
    rewritten rather than quietly left standing.
    """

    readme = _read("README.md")
    sh = _read("login.sh")
    cmd = _read("login.cmd")

    # 1. The asymmetry the README describes must still be the real one.
    assert '[ -t 0 ] && [ "${track_from_cli}" -eq 0 ]' in sh, (
        "login.sh must keep gating the handoff menu on a TTY; the README's "
        "'behaves exactly as it always did' promise rests on exactly this test."
    )
    prompt = 'set /p "TRACK_CHOICE=[login] Choice [Enter = A]: "'
    assert prompt in cmd, "login.cmd must still carry the track prompt."
    # Comments only: login.cmd EXPLAINS the missing `[ -t 0 ]` at length, so the
    # absence has to be measured on the executable lines, not on the file text.
    executable = "\n".join(
        line
        for line in cmd.splitlines()
        if not line.lstrip().lower().startswith("rem ")
    )
    assert "-t 0" not in executable, (
        "batch has no [ -t 0 ]: if login.cmd ever learns to detect a redirected "
        "stdin, the README paragraph below must be rewritten, not left standing."
    )

    # 2. The README must therefore scope the promise and name the exception.
    #    Whitespace-normalised: the wrapped line break must not decide the verdict.
    paragraph = " ".join(
        readme.split("asks which handoff you want", 1)[1].split("\n\n## ", 1)[0].split()
    )
    assert "`login.sh` gates the question on a real TTY" in paragraph, (
        "the unchanged-non-interactive promise must be attributed to login.sh, "
        "not to 'the launcher' as a whole."
    )
    for needle in ("`login.cmd` **cannot**", "consumes its first line"):
        assert needle in paragraph, (
            f"the README must state the Windows exception verbatim ({needle!r}): "
            "login.cmd asks unconditionally and eats a redirected first line."
        )

    # 3. And it must give the way out, which is the only thing a script can do.
    #    Deliberately NOT a bare "--track a" check: the paragraph opens with
    #    "Pass `--track a|b|c`", which would satisfy that substring even after the
    #    Windows instruction had been deleted (this guard caught exactly that).
    assert "Windows automation must therefore name the track explicitly" in paragraph, (
        "the README must tell Windows automation to name the track up front "
        "instead of answering the prompt through redirected stdin."
    )
    assert "`login.cmd --track a`" in paragraph, (
        "the way out must be spelled out as a runnable Windows command, not left "
        "to the reader to assemble from the bash-flavoured sentence above."
    )

    # 4. login.cmd already documents this next to the prompt; the two must agree.
    assert "CONSUMES its first line" in cmd, (
        "login.cmd's own comment is the second half of this pair; if it goes, "
        "the README claim loses its in-code anchor."
    )


def _dockerignore_rules() -> list[str]:
    """Return the effective (non-comment, non-blank) .dockerignore patterns, in order."""

    dockerignore = BUILD_CONTEXT / ".dockerignore"
    assert dockerignore.is_file(), (
        "A .dockerignore must exist at the build-context root "
        f"({BUILD_CONTEXT}) because the login image builds with `--build` and that "
        "context otherwise includes persisted OAuth/AAS credentials."
    )
    return [
        line.strip()
        for line in dockerignore.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def test_dockerignore_is_allowlist_excluding_every_secret_path() -> None:
    """.dockerignore must ignore the whole context and re-include only build inputs.

    A denylist that names only ``docker-login/data`` is whack-a-mole: the legacy
    ``Auth/secrets.json`` default of ``main.py::_resolve_secrets_path`` (and any
    future token/cache location) would still be uploaded with the ``--build``
    context. An allowlist -- ignore ``*``, then re-include only the paths the
    Dockerfile copies -- structurally keeps every secret out at once.
    """

    rules = _dockerignore_rules()

    # 1. The context is ignored wholesale by the very first effective rule.
    assert rules and rules[0] in {"*", "**"}, (
        ".dockerignore must start by ignoring the entire context (`*`), so that "
        "secrets in any location are excluded by default (allowlist model)."
    )

    # 2. The only `!`-re-includes may be the two Dockerfile COPY inputs plus the
    #    `docker-login` directory re-included SOLELY for traversal (Docker cannot
    #    descend into an ignored dir to reach entrypoint.sh). Never a secrets path.
    reincludes = {r[1:] for r in rules if r.startswith("!")}
    assert reincludes == {
        "requirements.txt",
        "docker-login",
        "docker-login/entrypoint.sh",
        "docker-login/start-novnc.sh",
    }, (
        "the only `!`-re-includes may be the Dockerfile's COPY inputs "
        "(requirements.txt, docker-login/entrypoint.sh, docker-login/start-novnc.sh) "
        f"plus `docker-login` for traversal; found {sorted(reincludes)}. Re-including "
        "anything else -- or the directory as a whole without re-excluding its "
        "contents -- risks shipping integration source or secrets into the image."
    )
    assert not any(
        r.startswith("!") and ("auth" in r.lower() or "secret" in r.lower())
        for r in rules
    ), ".dockerignore must never re-include an Auth/ or secrets path."

    # 3. The contents of the re-included docker-login/ are re-excluded WHOLESALE
    #    (`docker-login/*`) so a bare `!docker-login` cannot leak any other file
    #    placed under it (an exported secrets.json, a diagnostic log, a future
    #    addition) -- not merely docker-login/data. Order-sensitive (last match
    #    wins): `!docker-login` -> `docker-login/*` -> `!docker-login/entrypoint.sh`.
    assert "docker-login/*" in rules, (
        ".dockerignore must re-exclude the whole docker-login/ subtree "
        "(`docker-login/*`) after re-including the directory, so only the explicitly "
        "re-included entrypoint.sh survives -- not docker-login/data, an exported "
        "secrets.json, or any future file dropped under docker-login/."
    )
    i_dir = rules.index("!docker-login")
    i_star = rules.index("docker-login/*")
    i_entry = rules.index("!docker-login/entrypoint.sh")
    assert i_dir < i_star < i_entry, (
        "order must be `!docker-login` -> `docker-login/*` -> "
        "`!docker-login/entrypoint.sh` (last match wins), else the re-exclusion never "
        f"takes effect; got indices dir={i_dir}, star={i_star}, entry={i_entry}."
    )


def test_entrypoint_runs_handoff_from_exit_trap() -> None:
    """The ownership handoff must run via an EXIT trap, not only on the success path.

    Under ``set -e`` a nonzero ``main.py`` exit aborts the script; a linear handoff
    placed after ``main.py`` would be skipped, leaving the host unable to read or
    delete the container-owned credential file. Routing it through ``trap ... EXIT``
    guarantees it runs on failure and interruption too.
    """

    entrypoint = _read("entrypoint.sh")
    assert "trap cleanup EXIT" in entrypoint, (
        "entrypoint.sh must register the cleanup/handoff on EXIT so it also runs "
        "when main.py fails or is interrupted (set -e)."
    )
    # The handoff logic (host chown) must live inside the trapped cleanup function,
    # i.e. before it is registered on EXIT, not as trailing linear code.
    cleanup_def = entrypoint.find("function cleanup")
    trap_exit = entrypoint.find("trap cleanup EXIT")
    handoff = entrypoint.find("GFMY_HOST_UID", cleanup_def)
    assert 0 <= cleanup_def < handoff < trap_exit, (
        "the ownership handoff (GFMY_HOST_UID chown) must sit inside the cleanup() "
        "function that is trapped on EXIT."
    )


def test_entrypoint_forwards_signals_to_the_tracked_child() -> None:
    """SIGTERM/SIGINT must be forwarded to the CLI, which must run in the background.

    If ``python3 main.py`` runs in the FOREGROUND, bash defers the TERM/INT trap
    until the child exits on its own; a ``docker stop`` therefore never reaches the
    login process, Docker escalates to SIGKILL after its grace period, and the EXIT
    cleanup (ownership handoff + supervisor shutdown) is skipped -- leaving ``/data``
    owned by the container UID. The child must be backgrounded and waited on, and the
    signal traps must relay the signal to it, not merely ``exit``.
    """

    entrypoint = _read("entrypoint.sh")

    # 1) The CLI is started in the background and its PID captured. It is wrapped in a
    #    `( ... ) <&0 &` subshell that execs python (see the stdin/SIGINT guard below),
    #    so match the backgrounded exec form rather than a bare `python3 ... &`.
    assert "exec python3 main.py ${GFMY_ARGS:-} ) <&0 &" in entrypoint, (
        "main.py must be started in the BACKGROUND, otherwise bash defers the signal "
        "traps until it exits and docker stop hits SIGKILL."
    )
    assert "CHILD_PID=$!" in entrypoint, (
        "the entrypoint must capture the child PID (CHILD_PID=$!) so signals can "
        "be forwarded to it and its exit status waited on."
    )

    # 2) The entrypoint waits on that specific child (not a bare `wait`).
    assert 'wait "${CHILD_PID}"' in entrypoint, (
        "the entrypoint must wait on the tracked child PID so its real exit status "
        "propagates to the EXIT cleanup."
    )

    # 3) The TERM/INT traps forward to the child instead of exiting immediately.
    assert "trap 'on_signal TERM 143' TERM" in entrypoint
    assert "trap 'on_signal INT 130' INT" in entrypoint
    assert "trap 'exit 143' TERM" not in entrypoint, (
        "a bare `exit` on SIGTERM does not reach the foreground child; the trap must "
        "forward the signal to CHILD_PID (on_signal)."
    )
    assert "trap 'exit 130' INT" not in entrypoint

    # 4) on_signal actually relays the signal to the running child.
    assert 'kill -s "${sig}" "${CHILD_PID}"' in entrypoint, (
        "on_signal must relay the received signal to the tracked child via "
        "kill -s <sig> CHILD_PID."
    )


def test_entrypoint_preserves_stdin_and_sigint_for_background_child() -> None:
    """Backgrounding the CLI must not break the first-run login prompt or Ctrl-C.

    A non-interactive bash script with job control off applies two disruptive
    defaults to an async (``&``) child, both proven empirically:

    * its stdin is pointed at ``/dev/null`` unless an explicit redirection overrides
      it -- so ``main.py``'s ``input("Enter your Google account email:")`` prompt would
      hit EOF and abort the login (Codex: "Keep stdin attached when backgrounding the CLI");
    * SIGINT/SIGQUIT are hard-ignored (SIG_IGN), which Python inherits and then keeps
      instead of installing its ``KeyboardInterrupt`` handler -- so a relayed SIGINT
      is dropped and the re-wait loop hangs (Codex: "Restore SIGINT handling in the
      background child").

    The fix wraps the child in ``( trap - INT QUIT; exec python3 ... ) <&0 &``:
    ``<&0`` at the ASYNC BOUNDARY (not inside the subshell, where fd 0 is already
    ``/dev/null``) restores the terminal stdin, and ``trap - INT QUIT`` before ``exec``
    restores the default signal disposition so Python re-installs its handlers.
    """

    entrypoint = _read("entrypoint.sh")

    # stdin: the redirect must sit at the async boundary `) <&0 &`, not inside `( ... )`.
    assert "exec python3 main.py ${GFMY_ARGS:-} ) <&0 &" in entrypoint, (
        "the backgrounded CLI must restore terminal stdin via `<&0` at the async "
        "boundary, or the interactive `input()` login prompt reads EOF and aborts."
    )
    assert "<&0 ) &" not in entrypoint, (
        "the `<&0` redirect must be OUTSIDE the subshell (at the async boundary); "
        "inside the subshell fd 0 is already /dev/null, so it would be a no-op."
    )

    # SIGINT/SIGQUIT: reset to default inside the wrapper before exec.
    assert "trap - INT QUIT" in entrypoint, (
        "the wrapper must `trap - INT QUIT` before exec so bash's async SIG_IGN is "
        "cleared and Python installs its normal KeyboardInterrupt handler."
    )
    # The reset must precede the exec so the exec'd process inherits the default.
    reset_idx = entrypoint.index("trap - INT QUIT")
    exec_idx = entrypoint.index("exec python3 main.py")
    assert reset_idx < exec_idx, (
        "`trap - INT QUIT` must run BEFORE `exec python3` so the CLI child inherits "
        "the default SIGINT disposition."
    )


def _extract_backgrounding_line(entrypoint: str) -> str:
    """Return the real ``( ... exec python3 main.py ... ) <&0 &`` launch line.

    The behavioural test below runs the *actual* backgrounding construct taken
    verbatim from ``entrypoint.sh`` (only the executed program is swapped for a
    stub), so it fails if the file ever loses ``<&0`` or the ``trap - INT QUIT``
    reset -- it cannot pass on a regressed script the way a text guard could be
    fooled by matching a stale string elsewhere.
    """

    for line in entrypoint.splitlines():
        if "exec python3 main.py" in line and "<&0 &" in line:
            return line.strip()
    raise AssertionError(
        "could not locate the backgrounded `( ... exec python3 main.py ... ) <&0 &` "
        "launch line in entrypoint.sh"
    )


def _skip_unless_sigint_is_measurable() -> None:
    """Skip when this pytest process itself ignores ``SIGINT``.

    A process started as a background job of a shell without job control
    (``bash -c '... &'``, ``( nohup ... & )``) inherits ``SIG_IGN`` for ``SIGINT``,
    and POSIX lets neither ``trap`` nor ``trap - INT`` in a non-interactive child
    shell undo a disposition that was already ignored on entry. The test below
    would then report ``SIGINT_OK=False`` whatever ``entrypoint.sh`` does: a fact
    about how the runner was started, not about the launcher. Reporting it as a
    skip with the cause keeps that measurement failure from reading like a
    product defect (it did, once, in a preflight run started as a detached job).
    CI runners start pytest with the default disposition, so this never skips
    there.
    """

    if signal.getsignal(signal.SIGINT) is signal.SIG_IGN:
        pytest.skip(
            "this pytest process ignores SIGINT (started as a background job "
            "without job control?); the launcher's SIGINT handling cannot be "
            "measured from a runner that cannot receive the signal itself"
        )


def test_sigint_precondition_skips_when_the_runner_ignores_sigint() -> None:
    """The guard skips (does not fail) when the runner inherited ``SIG_IGN``."""

    previous = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        with pytest.raises(pytest.skip.Exception, match="ignores SIGINT"):
            _skip_unless_sigint_is_measurable()
    finally:
        signal.signal(signal.SIGINT, previous)


def test_sigint_precondition_is_silent_with_the_default_handler() -> None:
    """With the default handler installed the guard returns without skipping."""

    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        _skip_unless_sigint_is_measurable()
    finally:
        signal.signal(signal.SIGINT, previous)


def test_backgrounded_login_child_behaviourally_keeps_stdin_and_default_sigint(
    tmp_path: Path,
) -> None:
    """Behavioural proof (not text matching) that the launch construct works.

    Codex asked to "cover the behavior rather than only matching script text".
    This test takes the REAL backgrounding line out of ``entrypoint.sh``, swaps
    only the executed program for a Python stub, runs it under a non-interactive
    ``bash`` script with job control off (the container condition) and asserts at
    runtime:

    * the child still reads the entrypoint's terminal stdin (``<&0`` works) -- the
      first-run account-e-mail prompt does not hit EOF; and
    * the child ends up with Python's DEFAULT ``SIGINT`` handler installed rather
      than the inherited ``SIG_IGN`` -- so a relayed Ctrl-C is not dropped.

    A negative control (same construct with ``<&0`` removed) must fail the stdin
    read, proving the assertion actually discriminates on the redirect rather
    than passing unconditionally.

    Reproduction limit (stated honestly): bash only forces ``SIG_IGN`` on an async
    child when it is attached to a controlling terminal, which a CI runner without
    a TTY does not provide, so this test verifies the POSITIVE end-state invariant
    (child has a working handler) which holds on every platform; the ordering that
    guarantees it under a TTY is locked by the text guard above.
    """

    import subprocess
    import sys

    entrypoint = _read("entrypoint.sh")
    launch = _extract_backgrounding_line(entrypoint)

    stub = tmp_path / "stub_child.py"
    stub.write_text(
        "import signal, sys\n"
        "try:\n"
        "    line = sys.stdin.readline().rstrip('\\n')\n"
        "except EOFError:\n"
        "    line = ''\n"
        "disp = signal.getsignal(signal.SIGINT)\n"
        "stdin_ok = line == 'ENTER-FROM-TERMINAL'\n"
        "sigint_ok = disp is not signal.SIG_IGN\n"
        "print('STDIN_OK=%s SIGINT_OK=%s' % (stdin_ok, sigint_ok))\n",
        encoding="utf-8",
    )

    # Rebuild the launch line with our stub in place of `main.py ${GFMY_ARGS:-}`,
    # keeping the surrounding `( trap - INT QUIT; exec python3 ... ) <&0 &` verbatim.
    stubbed = launch.replace("main.py ${GFMY_ARGS:-}", f"{stub.name!s}").replace(
        "python3", sys.executable
    )

    def _run(launch_line: str) -> str:
        script = (
            "#!/usr/bin/env bash\n"
            "set -e\n"
            "trap 'on_signal INT 130' INT\n"
            "function on_signal { :; }\n"
            f"cd {tmp_path!s}\n"
            f"{launch_line}\n"
            "CHILD_PID=$!\n"
            'wait "${CHILD_PID}"\n'
        )
        proc = subprocess.run(
            ["bash", "-c", script],
            input=b"ENTER-FROM-TERMINAL\n",
            capture_output=True,
            timeout=30,
            check=False,
        )
        return proc.stdout.decode()

    out = _run(stubbed)
    assert "STDIN_OK=True" in out, (
        f"backgrounded child did not receive terminal stdin via `<&0`; got: {out!r}"
    )

    # Negative control: without the `<&0` async-boundary redirect the child's stdin
    # is /dev/null, so the read fails -- proving the assertion discriminates.
    control = stubbed.replace(") <&0 &", ") &")
    control_out = _run(control)
    assert "STDIN_OK=False" in control_out, (
        "control without `<&0` unexpectedly read stdin; the behavioural stdin "
        f"assertion is not discriminating. got: {control_out!r}"
    )

    # The stdin proofs above do not depend on signal dispositions, so they stay
    # measurable on every runner; only the SIGINT half needs a runner that can
    # receive SIGINT itself, which is why the guard sits here and not at the top.
    _skip_unless_sigint_is_measurable()
    assert "SIGINT_OK=True" in out, (
        f"backgrounded child kept the inherited SIG_IGN for SIGINT; got: {out!r}"
    )


def _code_lines(script: str) -> list[str]:
    """Return only the non-comment, non-blank lines of a shell script.

    Every guard below that counts occurrences or matches a construct runs on this,
    so an explanatory comment can neither satisfy a guard (false negative: the
    construct is only *described*, not present) nor break one (false positive: a
    comment that merely mentions ``CHILD_PID=$!`` inflating a count).
    """

    return [
        ln
        for ln in script.splitlines()
        if ln.strip() and not ln.lstrip().startswith("#")
    ]


def _extract_shell_function(entrypoint: str, name: str) -> str:
    """Return the verbatim ``function <name> { ... }`` block from ``entrypoint.sh``.

    The behavioural test below runs the REAL signal-handling helpers rather than a
    hand-written imitation, so it regresses with the script instead of drifting away
    from it. Relies on the file's own formatting: the body is indented and the block
    is closed by a ``}`` in column 0.
    """

    lines = entrypoint.splitlines()
    for i, line in enumerate(lines):
        if line.startswith(f"function {name} {{"):
            for j in range(i + 1, len(lines)):
                if lines[j] == "}":
                    return "\n".join(lines[i : j + 1])
            break
    raise AssertionError(f"could not locate `function {name}` in entrypoint.sh")


def _extract_cli_launch(entrypoint: str) -> str:
    """Return the real backgrounded ``main.py`` launch line.

    Feeding this verbatim into the behavioural test below is what makes it a
    WIRING test and not merely a construct test: if the CLI is ever moved back
    into the foreground, this raises instead of silently exercising a hand-written
    backgrounding line that no longer reflects the script.
    """

    for line in _code_lines(entrypoint):
        stripped = line.strip()
        if "exec python3" in stripped and "main.py" in stripped:
            if not stripped.endswith("&") or stripped.endswith("&&"):
                break
            return stripped
    raise AssertionError(
        "could not locate a BACKGROUNDED `( ... exec python3 main.py ... ) &` launch "
        "line in entrypoint.sh (a foreground invocation defers the signal traps for "
        "as long as the child runs)"
    )


def test_every_tracked_wait_is_followed_by_the_shutdown_check() -> None:
    """EVERY tracked wait must be guarded, not just the one a report named.

    A tracked child dies on the relayed signal and its ``wait`` returns NORMALLY, so
    the status alone cannot distinguish "finished" from "shutting down" -- ``main.py``
    even catches ``KeyboardInterrupt`` and returns 0. An unguarded wait therefore
    reads as a successful login and the run continues into the handoff track,
    printing the credential bundle into the container log on a plain `docker stop`.
    """

    code = _code_lines(_read("entrypoint.sh"))

    waits = [
        i
        for i, ln in enumerate(code)
        if ln.lstrip().startswith("wait_for_tracked_child")
    ]
    # At least one, so a failed extraction cannot make the pairing check below
    # vacuously true. The count itself is deliberately not pinned: it was two while
    # the token endpoint had its own tracked child, and pinning it would turn every
    # future tracked child into an edit here instead of a check.
    assert waits, "expected at least one tracked wait in entrypoint.sh, found none"
    for i in waits:
        following = code[i + 1].lstrip()
        assert following.startswith("exit_if_terminating "), (
            "every wait_for_tracked_child must be followed immediately by "
            f"exit_if_terminating; the wait at {code[i].strip()!r} is followed by "
            f"{following!r}."
        )
    assert sum(ln.startswith("function exit_if_terminating {") for ln in code) == 1, (
        "the shutdown check must exist exactly once, as a shared helper."
    )


def test_wait_helper_clears_the_slot_and_ignores_an_empty_one() -> None:
    """The tracked slot must be reset, and an empty slot must not read as failure.

    Without the reset, ``on_signal`` keeps relaying to a PID number that the kernel
    may since have recycled to an unrelated process. And ``wait ""`` returns 1, which
    the ``-eq 0`` handoff gates would silently read as "the login failed".
    """

    helper = _extract_shell_function(_read("entrypoint.sh"), "wait_for_tracked_child")

    assert 'CHILD_PID=""' in helper, (
        "wait_for_tracked_child must clear the tracked slot before returning, so "
        "on_signal cannot signal a recycled PID in the gap between children."
    )
    assert '[ -n "${CHILD_PID}" ] || return 0' in helper, (
        "an empty slot must return success, not the exit status of a failed `wait`."
    )


READY_TIMEOUT_SECONDS = 10.0


def _wait_until_ready(ready: Path) -> None:
    """Block until the stub child reports that its signal handler is installed.

    The two behavioural shutdown tests below relay SIGTERM to a Python child
    that must already own a handler when the signal lands; otherwise the
    default action kills it and the test measures interpreter start-up time,
    not the helper under test. A fixed ``sleep(0.5)`` was that measurement:
    it passed on an idle machine and failed under load during the preflight
    runs of PR #1274. The child writes ``ready`` right after
    ``signal.signal``, so the wait ends the moment the handler exists and
    never earlier; the ceiling only turns a hung child into a failure.
    """

    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    while not ready.exists():
        assert time.monotonic() < deadline, (
            f"the stub child never reported readiness via {ready} within "
            f"{READY_TIMEOUT_SECONDS:g}s"
        )
        time.sleep(0.02)


def _end(proc: subprocess.Popen[bytes]) -> None:
    """Bring the outer bash down, relaying TERM first so its child can follow.

    On the normal path the process has already exited and only the kill is a
    no-op. On the failure path of ``_wait_until_ready`` a bare ``kill`` would
    take the bash with SIGKILL, which runs no trap, and the stub child would
    sleep on as an orphan for its full 30 s; a TERM first lets the trap relay.
    """

    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    proc.kill()
    proc.wait(timeout=15)


def test_wait_helper_behaviourally_survives_a_child_that_outlives_one_signal(
    tmp_path: Path,
) -> None:
    """The re-wait loop -- the whole reason the helper exists -- must be exercised.

    ``wait`` returns >128 as soon as a forwarded signal interrupts it, even though the
    child is still shutting down. A child that handles the signal and exits with its
    own status a moment later is the case the loop is for; both other behavioural
    tests use a child that dies instantly and never enter it. Here the child traps
    TERM, sleeps, and exits 5 -- so a missing loop would surface as 143 instead.

    The stub is Python, matching the real children: a bash stub would have to hold
    its own ``sleep`` in the foreground and would therefore defer its own trap,
    reproducing the very defect under test instead of the shutdown being modelled.
    """

    entrypoint = _read("entrypoint.sh")
    on_signal = _extract_shell_function(entrypoint, "on_signal")
    wait_helper = _extract_shell_function(entrypoint, "wait_for_tracked_child")
    rc_file = tmp_path / "rc"
    ready = tmp_path / "ready"

    stub = tmp_path / "slow_shutdown_child.py"
    stub.write_text(
        "import pathlib, signal, sys, time\n"
        "def _handler(signum, frame):\n"
        "    time.sleep(1)  # graceful shutdown still in progress\n"
        "    sys.exit(5)\n"
        "signal.signal(signal.SIGTERM, _handler)\n"
        f"pathlib.Path({str(ready)!r}).touch()\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )

    script = (
        "#!/usr/bin/env bash\n"
        "set -e\n"
        'CHILD_PID=""\n'
        '_terminating=""\n'
        f"{on_signal}\n"
        f"{wait_helper}\n"
        "trap 'on_signal TERM 143' TERM\n"
        f"( trap - INT QUIT; exec {sys.executable} {stub!s} ) &\n"
        "CHILD_PID=$!\n"
        "_rc=0\n"
        "wait_for_tracked_child || _rc=$?\n"
        f'echo "${{_rc}}" > "{rc_file!s}"\n'
    )
    proc = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
        ["bash", "-c", script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    try:
        _wait_until_ready(ready)
        proc.terminate()
        proc.wait(timeout=15)
    finally:
        _end(proc)

    assert rc_file.exists(), "the helper never returned after the forwarded signal"
    assert rc_file.read_text().strip() == "5", (
        "the helper returned the interrupted-wait status instead of re-waiting for "
        f"the child's real exit code; got {rc_file.read_text().strip()!r}."
    )


def test_terminating_marker_catches_a_child_that_exits_cleanly_on_the_signal(
    tmp_path: Path,
) -> None:
    """The ``_terminating`` half of the shutdown check must not be dead weight.

    The status check (``_srv_rc > 128``) alone covers today's server, which returns
    130 on ``KeyboardInterrupt``. It does NOT cover a child that catches the signal
    and shuts down cleanly with 0 -- an entirely ordinary way to write a server, and
    one change away. Then only the marker set in ``on_signal`` still says "we are
    shutting down", and without it execution would fall through into the clear-text
    track on a plain ``docker stop``. Both halves are kept for that reason; this test
    pins the half the status check cannot reach.
    """

    entrypoint = _read("entrypoint.sh")
    on_signal = _extract_shell_function(entrypoint, "on_signal")
    wait_helper = _extract_shell_function(entrypoint, "wait_for_tracked_child")
    guard = (
        f"{_extract_shell_function(entrypoint, 'exit_if_terminating')}\n"
        'exit_if_terminating "${_srv_rc}"'
    )
    fallthrough = tmp_path / "fallthrough"
    ready = tmp_path / "ready"

    stub = tmp_path / "clean_exit_child.py"
    stub.write_text(
        "import pathlib, signal, sys, time\n"
        "signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))\n"
        f"pathlib.Path({str(ready)!r}).touch()\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )

    script = (
        "#!/usr/bin/env bash\n"
        "set -e\n"
        'CHILD_PID=""\n'
        '_terminating=""\n'
        f"{on_signal}\n"
        f"{wait_helper}\n"
        "trap 'on_signal TERM 143' TERM\n"
        f"( trap - INT QUIT; exec {sys.executable} {stub!s} ) &\n"
        "CHILD_PID=$!\n"
        "_srv_rc=0\n"
        "wait_for_tracked_child || _srv_rc=$?\n"
        f"{guard}\n"
        f'echo reached > "{fallthrough!s}"\n'
    )
    proc = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
        ["bash", "-c", script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    try:
        _wait_until_ready(ready)
        proc.terminate()
        proc.wait(timeout=15)
    finally:
        _end(proc)

    assert not fallthrough.exists(), (
        "a child that exited 0 on the relayed signal was treated as a normal "
        "completion, so execution continued into the post-server handoff tracks."
    )


def test_tracked_child_behaviourally_lets_cleanup_run_on_sigterm(
    tmp_path: Path,
) -> None:
    """Behavioural proof that a signal reaches cleanup while a long child is waiting.

    Runs the REAL ``on_signal`` / ``wait_for_tracked_child`` helpers AND the real
    CLI launch line, all taken verbatim out of ``entrypoint.sh``, around a
    long-sleeping stub child; sends SIGTERM to the outer bash and asserts the
    EXIT-trap cleanup runs PROMPTLY -- i.e. well within a Docker stop grace period
    rather than only after the child's own timeout. Taking the launch line from the
    file (rather than writing one here) is what also makes this a wiring test: moving
    the child back into the foreground fails extraction instead of quietly passing.

    The negative control is the construct Codex flagged: the very same script with the
    child in the FOREGROUND. There bash defers the trap until the child returns, so no
    cleanup marker appears in time -- which is what makes the positive assertion
    discriminating rather than vacuously true.
    """

    entrypoint = _read("entrypoint.sh")
    on_signal = _extract_shell_function(entrypoint, "on_signal")
    wait_helper = _extract_shell_function(entrypoint, "wait_for_tracked_child")

    # The child outlives the observation window by far, standing in for a token
    # server that is still waiting for an ack (up to its 300 s TTL). Only the
    # executed program is swapped; the surrounding construct stays verbatim.
    child_sleep = 30
    grace = 5.0
    real_launch = _extract_cli_launch(entrypoint)
    stub_launch = re.sub(
        r"exec python3 main\.py[^)]*", f"exec sleep {child_sleep} ", real_launch
    )
    assert "exec sleep" in stub_launch, (
        f"could not substitute the stub into the real launch line: {real_launch!r}"
    )

    guard = (
        f"{_extract_shell_function(entrypoint, 'exit_if_terminating')}\n"
        'exit_if_terminating "${_rc}"'
    )

    def _run(tracked: bool, *, with_guard: bool = True) -> bool:
        tag = f"{'tracked' if tracked else 'foreground'}_{int(with_guard)}"
        marker = tmp_path / f"cleanup_{tag}"
        # Stands in for the clear-text track that follows the server wait: it must
        # NOT be reached when the run is ending because of a signal.
        fallthrough = tmp_path / f"fallthrough_{tag}"
        launch = (
            f"{stub_launch}\n"
            "CHILD_PID=$!\n"
            "_rc=0\n"
            "wait_for_tracked_child || _rc=$?\n"
            f"{guard if with_guard else ''}\n"
            if tracked
            else f"sleep {child_sleep} || true\n"
        )
        script = (
            "#!/usr/bin/env bash\n"
            "set -e\n"
            "_cleaned=0\n"
            'CHILD_PID=""\n'
            '_terminating=""\n'
            "function cleanup {\n"
            '  [ "${_cleaned}" = 1 ] && return\n'
            "  _cleaned=1\n"
            f'  echo ran > "{marker!s}"\n'
            "}\n"
            f"{on_signal}\n"
            f"{wait_helper}\n"
            "trap cleanup EXIT\n"
            "trap 'on_signal TERM 143' TERM\n"
            f"{launch}"
            f'echo reached > "{fallthrough!s}"\n'
        )
        proc = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
            ["bash", "-c", script],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(0.5)  # let the child start and the wait settle
            proc.terminate()
            try:
                proc.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                pass
            return marker.exists(), fallthrough.exists()
        finally:
            proc.kill()
            proc.wait(timeout=grace)

    cleaned, fell_through = _run(tracked=True)
    assert cleaned, (
        "the EXIT cleanup did not run within the grace window while a long-lived "
        "tracked child was being waited on; a `docker stop` would escalate to "
        "SIGKILL and skip the /data ownership handoff."
    )
    # Backgrounding alone is not enough: the tracked child dies on the relayed
    # signal and its wait() returns NORMALLY, so without the terminating check the
    # script would continue into the clear-text track and dump the credential
    # bundle into the container log on a plain `docker stop`.
    assert not fell_through, (
        "execution continued into the post-server handoff tracks after a relayed "
        "SIGTERM; a `docker stop` would print the credential bundle to the "
        "container log and report success."
    )

    control_cleaned, _ = _run(tracked=False)
    assert not control_cleaned, (
        "control with a FOREGROUND child unexpectedly ran cleanup in time, so this "
        "test does not actually discriminate between the two constructs."
    )

    # Second control: the same tracked construct WITHOUT the terminating check does
    # fall through -- proving the assertion above discriminates on that guard and is
    # not merely satisfied by the backgrounding.
    _, unguarded_fell_through = _run(tracked=True, with_guard=False)
    assert unguarded_fell_through, (
        "control without the terminating check unexpectedly stopped anyway, so the "
        "fall-through assertion does not discriminate on that guard."
    )


def _docker_context_excluded(rules: list[str], path: str) -> bool:
    """Return True if ``path`` is EXCLUDED from the Docker build context by ``rules``.

    Faithfully models moby/patternmatcher (Docker's ``.dockerignore`` engine): the
    patterns are evaluated in order and the LAST pattern that matches the path -- or
    any of its parent directories -- decides; a plain pattern excludes, a
    ``!``-prefixed pattern re-includes. ``*`` matches a single path segment (it does
    not cross ``/``), per Go ``filepath.Match``. This "last-match-wins + parent-dir
    descent" behaviour is exactly why a bare ``!docker-login`` re-includes the whole
    subtree and why ``docker-login/*`` + ``!docker-login/entrypoint.sh`` is needed to
    let through only the one file. (Verified to agree with the ``pathspec`` reference
    matcher on the representative paths below; re-implemented inline so the guard adds
    no test-only dependency to the Poetry env.)
    """

    import fnmatch
    from pathlib import PurePosixPath

    posix = PurePosixPath(path)
    candidates = [str(posix)] + [str(par) for par in posix.parents if str(par) != "."]

    def _matches(pattern: str, candidate: str) -> bool:
        pat_parts = pattern.rstrip("/").split("/")
        cand_parts = candidate.split("/")
        if len(pat_parts) != len(cand_parts):
            return False
        return all(
            fnmatch.fnmatchcase(seg, pat)
            for pat, seg in zip(pat_parts, cand_parts, strict=True)
        )

    excluded = False
    for rule in rules:
        negated = rule.startswith("!")
        pattern = rule[1:] if negated else rule
        if any(_matches(pattern, cand) for cand in candidates):
            excluded = not negated
    return excluded


def test_dockerignore_behaviourally_excludes_files_dropped_under_docker_login() -> None:
    """Behavioural proof that the allowlist keeps stray secrets out of the context.

    Codex flagged that a bare ``!docker-login`` re-includes the ENTIRE directory, so a
    credential export / diagnostic log / any file placed under ``docker-login/`` other
    than ``data/`` (the only path the old denylist named) would be uploaded to the
    Docker daemon, contradicting the stated allowlist guarantee. Rather than only
    matching pattern text, this test EVALUATES the real ``.dockerignore`` with a
    faithful model of Docker's matcher and asserts the include/exclude DECISION for
    representative paths, with a discriminating control on the old rules that proves
    the leak the fix closes.
    """

    rules = _dockerignore_rules()

    # The two Dockerfile COPY inputs must stay IN the context, or the build breaks.
    assert not _docker_context_excluded(rules, "requirements.txt")
    assert not _docker_context_excluded(rules, "docker-login/entrypoint.sh")

    # Every secret/stray location is OUT -- including a file dropped directly under
    # docker-login/ (the exact hole Codex reported), not just docker-login/data.
    for secret in (
        "docker-login/secrets.json",  # exported straight into docker-login/
        "docker-login/diagnostic.log",  # a stray diagnostic artifact
        "docker-login/data/secrets.json",  # the container's writable volume
        "Auth/secrets.json",  # legacy _resolve_secrets_path default
        "token_cache.json",  # any future top-level token/cache file
    ):
        assert _docker_context_excluded(rules, secret), (
            f"{secret!r} must be excluded from the build context by the allowlist"
        )

    # Discriminating control: the previous rules (bare `!docker-login`, only
    # `docker-login/data` re-excluded) LEAK a file dropped directly under
    # docker-login/. This proves the matcher -- and thus the assertions above --
    # actually detect the regression the fix removes (mutation counter-check baked in).
    old_rules = ["*", "!requirements.txt", "!docker-login", "docker-login/data"]
    assert not _docker_context_excluded(old_rules, "docker-login/secrets.json"), (
        "sanity: the old denylist rules must (wrongly) keep docker-login/secrets.json "
        "in context, otherwise this test would pass even without the fix"
    )
    assert _docker_context_excluded(rules, "docker-login/secrets.json"), (
        "the fixed allowlist must exclude docker-login/secrets.json"
    )


def test_readme_container_station_does_not_present_compose_up_first_login() -> None:
    """Container Station guidance must not present compose-up as a first-login path.

    Codex: importing docker-compose.yml as a Container Station *application* starts it
    with ``docker compose up`` semantics, which does not forward terminal STDIN, so the
    first-run account-e-mail prompt can never be answered. Telling users to "keep
    it in the foreground for the first (login) run" is therefore wrong (foreground is
    not stdin attached). The README must route the interactive login through the
    stdin-attaching ``docker compose run`` (SSH) path and only offer Container Station
    for later, already-authenticated runs.
    """

    readme = _read("README.md")

    assert "**Container Station:**" in readme, (
        "README must document the Container Station option so this guard stays anchored."
    )
    # Isolate the Container Station bullet (up to the next blank line).
    bullet = readme.split("**Container Station:**", 1)[1].split("\n\n", 1)[0].lower()

    # It must NOT claim the first/login run works by keeping the compose-up
    # application "in the foreground" (the exact wrong claim Codex flagged).
    assert not ("foreground" in bullet and "first" in bullet and "login" in bullet), (
        "Container Station bullet still presents keeping the compose-up application "
        "'in the foreground' for the 'first (login)' run -- that path has no stdin, so "
        "the interactive login cannot proceed."
    )
    # It must steer the login to the stdin-attaching `docker compose run` path...
    assert "compose run" in bullet, (
        "Container Station bullet must route the interactive login to the "
        "`docker compose run` path (the only one that attaches stdin)."
    )
    # ...and warn that the app-import / compose-up path does not forward a terminal.
    assert "compose up" in bullet and "forward" in bullet, (
        "Container Station bullet must warn that the app-import/compose-up path does "
        "not forward a terminal, so the first-login prompt stalls."
    )


def _entrypoint_code_lines() -> list[str]:
    """Return ``entrypoint.sh`` without comment-only and blank lines.

    Structural assertions run on this projection so a guard can never be satisfied
    by prose in a comment that describes the intended shape.
    """

    return _code_lines(_read("entrypoint.sh"))


def test_entrypoint_keeps_one_clear_text_block_in_its_own_if(tmp_path: Path) -> None:
    """The clear-text track is exactly one block, in its own top-level ``if``.

    Two properties that behaviour alone cannot show: the print block exists
    exactly ONCE (a second copy would be a second place to keep in sync), and its
    condition re-tests the secrets file rather than trusting an earlier check.
    Both were once carried by the removed token-endpoint branch and must not fall
    away with it: the clear-text track deletes the bundle it printed, so a block that
    ran without the file present would claim a removal it never performed.
    """

    del tmp_path  # structural guard: no filesystem scenario needed

    code = _entrypoint_code_lines()

    cleartext_conditions = [
        i for i, line in enumerate(code) if "GFMY_CLEARTEXT" in line
    ]
    assert len(cleartext_conditions) == 1, (
        "expected exactly one GFMY_CLEARTEXT condition in entrypoint.sh; got "
        f"{[code[i] for i in cleartext_conditions]}"
    )
    cleartext_idx = cleartext_conditions[0]
    assert code[cleartext_idx].startswith("if "), (
        "the clear-text track must be its OWN top-level `if`, so a future second "
        f"track cannot chain onto it with an `elif`; got: {code[cleartext_idx]!r}"
    )

    # DRY: exactly one clear-text print block, and it re-tests the file's existence.
    assert sum("BEGIN secrets.json" in line for line in code) == 1, (
        "the clear-text block must exist exactly ONCE; a duplicate is a second "
        "place to keep in sync, not a fix."
    )
    assert '[ -f "${_secrets_path}" ]' in code[cleartext_idx], (
        "the clear-text condition must re-test the secrets file, so a bundle that "
        "is no longer there prints nothing."
    )


def _echo_lines(launcher: str) -> list[str]:
    """Return the launcher lines that actually PRINT something to the user.

    Only these lines are the user-facing contract; the surrounding comment
    blocks may (and do) still discuss ``localhost`` as a concept. For bash an
    output line starts with ``echo`` after optional indentation; for the batch
    file the ``echo`` may sit behind a one-line ``if`` guard, so any line
    carrying an ``echo`` token counts, minus the ``rem`` comments.
    """

    lines = _read(launcher).splitlines()
    if launcher.endswith(".cmd"):
        return [
            line
            for line in lines
            if not line.lstrip().lower().startswith("rem")
            and re.search(r"\becho\b", line, re.IGNORECASE)
        ]
    return [line for line in lines if re.match(r"\s*echo\b", line)]


@pytest.mark.parametrize("launcher", ["login.sh", "login.cmd"])
def test_launcher_never_prints_localhost_as_the_novnc_url(launcher: str) -> None:
    """No printed line may hand the user ``localhost:7900`` as the noVNC address.

    The noVNC viewer is opened by the operator's BROWSER, which in the common
    setup does not run on the Docker host at all (NAS, home server, VM). A
    printed ``http://localhost:7900`` then points the browser at the wrong
    machine and the login appears broken. The launchers therefore print the
    address they actually bound (or the one passed via ``--ip``), never the
    ``localhost`` alias.
    """

    offenders = [line for line in _echo_lines(launcher) if "localhost:7900" in line]
    assert not offenders, (
        f"{launcher} still prints a localhost noVNC URL ({offenders!r}). Print the "
        "bound/--ip address instead: the browser that opens this URL usually runs "
        "on a different machine than the Docker host."
    )


def test_login_sh_handles_the_ip_flag_in_its_argument_parser() -> None:
    """``login.sh`` must handle ``--ip`` in its argument parser.

    ``--ip <ADDRESS>`` is the supported way to make the noVNC viewer reachable
    from another machine, and it has to be handled by the argument parser
    itself, not merely mentioned in the usage text (which is the failure mode a
    plain substring check would miss).
    """

    text = _read("login.sh")
    # `--ip` must be handled inside the argument-dispatch LOOP. Anchoring on the
    # first `case "$1" in` would be wrong: the address-validation helpers use
    # that construct too, so the guard would pass on their text alone.
    lines = text.splitlines()
    loop_start = next(
        (
            i
            for i, line in enumerate(lines)
            if line.strip() == 'while [ "$#" -gt 0 ]; do'
        ),
        None,
    )
    assert loop_start is not None, (
        'login.sh must dispatch its arguments through a `while [ "$#" -gt 0 ]` '
        "loop; the --ip guard below anchors on it."
    )
    loop_end = next(
        i
        for i, line in enumerate(lines[loop_start:], loop_start)
        if line.strip() == "done"
    )
    parser = "\n".join(lines[loop_start:loop_end])
    assert "--ip" in parser, (
        "login.sh must handle `--ip` in its argument parser (documenting it in the "
        "usage text alone leaves the flag unimplemented)."
    )
    assert "--ip=*" in parser, (
        "login.sh must accept the `--ip=<addr>` spelling too; the README promises "
        "both spellings for parity with login.cmd."
    )


def test_reachability_hint_is_derived_from_the_bind_not_the_printed_address() -> None:
    """The "who can reach this" verdict must key on the BIND in both launchers.

    With a wildcard bind the PRINTED address is a concrete one (loopback for
    ``login.cmd``, a detected address for ``login.sh``). Branching the hint on
    that printed value therefore claims "only this Docker host reaches it" for a
    viewer that is in fact listening on every interface, protected by the fixed
    password ``secret`` -- a false all-clear in exactly the security note whose
    job is to be true. This regression shipped once and is pinned here.
    """

    sh = _read("login.sh")
    assert 'if is_loopback_addr "$novnc_bind"; then' in sh, (
        "login.sh must classify the BIND for the reachability hint; branching on "
        "$novnc_url_host gives a false all-clear for a wildcard bind."
    )
    assert 'case "$novnc_url_host" in' not in sh, (
        "login.sh must not branch its reachability hint on the printed address."
    )

    cmd = _read("login.cmd")
    assert "if defined IS_LOOPBACK goto :loopback_hint" in cmd, (
        "login.cmd must branch its reachability hint on the IS_LOOPBACK flag."
    )
    assert '"%NOVNC_URL_HOST%"=="127.0.0.1" goto :loopback_hint' not in cmd, (
        "login.cmd must not branch its reachability hint on the printed address."
    )
    loopback_flags = [
        line for line in cmd.splitlines() if 'set "IS_LOOPBACK=1"' in line
    ]
    assert loopback_flags, "login.cmd must set IS_LOOPBACK somewhere."
    assert all("%NOVNC_BIND" in line for line in loopback_flags), (
        "every IS_LOOPBACK assignment must be derived from %NOVNC_BIND%, not "
        f"from the printed address (found {loopback_flags!r})."
    )


def test_login_cmd_argument_parser_handles_both_ip_spellings() -> None:
    """``login.cmd`` must implement ``--ip X`` and ``--ip=X``, with a miss path.

    The README promises "the same ``--ip`` flag" as ``login.sh``. Batch has no
    argument parser, so the flag lives in hand-written labels; a missing label
    or a lost ``shift`` silently turns the flag into "unknown option". Every
    label referenced by a ``goto`` must therefore also be defined.
    """

    cmd = _read("login.cmd")
    for needle in (
        'if /i "%~1"=="--ip" goto :parse_ip',
        'if /i "%ARG:~0,5%"=="--ip=" goto :parse_ip_inline',
        ":parse_ip_inline",
        ":ip_missing",
    ):
        assert needle in cmd, f"login.cmd must contain {needle!r}"

    targets = set(re.findall(r"goto :([A-Za-z_][A-Za-z0-9_]*)", cmd))
    targets |= set(re.findall(r"call :([A-Za-z_][A-Za-z0-9_]*)", cmd))
    defined = set(re.findall(r"^:([A-Za-z_][A-Za-z0-9_]*)", cmd, re.MULTILINE))
    defined.add("eof")  # `goto :eof` is a cmd.exe builtin, never a real label
    missing = sorted(targets - defined)
    assert not missing, f"login.cmd jumps to undefined labels: {missing}"


def test_launchers_bracket_ipv6_before_printing_or_binding() -> None:
    """Both launchers must normalise an IPv6 literal to exactly one bracket pair.

    The ``--ip`` validators accept the bare and the bracketed spelling, so a bare
    ``2001:db8::1`` copied straight into the URL yields
    ``http://2001:db8::1:7900``, which no browser can parse, and docker's port
    syntax needs the brackets as well.
    """

    sh = _read("login.sh")
    assert "bracket_if_ipv6()" in sh, "login.sh must define bracket_if_ipv6()"
    for target in (
        'novnc_url_host="$(bracket_if_ipv6 "$novnc_url_host")"',
        'novnc_bind="$(bracket_if_ipv6 "$novnc_bind")"',
    ):
        assert target in sh, f"login.sh must normalise through {target!r}"

    cmd = _read("login.cmd")
    assert re.search(r"^:bracket_ipv6$", cmd, re.MULTILINE), (
        "login.cmd must define the :bracket_ipv6 subroutine."
    )
    assert 'set "BRACKETED=[%BRACKETED%]"' in cmd, (
        "login.cmd must add the IPv6 brackets when they are absent."
    )
    # The bracketing must run on BOTH roles and AFTER the classification, so the
    # GFMY_NOVNC_BIND environment path is normalised too and the loopback /
    # wildcard comparisons still see the spelling the operator typed.
    # Pinned by ROLE, not by a bare call count: a count would have to be edited
    # again for every further address role -- which is the kind of edit that
    # quietly drops a role instead of adding one. Each variable that ends up in a
    # docker port bind or in a printed URL must be assigned FROM the bracketing
    # subroutine's output.
    for role in ("NOVNC_BIND", "NOVNC_URL_HOST"):
        assert f'set "{role}=%BRACKETED%"' in cmd, (
            f"login.cmd must normalise {role} through :bracket_ipv6; an unbracketed "
            "IPv6 literal is printed as a broken URL and rejected by docker as a "
            "port bind."
        )
    assert cmd.count("call :bracket_ipv6 ") >= 2, (
        "login.cmd must bracket at least the bind AND the printed address (found "
        f"{cmd.count('call :bracket_ipv6 ')} call sites)."
    )
    classify_at = cmd.index('set "IS_LOOPBACK="')
    bracket_at = cmd.index("call :bracket_ipv6 ")
    assert classify_at < bracket_at, (
        "login.cmd must classify the bind BEFORE bracketing it; otherwise "
        "`--ip ::1` becomes `[::1]` and no longer matches the loopback test."
    )


def test_login_cmd_classifies_both_ipv6_spellings() -> None:
    """Every spelling that can reach a comparison must be covered by it.

    ``login.sh`` folds the spellings inside ``is_loopback_addr`` /
    ``is_wildcard_addr``; batch has no such helper, so each literal appears in
    the comparison chain and both spellings have to be listed explicitly. A
    missing one is a silent false all-clear (`--ip ::1` warning that the viewer
    is LAN-reachable) or a wildcard printed as a URL.
    """

    cmd = _read("login.cmd")
    for literal in ('"::1"', '"[::1]"'):
        assert f'if "%NOVNC_BIND%"=={literal} set "IS_LOOPBACK=1"' in cmd, (
            f"login.cmd must treat {literal} as a loopback bind."
        )
    for literal in ('"::"', '"[::]"', '"0.0.0.0"'):
        assert (
            f'if "%NOVNC_URL_HOST%"=={literal} set "NOVNC_URL_HOST=127.0.0.1"' in cmd
        ), f"login.cmd must replace the wildcard {literal} in the printed URL."


def test_login_cmd_validates_the_ip_value_like_login_sh() -> None:
    """``login.cmd --ip`` must reject a non-IP value, as ``login.sh`` does.

    Without it a mistyped host name, or a second option swallowed as the value,
    is printed as a working URL and then handed to docker as a port bind, where
    it fails much later with an opaque publish error. Both spellings (``--ip X``
    and ``--ip=X``) must be validated, and the failure must leave through the
    dedicated exit path rather than falling into the normal run.
    """

    cmd = _read("login.cmd")
    # Pinned per --ip PARSING BRANCH rather than by a global call count: a count
    # would grow with every new address input and stop testing "both --ip
    # spellings" specifically. Each branch is read up to the next label.
    for branch in (":parse_ip\n", ":parse_ip_inline\n"):
        block = cmd.split("\n" + branch, 1)[1].split("\n:", 1)[0]
        assert "call :validate_ip " in block, (
            f"the {branch.strip()} branch must validate its value before using it."
        )
        assert "goto :ip_invalid" in block, (
            f"the {branch.strip()} branch must route a rejected value to :ip_invalid."
        )
    assert re.search(r"^:ip_invalid$", cmd, re.MULTILINE), (
        "login.cmd must define the :ip_invalid exit path."
    )
    # The exit path must unwind like every other one: popd, endlocal, exit /b 2.
    # Anchor on the LABEL DEFINITION at line start, not on the first `goto`.
    invalid_block = cmd.split("\n:ip_invalid\n", 1)[1].split("\n:", 1)[0]
    for token in ("popd", "endlocal", "exit /b 2"):
        assert token in invalid_block, (
            f":ip_invalid must {token} like the other early exits."
        )
    # An IP-literal allowlist, not a substring check: a host name must not pass.
    assert 'if "%CAND:~0,1%"=="-" exit /b 1' in cmd, (
        "login.cmd must reject a value that is actually the next option."
    )
    assert "if defined REST exit /b 1" in cmd, (
        "login.cmd must reject any character outside the IP-literal allowlist."
    )


def test_login_sh_rejects_unbalanced_ipv6_brackets(tmp_path: Path) -> None:
    """``is_ip_literal`` must not accept a spelling ``bracket_if_ipv6`` cannot fix.

    Accepting ``[::1`` would let the normaliser produce ``[[::1]``, so the
    promise "exactly one pair" in both comment blocks would be untrue and the
    value would still reach docker as a malformed port bind.

    The matrix also covers the empty-field class. ``is_ip_literal`` cuts the
    value into fields with parameter expansion and counts them. The IPv6
    counting loop skips empty fields (it has to, for the ``::`` run), so its
    group count cannot testify to the shape of the raw value:
    ``:1:2:3:4:5:6:7:8`` and ``1:2:3:4:5:6:7:8:`` both yield the eight groups of
    a well-formed address. The IPv6 structure is therefore checked BEFORE the
    split, and the positive controls below pin that the legal leading and
    trailing colons of ``::1`` / ``1::`` / ``::`` survive that guard. The IPv4
    loop rejects an empty octet itself; the structure check before it is a
    second guard for ``1.2.3.4.`` and its neighbours.

    This runs the real script, so it is a behaviour test, not a re-implementation.
    """

    # Absolute: the script cd's to its own directory, and the run below sets a
    # cwd, so a repo-relative path would not resolve there.
    script = (DOCKER_LOGIN / "login.sh").resolve()

    # An ACCEPTED address does not end the script: it runs on to the final
    # `docker compose ... run --build --service-ports --rm`. On a machine with a
    # working Docker daemon (every GitHub runner) that really starts the login
    # container, which then waits for a sign-in that never comes. `capture_output`
    # plus a `timeout` does not save us there: the timeout kills only the direct
    # `bash` child, while the surviving `docker`/`docker-compose` grandchildren keep
    # the stdout pipe open, so the final `communicate()` blocks forever and the whole
    # pytest run hangs. Hence the same stub-`docker`-on-`PATH` harness that
    # `test_login_sh_behaviourally_selects_compose_files` already uses: the launcher
    # runs to completion, but its last step is a no-op echo.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text('#!/usr/bin/env bash\nprintf "DOCKER_ARGS: %s\\n" "$*"\n', "utf-8")
    stub.chmod(0o755)
    env = {"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path)}

    for value, expected_reject in (
        ("::1", False),
        ("[::1]", False),
        ("2001:db8::1", False),
        ("1:2:3:4:5:6:7:8", False),
        ("::", False),
        ("fe80::1", False),
        ("192.168.1.21", False),
        # Structural rules beyond "looks like IPv6": one compression run only,
        # at most four hex digits per group, eight groups unless compressed.
        ("1::2::3", True),
        ("12345::1", True),
        ("1:2:3:4:5:6:7:8:9", True),
        ("1:2:3:4:5:6:7", True),
        ("1:2:3:4:5:6:7:12345", True),
        ("1:2:3:4:5:6:7:abcd", False),
        # A compression run stands for at least one omitted group, so eight
        # written-out groups next to it are already one too many.
        ("1:2:3:4:5:6:7:8::", True),
        ("::1:2:3:4:5:6:7:8", True),
        ("1:2:3:4:5:6:7::", False),
        ("[::1", True),
        ("]::1[", True),
        ("[[::1]]", True),
        # A lone colon passes a pure character allowlist, which is why the
        # structural checks exist: it would reach docker as a port bind.
        (":", True),
        (":::", True),
        ("abc:", True),
        ("1.2.3.0a", True),
        (".", True),
        ("not an ip", True),
        # Empty-field class (Codex P2 on login.cmd, same defect in login.sh).
        # An empty octet / group is invisible in the token result. Measured
        # against the pre-fix script, exactly THREE of the five below used to be
        # accepted and would have reached docker as a port bind: "1.2.3.4.",
        # ":1:2:3:4:5:6:7:8" and "1:2:3:4:5:6:7:8:". The other two were already
        # rejected -- a non-whitespace IFS does produce an empty LEADING field,
        # and ".." was caught by its own guard -- so they are kept here as
        # regression sentinels for that older guard, not as evidence for the new
        # one. Do not read this block as "all five were broken".
        ("1..2.3.4", True),
        (".1.2.3.4", True),
        ("1.2.3.4.", True),
        (":1:2:3:4:5:6:7:8", True),
        ("1:2:3:4:5:6:7:8:", True),
        # Neighbours of the same class, for the guard's boundaries.
        ("..", True),
        ("1.2.3.", True),
        (".1.2.3", True),
        (":1", True),
        ("1:", True),
        (":1:2", True),
        ("1:2:", True),
        # Positive controls: a leading or trailing colon IS legal when it is
        # part of the "::" run, and the plain forms must stay accepted. If the
        # new guard were written as a blanket "no leading/trailing colon", every
        # one of these four would flip to rejected.
        ("1::", False),
        ("::1", False),
        ("::", False),
        ("2001:db8::", False),
        ("1.2.3.4", False),
        ("0.0.0.0", False),
        ("127.0.0.1", False),
        ("255.255.255.255", False),
        # Count and range of the octet loop.
        ("1.2.3.4.5", True),
        ("1.2.3", True),
        ("1.2.3.256", True),
    ):
        proc = subprocess.run(
            ["bash", str(script), "--ip", value],
            capture_output=True,
            text=True,
            check=False,
            cwd=str(DOCKER_LOGIN),
            env=env,
            timeout=60,
        )
        rejected = proc.returncode == 2 and "--ip needs an IP address" in proc.stderr
        assert rejected is expected_reject, (
            f"login.sh --ip {value!r}: rejected={rejected}, expected "
            f"{expected_reject} (rc={proc.returncode}, stderr={proc.stderr!r})"
        )


def test_login_sh_never_reassigns_ifs() -> None:
    """``login.sh`` must not change ``IFS`` except for the ``read`` prefix.

    ``is_ip_literal`` used to split on a reassigned ``IFS``; it now cuts fields
    with parameter expansion. Both forms give the same result, so the
    behaviour test above cannot tell them apart, and Semgrep's
    ``ifs-tampering`` rule is a ``WARNING``, which the Semgrep gate in
    ``semgrep.yml`` does not fail on. This text check keeps the reassignment
    from coming back. ``IFS= read`` only sets ``IFS`` for that one command and
    is allowed.
    """

    assert _ifs_reassignments(_read("login.sh")) == [], "login.sh reassigns IFS"


# Forms that set or unset IFS without the text ``IFS=``: builtins that take a
# variable name (``printf -v``, ``read``, ``mapfile``/``readarray``,
# ``getopts``, ``unset``), a declaration without a value (``local IFS`` and
# ``declare IFS`` make it unset in that scope), a loop variable, and the
# assigning expansions ``${IFS:=…}``/``${IFS=…}``. ``$IFS`` and ``${IFS…}``
# read the variable and are not names here.
# The name may be quoted (``read -r "IFS"``, ``printf -v 'IFS'``); the shell
# removes the quotes before the builtin sees the name.
_IFS_WORD = r"""(?:(?<![A-Za-z0-9_${'"])IFS|"IFS"|'IFS')(?![A-Za-z0-9_=])"""
_IFS_NAME_FORMS = re.compile(
    r"|".join(
        (
            rf"\bprintf\b[^;|&]*?\s-v\s*{_IFS_WORD}",
            rf"\b(?:read|mapfile|readarray|unset)\b[^;|&]*?\s{_IFS_WORD}",
            rf"\bgetopts\s+\S+\s+{_IFS_WORD}",
            rf"\b(?:local|declare|typeset)\b[^;|&=]*?\s{_IFS_WORD}",
            rf"\b(?:for|select)\s+{_IFS_WORD}",
            r"\$\{IFS:?=",
        )
    )
)


def _ifs_reassignments(script: str) -> list[str]:
    """Return the lines of ``script`` that assign, append to or unset ``IFS``.

    The allowed ``IFS= read`` prefix is removed first, per occurrence, so a
    second assignment on the same line is still reported. Besides ``IFS=`` and
    ``IFS+=`` (also behind ``local``/``export``), the forms of
    ``_IFS_NAME_FORMS`` count. Not seen: a nameref (``declare -n ref=IFS``
    followed by ``ref=…``), a variable name held in another variable
    (``printf -v "$var"``), text run through ``eval`` that hides the word, and
    a sourced file.
    """

    found = []
    for number, line in enumerate(script.splitlines(), 1):
        if line.lstrip().startswith("#"):
            continue
        rest = re.sub(r"(?<![A-Za-z0-9_])IFS= read\b", "", line)
        if re.search(r"(?<![A-Za-z0-9_])IFS\+?=", rest) or _IFS_NAME_FORMS.search(rest):
            found.append(f"{number}: {line.strip()}")
    return found


@pytest.mark.parametrize(
    ("line", "flagged"),
    [
        ("while IFS= read -r a; do", False),
        ("  # IFS=: in a comment", False),
        ("IFS=:", True),
        ("  local IFS=.", True),
        ("export IFS=$'\\n'", True),
        ("IFS+=:", True),
        ("while IFS= read -r a; do IFS=:; done", True),
        ("saved_ifs=$IFS", False),
        ("printf -v IFS %s :", True),
        ("printf '%s' \"$IFS\"", False),
        ("read -r IFS <<< :", True),
        ("read -r a b IFS", True),
        ('read -r line <<< "$IFS"', False),
        ("mapfile -t IFS < list", True),
        ("readarray IFS < list", True),
        ("getopts ab IFS", True),
        ("unset IFS", True),
        ("unset -v a IFS", True),
        (': "${IFS:=:}"', True),
        (': "${IFS=:}"', True),
        ('echo "${IFS:-default}"', False),
        ("local IFS", True),
        ("declare IFS", True),
        ("for IFS in a b; do :; done", True),
        ("select IFS in a b; do :; done", True),
        ('read -r "IFS" <<< :', True),
        ("printf -v 'IFS' %s :", True),
        ('unset "IFS"', True),
        ('echo "IFS is fine"', False),
    ],
)
def test_ifs_reassignment_check_sees_every_form(line: str, flagged: bool) -> None:
    """Pin the text check used above, including two forms on one line."""

    assert bool(_ifs_reassignments(line)) is flagged


def test_login_cmd_validates_ipv6_structure_not_just_a_colon() -> None:
    """The batch validator must mirror the IPv6 half of ``is_ip_literal()``.

    A character allowlist alone accepts a lone ``:``, and ``:bracket_ipv6``
    leaves any value that already starts with ``[`` untouched, so ``[::1``
    would survive unbalanced. Both would be printed as a working URL and then
    handed to Compose as a port bind. ``login.cmd`` cannot be executed here (no
    Windows), so the structural checks are pinned by text.
    """

    cmd = _read("login.cmd")
    assert re.search(r"^:validate_ipv6$", cmd, re.MULTILINE), (
        "login.cmd must route colon-bearing values into a :validate_ipv6 block "
        "instead of accepting them outright."
    )
    assert 'if not "%INNER:~-1%"=="]" exit /b 1' in cmd, (
        "an opening bracket must require the matching closing bracket."
    )
    for residual in ('set "_T=%INNER:[=%"', 'set "_T=%INNER:]=%"'):
        assert residual in cmd, (
            f"login.cmd must reject residual brackets ({residual!r})."
        )
    assert 'set "_T=%INNER::::=%"' in cmd, "login.cmd must forbid a ':::' run."
    assert 'set "_T=%INNER:::=%"' in cmd, (
        "login.cmd must detect the '::' run before falling back to counting separators."
    )
    assert 'if "%INNER%"=="::" exit /b 0' in cmd, (
        "'::' is the one addressable-digit exception and must be spelled out."
    )


def test_login_cmd_mirrors_the_full_ipv6_structural_rules() -> None:
    """The batch validator must carry the same rules, not a weaker subset.

    ``login.sh`` is executed by the matrix above; ``login.cmd`` cannot be run
    here, so the three rules that go beyond "contains a colon" are pinned by
    text: a single compression run, group width, and group count.
    """

    cmd = _read("login.cmd")
    assert 'set "_TAIL=%INNER:*::=%"' in cmd, (
        "login.cmd must isolate the tail after the first '::' run to detect a "
        "second one."
    )
    assert re.search(r"^:v6_no_second_run$", cmd, re.MULTILINE), (
        "login.cmd must define the second-run rejection helper."
    )
    assert re.search(r"^:v6_group$", cmd, re.MULTILINE), (
        "login.cmd must define the per-group width helper."
    )
    assert 'if not "%_G:~4%"=="" exit /b 1' in cmd, (
        "a group wider than four hex digits must be rejected."
    )
    assert cmd.count('call :v6_group "%%') == 8, (
        "all eight possible groups must be width-checked (found "
        f"{cmd.count('call :v6_group ' + chr(34) + '%%')})."
    )
    assert 'if not "%%i"=="" exit /b 1' in cmd, "a ninth group must be rejected."
    assert 'if not defined _HAS_RUN if "%%h"=="" exit /b 1' in cmd, (
        "without a compression run, all eight groups must be present."
    )
    assert 'if defined _HAS_RUN if not "%%h"=="" exit /b 1' in cmd, (
        "with a compression run at most seven groups may be written out; "
        "checking only the ninth token accepts `1:2:3:4:5:6:7:8::`."
    )


def test_login_cmd_checks_empty_fields_before_splitting() -> None:
    """The batch validator must not derive structure from the token result.

    ``for /f`` collapses a run of delimiters into one and discards leading and
    trailing ones. ``.1.2.3.4``, ``1.2.3.4.`` and ``1..2.3.4`` therefore all
    tokenise into the same four clean octets, and ``:1:2:3:4:5:6:7:8`` /
    ``1:2:3:4:5:6:7:8:`` into eight clean groups, so every count test in
    ``:validate_ip`` / ``:validate_ipv6`` passes on a malformed value. The
    emptiness only exists on the raw string and has to be rejected there.

    Honest limits of this test: there is no cmd.exe and no wine in this
    environment, so ``login.cmd`` cannot be executed. What follows pins the
    PRESENCE of the guards and their position relative to the ``for /f`` they
    protect. It does NOT prove that they behave as intended on Windows; the
    executed proof of the same rules lives in
    ``test_login_sh_rejects_unbalanced_ipv6_brackets`` against the mirror
    ``login.sh``.

    That transfer holds only as far as the two files really accept the same
    set, and THAT is an intent pinned by text here, never an executed result:
    the shared allowlist above ``:validate_ipv6`` permits ``.`` for the IPv4
    branch, so the IPv6 branch has to narrow it away again to match the
    ``*[!0-9A-Fa-f:[]]*`` case in ``is_ip_literal``. The pin below exists
    because that narrowing was missing once, which let ``::1.`` through in the
    batch file while ``login.sh`` rejected it.
    """

    cmd = _read("login.cmd")

    # Parity of the character allowlist: the IPv6 branch must drop "." again,
    # and it must do so BEFORE the bracket handling, so that "[::1.]" cannot
    # reach the structural rules either.
    dot_guard = 'set "_T=%CAND:.=%"'
    assert dot_guard in cmd, (
        "the IPv6 branch must narrow the shared allowlist by rejecting '.', "
        "as the IPv6 case of is_ip_literal() in login.sh does; without it "
        "'::1.' passes here but is rejected there."
    )
    assert cmd.index(dot_guard) < cmd.index('set "INNER=%CAND%"'), (
        "the '.' rejection must run before the value is unbracketed into INNER."
    )

    # IPv4: no ".." run, no leading dot, no trailing dot.
    ipv4_guards = (
        'set "_T=%CAND:..=%"',
        'if "%CAND:~0,1%"=="." exit /b 1',
        'if "%CAND:~-1%"=="." exit /b 1',
    )
    for guard in ipv4_guards:
        assert guard in cmd, (
            f"login.cmd must reject an empty octet on the raw string ({guard!r})."
        )
    octet_split_at = cmd.index('for /f "tokens=1-5 delims=." ')
    for guard in ipv4_guards:
        assert cmd.index(guard) < octet_split_at, (
            f"{guard!r} must run BEFORE the octet split; after it the empty "
            "octet is already gone and the guard can never fire."
        )

    # IPv6: a leading or trailing ":" is legal only as part of a "::" run.
    ipv6_guards = (
        'if "%INNER:~0,1%"==":" if not "%INNER:~0,2%"=="::" exit /b 1',
        'if "%INNER:~-1%"==":" if not "%INNER:~-2%"=="::" exit /b 1',
    )
    for guard in ipv6_guards:
        assert guard in cmd, (
            f"login.cmd must reject an empty group at either end ({guard!r})."
        )
    group_split_at = cmd.index('for /f "tokens=1-9 delims=:" ')
    two_sep_split_at = cmd.index('for /f "tokens=1-3 delims=:" ')
    for guard in ipv6_guards:
        assert cmd.index(guard) < min(group_split_at, two_sep_split_at), (
            f"{guard!r} must run BEFORE every colon split, including the "
            "two-separator probe, which would otherwise keep reading an "
            "unreliable token count."
        )
    # The exemption for the "::" run has to hold in the FILE, not merely in the
    # literals pinned above: an additional unconditional rejection anywhere in
    # the block would leave every assertion above green while killing "::1",
    # "1::" and "::". Assert against `cmd`, never against `ipv6_guards` -- a
    # check that reads the test's own literals proves nothing about the code.
    for unconditional in (
        'if "%INNER:~0,1%"==":" exit /b 1',
        'if "%INNER:~-1%"==":" exit /b 1',
    ):
        assert unconditional not in cmd, (
            f"{unconditional!r} rejects a leading/trailing colon outright and "
            "would also reject '::1', '1::' and '::'; the guard must stay "
            "conditional on the '::' run."
        )


def test_cleartext_cleanup_reports_a_failed_removal() -> None:
    """The clear-text track must not claim a removal it did not perform.

    The clear-text fallback prints the whole bundle and then deletes it. A
    silenced ``rm -f ... 2>/dev/null || true`` next to a message asserting the
    file "is now removed" lies whenever the delete fails (a readable file under
    a directory that is no longer writable), leaving the full credentials on
    disk with no warning.
    """

    entrypoint = _read("entrypoint.sh")
    assert 'rm -f "${_secrets_path}" 2>/dev/null || true' not in entrypoint, (
        "the clear-text cleanup must not force a successful exit status."
    )
    assert 'if rm -f "${_secrets_path}" 2>/dev/null; then' in entrypoint, (
        "the clear-text cleanup must branch on the actual removal result."
    )
    assert "could not remove" in entrypoint and "STILL on disk" in entrypoint, (
        "a failed removal must warn that the bundle is retained, naming the path."
    )
    # The unconditional past-tense claim must be gone from the preceding text.
    assert "The file is now removed." not in entrypoint, (
        "the message must not assert the removal before it has happened."
    )


def test_no_output_is_located_in_the_novnc_viewer() -> None:
    """Runtime output must be located where it is actually written.

    The container's own docs claimed twice that the pairing code and the
    ``GFMY_CLEARTEXT=1`` bundle can be read "inside the noVNC terminal"/"on the
    noVNC screen". Both are written to the entrypoint's stdout, i.e. the
    terminal running the launcher (and ``docker logs``), which is a different
    sink from the X display that noVNC renders: ``supervisord`` starts before
    ``GFMY_PAIRING_CODE`` exists, so no process under it inherits the value, and
    a shell opened inside the desktop is a fresh session that never sees this
    file descriptor.

    The guard pins the *predicate* ("output is located where it is written"),
    not the word "noVNC": performing the Google sign-in genuinely does happen in
    the noVNC window, and those true statements must survive. The three banned
    phrases below are location claims about *output* only.
    """

    entrypoint = _read("entrypoint.sh")
    readme = _read("README.md")

    for name, text in (("entrypoint.sh", entrypoint), ("README.md", readme)):
        for phrase in ("noVNC terminal", "noVNC screen", "inside the noVNC window"):
            assert phrase not in text, (
                f"{name} must not locate runtime output in the noVNC viewer "
                f"(found {phrase!r}); it is printed on the entrypoint's stdout."
            )

    # Positive counterpart: each of the two outputs names its real sink.
    assert "in the terminal that runs the" in entrypoint, (
        "the clear-text block must say it is printed in the launcher's terminal."
    )
    assert "docker logs" in entrypoint, (
        "the clear-text comment must name `docker logs` as the second sink, "
        "because that is what makes the block only as private as daemon access."
    )
    assert "in the terminal you started the launcher from" in readme, (
        "the README must tell the user where the pairing code and the "
        "clear-text block actually appear."
    )
    # The clear-text privacy note must not point at the wrong control knob:
    # GFMY_NOVNC_BIND governs port 7900 only and cannot limit stdout.
    cleartext_note = readme.split("### Terminal clear-text copy fallback")[-1]
    assert "does **not** limit it" in cleartext_note, (
        "the clear-text privacy note must state that GFMY_NOVNC_BIND does not "
        "constrain the terminal output."
    )


def test_login_cmd_trims_trailing_blanks_from_every_inbound_switch() -> None:
    """`set VAR=1 && login.cmd` must not silently start the wrong mode.

    cmd.exe stores everything up to the `&&` INCLUDING the blank in front of
    it, so that documented one-liner leaves the value as ``"1 "``. Measured
    with a real cmd.exe: without a trim, a strict ``=="1"`` comparison is false
    and the launcher starts in the wrong mode while still reporting success. The
    same environment is handed to `docker compose`, which interpolates it into
    the container, where `entrypoint.sh` compares ``GFMY_CLEARTEXT`` against
    ``"1"`` just as strictly.

    The expected set is therefore DERIVED from the compose file rather than
    restated here: every ``${GFMY_*}`` it interpolates travels into the container
    and has to be trimmed, so adding one there without adding it to the
    launcher's list is exactly the regression this guards.
    """

    text = _read("login.cmd")
    lines = text.splitlines()

    from_compose = set(re.findall(r"\$\{(GFMY_[A-Z_]+)", _read("docker-compose.yml")))
    assert from_compose, "no ${GFMY_*} found in the compose file"
    # Only this script reads it, so no compose file mentions it.
    expected = from_compose | {"GFMY_NOVNC_URL_HOST"}

    trim_lines = [
        i for i, line in enumerate(lines) if "call :trim_trailing_blanks" in line
    ]
    assert trim_lines, "login.cmd must call the trim routine"
    dispatch = min(trim_lines)
    covered = set(re.findall(r"GFMY_[A-Z_]+", lines[dispatch]))
    assert expected <= covered, (
        "login.cmd trims only part of the class; a user can set these and they "
        f"reach Compose or the container with a trailing blank: {sorted(expected - covered)}"
    )

    # The trim is worthless once a consumer has already read the value.
    for switch in sorted(expected):
        uses = [
            i
            for i, line in enumerate(lines)
            if f"%{switch}%" in line
            and not line.lstrip().startswith(("rem ", "REM ", "::"))
        ]
        if uses:
            assert dispatch < min(uses), (
                f"login.cmd reads {switch} at line {min(uses) + 1} before trimming "
                f"it at line {dispatch + 1}."
            )

    # The routine has to actually loop, and it has to loop on a BLANK: a body
    # that trims once leaves "1  " broken, and a body comparing against another
    # character never trims at all. Both mutations keep every other assertion
    # here green, so they are pinned explicitly.
    assert "\n:trim_trailing_blanks" in text, (
        "login.cmd calls :trim_trailing_blanks but never defines the label"
    )
    routine = text[text.index("\n:trim_trailing_blanks") :]
    assert ':~-1%"==" "' in routine, (
        "the trim routine must test the LAST character against a blank"
    )
    assert ":~0,-1%" in routine, "the trim routine must drop the last character"
    assert "goto :trim_trailing_blanks" in routine, (
        "the trim routine must recurse, otherwise it strips at most one blank"
    )


def test_login_cmd_forwards_novnc_url_host_before_compose_run() -> None:
    """Regression (Windows launcher, PR #1214 Codex follow-up).

    ``login.cmd`` normalises the browsable host into ``NOVNC_URL_HOST`` (including
    the ``--ip <ADDRESS>`` path) but must also re-export it as
    ``GFMY_NOVNC_URL_HOST`` *before* invoking ``docker compose``. Otherwise the
    compose interpolation ``${GFMY_NOVNC_URL_HOST:-localhost}`` falls back to
    ``localhost`` and a remote user is told to open an unreachable URL even though
    noVNC was published on the requested LAN address. ``login.sh`` exports it at
    the same point (see its ``export GFMY_NOVNC_URL_HOST`` line); this locks the
    Windows launcher to the same data path. Removing or moving the export after
    the compose run would restore the localhost fallback without this failing.
    """

    lines = _read("login.cmd").splitlines()

    export_idx = next(
        (
            index
            for index, line in enumerate(lines)
            if line.strip().lower().startswith('set "gfmy_novnc_url_host=')
        ),
        -1,
    )
    assert export_idx >= 0, (
        "login.cmd must re-export the normalised host as GFMY_NOVNC_URL_HOST; "
        "without it the in-container noVNC URL falls back to localhost for a "
        "`login.cmd --ip <ADDRESS>` run."
    )
    assert "%NOVNC_URL_HOST%" in lines[export_idx], (
        "GFMY_NOVNC_URL_HOST must carry the normalised %NOVNC_URL_HOST% value, "
        "not a stale or empty literal."
    )

    run_idx = next(
        (
            index
            for index, line in enumerate(lines)
            if "docker compose" in line and "run --build" in line
        ),
        -1,
    )
    assert run_idx >= 0, "login.cmd must invoke `docker compose ... run --build`."
    assert export_idx < run_idx, (
        "GFMY_NOVNC_URL_HOST must be exported BEFORE `docker compose run`; moving "
        "it afterwards restores the localhost fallback this fix removed."
    )


@pytest.mark.parametrize(
    ("launcher", "assignment"),
    [
        ("login.sh", "export GFMY_NOVNC_URL_HOST="),
        ("login.cmd", 'set "GFMY_NOVNC_URL_HOST='),
    ],
)
def test_both_launchers_reexport_the_novnc_url_host(
    launcher: str, assignment: str
) -> None:
    """Both launchers must ACTUALLY assign GFMY_NOVNC_URL_HOST (not merely mention
    it in a comment) so the printed browsable URL matches across platforms; the
    Windows path regressed once (PR #1214). Matching the real assignment form
    keeps the check from passing on the ``rem``/``#`` explanations alone."""

    assert assignment.lower() in _read(launcher).lower(), (
        f"{launcher} must re-export GFMY_NOVNC_URL_HOST via `{assignment}...`; "
        "otherwise the in-container noVNC URL falls back to localhost."
    )


def _pip_install_commands(dockerfile: str) -> list[str]:
    """Return every ``pip install`` invocation in the Dockerfile as one line.

    Full-line comments are dropped, backslash line-continuations are joined, and
    inline ``#`` comments are stripped, so neither a multi-line
    ``RUN pip install ... \\`` nor a trailing ``# setuptools provided by base``
    comment can hide or fake what the effective command actually installs.
    """

    code = "\n".join(
        line for line in dockerfile.splitlines() if not line.lstrip().startswith("#")
    )
    code = code.replace("\\\n", " ")
    commands: list[str] = []
    for raw in code.splitlines():
        line = re.sub(r"\s+#.*$", "", raw)  # strip inline comments
        if re.search(r"\bpip[0-9]*\b", line) and "install" in line:
            commands.append(line.strip())
    return commands


def test_dockerfile_installs_setuptools_for_the_distutils_shim() -> None:
    """The login image must install setuptools so undetected-chromedriver runs.

    undetected-chromedriver 3.5.5 (the current PyPI top, so a version bump is not
    available) still imports the stdlib ``distutils`` that Python 3.12+ removed
    (PEP 632). The ``selenium/standalone-chrome:latest`` base image is on Python
    3.14, so without ``setuptools`` re-providing ``distutils`` via its
    ``_distutils_hack`` the bare import fails (``ModuleNotFoundError: distutils``)
    and Chrome never starts. Dropping ``setuptools`` from the install step would
    pass every other guard here yet restore that failure, so pin it explicitly
    (Codex P1 on PR #1215).
    """

    dockerfile = _read("Dockerfile")
    pip_installs = _pip_install_commands(dockerfile)
    assert pip_installs, "Dockerfile must install Python dependencies via pip"
    assert any("requirements.txt" in cmd for cmd in pip_installs), (
        "Dockerfile must install the integration requirements via pip"
    )
    assert any(
        re.search(r"(?<![\w./-])setuptools(?![\w-])", cmd) for cmd in pip_installs
    ), (
        "the login image must install setuptools so undetected-chromedriver's "
        "distutils import keeps working on the Python 3.12+ base image "
        "(PEP 632 removed the stdlib distutils; setuptools re-provides it via "
        "_distutils_hack); do not drop it from the pip install step "
        "(Codex P1, PR #1215)."
    )


def test_pip_install_parser_ignores_inline_comments() -> None:
    """An inline ``# setuptools ...`` comment must not satisfy the setuptools
    guard.

    Without stripping inline comments, a Dockerfile such as
    ``RUN pip3 install -r /app/requirements.txt  # setuptools provided by base``
    would drop the real install yet still match the word ``setuptools`` in the
    comment, letting the ``distutils`` regression back in unnoticed. The parser
    strips inline comments so only the effective install arguments count
    (Codex P2, PR #1215).
    """

    faked = "RUN pip3 install -r /app/requirements.txt  # setuptools provided by base\n"
    commands = _pip_install_commands(faked)
    assert commands == ["RUN pip3 install -r /app/requirements.txt"]
    assert not any(
        re.search(r"(?<![\w./-])setuptools(?![\w-])", cmd) for cmd in commands
    ), "inline-comment setuptools must not count as an installed dependency"


def test_entrypoint_mints_password_before_supervisord_starts() -> None:
    """The per-run noVNC password is minted IN THE CONTAINER and exported before
    supervisord starts, so the base image's start-vnc.sh inherits it in time.

    The launchers only raise the GFMY_NOVNC_HARDEN verdict; moving the crypto here
    is what gives Windows (login.cmd) the same hardening without batch-side crypto.
    If the SE_VNC_PASSWORD export ever slipped below the ``supervisord`` launch,
    start-vnc.sh would have stored the public ``secret`` before the per-run
    password landed, silently defeating the hardening.
    """

    entrypoint = _read("entrypoint.sh")
    assert '"${GFMY_NOVNC_HARDEN:-}" = "1"' in entrypoint, (
        "entrypoint.sh must gate the password mint on the GFMY_NOVNC_HARDEN verdict"
    )
    export_idx = entrypoint.find("export SE_VNC_PASSWORD")
    launch_idx = entrypoint.find('bin/supervisord" --configuration')
    assert export_idx != -1, (
        "entrypoint.sh must export SE_VNC_PASSWORD for the hardened path"
    )
    assert launch_idx != -1, "entrypoint.sh must launch supervisord"
    assert export_idx < launch_idx, (
        "SE_VNC_PASSWORD must be exported BEFORE supervisord starts, or start-vnc.sh "
        "stores the public 'secret' before the per-run password lands."
    )


def test_entrypoint_password_charset_excludes_shell_hostile_symbols() -> None:
    """The in-container password alphabet stays letters + '. , = - _ + @' and never
    admits ``! ? % $``.

    Each excluded symbol has a SILENT-failure path in the code the value flows
    through (``%`` cmd.exe escape, ``!`` cmd.exe delayed expansion, ``?`` glob in
    the base image's unquoted ``x11vnc -storepasswd``, ``$`` shell metachar), so a
    well-meant charset widening would break logins without an error. Locking both
    the python3 alphabet and the /dev/urandom fallback here forces a conscious
    change if anyone edits them.
    """

    entrypoint = _read("entrypoint.sh")
    assert 'string.ascii_letters + ".,=-_+@"' in entrypoint, (
        "the python3 CSPRNG alphabet must stay the keyboard-safe set"
    )
    assert (
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.,=-_+@" in entrypoint
    ), "the /dev/urandom fallback alphabet must match the keyboard-safe set"


def test_entrypoint_password_generator_fails_closed_without_csprng() -> None:
    """The password mint must FAIL CLOSED, never emit a weak credential.

    If both python3's ``secrets`` and ``/dev/urandom`` are unavailable, the
    hardened launch must abort rather than derive the LAN-exposed VNC password
    from Bash's non-cryptographic ``$RANDOM`` -- a predictable credential on a
    network-reachable viewer is worse than not starting. Codex flagged exactly
    this fail-open path; this guard keeps ``$RANDOM`` out of the mint and pins the
    abort branch so a later edit cannot silently reintroduce the weak fallback.
    """

    entrypoint = _read("entrypoint.sh")
    start = entrypoint.find('if [ "${GFMY_NOVNC_HARDEN:-}" = "1" ]; then')
    end = entrypoint.find("export SE_VNC_PASSWORD=", start)
    assert start != -1 and end != -1, "could not locate the password-mint block"
    mint = entrypoint[start:end]
    # Strip comment lines: the code must not USE $RANDOM, but a comment may name it
    # to document WHY it is excluded. Checking the raw text would flag that comment.
    mint_code = "\n".join(
        line for line in mint.splitlines() if not line.lstrip().startswith("#")
    )
    assert "RANDOM" not in mint_code, (
        "the password mint must not fall back to Bash's non-crypto $RANDOM"
    )
    assert "exit 1" in mint_code, (
        "the mint must fail closed (abort) when no CSPRNG byte is available"
    )


def test_compose_generates_password_in_container_not_from_host() -> None:
    """docker-compose.yml must NOT pass a VNC password from the host: the container
    mints it. Passing SE_VNC_PASSWORD from the host is exactly what re-introduces
    the empty-string override that overwrites the base image's ``secret`` default.
    Only the GFMY_NOVNC_HARDEN verdict (+ TLS + SAN host) crosses the boundary.
    """

    compose = yaml.safe_load(_read("docker-compose.yml"))
    env = compose["services"][LOGIN_SERVICE].get("environment", {})
    keys = set(env) if isinstance(env, dict) else {e.split("=", 1)[0] for e in env}
    assert "SE_VNC_PASSWORD" not in keys, (
        "SE_VNC_PASSWORD must not be a compose environment key; the container mints it"
    )
    assert "GOOGLEFINDMY_NOVNC_PASSWORD" not in keys, (
        "GOOGLEFINDMY_NOVNC_PASSWORD must not be passed from the host either"
    )
    assert "GFMY_NOVNC_HARDEN" in keys, (
        "the LAN-hardening verdict must cross into the container"
    )


def test_auth_flow_prompt_reads_password_from_env() -> None:
    """The [AuthFlow] sign-in banner must show the real per-run password, read from
    GOOGLEFINDMY_NOVNC_PASSWORD, not a hardcoded ``secret`` (which was always wrong
    for a hardened LAN login).
    """

    text = Path("custom_components/googlefindmy/Auth/auth_flow.py").read_text(
        encoding="utf-8"
    )
    assert "GOOGLEFINDMY_NOVNC_PASSWORD" in text, (
        "auth_flow.py must read the noVNC password from the environment so the "
        "sign-in banner shows the real per-run value"
    )


def test_login_sh_does_no_host_side_password_crypto() -> None:
    """login.sh must not generate or export the VNC password any more (that moved
    into entrypoint.sh); it only raises the GFMY_NOVNC_HARDEN verdict for a LAN bind.
    """

    text = _read("login.sh")
    assert "gen_password" not in text, (
        "password generation moved into the container; login.sh must not mint it"
    )
    assert "export SE_VNC_PASSWORD" not in text, (
        "login.sh must not export SE_VNC_PASSWORD; the container mints it"
    )
    assert "export GFMY_NOVNC_HARDEN=1" in text, (
        "login.sh must raise the hardening verdict for a non-loopback bind"
    )


def test_compose_forwards_the_bind_for_container_side_verdict() -> None:
    """The container must SEE the bind so it can derive the LAN-hardening verdict on
    the direct ``docker compose run`` path (which runs no launcher). Without
    GFMY_NOVNC_BIND in the container environment, a direct LAN bind would publish
    port 7900 with the fixed ``secret`` over plain HTTP -- the fixed-credential hole
    the launchers close. Codex flagged exactly this gap.
    """

    compose = yaml.safe_load(_read("docker-compose.yml"))
    env = compose["services"][LOGIN_SERVICE].get("environment", {})
    keys = set(env) if isinstance(env, dict) else {e.split("=", 1)[0] for e in env}
    assert "GFMY_NOVNC_BIND" in keys, (
        "GFMY_NOVNC_BIND must cross into the container so entrypoint.sh can derive "
        "the hardening verdict for a direct `docker compose run` LAN bind"
    )


def _run_derivation_block(env_overrides: dict[str, str]) -> dict[str, str]:
    """Extract the ACTUAL direct-Compose derivation block from entrypoint.sh and run
    it in isolation, then report the resulting verdict env.

    This exercises the production code slice, not a reimplementation of the
    classifier: a behavioural test that rebuilt the loopback/wildcard logic would
    only prove the rebuild, never the shipped block (the construct-vs-wiring trap).
    The block is self-contained (it runs before supervisord), so slicing it between
    its banner comment and the password-mint banner and feeding it a minimal env is
    faithful.
    """

    entrypoint = _read("entrypoint.sh")
    start = entrypoint.find('if [ "${GFMY_NOVNC_HARDEN:-}" != "1" ]; then')
    end = entrypoint.find("# --- Per-run noVNC password", start)
    assert start != -1 and end != -1, "could not extract the derivation block"
    block = entrypoint[start:end].rstrip()
    harness = (
        "set -e\n"
        + block
        + "\nprintf 'HARDEN=%s\\nTLS=%s\\nURL_HOST=%s\\nURL=%s\\n' "
        + '"${GFMY_NOVNC_HARDEN:-}" "${GFMY_NOVNC_TLS:-}" '
        + '"${GFMY_NOVNC_URL_HOST:-}" "${GOOGLEFINDMY_NOVNC_URL:-}"\n'
    )
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
    # Mirror docker-compose.yml: it builds GOOGLEFINDMY_NOVNC_URL from URL_HOST,
    # defaulting the host to "localhost" when nothing set it on the host side.
    url_host_in = env_overrides.get("GFMY_NOVNC_URL_HOST", "") or "localhost"
    env["GOOGLEFINDMY_NOVNC_URL"] = f"http://{url_host_in}:7900"
    env.update(env_overrides)
    proc = subprocess.run(
        ["bash", "-c", harness],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"derivation block failed: {proc.stderr}"
    out: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        key, _, value = line.partition("=")
        out[key] = value
    return out


# (bind-env, extra-env), expected HARDEN, expected TLS, expected URL_HOST
_DERIVATION_CASES = [
    # Loopback (or the compose empty-string default): never harden, clear stray TLS.
    ("compose-empty-bind", {"GFMY_NOVNC_BIND": ""}, "", "", ""),
    ("loopback-127-1", {"GFMY_NOVNC_BIND": "127.0.0.1"}, "", "", ""),
    ("loopback-127-x", {"GFMY_NOVNC_BIND": "127.0.0.5"}, "", "", ""),
    ("loopback-localhost", {"GFMY_NOVNC_BIND": "localhost"}, "", "", ""),
    ("loopback-v6", {"GFMY_NOVNC_BIND": "::1"}, "", "", ""),
    ("loopback-v6-bracketed", {"GFMY_NOVNC_BIND": "[::1]"}, "", "", ""),
    # A stray inherited TLS on a loopback run must be cleared, not honoured.
    (
        "loopback-clears-stray-tls",
        {"GFMY_NOVNC_BIND": "127.0.0.1", "GFMY_NOVNC_TLS": "1"},
        "",
        "",
        "",
    ),
    # Concrete LAN IP: harden + TLS, adopt the bind as SAN and browse host.
    ("lan-ip-192", {"GFMY_NOVNC_BIND": "192.168.1.21"}, "1", "1", "192.168.1.21"),
    ("lan-ip-10", {"GFMY_NOVNC_BIND": "10.0.0.5"}, "1", "1", "10.0.0.5"),
    # Wildcard: harden (password) but no SAN, so TLS stays off and URL_HOST empty.
    ("wildcard-v4", {"GFMY_NOVNC_BIND": "0.0.0.0"}, "1", "", ""),
    ("wildcard-v6", {"GFMY_NOVNC_BIND": "::"}, "1", "", ""),
    # Explicit TLS opt-out on a LAN bind: password stays, transport goes plain.
    (
        "lan-ip-opt-out",
        {"GFMY_NOVNC_BIND": "192.168.1.21", "GFMY_NOVNC_TLS": "0"},
        "1",
        "",
        "192.168.1.21",
    ),
    # Wildcard WITH an explicit URL_HOST: a SAN exists, so TLS is honoured.
    (
        "wildcard-explicit-host",
        {"GFMY_NOVNC_BIND": "0.0.0.0", "GFMY_NOVNC_URL_HOST": "192.168.1.21"},
        "1",
        "1",
        "192.168.1.21",
    ),
    # Concrete bracketed IPv6: the classifier strips the brackets, but URL_HOST and
    # the browse URL must KEEP them (the URL-correct form). Regression guard for the
    # bracket-strip-vs-keep split, the trickiest path.
    (
        "lan-ip-v6-bracketed",
        {"GFMY_NOVNC_BIND": "[fd00::5]"},
        "1",
        "1",
        "[fd00::5]",
    ),
    # Concrete bind IP WITH a preset URL_HOST: the -z guard must not overwrite the
    # explicit host nor rebuild the URL (Browse-Host = the operator's chosen host).
    (
        "lan-ip-preset-host",
        {"GFMY_NOVNC_BIND": "192.168.1.21", "GFMY_NOVNC_URL_HOST": "myhost.lan"},
        "1",
        "1",
        "myhost.lan",
    ),
    # Bracketed wildcard: login.sh lists "[::]" explicitly; entrypoint strips to "::".
    ("wildcard-v6-bracketed", {"GFMY_NOVNC_BIND": "[::]"}, "1", "", ""),
    # Hostname bind (GFMY_NOVNC_BIND allows names, unlike --ip): non-loopback, adopt.
    ("lan-hostname", {"GFMY_NOVNC_BIND": "myhost.lan"}, "1", "1", "myhost.lan"),
    # Literal "*" wildcard: pins the quoted-literal case semantics (never a catch-all).
    ("wildcard-star", {"GFMY_NOVNC_BIND": "*"}, "1", "", ""),
]


@pytest.mark.parametrize(
    "label,env_overrides,exp_harden,exp_tls,exp_url_host",
    _DERIVATION_CASES,
    ids=[c[0] for c in _DERIVATION_CASES],
)
def test_entrypoint_derivation_hardens_every_network_reachable_bind(
    label: str,
    env_overrides: dict[str, str],
    exp_harden: str,
    exp_tls: str,
    exp_url_host: str,
) -> None:
    """Behaviourally run the shipped derivation block over the bind type-space. The
    security invariant: EVERY network-reachable bind (concrete LAN IP or wildcard)
    ends with GFMY_NOVNC_HARDEN=1 (so the password is minted), and NO loopback bind
    does (so the plain ``secret`` default is preserved byte-for-byte). Codex flagged
    the direct-Compose path publishing 7900 with the fixed ``secret``; this pins the
    fix across the whole input space, not just one grep-able line.
    """

    out = _run_derivation_block(env_overrides)
    assert out["HARDEN"] == exp_harden, (
        f"{label}: HARDEN {out['HARDEN']!r} != {exp_harden!r}"
    )
    assert out["TLS"] == exp_tls, f"{label}: TLS {out['TLS']!r} != {exp_tls!r}"
    assert out["URL_HOST"] == exp_url_host, (
        f"{label}: URL_HOST {out['URL_HOST']!r} != {exp_url_host!r}"
    )
    # WICHTIG-2 regression pin: when a concrete bind IP is adopted as the SAN, the
    # printed browse URL must name that SAME host, never the compose "localhost"
    # default -- otherwise the banner promises https://localhost while the cert
    # certifies the LAN IP (a mismatched, broken promise).
    if exp_url_host and exp_url_host not in ("0.0.0.0", "::"):
        assert exp_url_host in out["URL"], (
            f"{label}: browse URL {out['URL']!r} must name the SAN host {exp_url_host!r}"
        )
        if exp_tls == "1":
            assert "localhost" not in out["URL"], (
                f"{label}: browse URL {out['URL']!r} must not keep the localhost default"
            )


def test_entrypoint_derivation_never_overrides_a_launcher_verdict() -> None:
    """When a launcher already raised GFMY_NOVNC_HARDEN=1, the container-side
    derivation must NOT run at all -- so an intentional launcher --no-tls (HARDEN=1,
    TLS="") survives, and a launcher LAN run is never downgraded. This pins the outer
    guard behaviourally, not by grep.
    """

    # Launcher --no-tls on a LAN bind: HARDEN=1, TLS empty. Must stay untouched even
    # though the bind itself is non-loopback (the derivation would otherwise raise TLS).
    out = _run_derivation_block(
        {
            "GFMY_NOVNC_HARDEN": "1",
            "GFMY_NOVNC_TLS": "",
            "GFMY_NOVNC_BIND": "192.168.1.21",
        }
    )
    assert out["HARDEN"] == "1", "launcher HARDEN verdict must be preserved"
    assert out["TLS"] == "", "launcher --no-tls (TLS empty) must not be overridden to 1"


# --- Language and keyboard (the login has to be usable, not only secure) -------
#
# Two mandatory-but-invisible parts of a first login: the sign-in page has to be
# readable, and the "@" of the account name has to be typeable. Both were broken
# in the container -- English for everyone, and no "@" at all on an AltGr layout,
# which makes the login impossible rather than merely awkward.


def _launcher_env_probe(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    """A launcher sandbox whose ``docker`` stub reports its ENVIRONMENT.

    The shared sandbox stub echoes the command line, which is the wrong window
    for these guards: the launcher passes the language through the environment,
    where compose interpolates it. So this stub prints the variables instead.
    """

    work, env = _launcher_sandbox(tmp_path)
    stub = tmp_path / "bin" / "docker"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        'printf "PROBE GFMY_LOCALE=[%s]\\n" "${GFMY_LOCALE-unset}"\n'
        'printf "PROBE GFMY_KEYBOARD_FIX=[%s]\\n" "${GFMY_KEYBOARD_FIX-unset}"\n'
        'printf "PROBE GFMY_KEYBOARD_LAYOUT=[%s]\\n" "${GFMY_KEYBOARD_LAYOUT-unset}"\n',
        "utf-8",
    )
    stub.chmod(0o755)
    return work, env


def _probe_launcher(tmp_path: Path, extra_env: dict[str, str]) -> dict[str, str]:
    bash = shutil.which("bash")
    if bash is None:  # pragma: no cover - CI images all ship bash
        pytest.skip("bash is required to run the launcher")

    work, env = _launcher_env_probe(tmp_path)
    proc = subprocess.run(  # noqa: S603 - fixed argv, test-local stub PATH
        [bash, str(work / "login.sh")],
        capture_output=True,
        timeout=60,
        check=False,
        env={**env, **extra_env},
        stdin=subprocess.DEVNULL,
    )
    assert proc.returncode == 0, proc.stderr.decode()
    found: dict[str, str] = {}
    for line in proc.stdout.decode().splitlines():
        if line.startswith("PROBE "):
            name, _, value = line[len("PROBE ") :].partition("=")
            found[name] = value.strip()[1:-1]
    assert found, "the docker stub reported no environment"
    return found


def test_login_sh_hands_the_users_own_language_to_the_container(
    tmp_path: Path,
) -> None:
    """The sign-in page should arrive in the language the user reads.

    Behavioural rather than textual: the value has to survive as an *exported*
    variable, since that is the only channel compose interpolates. A guard that
    merely greps for the assignment would pass on a missing ``export``.
    """
    probe = _probe_launcher(tmp_path, {"LANG": "fr_FR.UTF-8"})

    assert probe["GFMY_LOCALE"] == "fr_FR.UTF-8"


def test_an_explicit_language_wins_over_the_shell_locale(tmp_path: Path) -> None:
    """Opting out has to be possible, and ``GFMY_LOCALE=en`` is how."""
    probe = _probe_launcher(tmp_path, {"LANG": "fr_FR.UTF-8", "GFMY_LOCALE": "en"})

    assert probe["GFMY_LOCALE"] == "en"


def test_an_explicitly_empty_language_is_not_overwritten(tmp_path: Path) -> None:
    """An empty GFMY_LOCALE is an answer, and it must survive the host locale.

    The guide tells the reader to hand the choice back to Chrome by leaving the
    variable empty. A ``-z`` test cannot tell that apart from "never set", so on
    a host with a real ``LANG`` the deliberate opt-out silently turned into the
    host locale -- exactly the value the reader was opting out of. ``LANG`` is
    set here on purpose: without it the assertion passes on the broken code too.
    """
    probe = _probe_launcher(tmp_path, {"LANG": "fr_FR.UTF-8", "GFMY_LOCALE": ""})

    assert probe["GFMY_LOCALE"] == "", (
        "GFMY_LOCALE= is a stated absence of a preference; replacing it with the "
        "host locale takes the choice back off the user"
    )


def test_the_posix_locale_reaches_the_container_untouched(tmp_path: Path) -> None:
    """Half one of the cross-platform opt-out, as a characterisation test.

    ``GFMY_LOCALE=C`` is the spelling the guide gives Windows users, because
    cmd.exe deletes a variable that is set to nothing and therefore cannot
    express the empty case at all. For that to work the launcher must pass the
    value ON rather than substitute the host locale, and the container must then
    read it as no preference (half two, ``test_chrome_driver.py``).

    Stated so it is not mistaken for a regression guard: this half is green on
    the old launcher too, because a non-empty value never entered the branch the
    fix changed. It is here to pin the END of the chain the guide promises, not
    to catch the bug the sibling test catches.
    """
    probe = _probe_launcher(tmp_path, {"LANG": "fr_FR.UTF-8", "GFMY_LOCALE": "C"})

    assert probe["GFMY_LOCALE"] == "C"


def test_both_halves_agree_on_what_counts_as_no_preference(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The launcher and the container must mean the SAME thing by "no locale".

    ``login.sh`` claims it drops a locale-free shell locale "by the same rule as"
    the container's, and the guide sells ``C`` as an opt-out that works on either
    side. Nothing pinned that: the two live in different languages (a ``case``
    arm and a Python regex), so extending one and forgetting the other would be
    silent -- a value the launcher forwards but the container warns about, or the
    reverse. This runs both over the same inputs and asserts they agree.

    What the container half asserts is the SILENCE, not the ``None``. ``None``
    alone is tautological here: ``C`` and ``POSIX`` are no language tags either
    way, so every one of these values already ended at ``None`` before the rule
    existed -- through the warning branch. Deleting the rule would put the
    "expected a language tag" warning back on the very spelling the guide sells
    as the opt-out, and a ``is None`` check would not notice.

    Extent today, stated so it is not mistaken for equivalence: twelve values,
    the six POSIX spellings below and six real locales. Drift OUTSIDE that set
    stays invisible -- widening the ``case`` arm to lower case without widening
    the regex, say, would keep this green. Widen both lists when you widen
    either rule. The real-locale half is a boundary probe rather than a
    regression guard: those values were forwarded before the rule too.
    """
    from custom_components.googlefindmy.chrome_driver import _login_locale

    neutral = ["C", "POSIX", "C.UTF-8", "POSIX.UTF-8", "C@euro", "POSIX@x"]
    real = ["de_DE.UTF-8", "fr", "ca", "cs", "cy", "pt_BR@euro"]

    def _case(label: str, index: int) -> Path:
        # A sandbox is built with mkdir(), so it is single-use; every value gets
        # its own root rather than loosening the shared helper for this one test.
        root = tmp_path / f"{label}{index}"
        root.mkdir()
        return root

    # Grip check for the silence assertions below: without it, every
    # `assert not caplog.text` would also pass when NO message reaches caplog at
    # all (propagation switched off, a logging setup in a future conftest). A
    # known-bad value therefore has to warn visibly first.
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        assert _login_locale({"GOOGLEFINDMY_LOGIN_LOCALE": "German"}) is None
    assert caplog.text, (
        "no warning reached caplog for a value that must warn; the silence "
        "assertions below would be vacuous"
    )

    for index, value in enumerate(neutral):
        probe = _probe_launcher(_case("neutral", index), {"LANG": value})
        assert probe["GFMY_LOCALE"] == "", (
            f"the launcher forwards {value!r} as a preference while the container "
            "reads it as none; the two rules have drifted apart"
        )
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            assert _login_locale({"GOOGLEFINDMY_LOGIN_LOCALE": value}) is None
        assert not caplog.text, (
            f"the launcher drops {value!r} without a word while the container "
            f"calls it a broken setting; got {caplog.text!r}"
        )

    for index, value in enumerate(real):
        probe = _probe_launcher(_case("real", index), {"LANG": value})
        assert probe["GFMY_LOCALE"] == value, (
            f"the launcher swallowed {value!r}, which is a real locale"
        )
        assert _login_locale({"GOOGLEFINDMY_LOGIN_LOCALE": value}) is not None


def test_the_guide_gives_windows_an_opt_out_it_can_actually_type() -> None:
    """cmd.exe has no empty-but-defined state, so the guide must not imply one.

    ``set GFMY_LOCALE=`` REMOVES the name on Windows, after which login.cmd asks
    the OS for the culture -- the opposite of opting out. The guide therefore has
    to name ``C`` for that shell, and login.cmd has to say why at the code.
    """
    readme = _read("README.md")
    windows_row = next(
        (line for line in readme.splitlines() if "`cmd.exe` (`login.cmd`)" in line),
        None,
    )
    assert windows_row is not None, (
        "the language section must tell Windows users how to opt out at all"
    )
    assert "`set GFMY_LOCALE=C`" in windows_row, (
        f"cmd.exe cannot express the empty case; got {windows_row!r}"
    )

    # Anchored at the branch it explains, not "somewhere in the file": the
    # whole-file form certifies its own opposite. Replacing the culture lookup
    # with a hard `set GFMY_LOCALE=C` would put every Windows user back on
    # English, silently, while leaving that literal in place. Same reasoning as
    # the verbatim-output rule in AGENTS.md.
    lines = _read("login.cmd").splitlines()

    def _is_comment(line: str) -> bool:
        # Blank lines count, so a paragraph break between the note and the
        # branch does not fail a launcher that is in fact correct. `REM` in
        # capitals is valid cmd.exe and must not fail it either.
        stripped = line.strip()
        return (
            not stripped
            or stripped.lower().startswith(("rem", "@rem"))
            or stripped.startswith("::")
        )

    branch = next(
        (
            i
            for i, line in enumerate(lines)
            if line.lstrip().lower().startswith("if not defined gfmy_locale")
        ),
        None,
    )
    assert branch is not None, (
        "login.cmd must still ask Windows for the culture when the user said "
        "nothing -- without that branch there is no default left to opt out of"
    )
    body_end = next(
        (i for i in range(branch + 1, len(lines)) if lines[i].strip() == ")"),
        None,
    )
    assert body_end is not None, "the culture branch must be a closed block"
    body = "\n".join(lines[branch + 1 : body_end])
    # The `if` line alone proves nothing: a body of `set "GFMY_LOCALE=en"` keeps
    # it, and puts every Windows user on a fixed language. What has to hold is
    # that the branch ASKS the OS and assigns what came back.
    assert "Get-Culture" in body, (
        f"the branch has to ask Windows for the culture; got body {body!r}"
    )
    # `%%[a-zA-Z]` rather than the current letter: renaming the FOR variable is
    # a stylistic change and must not fail a launcher that still does the right
    # thing. What matters is that the assignment comes FROM the loop.
    assert re.search(r'set\s+"?GFMY_LOCALE=%%[a-zA-Z]"?', body), (
        f"the branch must assign what Windows reported, not a fixed value; "
        f"got body {body!r}"
    )
    start = branch
    while start > 0 and _is_comment(lines[start - 1]):
        start -= 1
    lead_in = "\n".join(lines[start:branch])
    # Both quoting forms: login.cmd writes `set "VAR=..."` almost everywhere, so
    # matching only the bare spelling would fail on a stylistic tidy-up.
    assert re.search(r'set\s+"?GFMY_LOCALE=C"?', lead_in), (
        "the workaround has to be recorded in the comment block directly above "
        "the branch it works around, where the next reader of that branch is"
    )
    # Anchored at the START of the statement: a help line that merely mentions
    # `set GFMY_LOCALE=C` is documentation, not a default, and must not trip a
    # guard whose whole point is to protect that documentation.
    assigned = [
        line
        for line in lines
        if not _is_comment(line) and re.match(r'\s*@?set\s+"?GFMY_LOCALE=C"?\s*$', line)
    ]
    assert not assigned, (
        f"C is the user's opt-out, not the launcher's default; got {assigned!r}"
    )

    # The answer also has to be FINDABLE from the launcher itself, not only in
    # the guide -- `--help` is where someone whose sign-in came up in the wrong
    # language looks first, and on Windows it is the only spelling they have.
    for name in ("login.cmd", "login.sh"):
        text = _read(name).splitlines()
        start = next(
            (
                i
                for i, line in enumerate(text)
                if "Environment (see the comment" in line
            ),
            None,
        )
        assert start is not None, f"{name} --help must keep its Environment list"
        # Bounded at the list, not the file: `GFMY_LOCALE` appears in the code of
        # login.sh many times over, so a whole-file search would pass on a help
        # text that never mentions it.
        block = text[start : start + 12]
        listed = "\n".join(block)
        assert "GFMY_LOCALE" in listed, (
            f"{name} --help lists the environment but not GFMY_LOCALE. got {block!r}"
        )
        # The NAME alone is not the answer the reader came for. What has to
        # survive is the spelling that opts out, because on cmd.exe it is the
        # only one that exists.
        assert "GFMY_LOCALE=C" in listed, (
            f"{name} --help names the variable but not the C spelling that "
            f"hands the choice back. got {block!r}"
        )


@pytest.mark.parametrize("locale", ["C", "POSIX", "C.UTF-8", "C@euro", "POSIX@x"])
def test_a_locale_free_shell_expresses_no_preference(
    tmp_path: Path, locale: str
) -> None:
    """``LANG=C`` is the absence of a language, not a request for one.

    Forwarding it would make Chrome fall back anyway, but it would also make the
    logs claim a preference the user never expressed.
    """
    probe = _probe_launcher(tmp_path, {"LANG": locale})

    assert probe["GFMY_LOCALE"] == ""


@pytest.mark.parametrize(
    ("preset", "expected"),
    [({}, "1"), ({"GFMY_KEYBOARD_FIX": "0"}, "0")],
)
def test_the_keyboard_fix_is_on_unless_switched_off(
    tmp_path: Path, preset: dict[str, str], expected: str
) -> None:
    """A default of "off" would leave the AltGr layouts unable to log in at all."""
    assert _probe_launcher(tmp_path, preset)["GFMY_KEYBOARD_FIX"] == expected


def test_compose_forwards_the_language_and_keyboard_variables() -> None:
    """The launcher's work is wasted if compose does not carry it inside."""
    compose = _read("docker-compose.yml")

    assert 'GOOGLEFINDMY_LOGIN_LOCALE: "${GFMY_LOCALE:-}"' in compose, (
        "the container reads GOOGLEFINDMY_LOGIN_LOCALE (chrome_driver.py); the "
        "launcher-facing name is GFMY_LOCALE, and compose is what joins them"
    )
    assert 'GFMY_KEYBOARD_FIX: "${GFMY_KEYBOARD_FIX:-1}"' in compose
    assert 'GFMY_KEYBOARD_LAYOUT: "${GFMY_KEYBOARD_LAYOUT:-}"' in compose


def test_entrypoint_writes_the_keyboard_rc_before_starting_the_vnc_server() -> None:
    """Order is the whole property here.

    ``x11vnc`` reads ``$HOME/.x11vncrc`` once, at startup, and it is started by
    the supervisor this entrypoint launches. Writing the file afterwards would
    leave no trace in any log and no "@" on any keyboard -- the failure would
    look exactly like having no fix at all.
    """
    entrypoint = _read("entrypoint.sh")

    rc_write = entrypoint.index(".x11vncrc")
    supervisor = entrypoint.index('"${VENV_PATH}/bin/supervisord"')
    assert rc_write < supervisor, (
        "the .x11vncrc must be written before supervisord starts x11vnc"
    )
    assert "remap ISO_Level3_Shift-NoSymbol" in entrypoint, (
        "dropping the level-3 modifier is what makes AltGr characters arrive"
    )


def test_the_keyboard_opt_out_also_removes_a_file_from_an_earlier_run() -> None:
    """A switch that only works on a pristine container is not a switch.

    ``GFMY_KEYBOARD_FIX=0`` has to undo the file, not just skip writing it: the
    image layer and a reused container both survive a run.
    """
    entrypoint = _read("entrypoint.sh")

    opt_out = entrypoint.index("GFMY_KEYBOARD_FIX:-1")
    tail = entrypoint[opt_out : opt_out + 1500]
    assert "rm -f" in tail and ".x11vncrc" in tail


# --- Requested settings must not fail silently ---------------------------------
# Every switch this entrypoint offers says so when it cannot honour the request.
# The keyboard layout was the one exception: fifteen failed setxkbmap attempts
# fell through without a word, and the README points users at that very setting
# when characters do not reach the browser. These tests run the REAL shell code
# (extracted from entrypoint.sh) against stub binaries, because a text assertion
# on the warning cannot tell the fixed path from the broken one -- the string is
# present in the file either way.


_FI = re.compile(r"^fi\b")
_IF = re.compile(r"^if\b")


def _extract_shell_block(
    entrypoint: str,
    *,
    banner: str,
    opener: str,
    must_contain: tuple[str, ...],
    max_lines: int,
    label: str,
) -> str:
    """Return a verbatim block from ``entrypoint.sh``, or raise.

    Two extractors in this file learned the same lesson the expensive way, the
    second one *while fixing the first*: ending on "the next line that is exactly
    ``fi``" is not an anchor. Delete the guard, or merely append a comment to its
    ``fi``, and the search runs on -- once handing a test 63 lines including
    ``sudo chown -R ... /data``, once 74 including the backgrounded login CLI.
    A test fragment is EXECUTED, so a mis-grab is not a wrong answer, it is a
    side effect.

    Hence one helper for all of them, with three independent brakes: the end is
    found by ``if``/``fi`` balance (a trailing comment no longer hides it),
    ``must_contain`` proves the block is the one meant, and ``max_lines`` caps
    the blast radius if both are somehow satisfied by a larger stretch.
    """

    lines = entrypoint.splitlines()
    start = next((i for i, line in enumerate(lines) if line.startswith(banner)), None)
    if start is None:
        raise AssertionError(f"{label}: could not locate its opening line ({banner!r})")
    guard = next(
        (i for i in range(start, len(lines)) if lines[i].startswith(opener)), None
    )
    if guard is None:
        raise AssertionError(
            f"{label}: the guard {opener!r} is gone -- whatever it protected is "
            "unconditional now, which is exactly what these tests watch for"
        )
    depth = 0
    end = None
    for i in range(guard, len(lines)):
        stripped = lines[i].strip()
        if _IF.match(stripped):
            depth += 1
        elif _FI.match(stripped):
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        raise AssertionError(
            f"{label}: its guard is never closed by a `fi` on its own line. This "
            "balance does not model a single-line `if ...; then ...; fi`; if one "
            "was introduced here, teach the balance about it rather than widening "
            "the search"
        )
    block = lines[start : end + 1]
    if len(block) > max_lines:
        raise AssertionError(
            f"{label}: grabbed {len(block)} lines (cap {max_lines}); refusing to "
            "run an unknown stretch of the entrypoint as a test fragment"
        )
    # Against _code_lines, not the raw block: a body replaced by
    # `# apply_keyboard_layout (disabled)` plus `:` satisfies a raw substring
    # search while the feature is gone, which is the opposite of proof.
    code = _code_lines("\n".join(block))
    missing = [
        needle for needle in must_contain if not any(needle in line for line in code)
    ]
    if missing:
        raise AssertionError(f"{label}: extracted block no longer contains {missing!r}")
    return "\n".join(block)


def _extract_display_wait(entrypoint: str) -> str:
    """Return the X-display wait, from its banner through its warning."""

    return _extract_shell_block(
        entrypoint,
        banner='echo "[entrypoint] Waiting for X display',
        opener='if [ -z "${_display_ready}"',
        must_contain=("xdpyinfo", "_display_ready", "did not come up within"),
        max_lines=30,
        label="the X-display wait",
    )


def _extract_layout_call(entrypoint: str) -> str:
    """Return the ``if [ -n ... ]; then apply_keyboard_layout; fi`` guard.

    Extracting the FUNCTION alone left the guard untested in both directions:
    deleting the condition made every login report a switch to ``''``, and
    emptying the body made the whole feature a no-op. Both stayed green.
    """

    return _extract_shell_block(
        entrypoint,
        banner='if [ -n "${GFMY_KEYBOARD_LAYOUT:-}" ]',
        opener='if [ -n "${GFMY_KEYBOARD_LAYOUT:-}" ]',
        must_contain=("apply_keyboard_layout",),
        max_lines=6,
        label="the GFMY_KEYBOARD_LAYOUT guard",
    )


def _extract_layout_feature(entrypoint: str) -> str:
    """Return definition AND guard, in the order the file defines them.

    Assembling the two in an order the test picks would pin neither: with the
    definition moved below its caller, bash reaches `apply_keyboard_layout:
    command not found`, exits 127, and `set -e` ends the login -- while a test
    that concatenates them itself stays green.
    """

    definition = _extract_shell_function(entrypoint, "apply_keyboard_layout")
    call = _extract_layout_call(entrypoint)
    if entrypoint.index(definition) > entrypoint.index(call):
        raise AssertionError(
            "apply_keyboard_layout is defined AFTER its caller; under `set -e` "
            "that aborts the login with 127 instead of setting a layout"
        )
    return f"{definition}\n{call}"


def _stub_bin(tmp_path: Path, name: str, body: str) -> Path:
    """Drop an executable stub into ``tmp_path/bin`` and return that directory."""

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    stub = bin_dir / name
    stub.write_text(f"#!/usr/bin/env bash\n{body}", encoding="utf-8")
    stub.chmod(0o755)
    return bin_dir


def _run_shell_fragment(
    fragment: str, call: str, bin_dir: Path, env: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    """Run an extracted fragment under ``set -e`` with ``bin_dir`` first on PATH."""

    script = f"#!/usr/bin/env bash\nset -e\n{fragment}\n{call}\necho REACHED_END\n"
    return subprocess.run(  # noqa: S603 - fixed argv, test-local stub PATH
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", **env},
        check=False,
    )


def _layout_run(
    tmp_path: Path, setxkbmap_body: str | None, layout: str = "de"
) -> subprocess.CompletedProcess[str]:
    bin_dir = _stub_bin(tmp_path, "sleep", "exit 0\n")  # no-op: 15 tries cost 0s
    if setxkbmap_body is not None:
        _stub_bin(tmp_path, "setxkbmap", setxkbmap_body)
    fragment = _extract_shell_function(_read("entrypoint.sh"), "apply_keyboard_layout")
    return _run_shell_fragment(
        fragment,
        "apply_keyboard_layout",
        bin_dir,
        {"HOME": str(tmp_path), "DISPLAY": ":99", "GFMY_KEYBOARD_LAYOUT": layout},
    )


def test_an_exhausted_keyboard_layout_retry_warns_instead_of_falling_through(
    tmp_path: Path,
) -> None:
    """Fifteen failed attempts must end in a warning, not in silence.

    This is the defect: setxkbmap installed, every invocation failing (unknown
    layout, or the bounded X wait expired), and the login continuing on the
    default layout with nothing on screen to say the requested fallback was
    dropped. The warning must also survive ``set -e`` -- aborting the login over
    a convenience setting would be worse than the keyboard problem it fixes.
    """

    proc = _layout_run(
        tmp_path,
        'echo "Error loading new keyboard description" >&2\nexit 1\n',
        layout="de -variant nope",
    )

    assert "REACHED_END" in proc.stdout, (
        "the layout helper must not abort the script under `set -e`; it is "
        f"best-effort by contract (stdout={proc.stdout!r} stderr={proc.stderr!r})"
    )
    assert "could not apply the requested keyboard layout" in proc.stderr, (
        "an exhausted retry loop must warn; falling through silently is the "
        f"reported defect (stderr={proc.stderr!r})"
    )
    assert "de -variant nope" in proc.stderr, (
        "the warning must name the value that was refused, or the user cannot "
        "tell which of several settings was dropped"
    )
    assert "clipboard panel" in proc.stderr, (
        "the warning must name the way out, like the sibling ~/.x11vncrc warning "
        "does; a warning without a next step only tells the user they are stuck"
    )


def test_the_layout_warning_reports_what_setxkbmap_actually_said(
    tmp_path: Path,
) -> None:
    """The captured cause is the whole point of capturing stderr.

    ``Cannot open display`` (the X wait lost its race, retrying may help) and
    ``Error loading new keyboard description`` (the value is wrong, retrying
    never will) call for opposite reactions. Discarding stderr -- the previous
    ``2>/dev/null`` -- leaves the user guessing between them.
    """

    proc = _layout_run(tmp_path, 'echo "Cannot open display :99" >&2\nexit 1\n')

    assert "setxkbmap: Cannot open display :99" in proc.stderr, (
        "the final failure's own message must reach the user, prefixed so it is "
        f"attributable (stderr={proc.stderr!r})"
    )


def test_a_layout_that_applies_late_reports_success_and_stays_quiet(
    tmp_path: Path,
) -> None:
    """A retry that eventually wins must NOT also warn.

    This is the counter-direction: an assertion that only checks "warning is
    printed on failure" is satisfied by code that warns unconditionally, which
    would make every successful login look broken. A slow X display is the
    normal reason the first attempts fail, so this path is the common one.
    """

    counter = tmp_path / "tries"
    proc = _layout_run(
        tmp_path,
        f'n=$(cat "{counter}" 2>/dev/null || echo 0)\n'
        f'n=$((n + 1)); echo "$n" > "{counter}"\n'
        'if [ "$n" -ge 3 ]; then exit 0; fi\n'
        'echo "Cannot open display :99" >&2\nexit 1\n',
    )

    assert "Keyboard layout set to 'de'." in proc.stdout, (
        f"a late success must be reported (stdout={proc.stdout!r})"
    )
    assert "could not apply" not in proc.stderr, (
        f"a successful apply must not also warn (stderr={proc.stderr!r})"
    )
    assert counter.read_text(encoding="utf-8").strip() == "3", (
        "the loop must actually retry rather than give up after one attempt"
    )


def test_a_missing_setxkbmap_keeps_its_own_distinct_warning(tmp_path: Path) -> None:
    """The two failure modes need different words: absent tool vs refused value.

    "install it" and "check your value" are not interchangeable advice. Without
    this the exhaustion warning could simply be moved up to cover both cases and
    every assertion above would still pass.
    """

    # The same PATH the fragment runs under (see _run_shell_fragment), not the
    # pytest process's: a setxkbmap in /usr/local/bin would skip a branch the
    # subprocess could never have reached.
    real = shutil.which("setxkbmap", path="/usr/bin:/bin")
    if real is not None:  # pragma: no cover - environment dependent
        pytest.skip("a real setxkbmap on PATH would shadow the absent-tool branch")

    proc = _layout_run(tmp_path, None)

    assert "setxkbmap is not" in proc.stderr and "installed" in proc.stderr, (
        f"the absent-tool branch must keep its own message (stderr={proc.stderr!r})"
    )
    assert "could not apply the requested keyboard layout" not in proc.stderr, (
        "the exhaustion warning must not fire when no attempt was ever made; it "
        "would send the user to check a value that was never the problem"
    )


def test_a_display_that_never_comes_up_says_so(tmp_path: Path) -> None:
    """The X wait is the root cause of every later symptom, so it must speak.

    A silent expiry here surfaces later as a keyboard layout that will not apply
    and a browser that never appears -- two symptoms, no cause. Not fatal on
    purpose: Chrome may still come up a moment later, and aborting would turn a
    slow start into a failed login.
    """

    bin_dir = _stub_bin(tmp_path, "sleep", "exit 0\n")
    _stub_bin(tmp_path, "xdpyinfo", "exit 1\n")
    fragment = _extract_display_wait(_read("entrypoint.sh"))

    proc = _run_shell_fragment(fragment, ":", bin_dir, {"DISPLAY": ":99"})

    assert "REACHED_END" in proc.stdout, (
        f"the wait must not abort the script (stderr={proc.stderr!r})"
    )
    assert "did not come up within" in proc.stderr, (
        f"an expired display wait must name itself (stderr={proc.stderr!r})"
    )
    assert "Display ready." not in proc.stdout, (
        "a display that never answered must not be reported as ready"
    )


def test_a_display_that_comes_up_stays_quiet(tmp_path: Path) -> None:
    """The counter-direction: a healthy start must produce no warning at all."""

    bin_dir = _stub_bin(tmp_path, "sleep", "exit 0\n")
    _stub_bin(tmp_path, "xdpyinfo", "exit 0\n")
    fragment = _extract_display_wait(_read("entrypoint.sh"))

    proc = _run_shell_fragment(fragment, ":", bin_dir, {"DISPLAY": ":99"})

    assert proc.stdout.count("Display ready.") == 1, (
        "the loop must stop at the first answer; without the `break` it reports "
        f"readiness once per second for the whole bound (stdout={proc.stdout!r})"
    )
    assert "did not come up" not in proc.stderr, (
        f"a ready display must not warn (stderr={proc.stderr!r})"
    )


def test_no_requested_layout_means_the_helper_is_never_entered(
    tmp_path: Path,
) -> None:
    """A user who set nothing must see nothing -- neither report nor warning."""

    bin_dir = _stub_bin(tmp_path, "sleep", "exit 0\n")
    _stub_bin(tmp_path, "setxkbmap", "exit 1\n")
    fragment = _extract_layout_feature(_read("entrypoint.sh"))

    proc = _run_shell_fragment(fragment, ":", bin_dir, {"DISPLAY": ":99"})

    assert "REACHED_END" in proc.stdout, (
        f"an unset layout must not abort the script (stderr={proc.stderr!r})"
    )
    assert proc.stderr.strip() == "", (
        "nothing was requested, so nothing may be warned about "
        f"(stderr={proc.stderr!r})"
    )
    assert "Keyboard layout set" not in proc.stdout, (
        f"no layout was requested, so none may be reported (stdout={proc.stdout!r})"
    )


def test_the_layout_warning_survives_a_failing_sed(tmp_path: Path) -> None:
    """The warning's own plumbing must not be able to kill the login.

    ``printf | sed`` sits in statement position, so under ``set -e`` a failing or
    absent ``sed`` aborts the function between the headline and the way out --
    losing exactly the advice the warning exists to give, on exactly the path
    where it is needed. Both the block's own comment and the guide promise that
    the login continues instead.
    """

    bin_dir = _stub_bin(tmp_path, "sleep", "exit 0\n")
    _stub_bin(tmp_path, "setxkbmap", 'echo "boom" >&2\nexit 1\n')
    _stub_bin(tmp_path, "sed", "exit 1\n")
    fragment = _extract_shell_function(_read("entrypoint.sh"), "apply_keyboard_layout")

    proc = _run_shell_fragment(
        fragment,
        "apply_keyboard_layout",
        bin_dir,
        {"HOME": str(tmp_path), "DISPLAY": ":99", "GFMY_KEYBOARD_LAYOUT": "de"},
    )

    assert "REACHED_END" in proc.stdout, (
        "a broken `sed` must not abort the login; the block is best-effort by "
        f"contract (stderr={proc.stderr!r})"
    )
    assert "clipboard panel" in proc.stderr, (
        "the way out must still be printed after the cause line failed to render "
        f"(stderr={proc.stderr!r})"
    )


def test_the_warnings_report_the_bound_their_loop_actually_used(
    tmp_path: Path,
) -> None:
    """Rewire each loop's bound and watch the warning follow it.

    Both bounds used to appear twice, once in ``seq`` and once as free text in
    the message, so shortening a loop left its warning asserting a wait that no
    longer happened. Rewriting the assignment in the extracted fragment proves
    the two are now one value -- measured on BOTH ends, the message AND the number
    of attempts the stub actually saw, because pinning only the message leaves the
    loop free to run a literal while the warning reports the variable.

    What this does NOT pin, stated so it is not mistaken for coverage: the
    values 15 and 30 themselves. They are operating choices, and after this
    coupling a changed value yields a truthful message rather than a false one.
    """

    layout_tries = tmp_path / "layout_tries"
    display_tries = tmp_path / "display_tries"
    bin_dir = _stub_bin(tmp_path, "sleep", "exit 0\n")
    _stub_bin(
        tmp_path,
        "setxkbmap",
        f'echo x >> "{layout_tries}"\necho "nope" >&2\nexit 1\n',
    )
    _stub_bin(tmp_path, "xdpyinfo", f'echo x >> "{display_tries}"\nexit 1\n')
    entrypoint = _read("entrypoint.sh")

    layout = _extract_shell_function(entrypoint, "apply_keyboard_layout")
    layout, subs = re.subn(r"local _tries=\d+", "local _tries=2", layout)
    assert subs == 1, "apply_keyboard_layout must take its bound from `_tries`"
    proc = _run_shell_fragment(
        layout,
        "apply_keyboard_layout",
        bin_dir,
        {"HOME": str(tmp_path), "DISPLAY": ":99", "GFMY_KEYBOARD_LAYOUT": "de"},
    )
    assert "after 2 attempts" in proc.stderr, (
        "the layout warning must quote the bound the loop ran, not a literal "
        f"(stderr={proc.stderr!r})"
    )
    assert re.search(r"\b1[0-9]\b", proc.stderr) is None, (
        f"a stale literal survived in the layout warning (stderr={proc.stderr!r})"
    )
    assert layout_tries.read_text(encoding="utf-8").count("x") == 2, (
        "the LOOP must take its bound from `_tries` too. Reading only the message "
        "leaves the other half free: a `seq 1 15` under a rewritten assignment "
        "reports two attempts and runs fifteen"
    )

    display = _extract_display_wait(entrypoint)
    display, subs = re.subn(r"_display_wait_secs=\d+", "_display_wait_secs=2", display)
    assert subs == 1, "the X-display wait must take its bound from `_display_wait_secs`"
    proc = _run_shell_fragment(display, ":", bin_dir, {"DISPLAY": ":99"})
    assert "did not come up within 2s" in proc.stderr, (
        "the display warning must quote the bound the loop ran, not a literal "
        f"(stderr={proc.stderr!r})"
    )
    assert re.search(r"\b[2-9][0-9]s\b", proc.stderr) is None, (
        f"a stale literal survived in the display warning (stderr={proc.stderr!r})"
    )
    assert display_tries.read_text(encoding="utf-8").count("x") == 2, (
        "the display loop must take its bound from `_display_wait_secs` as well"
    )


def test_a_requested_layout_actually_reaches_setxkbmap(tmp_path: Path) -> None:
    """The whole feature, end to end: variable set, helper entered, tool called.

    Every other test here either calls ``apply_keyboard_layout`` directly, thus
    bypassing the guard, or asserts that nothing happens. Emptying the guard's
    body -- feature present, guard present, call gone -- left all of them green
    while the setting silently did nothing, which is the very shape of defect
    this pull request exists to remove.

    It also pins what the guard hands over: the value is deliberately unquoted so
    ``de -variant nodeadkeys`` arrives as three arguments, and the README
    promises exactly that. A stub that ignores its argv cannot tell the two
    apart, so this one records them.
    """

    argv = tmp_path / "argv"
    bin_dir = _stub_bin(tmp_path, "sleep", "exit 0\n")
    _stub_bin(
        tmp_path,
        "setxkbmap",
        f'printf "%s\\n" "$#" > "{argv}"\nprintf "%s\\n" "$@" >> "{argv}"\nexit 0\n',
    )
    fragment = _extract_layout_feature(_read("entrypoint.sh"))

    proc = _run_shell_fragment(
        fragment,
        ":",
        bin_dir,
        {
            "HOME": str(tmp_path),
            "DISPLAY": ":99",
            "GFMY_KEYBOARD_LAYOUT": "de -variant nodeadkeys",
        },
    )

    assert "Keyboard layout set to 'de -variant nodeadkeys'." in proc.stdout, (
        f"a requested layout must actually be applied (stdout={proc.stdout!r})"
    )
    recorded = argv.read_text(encoding="utf-8").splitlines()
    assert recorded[0] == "5", (
        "setxkbmap must receive `-display :99` plus the three words of the "
        f"layout; word splitting is deliberate here (got {recorded!r})"
    )
    assert recorded[1:] == ["-display", ":99", "de", "-variant", "nodeadkeys"], (
        f"the layout must arrive verbatim and unquoted (got {recorded!r})"
    )
