# tests/test_guard_prose_contract.py
"""Guard for the two prose rules of the repository contract.

``AGENTS.md`` states two rules about the prose a contributor writes, and until now
neither had a mechanism behind it:

* **English-only prose.** The "Language reminder" and "Language policy" paragraphs of
  ``AGENTS.md`` require inline comments, docstrings and documentation to stay English.
  Translation payloads are explicitly exempt ("Translation files remain multilingual").
  Referenced by their headings rather than by line number: this branch shifts those
  lines itself, and a line number in prose is exactly the kind of claim this guard
  otherwise asks people to ground.
* **Verifiable evidence.** ``AGENTS.md`` section "2. Mandatory evidence" requires every
  claim to cite a source *inside the project reality*. A pointer into an agent's private
  memory tree is unreachable for every human reader and is therefore not such a source.

Both are plain text rules with no linter behind them, so violations pass ``ruff``,
``mypy``, ``codespell`` and the whole test suite silently and only surface in review.
That is the gap ``test_guard_path_header`` was written for, and this guard follows its
shape: a sweep, a ``LEGACY_ALLOWLIST`` with a measured reason per entry, and a stale
test that goes red once an entry stops violating.

Why one file for two rules: both arms need the same prose extraction, which is the
non-trivial part. Two files would mean two copies of ``_prose_units`` and guaranteed
drift; the regexes are the cheap half.

Why extraction instead of a raw grep over the file: foreign-language text is legitimate
*data* here. ``tests/test_translation_placeholders.py`` asserts against the German
address form, and ``translations/*.json`` is multilingual by contract. Only separating
prose from literals makes the rule machine-checkable at all. Measured over the 503
Python sources of the sweep set, this file excluded (see the counting unit further
down): the prose arm flags 12 of 29992 prose units, while the same word list over raw text additionally hits 11 occurrences in three further
files (``map_i18n.py``, ``test_map_i18n.py``, ``test_map_view_blocking_io.py``), every
one of them translation data in a string literal.

Scope of the language arm: Python prose across the whole repository, not narrowed to
``tests/**`` like the path-header guard. That narrowing exists there because
``custom_components/**`` carries pre-existing offenders; here it carries zero
(measured), so there is no retrofit debate to avoid.

Scope of the evidence arm: Python prose plus Markdown and workflow files, because a
source is cited in documentation at least as often as in code. Measured over 553 files
(503 Python sources including type stubs, 50 Markdown and workflow): zero hits.

Markdown is deliberately **out** of scope for the language arm, although the contract
covers "documentation updates" too. ``AGENTS.md`` itself quotes the German Home
Assistant translation guideline and would need an allowlist entry from day one, which
is the shape that erodes. The gap is named here rather than left for a reader to
discover.

Detection limits, stated rather than implied. The language arm keys on German function
words that have no English homograph, so German prose built only from words outside
that set passes. That is deliberate. The wider variants were measured and rejected: an
umlaut character class flags the proper name in every copyright header and misses
umlaut-free German entirely, and a two-distinct-words threshold only passed its positive
control because the three nouns of one concrete incident had been added to the list. A
guard that cries wolf gets switched off rather than obeyed, as ``tests/AGENTS.md`` puts
it, so the arm is tuned for precision and says so.

A second note, on `codespell`: German function words look like English typos to a
dictionary checker, so the word list below and one sample near the bottom trip it, as do
the address-form checks in ``tests/test_translation_placeholders.py``. Each such line
carries a line-scoped ``codespell:ignore`` directive without a word list, because a word
list would write the German word into the file once more. The CI spelling step is
non-blocking (``continue-on-error: true``), but the pre-commit hook is not, and CI runs
the hooks as the ``pre-commit`` entry of the lint matrix, so an unmarked line turns that
entry red. Widening ``[tool.codespell] ignore-words-list`` instead would hide the real
English typos that several of those words shadow, across the whole tree. The price of
the directive: it silences every word on its line, so it belongs only on lines whose
other words are not typo candidates.

That list is a trade, not a rule, and the ``Write lint summary`` step in
``.github/workflows/ci.yml`` points a contributor at it: an entry buys quiet in the
report and pays with detection across the whole tree. It is worth it where the word
shadows nothing, and where it shadows a real typo only for a name the tree cannot avoid,
which is why ``hass`` sits on it and the words below do not. The hyphenated ``pre-empt``
family is the cheap case: the 2.4.3 dictionary maps all six members to the American
spelling, and neither half is a typo target of its own, so the entries cost only the
split itself, which two files write the other way
(``docs/TRIGGER_MECHANISMS.md``, ``tests/test_eid_resolver_executor_offload.py``). Three
of the six have no occurrence here today and are listed to close the family. The
``unparseable``, ``addressings`` and hyphenated ``re-use`` families sit on the list for
the same reason. For a single mention the sharper tool is the line-scoped directive;
the 2.2.6 build the hook used to pin knew neither that directive nor more than
``pre-empt`` of the family, which is why the hook rev now follows ``poetry.lock``.

Note this paragraph cannot name those typo pairs: this guard reads its own docstring, so
writing the German half of such a pair here turns the file red. The same reflex applies
to every explanation added below.

What would make this guard obsolete: a linter with real language detection in the
pre-commit chain, or a ``ruff`` rule for prose language. Until one exists, a word list
with measured exclusions is the cheapest thing that fails loudly.

One environment note that cost a measurement: this tree uses PEP 695 syntax, which only
parses on Python 3.12 and newer. Under an older interpreter seven modules fail to parse
and the language arm would be blind to them. ``test_every_swept_file_parses`` turns that
into a red test instead of a silent gap.
"""

from __future__ import annotations

import ast
import functools
import io
import os
import re
import subprocess
import sys
import tokenize
from pathlib import Path
from urllib.parse import unquote

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Directories the walk never descends into: version control, environments and caches.
#
# Deliberately NOT here: ProtoDecoders, Auth/firebase_messaging/proto and vendor,
# although [tool.ruff] extend-exclude lists the first two. All three hold hand-written,
# tracked Python next to generated or imported code: decoder.py and two __init__.py in
# ProtoDecoders, and vendor/openlocationcode/openlocationcode.py, which this repository
# modifies in tree. Pruning by directory took them out of the sweep three times in a
# row during review, which is why the exclusion is by file suffix now and why
# test_every_tracked_python_source_is_swept and its prose sibling hold the whole
# thing to git ls-files
# rather than to anybody's judgement about which directory is "generated".
#
# Pruning during the walk rather than filtering afterwards: the unpruned walk visits
# roughly 75k entries, nearly
# all of them in .venv and the tool caches. Order of magnitude, not a pinned figure: the
# number moves with every test run.
_EXCLUDED_DIRS = frozenset(
    {
        # version control, environments and dependency trees
        ".git",
        ".venv",
        "venv",
        "env",
        "node_modules",
        "site-packages",
        # build and tool caches; .pytest_cache/README.md was measured in the sweep
        # set before these were added, an untracked file the runner itself writes
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".hypothesis",
        "htmlcov",
        "build",
        "dist",
    }
)

# German function words with no English homograph in this tree.
_GERMAN_MARKERS = frozenset(
    {
        "aber",
        "auch",
        "auf",
        "bei",
        "beim",
        "damit",
        "dann",
        "das",
        "dass",
        "diese",
        "dieser",
        "dieses",
        "dort",
        "durch",
        "eine",
        "einem",
        "einen",
        "einer",
        "einige",
        "etwa",
        "fuer",
        "f\u00fcr",
        "gegen",
        "gegenprobe",
        "hatte",
        "hier",
        "ich",
        "ihre",
        "immer",
        "jede",
        "jeder",
        "kein",
        "keine",
        "koennen",
        "k\u00f6nnen",
        "muessen",
        "m\u00fcssen",
        "nach",
        "nicht",
        "noch",
        "ohne",
        "oder",  # codespell:ignore
        "schon",
        "sehr",
        "sein",
        "seine",
        "sich",
        "sie",  # codespell:ignore
        "sind",
        "soll",
        "ueber",
        "\u00fcber",
        "und",
        "unser",  # codespell:ignore
        "unter",  # codespell:ignore
        "vom",
        "von",
        "werde",
        "werden",
        "wie",
        "wenn",
        "weil",
        "wir",
        "wird",
        "wurde",
        "wurden",
        "zum",
        "zur",
        "zwar",
        "zwischen",
    }
)

# Words that look German but are ordinary English, established identifiers, or
# acronyms this domain uses. Subtracted from the marker set in _german_markers_in.
#
# Counting unit for every number in this file: a PROSE UNIT is one docstring or one
# comment token, and a hit is a unit containing the word. Measured with the project
# interpreter over the sweep set MINUS THIS FILE (503 Python sources, 29992 prose units
# without it). Excluding this file is not cosmetic: its own comments name the words
# below, so a figure that included it would move every time somebody edits this very
# list, and a number that cannot survive its own file being touched is not a
# measurement. Without these counts the list gets tidied up later and the guard starts
# crying wolf, so each measured entry carries one.
_HOMOGRAPH_EXCLUSIONS = frozenset(
    {
        # measured, ordinary English in this tree
        "also",  # 231
        "falls",  # 127, "falls back" / "falls through"
        "probe",  # 121, plus script/connectivity_probe.py
        "dies",  # 23, the English verb
        "der",  # 12, the DER encoding in the credential tests
        "mit",  # 5, the MIT licence header
        "wichtig",  # 1, the WICHTIG-2 ticket label, an identifier
        # measured at 0 today, kept out pre-emptively: each collides with an acronym
        # this domain really uses, exactly like der/DER above
        "des",  # DES cipher, next to FMDNCrypto/ and KeyBackup/
        "ist",  # IST timezone, in a location integration  # codespell:ignore
        "dem",  # DEM, digital elevation model, in a geo context
        "aus",  # AUS country code
        "als",  # ALS, ambient light sensor, in a Home Assistant integration  # codespell:ignore
        "vor",  # VOR, VHF omnidirectional range, in a location integration  # codespell:ignore
        # ordinary English, never measured because they were never candidates
        "die",
        "war",
        "man",
        "bald",
        "gift",
        "rat",
        "fast",
        "kind",
        "not",
        "rot",
        "tag",
        "den",
        "list",
        "band",
        "arm",
        "art",
        "hand",
    }
)

# Word boundary for the language arm. The umlaut range and the four umlaut markers above
# are written as escapes so that this file stays pure ASCII: it is the one file in the
# tree whose data is German, and keeping it ASCII means no tool, terminal or diff view
# has to agree on an encoding to render it. The escapes are semantically identical to
# the literal characters, which test_umlaut_escapes_match_their_literal_form pins.
_WORD_RE = re.compile("[A-Za-z\u00c4\u00d6\u00dc\u00e4\u00f6\u00fc\u00df]+")

# The private trees of the agent tools used on this repository. This one IS an
# enumeration and cannot be anything else: no property of a directory name marks it as
# an agent's private tree, and a generic "any dot-directory under a home" would swallow
# legitimate documentation of a user's own configuration. The criterion for adding a
# member is therefore stated rather than guessed: a directory that holds an assistant's
# own notes, plans or instructions, which no reader of this repository can open.
_AGENT_PRIVATE_DIRS = ("claude", "codex")

# Paths inside an agent's private memory tree or home configuration. No reader of this
# repository can open one, so it is never a "verifiable source inside the project
# reality". "/app/" is deliberately absent: both of its measured occurrences were
# "/app/requirements.txt", the container path this project itself uses.
# What ends a citation of the directory itself. Not a word character, because
# .claudeignore is a different name; not a separator, because a path continues; not "<",
# because <file> is this repository's placeholder spelling and not a citation; and not
# "%", because an escape there means the path goes on in encoded form.
_DIR_BOUNDARY = r"(?![\w.$<%\\/-])"

_AGENT_LOCAL_PATH = re.compile(
    r"(?<![\w/])("
    # any second level under memory/, not an enumeration: the scopes an agent memory
    # grows are not knowable from here, and an enumeration silently misses new ones
    r"memory/[\w-]+/[\w./-]+"
    # anything under one of the agent-private directories, whatever the home spelling
    # in front of it.
    # Enumerating home directories was tried and failed twice in review: first /home/
    # only, then /home/ plus /root/, and macOS, Windows and $HOME were still missing.
    # The prefix is therefore generic, over both separators, and it does not have to be
    # absolute: a relative reference is just as unopenable for a reader.
    #
    # The second lookbehind is what makes it a path segment rather than a suffix. A
    # prefix class that admits dots can otherwise be split so that the tail of a
    # directory name becomes the match, which flagged two innocent shapes (a temp
    # directory and an unrelated dotted name) before this was pinned. Requiring a
    # separator after the directory keeps the ignore-file spelling out.
    #
    # The directory ITSELF is a citation too. A home prefix followed by one of these
    # directories, with or without a trailing separator, names something no reader can
    # open just as much as a file under it, and requiring a non-empty tail let every such
    # spelling through in silence. The tail was doing two jobs, boundary and content, so
    # the boundary is explicit now. A bare mention of a directory name in prose must stay
    # silent, which is why the directory alone counts only behind a home or path prefix,
    # and why a separator is still required without one. The boundary also excludes the
    # opening angle bracket, so this repository's placeholder spelling for a file name is
    # not read as a citation, and the percent sign, because an escape right after the
    # directory means the path continues in encoded form and the decoded pass is the one
    # that should read it.
    #
    # One carrier for that boundary, not two. An extra (?![\w-]) here was measured
    # redundant: both tails already forbid a word character and a hyphen right after the
    # directory, the first by requiring a separator and the second through _DIR_BOUNDARY.
    # A red probe that removed it changed nothing, which is the definition of a second
    # carrier that can silently drift away from the first.
    r"|(?:~|\$HOME|[\w.$:-]*(?:[\\/][\w.$-]+)*)[\\/]"
    r"(?<![\w.$-])\.(?:" + "|".join(_AGENT_PRIVATE_DIRS) + r")"
    r"(?:[\\/][\w.$\\/-]+|[\\/]?" + _DIR_BOUNDARY + r")"
    r"|(?<![\w.$-])\.(?:" + "|".join(_AGENT_PRIVATE_DIRS) + r")"
    r"(?:[\\/][\w.$\\/-]+|[\\/]" + _DIR_BOUNDARY + r")"
    r")"
)

# A URI whose scheme names a local file instead of a network location. It is normalised
# before the public-URL rule below, which judges a scheme by its grammar and would
# otherwise blank this one too.
#
# The authority component is optional and is consumed with the scheme. file:/path,
# file:///path with an empty authority and file://localhost/path with a named one are
# three standard spellings of one thing (RFC 8089), and a pattern that accepted only the
# middle one let the other two through with their scheme intact, so the path pattern
# found nothing and an unreadable citation stayed silent. That is the failure direction
# a guard must not have.
_LOCAL_URI_SCHEMES = ("file",)
_FILE_URI_PREFIX = re.compile(
    r"\b(?:" + "|".join(_LOCAL_URI_SCHEMES) + r"):(?://[^/\s]*)?(?=/)",
    re.IGNORECASE,
)

# A URL a reader can follow. Two properties make it one, and both are needed: a scheme,
# matched by its grammar (RFC 3986 3.1) rather than by a list of names, AND an authority
# that is a NETWORK HOST - a bracketed IPv6 literal, a dotted name, or localhost.
#
# The list of names was written once with four entries (104c4041), and each review round
# after it found a spelling the list did not carry: an uppercase scheme, then git://.
# Dropping the list for the grammar alone was the obvious repair and it was WRONG in the
# dangerous direction: an editor URI naming a local file under one of the private
# directories was then read as a public source and blanked whole, so an unopenable
# citation passed in silence. The host test puts that back the safe way round, since such
# a URI has a single-label authority and no host. An unregistered scheme over a real host
# is public; any scheme over something that is not a host is not, and a single-label host
# fails loudly rather than silently - which is the direction this guard should fail in.
#
# The bracketed alternative is not decoration. With "]" among the terminators below, an
# IPv6 literal would otherwise cut the URL after the scheme and its path would be read as
# a private citation.
#
# The body stops at the delimiters that close a link in prose, not merely at whitespace.
# Two Markdown links written back to back have no space between them, so a run to the
# next space swallowed the second one whole, and a private citation inside it disappeared
# with the public URL. That direction is the dangerous one: the sweep stays green over a
# real violation. Declared limit in exchange: a URL with a closing parenthesis inside it
# is cut short there, and the remainder can produce a false positive. That way round
# fails loudly, which is the direction a guard should fail in.
_URL_TERMINATORS = ")]}>\"'`"
_PUBLIC_URL = re.compile(
    r"\b[A-Za-z][A-Za-z0-9+.-]*://"
    r"(?:[^\s/@]*@)?"
    r"(?:\[[0-9A-Fa-f:.]+\]|[^\s/?#\[\]]*\.[^\s/?#\[\]]+|localhost)"
    r"(?::[0-9]+)?"
    r"[^\s" + re.escape(_URL_TERMINATORS) + r"]*"
)

# Everything else that carries a scheme: an editor URI, an app URI, an unregistered
# scheme, anything whose authority is not a network host. Only the scheme and authority
# are removed, so the path behind them is judged as the local path it names. Without this
# step the path pattern cannot even see it, because its own left boundary refuses to
# start a match directly after a word character, and the last letter of such an authority
# sits exactly there - one character was enough to hide the whole citation.
_OPAQUE_URI_PREFIX = re.compile(r"\b[A-Za-z][A-Za-z0-9+.-]*://[^\s/]*")

# Files whose prose legitimately carries non-English words. One entry, with its reason.
#   tests/test_translation_placeholders.py -- that guard asserts that German UI text
#   uses the informal address form, so its prose has to name the formal pronouns it
#   rejects and quote the upstream translation guideline verbatim (12 prose units,
#   measured).
# Do not add new entries; translate the prose instead. The stale test below turns red
# once an entry stops violating, so the list keeps telling the truth about the tree.
LEGACY_ALLOWLIST: set[str] = {
    "tests/test_translation_placeholders.py",
}

# Measured baseline is zero, so this list starts empty and should stay that way: a
# source that a reader cannot open is not a source.
AGENT_LOCAL_PATH_ALLOWLIST: set[str] = set()

# Python sources the language rule covers. Type stubs are Python sources too: eleven of
# the nineteen tracked .pyi files are hand-written (gpsoauth.pyi, protobuf_typing.pyi and
# the google/protobuf stubs), so leaving them out would be a silent hole.
_PY_SUFFIXES = (".py", ".pyi")

# Generated protobuf output, excluded by file rather than by directory so that the
# hand-written modules beside it stay in the sweep.
_GENERATED_SUFFIXES = ("_pb2.py", "_pb2.pyi")
_TEXT_SUFFIXES = (".md", ".yml", ".yaml")

Offenders = dict[str, list[tuple[int, list[str]]]]


def _iter_files(root: Path, suffixes: tuple[str, ...]) -> list[tuple[Path, str]]:
    """Return (path, root-relative posix path) for every swept file under *root*."""
    found: list[tuple[Path, str]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in _EXCLUDED_DIRS)
        for name in sorted(filenames):
            if not name.endswith(suffixes) or name.endswith(_GENERATED_SUFFIXES):
                continue
            path = Path(dirpath) / name
            found.append((path, path.relative_to(root).as_posix()))
    return found


_DEFINITION = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def _definition_nodes(tree: ast.Module) -> list[ast.AST]:
    """Return every node that can carry a docstring, without walking expressions.

    ``ast.walk`` visits every node: measured 978084 visits costing 1.96 s of the 12 s
    the sweep spends, against 0.51 s here. The bigger items are ``ast.parse`` at 8.7 s
    and ``tokenize`` at 2.9 s, which this does not touch, so the honest claim is a
    modest saving, not a rewrite. Docstrings only live on modules, classes and functions, and those
    are only ever reachable through statements, so the descent stops at expression
    boundaries. Nested definitions inside if/for/while/with/try are still reached,
    which is why the descent follows statements rather than only definition bodies;
    test_extraction_reaches_a_deeply_nested_docstring pins that.
    """
    found: list[ast.AST] = []
    stack: list[ast.AST] = [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, _DEFINITION):
            found.append(node)
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.stmt, ast.excepthandler, ast.match_case)):
                stack.append(child)
    return found


def _prose_units(source: str) -> list[tuple[int, str]]:
    """Return (line, text) for every docstring and comment in *source*.

    String literals are excluded on purpose: foreign language inside a literal is test
    data or a translation payload, both of which the contract allows.
    """
    units: list[tuple[int, str]] = []
    for node in _definition_nodes(ast.parse(source)):
        doc = ast.get_docstring(node, clean=False)
        if doc:
            units.append((getattr(node, "lineno", 1), doc))
    try:
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type == tokenize.COMMENT:
                units.append((token.start[0], token.string))
    except tokenize.TokenError:  # pragma: no cover - no such file in this tree
        units.append((1, "<untokenizable>"))
    return units


def _german_markers_in(
    text: str, markers: frozenset[str] = _GERMAN_MARKERS
) -> set[str]:
    """Return the German marker words found in *text*.

    The exclusions are subtracted here **and** kept out of the marker set, which is
    redundant on purpose. Two ways to state the same rule, so that adding a measured
    homograph to the marker list by accident cannot resurrect it, and so that the
    prose about "excluded homographs" describes something the code actually does.
    test_subtraction_survives_a_word_in_both_lists pins the subtraction itself; without
    it, emptying the exclusion set is a no-op and the claim is decoration.
    """
    words = {word.lower() for word in _WORD_RE.findall(text)}
    return words & (markers - _HOMOGRAPH_EXCLUSIONS)


def _agent_local_paths_in(text: str) -> list[str]:
    """Return every agent-local path cited in *text*.

    Prose is not a list of paths, so the text is normalised before the pattern sees it.
    Two spellings would otherwise be judged by what they look like rather than by what
    they are, and both were found in review:

    A public URL is openable by any reader, whatever its path component happens to
    contain, so a link to someone else's repository that has one of these directories in
    it is a source, not a private citation. Widening the prefix to accept relative paths
    made those URLs match from the host name onwards.

    A file URI is the opposite case: it names a local path in a different spelling, and
    the extra slashes hid it from the pattern entirely. It is normalised first, because
    the public rule accepts any scheme and would otherwise consume this one as well.

    Both are handled by replacing the offending run with spaces of the same length, so
    the surrounding text keeps its shape and nothing else shifts.

    A third spelling is not a run but an encoding: percent escapes are standard inside a
    URI, and %2Ecodex is the same directory as .codex to everyone except a pattern that
    reads it literally. The text is therefore matched twice, once as written and once
    decoded, and the second result is appended to the first. Two passes rather than one
    substitution, because decoding in place would shorten the text and move every later
    citation; and an append rather than a replacement, because a decoded pass must not
    lose a path the raw text already showed. Only the second pass drops what the first
    already found, so a path written twice in one unit is still reported twice, as it
    was before this pass existed. The decoded pass is skipped when the unit carries no
    percent sign, which is an optimisation and not a rule: there is nothing to decode in
    that case.
    """
    normalised = _FILE_URI_PREFIX.sub(lambda m: " " * len(m.group()), text)
    normalised = _PUBLIC_URL.sub(lambda m: " " * len(m.group()), normalised)
    normalised = _OPAQUE_URI_PREFIX.sub(lambda m: " " * len(m.group()), normalised)
    found = [match.group(1) for match in _AGENT_LOCAL_PATH.finditer(normalised)]
    if "%" in normalised:
        found.extend(
            match.group(1)
            for match in _AGENT_LOCAL_PATH.finditer(unquote(normalised))
            if match.group(1) not in found
        )
    return found


def scan_tree(
    root: Path, only: frozenset[str] | None = None
) -> tuple[Offenders, Offenders, list[str]]:
    """Sweep *root* once and return (language hits, path hits, unparsable files).

    The root is a parameter, not the module-level constant, so the polarity probes can
    sweep a synthetic tree without touching the working copy. That is the one deliberate
    deviation from the sibling guards, and it is what lets the red probes exercise the
    wiring of extraction, iteration and exclusion rather than the regex alone.

    *only* restricts the sweep to a set of root-relative paths. The repository scan
    passes the tracked set; the synthetic probes pass nothing, because a temporary
    directory has no index to ask.
    """
    language: Offenders = {}
    paths: Offenders = {}
    unparsable: list[str] = []
    for path, rel in _iter_files(root, _PY_SUFFIXES):
        if only is not None and rel not in only:
            continue
        source = path.read_text(encoding="utf-8", errors="replace")
        try:
            units = _prose_units(source)
        except SyntaxError:
            unparsable.append(rel)
            continue
        for line, text in units:
            found = sorted(_german_markers_in(text))
            if found:
                language.setdefault(rel, []).append((line, found))
            cited = _agent_local_paths_in(text)
            if cited:
                paths.setdefault(rel, []).append((line, cited))
    for path, rel in _iter_files(root, _TEXT_SUFFIXES):
        if only is not None and rel not in only:
            continue
        for lineno, line_text in enumerate(
            path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1
        ):
            cited = _agent_local_paths_in(line_text)
            if cited:
                paths.setdefault(rel, []).append((lineno, cited))
    return language, paths, unparsable


@functools.cache
def _repo_scan() -> tuple[Offenders, Offenders, list[str]]:
    """Sweep the repository once and share the result across all tests here."""
    return scan_tree(_REPO_ROOT, _tracked_paths())


def _unallowlisted(offenders: Offenders, allowlist: set[str]) -> Offenders:
    """Drop the allowlisted files from *offenders*, keep everything else.

    Extracted rather than inlined into the two sweep tests so that the filter itself is
    testable. Inline, a mutation that drops every entry (an always-false condition) left
    both sweep tests green, because at a clean tree an empty result is the expected
    result. See test_allowlist_filter_only_removes_listed_files.
    """
    return {rel: hits for rel, hits in offenders.items() if rel not in allowlist}


# --- sweep arms -------------------------------------------------------------


def test_prose_is_english_only() -> None:
    """No German prose in a docstring or comment (AGENTS.md, the language paragraphs)."""
    language, _, _ = _repo_scan()
    offenders = _unallowlisted(language, LEGACY_ALLOWLIST)
    assert not offenders, (
        "German words found in docstrings or comments; AGENTS.md requires English "
        "prose. Translate the text, or, if the word is ordinary English here, add it "
        f"to _HOMOGRAPH_EXCLUSIONS with its measured hit count: {offenders}"
    )


def test_prose_cites_no_agent_local_paths() -> None:
    """Evidence must be openable by a reader of this repository (AGENTS.md, section 2)."""
    _, paths, _ = _repo_scan()
    offenders = _unallowlisted(paths, AGENT_LOCAL_PATH_ALLOWLIST)
    assert not offenders, (
        "Prose cites a path inside an agent's private memory tree. No reader can open "
        "it, so it is not a verifiable source. Commit a redacted artefact under docs/ "
        f"and cite that instead: {offenders}"
    )


def test_every_swept_file_parses() -> None:
    """A file the sweep cannot parse is an unmeasured file, not a clean one.

    This is the interpreter guard: under Python 3.11 the PEP 695 modules of this tree
    fail to parse and the language arm would go silently blind on them.
    """
    _, _, unparsable = _repo_scan()
    assert not unparsable, (
        "Files the guard could not parse, so they were never checked. Run the suite on "
        f"the interpreter pinned by the project (PEP 695 needs 3.12+): {unparsable}"
    )


# --- allowlist hygiene ------------------------------------------------------


def _git_ls_files(patterns: list[str], label: str) -> set[str]:
    """Return the tracked paths matching *patterns*, or raise.

    Every failure mode is fail-closed on purpose. A missing git, a non-zero exit, an
    empty result or an undecodable name all raise, because a silent skip here would look
    exactly like full coverage, which is the shape of every finding in this file's
    history. The timeout is part of that: without it a hung index lock stalls the suite
    instead of reporting anything.
    """
    result = subprocess.run(  # noqa: S603
        ["git", "ls-files", "-z", *patterns],
        cwd=_REPO_ROOT,
        capture_output=True,
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", "replace").strip()
        raise AssertionError(
            f"git ls-files failed, so nothing was measured about {label}. A silent "
            f"fallback here would look identical to full coverage: {stderr}"
        )
    # Bytes, not text=True: universal newlines would rewrite a carriage return inside a
    # file name and every comparison against the walk would then miss a file that is in
    # fact swept. -z already removes the quoting question for names with spaces.
    raw = result.stdout.decode("utf-8", "surrogateescape")
    tracked = {name for name in raw.split("\0") if name}
    assert tracked, f"git ls-files returned no {label}, which cannot be right"
    return tracked


@functools.cache
def _tracked_paths() -> frozenset[str]:
    """Every tracked path in the repository, as root-relative posix strings.

    The repository sweep is restricted to these. Walking the working copy instead would
    make the result depend on whatever a developer happens to have lying around:
    .gitignore reserves .plans/, .bootstrap/ and .wheelhouse/, and an agent-local
    citation in one of those would fail this suite for its author alone, over a file
    that can never enter a commit. Pruning those three names would be the same
    enumeration this file has already lost four rounds to; asking git is not.
    """
    return frozenset(_git_ls_files([], "tracked files"))


def _assert_tracked_files_are_swept(suffixes: tuple[str, ...], label: str) -> None:
    """Fail unless every tracked, non-generated file with *suffixes* is in the sweep.

    Every failure mode of the measurement is fail-closed on purpose. A missing git, a
    non-zero exit, an empty result or an undecodable name all raise, because a silent
    skip here would look exactly like full coverage, which is the shape of every
    finding in this file's history. The timeout is part of that: without it a hung
    index lock stalls the suite instead of reporting anything.
    """
    tracked = _git_ls_files([f"*{suffix}" for suffix in suffixes], label)
    swept = {rel for _, rel in _iter_files(_REPO_ROOT, suffixes)}
    unswept = sorted(
        name for name in tracked - swept if not name.endswith(_GENERATED_SUFFIXES)
    )
    assert not unswept, (
        f"Tracked, hand-written {label} outside the sweep. Prose in them can violate "
        "the contract with this guard green; exclude generated files by suffix rather "
        f"than pruning their directory: {unswept}"
    )


def test_every_tracked_python_source_is_swept() -> None:
    """The sweep must cover every tracked Python source except generated bindings.

    Three review rounds each found one more hand-written file hidden behind a directory
    exclusion (decoder.py, the vendor __init__, then openlocationcode.py). Fixing them
    one at a time invites a fourth, so the coverage itself is asserted here: whatever is
    tracked and not generated has to be in the sweep set. This is the structural answer
    to that class, not another special case.
    """
    _assert_tracked_files_are_swept(_PY_SUFFIXES, "Python sources")


def test_every_tracked_text_source_is_swept() -> None:
    """The same coverage claim for the prose half of the sweep.

    Holding only the Python half to git ls-files would have left the shape of the
    original finding intact for Markdown and workflow files: a tracked file behind a
    directory exclusion, prose in it free to violate the contract, and the guard green.
    The two halves use different suffix sets, so they need two measurements.
    """
    _assert_tracked_files_are_swept(_TEXT_SUFFIXES, "prose sources")


def test_path_allowlist_has_no_stale_entries() -> None:
    """Symmetry with the language allowlist: an entry that stopped violating must go.

    The list is empty today, which is exactly when the check is cheap to add. Without
    it a future entry could outlive its reason and quietly exempt a file forever.
    """
    _, paths, _ = _repo_scan()
    known = {rel for _, rel in _iter_files(_REPO_ROOT, _PY_SUFFIXES)}
    known |= {rel for _, rel in _iter_files(_REPO_ROOT, _TEXT_SUFFIXES)}
    stale = sorted(
        rel
        for rel in AGENT_LOCAL_PATH_ALLOWLIST
        if rel not in known or rel not in paths
    )
    assert not stale, (
        "Stale AGENT_LOCAL_PATH_ALLOWLIST entries (the file cites no agent-local path "
        f"now, or is gone); remove them so the list stays truthful: {stale}"
    )


def test_language_allowlist_has_no_stale_entries() -> None:
    """An allowlisted file that stopped violating, or vanished, must leave the list."""
    language, _, _ = _repo_scan()
    known = {rel for _, rel in _iter_files(_REPO_ROOT, _PY_SUFFIXES)}
    stale = sorted(
        rel for rel in LEGACY_ALLOWLIST if rel not in known or rel not in language
    )
    assert not stale, (
        "Stale LEGACY_ALLOWLIST entries (the file is clean now, or gone); remove them "
        f"so the list keeps telling the truth about the tree: {stale}"
    )


def test_allowlist_filter_only_removes_listed_files() -> None:
    """The filter must remove the listed file and nothing else.

    At a clean tree both sweep tests expect an empty result, so a filter that drops
    everything is indistinguishable from a filter that works. This probe supplies its
    own offenders and therefore does discriminate.
    """
    sample: Offenders = {"a.py": [(1, ["und"])], "b.py": [(2, ["nicht"])]}
    assert _unallowlisted(sample, {"a.py"}) == {"b.py": [(2, ["nicht"])]}
    assert _unallowlisted(sample, set()) == sample
    assert _unallowlisted(sample, {"a.py", "b.py"}) == {}


def test_homograph_exclusions_stay_out_of_the_marker_set() -> None:
    """The measured English homographs must never re-enter the marker set."""
    collision = sorted(_GERMAN_MARKERS & _HOMOGRAPH_EXCLUSIONS)
    assert not collision, (
        "These words were measured as ordinary English in this tree and would make the "
        f"guard cry wolf: {collision}"
    )


def test_guard_file_is_itself_swept() -> None:
    """No self-exemption: this file is in the sweep set like any other."""
    swept = {rel for _, rel in _iter_files(_REPO_ROOT, _PY_SUFFIXES)}
    assert "tests/test_guard_prose_contract.py" in swept
    assert "tests/test_guard_prose_contract.py" not in LEGACY_ALLOWLIST


# --- polarity probes --------------------------------------------------------
#
# Every probe keeps its offending text in a string literal or under tmp_path. A
# violation planted in the working tree would not be reproducible and could be
# committed by accident.

_GERMAN_DOCSTRING_SAMPLE = '''"""Behavioral tests for something.

PROVENANCE: extracted from the recorder database. Erhebungsweg, Vorbehalte und
Rohwerte are documented elsewhere.
"""
'''

# Comment-only. Four of the five markers this guard was written for lived in comments,
# not docstrings, so the comment arm needs a probe that fails without it. Sharing one
# sample between both arms hid that: tokenize could be removed and everything stayed
# green.
_GERMAN_COMMENT_SAMPLE = '''"""Plain English module docstring."""

# Kurz notiert: dieser Kommentar gehoert nicht in ein englisches Repository.
VALUE = 1
'''

# Nested one level down inside a conditional, to pin that the descent follows
# statements and not only definition bodies.
_GERMAN_NESTED_SAMPLE = '''"""Plain English module docstring."""

if True:
    def helper() -> None:
        """Diese Erklaerung ist auf Deutsch und gehoert uebersetzt."""  # codespell:ignore
'''

_AGENT_PATH_SAMPLE = (
    '"""Behavioral tests for something.\n\n'
    "Source: memory/projects/some-project/quellen/measurement.md\n"
    '"""\n'
)

_AGENT_PATH_COMMENT_SAMPLE = '''"""Plain English module docstring."""

# Raw values: ~/.claude/projects/notes.md
VALUE = 1
'''

_CLEAN_SAMPLE = '''"""Behavioral tests for something.

Collection method, caveats and raw values: docs/ACCURACY_GATE_FIELD_DATA.md
"""

# Nothing to see here, plain English only.
VALUE = 1
'''


def test_language_detector_flags_the_incident_phrasing() -> None:
    """Red probe, detector level: the phrasing that caused this guard must trip."""
    found = {
        word
        for _, text in _prose_units(_GERMAN_DOCSTRING_SAMPLE)
        for word in _german_markers_in(text)
    }
    assert found, "the incident docstring must produce at least one German marker"


def test_language_detector_does_not_depend_on_incident_specific_nouns() -> None:
    """The detector must not be overfitted to the one incident it came from.

    An earlier draft only passed its positive control because the three nouns of the
    incident had been added to the marker set. Strip any such noun and the phrase must
    still trip, here through a function word.
    """
    incident_nouns = {"erhebungsweg", "vorbehalte", "rohwerte"}
    assert not (_GERMAN_MARKERS & incident_nouns), (
        "the incident nouns must never enter the marker set; with them in it, the probe "
        "below passes for the wrong reason and the guard only recognises its own origin"
    )
    lean = _GERMAN_MARKERS - incident_nouns
    found = {
        word
        for _, text in _prose_units(_GERMAN_DOCSTRING_SAMPLE)
        for word in _german_markers_in(text, lean)
    }
    assert found, "the phrase must trip on function words alone, not on incident nouns"


def test_language_detector_passes_clean_prose() -> None:
    """Green probe, detector level: English prose must not trip."""
    found = {
        word
        for _, text in _prose_units(_CLEAN_SAMPLE)
        for word in _german_markers_in(text)
    }
    assert not found, f"clean English prose must not trip, but got {found}"


def test_umlaut_escapes_match_their_literal_form() -> None:
    """The escaped markers must be the words they stand for, byte differences aside.

    The file is kept pure ASCII, so a typo in an escape sequence would silently remove a
    marker instead of failing loudly. This compares each escape against the character it
    encodes, built from code points rather than from a literal.
    """
    expected = {
        "f" + chr(0x00FC) + "r",
        "k" + chr(0x00F6) + "nnen",
        "m" + chr(0x00FC) + "ssen",
        chr(0x00FC) + "ber",
    }
    assert expected <= _GERMAN_MARKERS, sorted(expected - _GERMAN_MARKERS)
    assert _german_markers_in("wir m" + chr(0x00FC) + "ssen das pruefen")


def _non_ascii_offsets(path: Path) -> list[int]:
    """Return the byte offsets of every non-ASCII byte in *path*.

    Extracted so the ratchet below can be proved to work on a file that does violate.
    Inline, emptying its condition left every test green: at a clean file an empty
    result is the expected result either way.
    """
    return [i for i, byte in enumerate(path.read_bytes()) if byte > 0x7F]


def test_file_stays_pure_ascii() -> None:
    """The escapes only help while nobody pastes a literal umlaut back in."""
    offenders = _non_ascii_offsets(Path(__file__))
    assert not offenders, f"non-ASCII bytes at offsets {offenders[:5]}"


def test_the_ascii_check_detects_a_literal_umlaut(tmp_path: Path) -> None:
    """The ratchet must fire on a file that does carry one."""
    offender = tmp_path / "with_umlaut.py"
    offender.write_text("# f" + chr(0x00FC) + "r\n", encoding="utf-8")
    assert _non_ascii_offsets(offender)
    clean = tmp_path / "plain.py"
    clean.write_text("# for\n", encoding="utf-8")
    assert not _non_ascii_offsets(clean)


def test_path_detector_flags_an_agent_local_source() -> None:
    """Red probe, detector level: a memory-tree pointer must trip."""
    found = [
        p
        for _, text in _prose_units(_AGENT_PATH_SAMPLE)
        for p in _agent_local_paths_in(text)
    ]
    assert found, "a memory-tree source pointer must be detected"


def test_path_detector_passes_a_committed_source() -> None:
    """Green probe, detector level: a committed docs/ source must not trip."""
    found = [
        p
        for _, text in _prose_units(_CLEAN_SAMPLE)
        for p in _agent_local_paths_in(text)
    ]
    assert not found, f"a committed source must not trip, but got {found}"


def test_path_detector_passes_the_project_container_path() -> None:
    """Negative control: /app/ is this project's container path, not an agent path."""
    assert not _agent_local_paths_in("see /app/requirements.txt for the pinned set")


def test_documented_placeholder_form_does_not_trip() -> None:
    """The contract documents the forbidden shape with a placeholder, which must pass.

    Written out literally, the rule text would make its own guard red. The placeholder
    form is pinned here so nobody reverts it while improving the wording.
    """
    assert not _agent_local_paths_in("never cite memory/<scope>/<file>.md as a source")
    assert not _agent_local_paths_in("nor ~/.claude/<file>, for the same reason")
    assert _agent_local_paths_in("memory/projects/x/quellen/y.md")
    assert _agent_local_paths_in("~/.claude/real/file.md")


def test_sweep_finds_a_planted_german_docstring(tmp_path: Path) -> None:
    """Red probe, sweep level: extraction, iteration and filters must be wired up."""
    module = tmp_path / "pkg" / "mod.py"
    module.parent.mkdir(parents=True)
    module.write_text(_GERMAN_DOCSTRING_SAMPLE, encoding="utf-8")
    assert "pkg/mod.py" in scan_tree(tmp_path)[0]
    module.write_text(_CLEAN_SAMPLE, encoding="utf-8")
    assert not scan_tree(tmp_path)[0]


def test_sweep_finds_a_planted_agent_local_path(tmp_path: Path) -> None:
    """Red probe, sweep level, second arm, including the Markdown reader."""
    module = tmp_path / "pkg" / "mod.py"
    module.parent.mkdir(parents=True)
    module.write_text(_AGENT_PATH_SAMPLE, encoding="utf-8")
    assert "pkg/mod.py" in scan_tree(tmp_path)[1]
    module.write_text(_CLEAN_SAMPLE, encoding="utf-8")
    assert not scan_tree(tmp_path)[1]
    (tmp_path / "notes.md").write_text(
        "Source: memory/_store/some_note.md\n", encoding="utf-8"
    )
    assert "notes.md" in scan_tree(tmp_path)[1]


def test_sweep_reports_an_unparsable_file_instead_of_skipping_it(
    tmp_path: Path,
) -> None:
    """Red probe for the interpreter guard: a broken file must be named, not ignored."""
    broken = tmp_path / "broken.py"
    broken.write_text("def (:\n", encoding="utf-8")
    assert scan_tree(tmp_path)[2] == ["broken.py"]


def test_language_detector_flags_a_german_comment() -> None:
    """Red probe for the comment arm, which the incident actually lived in.

    Four of the five markers translated on this branch were comments. With only
    docstring probes the whole tokenize path could be removed and every test stayed
    green, so this probe carries no docstring German at all.
    """
    found = {
        word
        for _, text in _prose_units(_GERMAN_COMMENT_SAMPLE)
        for word in _german_markers_in(text)
    }
    assert found, "a German comment must produce at least one marker"


def test_extraction_reaches_a_deeply_nested_docstring() -> None:
    """The descent must follow statements, not just definition bodies."""
    found = {
        word
        for _, text in _prose_units(_GERMAN_NESTED_SAMPLE)
        for word in _german_markers_in(text)
    }
    assert found, "a docstring nested inside a conditional must still be extracted"


def test_path_detector_flags_every_documented_scope() -> None:
    """Each alternative of the pattern needs its own positive case, captured in full.

    Measured before adding this: neutralising the dot-claude branch, or reducing
    the memory scopes to the first one, left every test green. An untested alternative
    can vanish in a refactor without anything going red.

    The cases assert the captured text, not merely that something matched. A review
    round found the drive letter of a Windows path being dropped while the case still
    passed, so the test documented coverage the pattern did not have. What the guard
    reports is what a reader has to act on, so the report is what gets pinned.
    """
    cases = [
        # memory scopes, generic rather than enumerated
        "memory/projects/x/y.md",
        "memory/_store/note.md",
        "memory/plans/active/PLAN_X.md",
        "memory/briefings/INDEX.md",
        "memory/wings/tech/x.md",
        "memory/erfahrungen/x.md",
        # home spellings. Every one of these was missed by an earlier draft that
        # enumerated home directories instead of accepting any prefix.
        "~/.claude/settings.json",
        "$HOME/.claude/projects/x.md",
        ".claude/projects/x.md",
        "/home/someone/.claude/projects/x.md",
        "/root/.claude/x.md",
        "/Users/alice/.claude/projects/x.md",
        "C:/Users/alice/.claude/x.md",
        # native Windows separators, and a relative reference that never starts at root
        "C:\\Users\\alice\\.claude\\x.md",
        ".claude\\settings.json",
        "project/.claude/settings.json",
        # every member of _AGENT_PRIVATE_DIRS needs its own case: the set is an
        # enumeration by necessity, so nothing else would notice a member going missing
        "~/.codex/skills/example/SKILL.md",
        "/root/.codex/instructions.md",
        ".codex/config.toml",
    ]
    wrong = [
        (case, _agent_local_paths_in(case))
        for case in cases
        if _agent_local_paths_in(case) != [case]
    ]
    assert not wrong, f"alternatives not matched, or not captured in full: {wrong}"


def test_every_private_directory_is_wired_into_the_pattern() -> None:
    """The named set and the compiled pattern must not drift apart.

    The positive cases above name the members as literals. This asserts the other
    direction: a member added to the constant but never reached by the pattern, or a
    pattern that stopped consuming the constant, fails here rather than passing silently
    while the guard stops looking for one of the trees.
    """
    unreached = [
        name
        for name in _AGENT_PRIVATE_DIRS
        if _agent_local_paths_in(f"~/.{name}/notes.md") != [f"~/.{name}/notes.md"]
    ]
    assert not unreached, (
        f"members of _AGENT_PRIVATE_DIRS the pattern never matches: {unreached}"
    )


# Every shape the detector has been wrong about, with what it must return. Four review
# rounds ran the same way: a narrowing left a gap, the widening that closed it produced a
# false positive somewhere else, and the reviewer found each one. What would have caught
# them earlier is not another rule but a measurement, so the measurement lives here. A
# change to the pattern is diffed against this table rather than against the cases its
# author happened to think of.
_DETECTOR_CORPUS: dict[str, list[str]] = {
    # citations a reader cannot open
    "~/.claude/settings.json": ["~/.claude/settings.json"],
    "$HOME/.claude/projects/x.md": ["$HOME/.claude/projects/x.md"],
    ".claude/projects/x.md": [".claude/projects/x.md"],
    "/home/a/.claude/x.md": ["/home/a/.claude/x.md"],
    "/root/.codex/instructions.md": ["/root/.codex/instructions.md"],
    "~/.codex/skills/x/SKILL.md": ["~/.codex/skills/x/SKILL.md"],
    "project/.claude/settings.json": ["project/.claude/settings.json"],
    "C:/Users/a/.claude/x.md": ["C:/Users/a/.claude/x.md"],
    "C:\\Users\\a\\.claude\\x.md": ["C:\\Users\\a\\.claude\\x.md"],
    ".codex/config.toml": [".codex/config.toml"],
    "file:///root/.codex/instructions.md": ["/root/.codex/instructions.md"],
    "file://localhost/root/.codex/instructions.md": ["/root/.codex/instructions.md"],
    "file://myhost/home/a/.claude/x.md": ["/home/a/.claude/x.md"],
    "FILE:///root/.codex/instructions.md": ["/root/.codex/instructions.md"],
    "[a](file://localhost/home/a/.claude/x.md)": ["/home/a/.claude/x.md"],
    "file:/root/.codex/instructions.md": ["/root/.codex/instructions.md"],
    # percent escapes: standard inside a URI, and the same directory to every reader.
    # Read literally, %2Ecodex is not .codex, so the citation stayed silent.
    "file:///root/%2Ecodex/instructions.md": ["/root/.codex/instructions.md"],
    "file:///root/.codex%2Finstructions.md": ["/root/.codex/instructions.md"],
    "file:///root/%2Eclaude/%78.md": ["/root/.claude/x.md"],
    "see /root/%2Eclaude/x.md": ["/root/.claude/x.md"],
    # a percent sign that is not an escape must leave the unit alone
    "100% of diff hit, then ~/.codex/n.md": ["~/.codex/n.md"],
    # the directory itself, with and without a trailing separator
    "~/.codex": ["~/.codex"],
    "~/.codex/": ["~/.codex/"],
    "/root/.claude/": ["/root/.claude/"],
    "$HOME/.claude": ["$HOME/.claude"],
    ".codex/": [".codex/"],
    # ... but a bare mention in prose is not a citation, and neither is the placeholder
    # spelling this repository uses for a file name
    "the .codex directory": [],
    "`.claude` and `.codex`": [],
    "nor ~/.claude/<file>, for the same reason": [],
    "~/.claude/<file>": [],
    "memory/_store/note.md": ["memory/_store/note.md"],
    # near misses: a name that merely ends in one of the directories, or a scope that is
    # part of a longer documentation path
    ".claudeignore": [],
    ".codexignore": [],
    "not.claude/file.md": [],
    "/tmp/not.claude/f.md": [],
    "/etc/app.claude/c.json": [],
    "docs/memory/plans/rollout.md": [],
    "/app/requirements.txt": [],
    "tests/fixtures/not.codex/x.md": [],
    "profile://h/y and /root/.codex/z.md": ["/root/.codex/z.md"],
    # public sources, whatever their path component contains
    "https://github.com/openai/codex/blob/main/.codex/config.toml": [],
    "see https://example.org/x/.claude/y.md for details": [],
    "<https://example.org/x/.claude/y.md>": [],
    "`https://example.org/x/.claude/y.md`": [],
    "(see https://example.org/x/.claude/y.md)": [],
    'href="https://example.org/x/.claude/y.md"': [],
    "ssh://git@example.org/x/.claude/y.md": [],
    "HTTPS://github.com/openai/codex/blob/main/.codex/config.toml": [],
    "Https://example.org/x/.claude/y.md": [],
    "SSH://git@example.org/x/.claude/y.md": [],
    # any scheme that names a network location, judged by its grammar and not by a
    # list of four names that cost one review round each
    "git://github.com/example/.codex/project.git": [],
    "git+ssh://git@example.org/x/.claude/y.md": [],
    "rsync://example.org/x/.codex/y": [],
    "svn+ssh://example.org/x/.claude/y.md": [],
    "http://localhost:8080/x/.claude/y.md": [],
    "http://192.168.1.10/x/.codex/y": [],
    # an IPv6 literal host: "]" closes a link in prose, so the bracket has to be read as
    # part of the authority or the path behind it is reported as a private citation
    "http://[2606:4700:4700::1111]/repo/.codex/config.toml": [],
    # ... and a scheme WITHOUT a network host is not a public source, whatever its name.
    # Only its scheme and authority are removed, so the local path behind them is judged
    # as what it is.
    "vscode://file/root/.codex/instructions.md": ["/root/.codex/instructions.md"],
    "myapp://x/root/.claude/a.md": ["/root/.claude/a.md"],
    "editor://open/home/a/.claude/x.md": ["/home/a/.claude/x.md"],
    # a public source and a private citation in the same line, in both orders and with
    # no whitespace between them
    "https://example.org/x/.claude/y.md and /home/a/.claude/x.md": [
        "/home/a/.claude/x.md"
    ],
    "/home/a/.claude/x.md and https://example.org/x/.claude/y.md": [
        "/home/a/.claude/x.md"
    ],
    "[public](https://example.org/x/.codex/y)[private](~/.codex/notes.md)": [
        "~/.codex/notes.md"
    ],
    "http://example.org/.codex/a, then ~/.claude/b.md": ["~/.claude/b.md"],
    # The declared limit, pinned so that it stays a measured fact rather than a claim in
    # a comment: a URL carrying a closing parenthesis is cut short there, and the tail
    # reads as a citation. It fails loudly, which is the direction a guard should fail
    # in, and the escape hatch is the allowlist. Change this expectation only together
    # with the terminator set.
    "https://en.wikipedia.org/wiki/A_(B)/.claude/y.md": ["/.claude/y.md"],
}


def test_detector_corpus_holds() -> None:
    """The whole table at once, so a change is measured against every known shape.

    The individual tests below say why each shape matters. This one says that none of
    them regressed, which is the property four review rounds were spent discovering one
    case at a time.
    """
    wrong = {
        text: (expected, _agent_local_paths_in(text))
        for text, expected in _DETECTOR_CORPUS.items()
        if _agent_local_paths_in(text) != expected
    }
    assert not wrong, f"detector corpus regressions (expected, actual): {wrong}"


def test_file_uris_are_read_as_the_local_paths_they_are() -> None:
    """A file URI names a local path, so the path is what gets reported.

    The extra slashes hid this spelling from the pattern entirely, which made an
    unreadable citation look clean. Pairs rather than a truthiness check, because the
    interesting part is which substring comes back.
    """
    cases = {
        "file:///root/.codex/instructions.md": ["/root/.codex/instructions.md"],
        "file:///home/a/.claude/x.md": ["/home/a/.claude/x.md"],
        # The host-qualified spelling is as standard as the empty-authority one, and it
        # was the shape that stayed silent: the scheme survived normalisation, so the
        # path pattern saw a run it does not accept and reported nothing.
        "file://localhost/root/.codex/instructions.md": [
            "/root/.codex/instructions.md"
        ],
        "file://myhost/home/a/.claude/x.md": ["/home/a/.claude/x.md"],
        "FILE:///root/.codex/instructions.md": ["/root/.codex/instructions.md"],
    }
    wrong = {
        text: _agent_local_paths_in(text)
        for text, expected in cases.items()
        if _agent_local_paths_in(text) != expected
    }
    assert not wrong, f"file URIs not resolved to their local path: {wrong}"


def test_url_schemes_are_recognised_whatever_their_case() -> None:
    """A scheme is case-insensitive, so an uppercase link stays a public source.

    Matching the scheme case-sensitively did not hide a violation; it invented one. The
    uppercase URL was never blanked, so the path component of somebody else's repository
    was reported as a private citation and an openable source got rejected.
    """
    cases = {
        "HTTPS://github.com/openai/codex/blob/main/.codex/config.toml": [],
        "Https://example.org/x/.claude/y.md": [],
        "SSH://git@example.org/x/.claude/y.md": [],
        "HTTP://example.org/.codex/a, then ~/.claude/b.md": ["~/.claude/b.md"],
    }
    wrong = {
        text: _agent_local_paths_in(text)
        for text, expected in cases.items()
        if _agent_local_paths_in(text) != expected
    }
    assert not wrong, f"case-sensitive scheme matching produced: {wrong}"


def test_any_network_scheme_counts_as_public() -> None:
    """A scheme is public because it names a network location, not because it is listed.

    A list of four names was written once, and the two review rounds after it each
    found a spelling it did not carry - an uppercase scheme, then git://. The pattern
    now reads the scheme grammar of RFC 3986, so this test is about the shapes nobody
    enumerated rather than about the four that were.
    """
    cases = {
        "git://github.com/example/.codex/project.git": [],
        "git+ssh://git@example.org/x/.claude/y.md": [],
        "rsync://example.org/x/.codex/y": [],
        "svn+ssh://example.org/x/.claude/y.md": [],
        "ftp://example.org/x/.codex/y": [],
        # the local scheme is the one exception, and it stays one: it is normalised
        # before this rule runs, so it still reports the path it names
        "file:///root/.codex/instructions.md": ["/root/.codex/instructions.md"],
    }
    wrong = {
        text: _agent_local_paths_in(text)
        for text, expected in cases.items()
        if _agent_local_paths_in(text) != expected
    }
    assert not wrong, f"scheme grammar did not hold: {wrong}"


def test_percent_encoded_citations_are_decoded_before_matching() -> None:
    """%2Ecodex is .codex to every reader, so the guard has to read it that way.

    This is the silent direction: the encoded spelling matched nothing at all, so a
    citation a reader cannot open passed the sweep. The escape may sit in a file URI,
    where it is the standard spelling, or in bare prose, so both are pinned here.
    """
    cases = {
        "file:///root/%2Ecodex/instructions.md": ["/root/.codex/instructions.md"],
        "file:///root/.codex%2Finstructions.md": ["/root/.codex/instructions.md"],
        "file:///home/a/%2Eclaude/x.md": ["/home/a/.claude/x.md"],
        "see /root/%2Eclaude/x.md": ["/root/.claude/x.md"],
    }
    wrong = {
        text: _agent_local_paths_in(text)
        for text, expected in cases.items()
        if _agent_local_paths_in(text) != expected
    }
    assert not wrong, f"percent-encoded citations stayed hidden: {wrong}"

    # A percent sign that is not an escape must change nothing, and a public URL that
    # carries one must stay public after decoding.
    assert _agent_local_paths_in("100% of diff hit, then ~/.codex/n.md") == [
        "~/.codex/n.md"
    ]
    assert _agent_local_paths_in("https://example.org/a%2Eb/.claude/y.md") == []


def test_the_private_directory_itself_is_a_citation() -> None:
    """Naming the directory is naming something no reader can open.

    Requiring a file under it let every such spelling through in silence, which is the
    direction that matters. The counter-cases belong in the same test: the boundary that
    admits the bare directory must not admit a bare mention in prose, an ignore-file
    name, or the placeholder spelling this repository writes for a file.
    """
    cites = {
        "~/.codex": ["~/.codex"],
        "~/.codex/": ["~/.codex/"],
        "/root/.claude/": ["/root/.claude/"],
        "$HOME/.claude": ["$HOME/.claude"],
        ".codex/": [".codex/"],
    }
    quiet = {
        "the .codex directory": [],
        "`.claude` and `.codex`": [],
        ".claudeignore": [],
        ".codexignore": [],
        "~/.claude/<file>": [],
        "nor ~/.claude/<file>, for the same reason": [],
    }
    wrong = {
        text: _agent_local_paths_in(text)
        for text, expected in {**cites, **quiet}.items()
        if _agent_local_paths_in(text) != expected
    }
    assert not wrong, f"directory citations misjudged: {wrong}"


def test_a_scheme_without_a_network_host_is_not_a_public_source() -> None:
    """Two properties make a URL openable, and the scheme alone is only one of them.

    Reading any scheme as public was the obvious repair for an enumeration that kept
    missing spellings, and it failed in the silent direction: a URI naming a local file
    under a private directory was blanked whole and the citation vanished. Only the
    scheme and authority are removed from such a URI now, so the path behind them is
    judged as the local path it is.
    """
    cases = {
        "vscode://file/root/.codex/instructions.md": ["/root/.codex/instructions.md"],
        "myapp://x/root/.claude/a.md": ["/root/.claude/a.md"],
        "editor://open/home/a/.claude/x.md": ["/home/a/.claude/x.md"],
        # a real host keeps the URL public, whatever the scheme is called
        "git://github.com/example/.codex/project.git": [],
        "rsync://example.org/x/.codex/y": [],
        "http://localhost:8080/x/.claude/y.md": [],
        "http://192.168.1.10/x/.codex/y": [],
    }
    wrong = {
        text: _agent_local_paths_in(text)
        for text, expected in cases.items()
        if _agent_local_paths_in(text) != expected
    }
    assert not wrong, f"host test did not hold: {wrong}"


def test_an_ipv6_host_survives_the_url_terminators() -> None:
    """A closing bracket ends a link in prose and opens nothing in an IPv6 literal.

    Treating it as a terminator unconditionally cut the URL after its scheme, and the
    path of an openable source was then reported as a private citation. Loud rather than
    silent, but wrong, and a reader citing over IPv6 has no way to comply.
    """
    assert (
        _agent_local_paths_in("http://[2606:4700:4700::1111]/repo/.codex/config.toml")
        == []
    )
    assert _agent_local_paths_in("[a](http://[::1]/x/.claude/y.md)") == []
    # the terminator still does its work outside an authority
    assert _agent_local_paths_in("[link](https://example.org/a) ~/.codex/n.md") == [
        "~/.codex/n.md"
    ]


def test_a_path_written_twice_is_still_reported_twice() -> None:
    """The decoded pass adds, it does not deduplicate what the raw text already showed.

    Dropping repeats would be a quiet change of an unrelated property: the raw pass
    behaved this way before the second pass existed, and a report is allowed to say that
    a unit cites the same unopenable path more than once.
    """
    line = "~/.claude/a.md and ~/.claude/a.md"
    assert _agent_local_paths_in(line) == ["~/.claude/a.md", "~/.claude/a.md"]

    # The same line with a percent sign takes the second pass, and must not collapse
    # either: the raw findings are kept in full and only the decoded extras are filtered.
    with_escape = "~/.claude/a.md and ~/.claude/a.md, 50%25 done"
    assert _agent_local_paths_in(with_escape) == ["~/.claude/a.md", "~/.claude/a.md"]


def test_a_public_url_next_to_a_private_path_hides_neither() -> None:
    """Blanking the URL must not swallow a real citation on the same line.

    A normalisation step that removed too much would be invisible: the sweep would stay
    green and nobody would know which of the two reasons produced the silence.
    """
    after = "/home/a/.claude/x.md and https://example.org/x/.claude/y.md"
    assert _agent_local_paths_in(after) == ["/home/a/.claude/x.md"]

    # The citation ahead of the URL is the case that pins how far the blanking reaches:
    # with the URL last, a run to end of line looks the same as a run to the next space.
    before = "https://example.org/x/.claude/y.md and /home/a/.claude/x.md"
    assert _agent_local_paths_in(before) == ["/home/a/.claude/x.md"]


def test_scan_honours_the_restriction_to_known_paths(tmp_path: Path) -> None:
    """A file outside the restriction is not swept, one inside it still is.

    The repository scan passes the tracked set, so that an ignored local tree cannot
    fail this suite for the developer who happens to have one. Pinned on a synthetic
    tree rather than on the working copy: writing an untracked offender into the
    repository to observe the effect would be the wrong kind of test.
    """
    for name in ("tracked.py", "ignored.py"):
        (tmp_path / name).write_text(_GERMAN_COMMENT_SAMPLE, encoding="utf-8")

    unrestricted, _, _ = scan_tree(tmp_path)
    assert set(unrestricted) == {"tracked.py", "ignored.py"}

    restricted, _, _ = scan_tree(tmp_path, frozenset({"tracked.py"}))
    assert set(restricted) == {"tracked.py"}


def test_repository_scan_restricts_itself_to_tracked_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The repository scan has to hand the tracked set down, not just be inside it.

    The assertion below that the reported files are tracked cannot see this: at a clean
    tree it holds either way, so removing the restriction leaves it green. This calls
    the uncached body with a stand-in for the sweep and reads what it was given, which
    is the only observation that distinguishes the two.
    """
    captured: dict[str, object] = {}

    def spy(
        root: Path, only: frozenset[str] | None = None
    ) -> tuple[Offenders, Offenders, list[str]]:
        captured["root"], captured["only"] = root, only
        return {}, {}, []

    monkeypatch.setattr(sys.modules[__name__], "scan_tree", spy)
    _repo_scan.__wrapped__()
    assert captured["root"] == _REPO_ROOT
    assert captured["only"] == _tracked_paths(), (
        "The repository sweep did not restrict itself to tracked files. An ignored "
        "local tree would then fail this suite for whoever happens to have one."
    )


def test_repository_scan_stays_inside_the_tracked_set() -> None:
    """Whatever the repository sweep reports has to be a file a reader can fetch."""
    language, paths, unparsable = _repo_scan()
    tracked = _tracked_paths()
    outside = sorted((set(language) | set(paths) | set(unparsable)) - tracked)
    assert not outside, (
        "The repository sweep reported untracked files. Their contents depend on the "
        f"developer's working copy, so the result would not be reproducible: {outside}"
    )


def test_path_detector_respects_the_word_boundary() -> None:
    """A path that merely ends in one of the scopes must not trip.

    Pins the lookbehind: without it, any longer path containing the scope name would
    match, which would flag legitimate documentation directories.
    """
    benign = [
        "see docs/memory/plans/rollout.md for the plan",
        "see /app/requirements.txt for the pinned set",
        "the .claudeignore file lists them",
        "tests/fixtures/not.claude/file.md is a fixture",
        # absolute siblings of the line above: a dotted directory name whose tail
        # happens to read .claude. An earlier prefix class could be split here, so
        # both shapes are pinned rather than only the relative one.
        "/tmp/not.claude/file.md is a fixture",
        "/etc/app.claude/config.json is unrelated",
        "the .codexignore file lists them",
        "tests/fixtures/not.codex/file.md is a fixture",
        # public URLs whose path component happens to contain one of the directories.
        # A reader can open these, so they are sources rather than private citations.
        "see https://github.com/openai/codex/blob/main/.codex/config.toml for it",
        "documented at https://example.org/x/.claude/y.md today",
    ]
    tripped = [text for text in benign if _agent_local_paths_in(text)]
    assert not tripped, f"benign text flagged: {tripped}"


def test_sweep_finds_a_planted_german_comment(tmp_path: Path) -> None:
    """Red probe, sweep level, comment arm."""
    module = tmp_path / "pkg" / "mod.py"
    module.parent.mkdir(parents=True)
    module.write_text(_GERMAN_COMMENT_SAMPLE, encoding="utf-8")
    assert "pkg/mod.py" in scan_tree(tmp_path)[0]


def test_sweep_finds_a_planted_agent_path_in_a_comment(tmp_path: Path) -> None:
    """Red probe, sweep level, second arm, comment rather than docstring."""
    module = tmp_path / "pkg" / "mod.py"
    module.parent.mkdir(parents=True)
    module.write_text(_AGENT_PATH_COMMENT_SAMPLE, encoding="utf-8")
    assert "pkg/mod.py" in scan_tree(tmp_path)[1]


def test_sweep_skips_generated_protobuf_bindings(tmp_path: Path) -> None:
    """Generated bindings are excluded by suffix, both source and stub."""
    (tmp_path / "x_pb2.py").write_text(_GERMAN_DOCSTRING_SAMPLE, encoding="utf-8")
    (tmp_path / "x_pb2.pyi").write_text(_GERMAN_DOCSTRING_SAMPLE, encoding="utf-8")
    assert not scan_tree(tmp_path)[0]


def test_sweep_covers_hand_written_files_beside_generated_ones(tmp_path: Path) -> None:
    """Excluding by file, not by directory, keeps the hand-written neighbours in.

    ProtoDecoders holds decoder.py and two __init__.py next to its generated bindings.
    Pruning the directory, as [tool.ruff] extend-exclude does, would take those out of
    the sweep and leave an active production module unguarded.
    """
    proto_dir = tmp_path / "ProtoDecoders"
    proto_dir.mkdir()
    (proto_dir / "generated_pb2.py").write_text(
        _GERMAN_DOCSTRING_SAMPLE, encoding="utf-8"
    )
    (proto_dir / "decoder.py").write_text(_GERMAN_DOCSTRING_SAMPLE, encoding="utf-8")
    offenders = scan_tree(tmp_path)[0]
    assert "ProtoDecoders/decoder.py" in offenders
    assert "ProtoDecoders/generated_pb2.py" not in offenders


def test_sweep_covers_hand_written_type_stubs(tmp_path: Path) -> None:
    """Type stubs are Python sources and carry prose the contract governs."""
    (tmp_path / "vendorlib.pyi").write_text(_GERMAN_DOCSTRING_SAMPLE, encoding="utf-8")
    assert "vendorlib.pyi" in scan_tree(tmp_path)[0]


def test_excluded_words_do_not_flag_english_prose() -> None:
    """English prose built from the measured homographs must stay clean.

    A behaviour assertion, not a mutation probe: the words are absent from the marker
    set as well, so this stays green even if the subtraction is removed. The probe
    below is the one that pins the subtraction.
    """
    english = "the retry falls through, so the probe is also skipped and the value dies"
    assert not _german_markers_in(english)


def test_subtraction_survives_a_word_in_both_lists() -> None:
    """The subtraction must win over a marker set that wrongly contains an exclusion.

    This is the probe that makes _HOMOGRAPH_EXCLUSIONS load-bearing rather than
    decorative: remove the subtraction in _german_markers_in and this goes red, while
    every other test stays green because the marker set does not carry those words.
    """
    contaminated = _GERMAN_MARKERS | {"falls", "probe"}
    assert not _german_markers_in("the retry falls through the probe", contaminated)


def test_sweep_skips_excluded_directories(tmp_path: Path) -> None:
    """A violation inside an excluded tree must not be reported.

    Uses .venv rather than vendor: vendor was removed from the exclusion list because it
    holds tracked, hand-written Python in this repository.
    """
    hidden = tmp_path / ".venv" / "mod.py"
    hidden.parent.mkdir(parents=True)
    hidden.write_text(_GERMAN_DOCSTRING_SAMPLE, encoding="utf-8")
    assert not scan_tree(tmp_path)[0]
