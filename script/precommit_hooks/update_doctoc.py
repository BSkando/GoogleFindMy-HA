# script/precommit_hooks/update_doctoc.py
"""Generate DocToc-compatible tables without requiring network access."""

from __future__ import annotations

import argparse
import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import quote

import cmarkgfm
from cmarkgfm.cmark import Options

START_MARKER = (
    "<!-- START doctoc generated TOC please keep comment here to allow auto update -->"
)
END_MARKER = (
    "<!-- END doctoc generated TOC please keep comment here to allow auto update -->"
)
HEADER_LINE = "**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*"

# CommonMark requires a space or a tab after the hashes, no other white space.
_HEADING_RE = re.compile(r"^(?P<hashes>#{1,6})[ \t]+(?P<title>.+?)\s*$")
_LINE_END_RE = re.compile(r"\r\n|\r|\n")
# A rendered heading starting in the first column of line ``line``. Text and
# attribute values are escaped by the renderer and raw HTML is omitted, so
# every match is a heading the renderer produced. ``_HEADING_RE`` on the
# source line already rules out headings in list items, block quotes and
# indented ones; the column check states the same on the rendered side.
_RENDERED_HEADING_RE = re.compile(r'<h[1-6] data-sourcepos="(?P<line>\d+):1-')
# Appended after a blank line: only a block left open can hide this heading.
_PROBE = ("", "# probe")
_PERCENT_ESCAPE_RE = re.compile(r"%([a-fA-F]|\d){2}")
_REMOVE_CHARS = "/?!:[]`.,()*\"';{}+=<>~$|#@&–—\\"
_REMOVE_TRANSLATION = str.maketrans("", "", _REMOVE_CHARS)
# The join controls ZWNJ and ZWJ belong to ``\w`` in Unicode regular
# expressions, and GitHub keeps them; every other format character goes.
_KEPT_FORMAT_CHARS = frozenset("\u200c\u200d")
_CJK_PUNCTUATION = set("。？！、；：“”【】（）〔〕［］﹃﹄‘’﹁﹂—…－～《》〈〉「」")


class TocGenerationError(RuntimeError):
    """Raised when the DocToc markers or the document structure are malformed."""


def _render(lines: list[str]) -> str:
    """Render ``lines`` as GitHub does, with the source position of each block."""

    html: str = cmarkgfm.github_flavored_markdown_to_html(
        "\n".join(lines) + "\n", options=Options.CMARK_OPT_SOURCEPOS
    )
    return html


def _heading_lines(html: str) -> set[int]:
    """Return the lines on which a heading starts in the first column.

    A heading inside a list item or a block quote, or an indented one, starts
    in a later column.
    """

    return {int(match.group("line")) for match in _RENDERED_HEADING_RE.finditer(html)}


def _hides_probe(lines: list[str]) -> bool:
    """Return True when a block left open at the end of ``lines`` hides a heading.

    Only a code fence, or an HTML block that ends at a marker such as ``-->``,
    can still be open after the blank line of ``_PROBE``; every other block
    ends there or at the unindented heading.
    """

    return len(lines) + len(_PROBE) not in _heading_lines(_render([*lines, *_PROBE]))


def _raise_unclosed(lines: list[str], html: str) -> None:
    """Raise ``TocGenerationError`` naming the line of the block left open."""

    # The block starts right after the longest prefix that leaves the probe
    # visible. A block closed later can hide the probe behind a shorter
    # prefix, so the search runs backwards.
    start = next(
        end + 1
        for end in range(len(lines) - 1, -1, -1)
        if not _hides_probe(lines[:end])
    )
    kind = "code fence" if f'<pre data-sourcepos="{start}:' in html else "HTML block"
    raise TocGenerationError(f"{kind} opened on line {start} is never closed")


def _iter_headings(lines: Iterable[str]) -> list[tuple[int, str]]:
    """Collect Markdown headings and their levels in document order.

    The document is parsed by ``cmark-gfm``, the renderer behind GitHub, so a
    line counts as a heading exactly when GitHub renders it as one. A shell
    comment such as ``# 1. Edit pyproject.toml`` inside a code fence, a ``#``
    line inside an HTML block and raw ``<h2>`` markup are therefore never
    listed; each would yield a dead link in the table of contents.

    Only ATX headings (``## Title``) that start in the first column on top
    level are collected, as before: headings inside list items and block
    quotes, indented headings and setext headings are left out.

    A code fence, or an HTML block that ends only at a marker such as ``-->``,
    that is still open at the end of the document raises
    ``TocGenerationError`` instead of silently dropping every later heading.
    """

    source = list(lines)
    # The probe cannot change how the lines above it are read, so one
    # rendering serves both the check and the headings.
    html = _render([*source, *_PROBE])
    found = _heading_lines(html)
    if len(source) + len(_PROBE) not in found:
        _raise_unclosed(source, html)
    headings: list[tuple[int, str]] = []
    for line_number in sorted(found):
        if line_number > len(source):
            continue
        match = _HEADING_RE.match(source[line_number - 1])
        if match is None:
            continue
        title = match.group("title").strip()
        if title:
            headings.append((len(match.group("hashes")), title))
    return headings


def _ascii_only_lower(text: str) -> str:
    """Lowercase ASCII characters without altering Unicode symbols."""

    return "".join(char.lower() if "A" <= char <= "Z" else char for char in text)


def _basic_github_id(text: str) -> str:
    """Mirror GitHub's heading ids.

    The ASCII removals follow ``anchor-markdown-header``. GitHub additionally
    drops every other punctuation mark and symbol, such as ``\u2265`` or ``%``,
    while keeping ``-`` and ``_``; without that step a heading like "mode
    (\u2265 90 %)" got an anchor GitHub does not render. Separators other than
    the plain space (categories ``Zs``, ``Zl``, ``Zp``, such as the no-break
    space) and format characters (``Cf``, such as the soft hyphen) disappear as
    well, except the join controls ZWNJ and ZWJ, measured against GitHub's
    rendering with ``gh api markdown``. Known limit: letters outside ASCII keep
    their case, because ``_ascii_only_lower`` runs first.
    """

    text = text.replace(" ", "-")
    text = _PERCENT_ESCAPE_RE.sub("", text)
    text = text.translate(_REMOVE_TRANSLATION)
    text = "".join(char for char in text if char not in _CJK_PUNCTUATION)
    return "".join(
        char
        for char in text
        if char in "-_"
        or char in _KEPT_FORMAT_CHARS
        or (
            unicodedata.category(char)[0] not in "PSZ"
            and unicodedata.category(char) != "Cf"
        )
    )


def _slugify(title: str, seen: dict[str, int]) -> str:
    """Return a GitHub-compatible anchor slug for the provided heading.

    A heading of symbols only has an empty anchor on GitHub (``#``, then
    ``#-1``, ``#-2`` for repeats), so an empty slug stays empty.
    """

    normalized = _basic_github_id(_ascii_only_lower(title.strip()))

    count = seen[normalized]
    seen[normalized] += 1
    if count:
        normalized = f"{normalized}-{count}"

    return quote(normalized, safe="-_.~")


def _render_toc(headings: list[tuple[int, str]]) -> list[str]:
    """Render DocToc-compatible Markdown for the collected headings."""

    seen_slugs: dict[str, int] = defaultdict(int)
    toc_lines = [
        START_MARKER,
        "<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->",
        HEADER_LINE,
        "",
    ]

    for level, title in headings:
        indent = "  " * (level - 1)
        slug = _slugify(title, seen_slugs)
        toc_lines.append(f"{indent}- [{title}](#{slug})")

    toc_lines.extend(["", END_MARKER])
    return toc_lines


def _replace_toc(content: str) -> str:
    """Return updated Markdown content with an inlined DocToc."""

    start_index = content.find(START_MARKER)
    end_index = content.find(END_MARKER)
    if start_index == -1 or end_index == -1 or end_index <= start_index:
        raise TocGenerationError("DocToc markers are missing or malformed")

    before = content[:start_index]
    after = content[end_index + len(END_MARKER) :]

    # Split at the line endings cmark-gfm knows, so line numbers agree.
    headings = _iter_headings(_LINE_END_RE.split(content))
    toc_lines = _render_toc(headings)
    toc_block = "\n".join(toc_lines)

    # Ensure the section separation mirrors DocToc's blank-line convention.
    if before:
        before = before.rstrip("\n") + "\n"
        prefix = before
    else:
        prefix = ""
    after = after.lstrip("\n")
    return f"{prefix}{toc_block}\n\n{after}"


def update_file(path: Path) -> bool:
    """Update the DocToc section for ``path`` and return True when modified."""

    original = path.read_text(encoding="utf-8")
    updated = _replace_toc(original)
    if updated == original:
        return False
    path.write_text(updated, encoding="utf-8")
    return True


def main(argv: list[str] | None = None) -> int:
    """Entry point for the DocToc refresh pre-commit hook."""

    parser = argparse.ArgumentParser(description="Refresh in-repo DocToc tables")
    parser.add_argument("files", nargs="+", help="Markdown files managed by DocToc")
    args = parser.parse_args(argv)

    changed = False
    for filename in args.files:
        path = Path(filename)
        if not path.exists():
            continue
        try:
            if update_file(path):
                changed = True
        except TocGenerationError as exc:
            parser.error(f"{filename}: {exc}")

    if changed:
        print("DocToc updated; re-run pre-commit to verify staging.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
