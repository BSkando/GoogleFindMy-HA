# tests/test_precommit_update_doctoc.py
"""Unit tests for the in-repo DocToc refresh pre-commit hook."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pytest

from script.precommit_hooks import update_doctoc


def _titles(document: str) -> list[str]:
    """Return the heading titles the hook would put into the table of contents."""

    return [
        title
        for _level, title in update_doctoc._iter_headings(
            update_doctoc._LINE_END_RE.split(document)
        )
    ]


def test_headings_outside_code_are_collected() -> None:
    """Plain headings keep their level and document order."""

    document = "# Top\n\ntext\n\n## Second\n\n### Third\n"

    assert update_doctoc._iter_headings(document.splitlines()) == [
        (1, "Top"),
        (2, "Second"),
        (3, "Third"),
    ]


def test_comment_inside_backtick_fence_is_not_a_heading() -> None:
    """A shell comment inside a fenced block must not become a heading."""

    document = (
        "# Real\n\n```bash\n# 1. Edit pyproject.toml\npoetry lock\n```\n\n## After\n"
    )

    assert _titles(document) == ["Real", "After"]


def test_comment_inside_tilde_fence_is_not_a_heading() -> None:
    """Tilde fences delimit code blocks just like backtick fences."""

    document = "# Real\n\n~~~bash\n# Not a heading\n~~~\n\n## After\n"

    assert _titles(document) == ["Real", "After"]


def test_shorter_fence_does_not_close_a_longer_one() -> None:
    """A closing fence must be at least as long as the opening fence."""

    document = "````text\n```\n# Still code\n````\n\n# After\n"

    assert _titles(document) == ["After"]


def test_fence_of_a_different_character_does_not_close() -> None:
    """A tilde run does not close a backtick fence."""

    document = "```text\n~~~\n# Still code\n```\n\n# After\n"

    assert _titles(document) == ["After"]


def test_closing_fence_must_not_carry_an_info_string() -> None:
    """Only the opening fence may carry an info string."""

    document = "```text\n```python\n# Still code\n```\n\n# After\n"

    assert _titles(document) == ["After"]


def test_indented_fence_up_to_three_spaces_opens_a_block() -> None:
    """Up to three leading spaces still open a fenced block."""

    document = "# Real\n\n   ```bash\n# Not a heading\n   ```\n\n## After\n"

    assert _titles(document) == ["Real", "After"]


def test_unclosed_fence_raises_instead_of_dropping_headings() -> None:
    """An unterminated fence is reported rather than hiding later headings."""

    document = "# Real\n\ntext\n\n```bash\n# Not a heading\n\n## Also not a heading\n"

    with pytest.raises(
        update_doctoc.TocGenerationError, match="line 5 is never closed"
    ):
        _titles(document)


def test_closing_fence_may_carry_trailing_spaces() -> None:
    """Trailing whitespace after a closing fence is not an info string."""

    document = "```\n# Not a heading\n```   \n# After\n"

    assert _titles(document) == ["After"]


def test_longer_fence_closes_a_shorter_one() -> None:
    """A closing fence may be longer than the opening fence."""

    document = "```\n# Not a heading\n````\n# After\n"

    assert _titles(document) == ["After"]


def test_two_backticks_do_not_open_a_fence() -> None:
    """A fence needs at least three backticks."""

    assert _titles("``\n# Heading\n") == ["Heading"]


def test_fence_indented_four_spaces_does_not_open_a_block() -> None:
    """Four leading spaces make an indented code line, not a fence."""

    assert _titles("    ```\n# Heading\n") == ["Heading"]


def test_backtick_in_info_string_does_not_open_a_fence() -> None:
    """A backtick fence's info string may not contain a backtick."""

    assert _titles("```a`b\n# Heading\n") == ["Heading"]


def test_list_item_fence_closes_with_deeper_indentation() -> None:
    """A fence inside a list item may close with up to three more spaces."""

    document = "- item\n\n  ```bash\n  x\n     ```\n\n# After\n\n## Later\n"

    assert _titles(document) == ["After", "Later"]


def test_deeply_indented_fence_does_not_close_the_block() -> None:
    """A fence line indented far beyond the opener is content, not a closer."""

    document = "```\n        ```\n# Not a heading\n```\n# After\n"

    assert _titles(document) == ["After"]


def test_two_tildes_do_not_open_a_fence() -> None:
    """A tilde fence also needs at least three characters."""

    assert _titles("~~\n# Heading\n") == ["Heading"]


def test_closer_indented_four_spaces_past_the_opener_does_not_close() -> None:
    """The closer tolerance ends at three spaces beyond the opener."""

    document = "```\n    ```\n# Not a heading\n```\n# After\n"

    assert _titles(document) == ["After"]


def test_backtick_in_info_string_still_opens_a_tilde_fence() -> None:
    """Only backtick fences reject a backtick in the info string."""

    document = "~~~a`b\n# Not a heading\n~~~\n# After\n"

    assert _titles(document) == ["After"]


def test_closer_indented_past_three_spaces_is_fence_content() -> None:
    """A fence line indented four spaces past the margin does not close a fence.

    The line is code, so the heading below stays inside the fence; with no
    closer after it the fence stays open and the hook fails loudly.
    """

    document = "   ```\n      ```\n# Code\n   ```\n# After\n"

    assert _titles(document) == ["After"]
    with pytest.raises(update_doctoc.TocGenerationError, match="line 1 "):
        _titles("   ```\n      ```\n# Code\n")


def test_main_leaves_file_untouched_when_a_fence_is_unclosed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The hook fails loudly and does not rewrite the table of contents."""

    document = "\n".join(
        [
            update_doctoc.START_MARKER,
            update_doctoc.END_MARKER,
            "",
            "# Real",
            "",
            "```bash",
            "# Not a heading",
            "",
        ]
    )
    path = tmp_path / "AGENTS.md"
    path.write_text(document, encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        update_doctoc.main([str(path)])

    assert excinfo.value.code == 2
    assert "never closed" in capsys.readouterr().err
    assert path.read_text(encoding="utf-8") == document


def test_replace_toc_skips_headings_inside_code_blocks() -> None:
    """End-to-end: the rendered table of contents omits code block lines."""

    document = "\n".join(
        [
            update_doctoc.START_MARKER,
            update_doctoc.END_MARKER,
            "",
            "# Poetry lock file management",
            "",
            "```bash",
            "# 1. Edit pyproject.toml",
            "poetry lock",
            "```",
            "",
        ]
    )

    updated = update_doctoc._replace_toc(document)

    assert "- [Poetry lock file management](#poetry-lock-file-management)" in updated
    assert "1. Edit pyproject.toml" not in updated.split(update_doctoc.END_MARKER)[0]


def test_fence_marker_inside_html_comment_opens_no_code_block() -> None:
    """A fence line inside an HTML comment neither opens a block nor raises."""

    document = "<!--\n```\n-->\n\n# Real\n"

    assert _titles(document) == ["Real"]


def test_fence_pair_inside_html_comments_hides_no_heading() -> None:
    """Two commented fence lines must not pair up and swallow a heading."""

    document = "<!--\n```\n-->\n\n# Kept\n\n<!--\n```\n-->\n"

    assert _titles(document) == ["Kept"]


def test_heading_inside_html_comment_is_not_collected() -> None:
    """GitHub renders no anchor for a heading line inside an HTML comment."""

    document = "<!--\n# Hidden\n-->\n\n# Real\n"

    assert _titles(document) == ["Real"]


def test_single_line_html_comment_closes_on_its_own_line() -> None:
    """An HTML comment that ends on its start line leaves later headings alone."""

    document = "<!-- note -->\n# Real\n"

    assert _titles(document) == ["Real"]


@pytest.mark.parametrize(
    ("opener", "closer"),
    [
        ("<pre>", "</pre>"),
        ("<?php", "?>"),
        ("<!DOCTYPE html", ">"),
        ("<![CDATA[", "]]>"),
    ],
)
def test_marker_ended_html_block_spans_blank_lines(opener: str, closer: str) -> None:
    """Conditions 1, 3, 4 and 5 end at their marker, not at a blank line."""

    document = f"{opener}\n\n# Hidden\n{closer}\n# Real\n"

    assert _titles(document) == ["Real"]


def test_block_tag_html_ends_at_blank_line() -> None:
    """Condition 6 (a known block tag) ends at the next blank line."""

    document = "<DIV class='note'>\n# Hidden\n\n# Real\n</div>\n"

    assert _titles(document) == ["Real"]


def test_block_tag_with_trailing_text_opens_an_html_block() -> None:
    """Condition 6 needs no lone tag: text after ``<div>`` still opens a block."""

    document = "<div>text\n# Hidden\n\n# Real\n"

    assert _titles(document) == ["Real"]


def test_uppercase_block_tag_interrupts_a_paragraph() -> None:
    """Unlike condition 7, a block tag in any case interrupts a paragraph."""

    document = "text\n<DIV>\n# Hidden\n\n# Real\n"

    assert _titles(document) == ["Real"]


def test_heading_ends_the_paragraph_before_a_lone_tag() -> None:
    """After an ATX heading no paragraph is open, so a lone tag opens a block."""

    document = "text\n# Top\n<span>\n# Hidden\n\n# Real\n"

    assert _titles(document) == ["Top", "Real"]


def test_lone_tag_after_blank_line_opens_an_html_block() -> None:
    """Condition 7: a lone complete tag starts a block that ends at a blank line."""

    document = "text\n\n<custom-tag/>\n# Hidden\n\n# Real\n"

    assert _titles(document) == ["Real"]


def test_lone_tag_cannot_interrupt_a_paragraph() -> None:
    """Condition 7 inside a paragraph is inline HTML; a heading still follows."""

    document = "text\n<span>\n# Real\n"

    assert _titles(document) == ["Real"]


def test_anchor_line_before_heading_opens_no_html_block() -> None:
    """``<a id="x"></a>`` as used in AGENTS.md is no lone tag; the heading stays."""

    document = '<a id="rule"></a>\n# Real\n'

    assert _titles(document) == ["Real"]


def test_indented_code_line_does_not_count_as_paragraph() -> None:
    """After an indented code block a lone tag may open an HTML block."""

    document = "\n    code\n<span>\n# Hidden\n\n# Real\n"

    assert _titles(document) == ["Real"]


def test_html_comment_inside_fence_opens_no_html_block() -> None:
    """Inside a fenced code block ``<!--`` is code, not an HTML block."""

    document = "```\n<!--\n```\n# Real\n"

    assert _titles(document) == ["Real"]


def test_unclosed_html_comment_raises_instead_of_dropping_headings() -> None:
    """A marker-ended HTML block left open fails loudly like an open fence."""

    document = "# Top\n<!--\n# Hidden\n"

    with pytest.raises(update_doctoc.TocGenerationError, match="line 2"):
        update_doctoc._iter_headings(document.splitlines())


def test_block_tag_html_open_at_end_of_document_does_not_raise() -> None:
    """Conditions 6 and 7 may legally run to the end of the document."""

    document = "# Top\n\n<div>\n# Hidden\n"

    assert _titles(document) == ["Top"]
    # Without a final newline the blank line before the probe comes from the
    # probe alone, and it is what ends such a block.
    assert _titles(document.rstrip("\n")) == ["Top"]


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        pytest.param(
            "text\n```\n```\n<span>\n# H\n", [], id="fence-close-ends-paragraph"
        ),
        pytest.param(
            "text\n<!-- c -->\n<span>\n# H\n", [], id="one-line-html-ends-paragraph"
        ),
        pytest.param("<PRE>\n\n# H\n</pre>\n# R\n", ["R"], id="type1-start-any-case"),
        pytest.param("<pre>\n\n# H\n</PRE>\n# R\n", ["R"], id="type1-end-any-case"),
        pytest.param("<preview>\n\n# H\n", ["H"], id="type1-needs-name-boundary"),
        pytest.param("<!-x\n\n# H\n", ["H"], id="type4-needs-a-letter"),
        pytest.param("<!x\n# H\n", ["H"], id="type4-needs-an-uppercase-letter"),
        pytest.param("<!X\n# H\n>\n# R\n", ["R"], id="type4-uppercase-letter"),
        pytest.param("text\n<hr/>\n# H\n", [], id="type6-self-closing"),
        pytest.param("text\n<divx>\n# H\n", ["H"], id="type6-needs-name-boundary"),
        pytest.param("text\n</div>\n# H\n", [], id="type6-closing-tag"),
        pytest.param("\n    <div>\n# H\n", ["H"], id="type6-indented-four-is-code"),
        pytest.param("\n</span>\n# H\n", [], id="type7-closing-tag"),
        pytest.param("    <!--\n# H\n", ["H"], id="type2-indented-four-is-code"),
        pytest.param("<div>\n   \n# H\n", ["H"], id="whitespace-line-is-blank"),
        pytest.param(
            "text\n    <div>\n<span>\n# H\n", ["H"], id="type6-indented-four-continues"
        ),
        pytest.param(
            "text\n    <!--\n<span>\n# H\n", ["H"], id="type2-indented-four-continues"
        ),
    ],
)
def test_html_block_start_and_end_follow_commonmark(
    document: str, expected: list[str]
) -> None:
    """Each HTML block rule matches the CommonMark reference renderer."""

    assert _titles(document) == expected


@pytest.mark.parametrize(
    "document",
    [
        pytest.param("1. Step\n\n   <details>\n# Real\n", id="type6-after-blank"),
        pytest.param("- item\n  <div>\n# Real\n", id="type6-in-paragraph"),
        pytest.param("- item\n  <!-- note\n\n# Real\n", id="type2-unclosed"),
        pytest.param(
            "- item\n  <!-- note\n\n# Real\n\n<!-- x -->\n", id="type2-closed-later"
        ),
    ],
)
def test_indented_html_block_ends_with_its_list_item(document: str) -> None:
    """A dedented line leaves the list item and the HTML block inside it."""

    assert _titles(document) == ["Real"]


def test_blank_line_keeps_an_indented_html_comment_open() -> None:
    """A blank line inside a list item does not end a comment started there.

    The fence line after the blank line is still comment text. Expectation
    checked against cmark-gfm, which GitHub uses.
    """

    document = "- item\n  <!--\n\n  ```\n  -->\n# Real\n"

    assert _titles(document) == ["Real"]


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        pytest.param("  <div>\n# Hidden\n\n# Real\n", ["Real"], id="type6"),
        pytest.param(
            "  <!--\n```\n-->\n\n# Kept\n\n  <!--\n```\n-->\n",
            ["Kept"],
            id="type2-fence-pair",
        ),
        pytest.param("  <!--\n```\n-->\n# Real\n", ["Real"], id="type2-fence"),
        pytest.param(
            "  <div>\n<!--\n</div>\n\n# Real\n-->\n",
            ["Real"],
            id="type6-comment-start",
        ),
        pytest.param(
            "text\n*\n  <div>\n# Hidden\n\n# Real\n",
            ["Real"],
            id="empty-item-cannot-interrupt",
        ),
        pytest.param(
            "text\n2. x\n   <div>\n# Hidden\n\n# Real\n",
            ["Real"],
            id="ordered-item-cannot-interrupt",
        ),
        pytest.param(
            "-x\n  <div>\n# Hidden\n\n# Real\n", ["Real"], id="marker-needs-space"
        ),
        pytest.param(
            "- a\n# Top\n  <div>\n# Hidden\n\n# Real\n",
            ["Top", "Real"],
            id="heading-closes-item",
        ),
        pytest.param(
            "- a\n```\n```\n  <div>\n# Hidden\n\n# Real\n",
            ["Real"],
            id="fence-closes-item",
        ),
        pytest.param(
            "1.\n  <div>\n# Hidden\n\n# Real\n", ["Real"], id="empty-item-column"
        ),
        pytest.param(
            "-  a\n  <div>\n# Hidden\n\n# Real\n", ["Real"], id="gap-sets-column"
        ),
        pytest.param(
            "-    x\n  <!--\n# Hidden\n-->\n# Real\n",
            ["Real"],
            id="gap-of-four-sets-column",
        ),
        pytest.param(
            "* * *\n  <div>\n# Hidden\n\n# Real\n",
            ["Real"],
            id="thematic-break-is-no-item",
        ),
        pytest.param(
            "- - -\n  <div>\n# Hidden\n\n# Real\n",
            ["Real"],
            id="dashed-thematic-break-is-no-item",
        ),
        pytest.param(
            "- a\n---\n  <div>\n# Hidden\n\n# Real\n",
            ["Real"],
            id="thematic-break-closes-item",
        ),
        pytest.param(
            "- a\nb\n* * *\n  <div>\n# Hidden\n\n# Real\n",
            ["Real"],
            id="thematic-break-is-not-lazy",
        ),
        pytest.param(
            "- a\n___\n  <div>\n# Hidden\n\n# Real\n",
            ["Real"],
            id="underscore-thematic-break",
        ),
        pytest.param(
            "text\n* * *\n<span>\n# Hidden\n\n# Real\n",
            ["Real"],
            id="thematic-break-ends-paragraph",
        ),
    ],
)
def test_indented_html_block_outside_a_list_ends_only_at_its_end_condition(
    document: str, expected: list[str]
) -> None:
    """Without an open list item, a dedent does not end an HTML block.

    Expectations checked against cmark-gfm, which GitHub uses.
    """

    assert _titles(document) == expected


@pytest.mark.parametrize(
    "document",
    [
        pytest.param("- a\n\n  x\n- b\n  <div>\n# Real\n", id="sibling-item"),
        pytest.param(
            "1.  Step\n    x\n\n   <!--\n# Hidden\n-->\n# Real\n",
            id="wide-content-column",
        ),
        pytest.param("-      code\n  <div>\n# Real\n", id="wide-gap-is-code"),
        pytest.param("-   a\n- b\n  <!--\n# Real\n-->\n", id="marker-is-not-lazy"),
        pytest.param("+ a\n  <div>\n# Real\n", id="plus-marker"),
        pytest.param("1) a\n   <div>\n# Real\n", id="parenthesis-marker"),
        pytest.param("- a\n\n\n  <!--\n# Real\n-->\n", id="blank-lines-keep-item"),
        pytest.param(
            "- a\n\n    x\nb\n  <!--\n# Real\n-->\n",
            id="paragraph-indent-is-relative",
        ),
        pytest.param(
            "- a\n\n    x\nb\n  <div>\n# Real\n",
            id="paragraph-indent-is-relative-type6",
        ),
    ],
)
def test_list_item_content_column_bounds_the_html_block(document: str) -> None:
    """The content column of the open list item decides where the block ends."""

    assert _titles(document) == ["Real"]


@pytest.mark.parametrize(
    "document",
    [
        pytest.param("-\n<span>\n# Hidden\n\n# Real\n", id="empty-item-no-paragraph"),
        pytest.param(
            "-     code\n<span>\n# Hidden\n\n# Real\n", id="code-item-no-paragraph"
        ),
    ],
)
def test_item_without_paragraph_text_lets_a_lone_tag_open_a_block(
    document: str,
) -> None:
    """An empty item or one holding indented code leaves no paragraph open."""

    assert _titles(document) == ["Real"]


@pytest.mark.parametrize(
    ("document", "github_titles"),
    [
        pytest.param(
            "- a\n<span>\n# Hidden\n\n# Real\n", ["Real"], id="lone-tag-after-item"
        ),
        pytest.param(
            "- a\n</pre>\n# Hidden\n\n# Real\n", ["Real"], id="closing-tag-after-item"
        ),
        pytest.param(
            "1. a\n<x-y/>\n# Hidden\n\n# Real\n", ["Real"], id="custom-tag-after-item"
        ),
        pytest.param(
            "1. a\n-\n  <div>\n# Real\n", ["Real"], id="empty-item-after-item"
        ),
        pytest.param(
            "-   a\n2. b\n   <div>\n# Kept\n\n# Real\n",
            ["Kept", "Real"],
            id="numbered-item-after-item",
        ),
        pytest.param(
            "   1. a\n2. b\nc\n   <div>\n# Real\n", ["Real"], id="div-after-numbered"
        ),
    ],
)
def test_line_leaving_the_item_of_a_paragraph_may_interrupt_it(
    document: str, github_titles: list[str]
) -> None:
    """A line that leaves the paragraph's list item is not held by the paragraph.

    An empty item, an item not numbered 1 and a lone HTML tag cannot interrupt
    a paragraph. cmark-gfm (GitHub) applies that only to a line that stays in
    the paragraph's list item; a line indented less closes the item first and
    may then open either.
    """

    assert _titles(document) == github_titles


def test_lone_tag_leaving_the_item_holds_the_fence_below() -> None:
    """GitHub reads the fence and the heading below a lone tag as HTML.

    The HTML block ends at a blank line, so nothing is left open.
    """

    assert _titles("- a\n<span>\n```\n# Hidden\n") == []
    assert _titles("- a\n<span>\n```\n# Hidden\n\n# Real\n") == ["Real"]


def test_lazy_line_keeps_the_list_item_open() -> None:
    """A lazy paragraph continuation does not close its list item."""

    document = "- item\ntext\n  <!--\n# Hidden\n-->\n# Real\n"

    assert _titles(document) == ["Hidden", "Real"]


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        pytest.param("source", ["Real"], id="source-is-a-block-tag"),
        pytest.param("search", ["Hidden", "Real"], id="search-is-not"),
    ],
)
def test_block_tag_names_follow_cmark_gfm(tag: str, expected: list[str]) -> None:
    """GitHub's cmark-gfm knows ``source`` as a block tag, but not ``search``."""

    document = f"text\n<{tag}>\n# Hidden\n\n# Real\n"

    assert _titles(document) == expected


def test_html_indented_four_spaces_opens_a_block_in_a_nested_item() -> None:
    """In a nested list item, HTML indented four spaces starts an HTML block.

    The comment holds the fence line, and ``Real`` closes both items.
    """

    document = "- a\n  - b\n    <!--\n  ```\n  -->\n# Real\n"

    assert _titles(document) == ["Real"]


@pytest.mark.parametrize(
    "document",
    [
        pytest.param("- # In item\n\n# Real\n", id="list-item"),
        pytest.param("> # In quote\n\n# Real\n", id="block-quote"),
        pytest.param("   # Indented\n\n# Real\n", id="indented"),
        pytest.param("Setext\n======\n\n# Real\n", id="setext"),
        pytest.param("# Setext\n===\n\n# Real\n", id="setext-after-nbsp"),
        pytest.param("# Paragraph\n\n# Real\n", id="nbsp-is-no-separator"),
        pytest.param("#\n\n# Real\n", id="empty-heading"),
        pytest.param("#  \n\n# Real\n", id="empty-heading-with-spaces"),
    ],
)
def test_only_top_level_atx_headings_in_the_first_column_are_listed(
    document: str,
) -> None:
    """Headings in containers, indented, setext or empty ones stay out."""

    assert _titles(document) == ["Real"]


def test_tab_after_the_hashes_separates_the_title() -> None:
    """CommonMark accepts a tab after the hashes, as GitHub does."""

    assert update_doctoc._iter_headings(["##\tTabbed"]) == [(2, "Tabbed")]


def test_raw_html_cannot_pose_as_a_rendered_heading() -> None:
    """Raw HTML is omitted by the renderer, even with a forged source position."""

    document = '<h1 data-sourcepos="4:1-4:5">x</h1>\n\n```\n# Code\n```\n# Real\n'

    assert _titles(document) == ["Real"]


def test_lone_tag_right_after_a_table_hides_the_next_heading() -> None:
    """A table is no paragraph, so on GitHub a lone tag after it opens a block.

    Plain CommonMark has no tables and would read the same lines as a
    paragraph that the heading interrupts.
    """

    document = "| a | b |\n|---|---|\n<span>\n## Hidden\n\n## Real\n"

    assert _titles(document) == ["Real"]


def test_a_real_heading_named_like_the_probe_is_kept() -> None:
    """The probe is recognised by its line, never by its text."""

    assert _titles("# probe\n\n# Real\n# probe\n") == ["probe", "Real", "probe"]
    with pytest.raises(update_doctoc.TocGenerationError, match="line 2 "):
        _titles("# Real\n```\n# probe\n")


@pytest.mark.parametrize(
    ("document", "message"),
    [
        pytest.param(
            "```\n# a\n```\n\n<!--\n# b\n",
            "HTML block opened on line 5 is never closed",
            id="comment-after-closed-fence",
        ),
        pytest.param(
            "<!--\n# a\n-->\n\n~~~\n# b\n",
            "code fence opened on line 5 is never closed",
            id="fence-after-closed-comment",
        ),
        pytest.param(
            "<!--\n```\n# a\n",
            "HTML block opened on line 1 is never closed",
            id="fence-line-inside-open-comment",
        ),
    ],
)
def test_unclosed_block_names_its_kind_and_opening_line(
    document: str, message: str
) -> None:
    """The error points at the block that is still open, not at an earlier one."""

    with pytest.raises(update_doctoc.TocGenerationError, match=f"^{message}$"):
        _titles(document)


@pytest.mark.parametrize(
    ("line_end", "expected"),
    [
        pytest.param("\r\n", ["A", "B"], id="crlf"),
        pytest.param("\r", ["A", "B"], id="cr"),
    ],
)
def test_replace_toc_reads_the_line_endings_of_cmark_gfm(
    line_end: str, expected: list[str]
) -> None:
    """CR and CRLF end a line; the headings are found as with LF."""

    document = line_end.join(
        [update_doctoc.START_MARKER, update_doctoc.END_MARKER, "# A", "", "## B", ""]
    )

    toc = update_doctoc._replace_toc(document).split(update_doctoc.END_MARKER)[0]

    assert [line for line in toc.splitlines() if line.lstrip().startswith("- [")] == [
        f"{'  ' * index}- [{title}](#{title.lower()})"
        for index, title in enumerate(expected)
    ]


def test_replace_toc_does_not_split_at_a_form_feed() -> None:
    """A form feed is no line end for cmark-gfm, so the text stays a paragraph."""

    document = "\n".join(
        [update_doctoc.START_MARKER, update_doctoc.END_MARKER, "x\x0c# Hidden", ""]
    )

    toc = update_doctoc._replace_toc(document).split(update_doctoc.END_MARKER)[0]

    assert "Hidden" not in toc


def test_slug_drops_unicode_symbols_and_punctuation_like_github() -> None:
    """A heading with a symbol gets the id GitHub renders, not the npm one.

    The expected value is the ``id`` GitHub's own HTML rendering of the root
    ``AGENTS.md`` gives this heading (``user-content-`` prefix removed): the
    ``\\u2265`` sign and the ``%`` sign disappear, and the two spaces around the
    removed sign leave a double hyphen.
    """

    slug = update_doctoc._slugify("1. High-confidence mode (≥ 90 %)", defaultdict(int))

    assert slug == "1-high-confidence-mode--90-"


def test_slug_keeps_hyphen_and_underscore_and_drops_ascii_punctuation() -> None:
    """The ASCII rules stay as they were; ``-`` and ``_`` survive the filter."""

    slug = update_doctoc._slugify("Cache_dir & re-use: `foo.bar`!", defaultdict(int))

    assert slug == "cache_dir--re-use-foobar"


def test_slug_keeps_letters_outside_ascii() -> None:
    """Letters are kept, whatever the script; only punctuation and symbols go."""

    slug = update_doctoc._slugify("Grüße aus Köln", defaultdict(int))

    assert slug == "gr%C3%BC%C3%9Fe-aus-k%C3%B6ln"
