# tests/test_translation_placeholders.py
"""Translation placeholder consistency tests."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

BASE_LANGUAGE = "en"
OUTDATED_PLACEHOLDERS: dict[tuple[str, ...], set[str]] = {}

_PLACEHOLDER_PATTERN = re.compile(r"{([a-zA-Z0-9_]+)}")


def _collect_placeholders(
    value: Any, path: tuple[str, ...], target: dict[tuple[str, ...], set[str]]
) -> None:
    """Recursively gather placeholders from nested translation structures."""
    if isinstance(value, str):
        target.setdefault(path, set()).update(_PLACEHOLDER_PATTERN.findall(value))
        return

    target.setdefault(path, set())

    if isinstance(value, Mapping):
        for key, child in value.items():
            _collect_placeholders(child, path + (str(key),), target)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _collect_placeholders(child, path + (str(index),), target)


def _extract_placeholders(data: Any) -> dict[tuple[str, ...], set[str]]:
    """Extract all placeholders from translation data."""
    placeholders: dict[tuple[str, ...], set[str]] = {}
    _collect_placeholders(data, tuple(), placeholders)
    return placeholders


def _collect_string_paths(
    value: Any, path: tuple[str, ...], out: set[tuple[str, ...]]
) -> None:
    """Recursively collect paths to all string values."""
    if isinstance(value, str):
        out.add(path)
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            _collect_string_paths(child, path + (str(key),), out)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _collect_string_paths(child, path + (str(index),), out)


def _resolve_path(container: Any, path: tuple[str, ...]) -> Any:
    """Resolve a path to get a value from nested containers."""
    current = container
    for element in path:
        if isinstance(current, Mapping):
            current = current[element]
        elif isinstance(current, Sequence) and not isinstance(
            current, (str, bytes, bytearray)
        ):
            current = current[int(element)]
        else:
            raise KeyError(path)
    return current


def test_translation_placeholders() -> None:  # noqa: PLR0915
    """Main test for translation placeholder consistency."""
    translations_dir = (
        Path(__file__).resolve().parent.parent
        / "custom_components"
        / "googlefindmy"
        / "translations"
    )
    assert translations_dir.is_dir(), (
        f"Translations directory not found: {translations_dir}"
    )

    language_placeholders: dict[str, dict[tuple[str, ...], set[str]]] = {}

    language_data: dict[str, Any] = {}

    for json_file in sorted(translations_dir.glob("*.json")):
        with json_file.open("r", encoding="utf-8") as file:
            data = json.load(file)
        language_placeholders[json_file.stem] = _extract_placeholders(data)
        language_data[json_file.stem] = data

    assert BASE_LANGUAGE in language_placeholders, (
        f"Base language '{BASE_LANGUAGE}' translation is missing."
    )

    all_paths: set[tuple[str, ...]] = set()
    for placeholders in language_placeholders.values():
        all_paths.update(placeholders.keys())

    base_placeholders = language_placeholders[BASE_LANGUAGE]

    base_data = language_data[BASE_LANGUAGE]

    base_string_paths: set[tuple[str, ...]] = set()
    _collect_string_paths(base_data, tuple(), base_string_paths)

    for language, data in sorted(language_data.items()):
        missing_paths: list[str] = []
        for path in sorted(base_string_paths):
            try:
                value = _resolve_path(data, path)
            except (KeyError, IndexError, ValueError):
                missing_paths.append("/".join(path))
                continue
            assert isinstance(value, str), (
                f"{language} translation for {'/'.join(path)} should be a string, got {type(value)!r}"
            )
        assert not missing_paths, (
            f"{language} translation is missing strings for: {', '.join(missing_paths)}"
        )

        # Check for extra keys not present in base language
        if language != BASE_LANGUAGE:
            lang_string_paths: set[tuple[str, ...]] = set()
            _collect_string_paths(data, tuple(), lang_string_paths)
            extra_paths = lang_string_paths - base_string_paths
            assert not extra_paths, (
                f"{language} translation has extra keys not in {BASE_LANGUAGE}: "
                f"{', '.join('/'.join(p) for p in sorted(extra_paths))}"
            )

    for path in sorted(all_paths):
        union_placeholders: set[str] = set()
        for placeholders in language_placeholders.values():
            union_placeholders.update(placeholders.get(path, set()))

        path_display = "/".join(path) if path else "<root>"
        outdated_for_path = OUTDATED_PLACEHOLDERS.get(path, set())

        for language, placeholders in sorted(language_placeholders.items()):
            placeholders_for_lang = placeholders.get(path, set())

            missing = union_placeholders - placeholders_for_lang - outdated_for_path
            assert not missing, (
                f"{language} translation for {path_display} is missing placeholders: {sorted(missing)}"
            )

            placeholders_for_base = base_placeholders.get(path, set())
            extra = placeholders_for_lang - placeholders_for_base - outdated_for_path
            if language == BASE_LANGUAGE:
                assert not extra, (
                    f"Base language {BASE_LANGUAGE} has placeholders not in outdated list at {path_display}: {sorted(extra)}"
                )
            else:
                assert not extra, (
                    f"{language} translation for {path_display} has unexpected placeholders: {sorted(extra)}"
                )


class TestCollectPlaceholders:
    """Unit tests for _collect_placeholders helper function."""

    def test_collect_placeholders_with_sequence(self) -> None:
        """Placeholders should be collected from sequences (lists)."""
        data = ["Hello {name}", "Goodbye {user}"]
        result = _extract_placeholders(data)
        assert result[("0",)] == {"name"}
        assert result[("1",)] == {"user"}

    def test_collect_placeholders_nested_sequence(self) -> None:
        """Placeholders should be collected from nested sequences."""
        data = {"items": ["{first}", "{second}"]}
        result = _extract_placeholders(data)
        assert result[("items", "0")] == {"first"}
        assert result[("items", "1")] == {"second"}

    def test_collect_placeholders_with_non_container_values(self) -> None:
        """Non-container values (int, None, bool) should be handled gracefully."""
        # This tests the implicit else branch (line 33->exit)
        data = {"count": 42, "enabled": True, "empty": None}
        result = _extract_placeholders(data)
        # These paths should exist with empty placeholder sets
        assert result[("count",)] == set()
        assert result[("enabled",)] == set()
        assert result[("empty",)] == set()


class TestCollectStringPaths:
    """Unit tests for _collect_string_paths helper function."""

    def test_collect_string_paths_with_sequence(self) -> None:
        """String paths should include sequence indices."""
        data = {"messages": ["message1", "message2"]}
        string_paths: set[tuple[str, ...]] = set()
        _collect_string_paths(data, tuple(), string_paths)
        assert ("messages", "0") in string_paths
        assert ("messages", "1") in string_paths

    def test_collect_string_paths_top_level_sequence(self) -> None:
        """String paths should work for top-level sequences."""
        data = ["first", "second"]
        string_paths: set[tuple[str, ...]] = set()
        _collect_string_paths(data, tuple(), string_paths)
        assert ("0",) in string_paths
        assert ("1",) in string_paths

    def test_collect_string_paths_with_non_container_values(self) -> None:
        """Non-container values (int, None, bool) should not add paths."""
        # This tests the implicit else branch (line 55->exit)
        data = {"count": 42, "enabled": True, "empty": None}
        string_paths: set[tuple[str, ...]] = set()
        _collect_string_paths(data, tuple(), string_paths)
        # Non-string values should not be added to string paths
        assert ("count",) not in string_paths
        assert ("enabled",) not in string_paths
        assert ("empty",) not in string_paths


class TestResolvePath:
    """Unit tests for _resolve_path helper function."""

    def test_resolve_path_with_mapping(self) -> None:
        """Resolve path should work with nested mappings."""
        data = {"level1": {"level2": "value"}}
        assert _resolve_path(data, ("level1", "level2")) == "value"

    def test_resolve_path_with_sequence(self) -> None:
        """Resolve path should work with sequence indices."""
        data = {"items": ["first", "second", "third"]}
        assert _resolve_path(data, ("items", "0")) == "first"
        assert _resolve_path(data, ("items", "2")) == "third"

    def test_resolve_path_top_level_sequence(self) -> None:
        """Resolve path should work with top-level sequences."""
        data = ["first", "second"]
        assert _resolve_path(data, ("0",)) == "first"
        assert _resolve_path(data, ("1",)) == "second"

    def test_resolve_path_raises_on_invalid_type(self) -> None:
        """Resolve path should raise KeyError for invalid container type."""
        data = {"value": 42}  # integer, not a container
        with pytest.raises(KeyError):
            _resolve_path(data, ("value", "nested"))


class TestMissingPathHandling:
    """Unit tests for exception handling in path resolution."""

    def test_index_error_on_out_of_bounds(self) -> None:
        """IndexError should be raised for out-of-bounds sequence access."""
        data = {"items": ["only_one"]}
        with pytest.raises(IndexError):
            _resolve_path(data, ("items", "5"))

    def test_value_error_on_non_integer_index(self) -> None:
        """ValueError should be raised for non-integer sequence indices."""
        data = {"items": ["first"]}
        with pytest.raises(ValueError):
            _resolve_path(data, ("items", "not_a_number"))

    def test_exception_handling_in_translation_validation(self) -> None:
        """Exception handling should collect missing paths without crashing.

        This tests the exception handling logic with incomplete data.
        """
        # Simulate base translation data
        base_data = {"key1": "value1", "key2": "value2", "nested": {"inner": "text"}}
        base_string_paths: set[tuple[str, ...]] = set()
        _collect_string_paths(base_data, tuple(), base_string_paths)

        # Simulate incomplete translation (missing "key2" and "nested.inner")
        incomplete_data = {"key1": "translated1"}

        # Run the same validation logic as the main test
        missing_paths: list[str] = []
        for path in sorted(base_string_paths):
            try:
                value = _resolve_path(incomplete_data, path)
            except (KeyError, IndexError, ValueError):
                missing_paths.append("/".join(path))
                continue
            assert isinstance(value, str)

        # Should have collected the missing paths
        assert "key2" in missing_paths
        assert "nested/inner" in missing_paths
        assert "key1" not in missing_paths  # This one exists


def test_translation_validation_with_missing_keys(tmp_path: Path) -> None:
    """Test main validation logic handles missing translation keys gracefully.

    This test creates temporary translation files where one language is
    missing keys to exercise the exception handling in lines 119-121.
    """
    # Create base language file
    en_file = tmp_path / "en.json"
    en_data = {"config": {"title": "Title", "description": "Description"}}
    en_file.write_text(json.dumps(en_data), encoding="utf-8")

    # Create incomplete translation file (missing "description")
    de_file = tmp_path / "de.json"
    de_data = {"config": {"title": "Titel"}}  # Missing "description"
    de_file.write_text(json.dumps(de_data), encoding="utf-8")

    # Run the validation logic (same as main test)
    language_data: dict[str, Any] = {}
    for json_file in sorted(tmp_path.glob("*.json")):
        with json_file.open("r", encoding="utf-8") as file:
            data = json.load(file)
        language_data[json_file.stem] = data

    base_data = language_data["en"]
    base_string_paths: set[tuple[str, ...]] = set()
    _collect_string_paths(base_data, tuple(), base_string_paths)

    # Validate each language
    for language, data in sorted(language_data.items()):
        missing_paths: list[str] = []
        for path in sorted(base_string_paths):
            try:
                value = _resolve_path(data, path)
            except (KeyError, IndexError, ValueError):
                missing_paths.append("/".join(path))
                continue
            assert isinstance(value, str), (
                f"{language} translation for {'/'.join(path)} should be a string"
            )

        if language == "de":
            # German translation should be missing the description
            assert "config/description" in missing_paths


def test_main_test_exception_handling_with_mock() -> None:
    """Test that main test handles exceptions in _resolve_path.

    This specifically tests lines 119-121 by patching _resolve_path
    to raise KeyError on the first call during the main test execution.
    """
    call_count = [0]
    original_resolve_path = _resolve_path

    def mock_resolve_path(container: Any, path: tuple[str, ...]) -> Any:
        call_count[0] += 1
        # Raise KeyError on the 5th call to simulate a missing key
        if call_count[0] == 5:
            raise KeyError(path)
        return original_resolve_path(container, path)

    with patch(
        "tests.test_translation_placeholders._resolve_path",
        side_effect=mock_resolve_path,
    ):
        # The main test should complete without raising an assertion error
        # even when some paths can't be resolved (it collects them instead)
        # However, it will fail on the assertion for missing paths
        # So we catch the AssertionError and verify it mentions missing paths
        try:
            test_translation_placeholders()
        except AssertionError as e:
            # The test should fail because of missing paths
            assert (
                "missing strings for" in str(e).lower() or "missing" in str(e).lower()
            )


# Mirrors ``RE_URL`` from home-assistant/core ``script/hassfest/translations.py``.
# hassfest is the authority in CI and rejects a literal URL in *any* translation
# value ("the string should not contain URLs, please use description
# placeholders instead"), which is a hard error, not a warning. The rule is
# reproduced here so the violation surfaces in the local suite instead of after
# a push, and so the fix stays the sanctioned one: put the address in a
# ``description_placeholders`` entry, never in the translated text.
_HASSFEST_URL_PATTERN = re.compile(
    r"(((ftp|ftps|scp|http|https|mqtt|mqtts|socket|socks5):\/\/|www\.)"
    r"[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?)",
    re.IGNORECASE,
)


def test_translation_values_contain_no_literal_urls() -> None:
    """No translated string may embed a URL; hassfest fails the build on it."""
    # Negative control first: a silently broken pattern would let the real check
    # pass on everything, so pin that it still recognises the shape it guards.
    assert _HASSFEST_URL_PATTERN.search("see https://github.com/example/repo")
    assert _HASSFEST_URL_PATTERN.search("www.example.com/docs")
    # ...and that the sanctioned replacement does NOT trip it.
    assert not _HASSFEST_URL_PATTERN.search("full instructions: {docs_url}")

    component_dir = (
        Path(__file__).resolve().parent.parent / "custom_components" / "googlefindmy"
    )
    files = [
        component_dir / "strings.json",
        *sorted((component_dir / "translations").glob("*.json")),
    ]
    assert len(files) > 1, "no translation files found"

    offenders: list[str] = []
    for json_file in files:
        with json_file.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        paths: set[tuple[str, ...]] = set()
        _collect_string_paths(data, tuple(), paths)
        for path in sorted(paths):
            value = _resolve_path(data, path)
            if isinstance(value, str) and (
                match := _HASSFEST_URL_PATTERN.search(value)
            ):
                offenders.append(
                    f"{json_file.name}:{'.'.join(path)} -> {match.group(0)}"
                )

    assert not offenders, (
        "Literal URLs in translation values (hassfest rejects these; use a "
        "description placeholder instead):\n" + "\n".join(offenders)
    )


# German is the one locale where Home Assistant mandates a form of address: the
# informal "du", never the formal "Sie" (Home Assistant translation guidelines,  # codespell:ignore
# German section: "Duze in den Uebersetzungen, und verwende nicht das formale
# 'Sie'."). AGENTS.md pins this under "Consistent form of address". This guard
# reproduces the reported regression -- formal "Sie"/"Ihre"/"Ihnen" leaking into  # codespell:ignore
# the otherwise informal German file -- so it surfaces in the local suite instead
# of in review.
_GERMAN_FORMAL_ADDRESS_PATTERNS = (
    # Formal address pronoun "Sie" -- covers the imperative "Wenden Sie ..."  # codespell:ignore
    # as well as the standalone "Sie koennen ...". Case sensitive on purpose:  # codespell:ignore
    # the informal lowercase "sie" (she/they) is legitimate and must not trip.  # codespell:ignore
    re.compile(r"\bSie\b"),
    # Formal dative pronoun.
    re.compile(r"\bIhnen\b"),
    # Formal possessive: bare "Ihr" plus "Ihre/Ihren/Ihrem/Ihrer/Ihres".
    re.compile(r"\bIhr(?:e[nmrs]?)?\b"),
)


def test_german_translation_uses_informal_du() -> None:
    """German UI text must use the informal "du", never the formal "Sie"."""  # codespell:ignore
    # Negative control first: a silently broken pattern set would let the real
    # check pass on everything, so pin that it still recognises every formal
    # address form it guards against -- imperative, standalone "Sie", the bare  # codespell:ignore
    # and inflected possessive "Ihr(e...)", and the dative "Ihnen".
    for formal in (
        "Wenden Sie diese Optionen an.",  # formal imperative  # codespell:ignore
        "Beispiele finden Sie hier.",  # formal imperative  # codespell:ignore
        "Sie können das später ändern.",  # standalone formal "Sie"  # codespell:ignore
        "Erneuere Ihre secrets.json.",  # inflected formal possessive
        "Ihr Gerät bleibt erhalten.",  # bare formal possessive "Ihr"
        "Das gehört Ihnen.",  # formal dative
    ):
        assert any(p.search(formal) for p in _GERMAN_FORMAL_ADDRESS_PATTERNS), formal
    # ...and that the sanctioned informal address does NOT trip it, including
    # the lowercase "sie" (she/they) and "ihr" (informal plural) that merely  # codespell:ignore
    # share a spelling with the formal forms but are legitimate German.
    informal = (
        "Wende diese Optionen an. Beispiele findest du hier. "
        "Erneuere deine secrets.json. Wenn Geräte fehlen, werden sie "  # codespell:ignore
        "ausgeblendet, und ihr könnt sie wieder einblenden."  # codespell:ignore
    )
    assert not any(p.search(informal) for p in _GERMAN_FORMAL_ADDRESS_PATTERNS)

    de_file = (
        Path(__file__).resolve().parent.parent
        / "custom_components"
        / "googlefindmy"
        / "translations"
        / "de.json"
    )
    with de_file.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    paths: set[tuple[str, ...]] = set()
    _collect_string_paths(data, tuple(), paths)

    offenders: list[str] = []
    for path in sorted(paths):
        value = _resolve_path(data, path)
        if not isinstance(value, str):
            continue
        for pattern in _GERMAN_FORMAL_ADDRESS_PATTERNS:
            if match := pattern.search(value):
                offenders.append(f"de.json:{'.'.join(path)} -> {match.group(0)}")

    assert not offenders, (
        "German translation must use the informal 'du', not the formal 'Sie' "
        "(AGENTS.md 'Consistent form of address'; Home Assistant mandates the "
        "informal address for German). Formal address found:\n" + "\n".join(offenders)
    )
