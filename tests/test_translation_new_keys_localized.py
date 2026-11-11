# tests/test_translation_new_keys_localized.py
"""New translation keys must be translated, not copied from English.

``script/sync_translations.py`` fills a missing key in every language file with
the English value. That keeps the files in sync, but a user of another language
then sees English. This gate lists the keys added by a feature and requires,
for each of them: a non-empty value, the same value in ``strings.json`` and
``en.json``, and in every other language file a non-empty value that differs
from the English one. A language may keep the English word only with an entry
and a reason in ``ALLOWED_IDENTICAL``.

Each finding is one line: ``MISSING <file> <key>``, ``ENGLISH <file> <key>`` or
``MISMATCH en/strings <key>``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1] / "custom_components" / "googlefindmy"

NEW_KEYS: tuple[str, ...] = ("entity.switch.automatic_location_polling.name",)

# (language file name, key) -> reason why the English value is correct there.
ALLOWED_IDENTICAL: Mapping[tuple[str, str], str] = {}

_MISSING = object()


def _lookup(tree: Any, key: str) -> Any:
    node = tree
    for part in key.split("."):
        if not isinstance(node, Mapping) or part not in node:
            return _MISSING
        node = node[part]
    return node


def _empty(value: Any) -> bool:
    return value is _MISSING or not isinstance(value, str) or not value.strip()


def localization_findings(
    strings: Mapping[str, Any],
    english: Mapping[str, Any],
    others: Mapping[str, Mapping[str, Any]],
    keys: tuple[str, ...],
    allowed: Mapping[tuple[str, str], str],
) -> list[str]:
    """Return one line per finding; an empty list means every key is localized."""
    findings: list[str] = []
    for key in keys:
        en_value = _lookup(english, key)
        strings_value = _lookup(strings, key)
        if _empty(en_value) or _empty(strings_value):
            findings.append(f"MISSING en/strings {key}")
            continue
        if en_value != strings_value:
            findings.append(f"MISMATCH en/strings {key}")
        for name in sorted(others):
            value = _lookup(others[name], key)
            if _empty(value):
                findings.append(f"MISSING {name} {key}")
            elif value == en_value and (name, key) not in allowed:
                findings.append(f"ENGLISH {name} {key}")
    return findings


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_new_keys_are_localized() -> None:
    """Every key in NEW_KEYS is translated in every language file."""
    translations = _ROOT / "translations"
    others = {
        path.name: _load(path)
        for path in sorted(translations.glob("*.json"))
        if path.name != "en.json"
    }
    findings = localization_findings(
        _load(_ROOT / "strings.json"),
        _load(translations / "en.json"),
        others,
        NEW_KEYS,
        ALLOWED_IDENTICAL,
    )
    assert not findings, "\n".join(findings)


def test_predicate_reports_each_kind_of_finding() -> None:
    """Negative path: the predicate names exactly the defective files and keys."""
    key = "entity.switch.demo.name"
    missing_key = "entity.switch.absent.name"
    unequal_key = "entity.switch.unequal.name"

    def tree(**names: str) -> dict[str, Any]:
        return {"entity": {"switch": {k: {"name": v} for k, v in names.items()}}}

    english = tree(demo="Demo", unequal="Unequal")
    strings = tree(demo="Demo", unequal="Different")
    others = {
        "de.json": tree(demo="Vorführung", unequal="Ungleich"),
        "it.json": tree(demo="Demo", unequal="Disuguale"),
        "nl.json": tree(demo="   ", unequal="Ongelijk"),
        "pl.json": tree(unequal="Nierówny"),
    }

    findings = localization_findings(
        strings, english, others, (key, missing_key, unequal_key), {}
    )

    assert findings == [
        f"ENGLISH it.json {key}",
        f"MISSING nl.json {key}",
        f"MISSING pl.json {key}",
        f"MISSING en/strings {missing_key}",
        f"MISMATCH en/strings {unequal_key}",
    ]
    assert localization_findings(
        strings, english, others, (key,), {("it.json", key): "brand name"}
    ) == [f"MISSING nl.json {key}", f"MISSING pl.json {key}"]
