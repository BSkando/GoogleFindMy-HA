# tests/test_check_ha_compatibility_https.py
"""The compatibility checker only ever opens ``https`` URLs.

``urllib.request.urlopen`` also opens ``file:`` and ``ftp:`` URLs and any scheme
a custom handler registers. The checker builds its URLs from ``https`` literals,
but one of them interpolates the ``--ha-version`` argument, so the scheme check
sits in front of each ``urlopen`` call rather than being implied by the
literals. bandit reports B310 for every ``urlopen`` call with a ``Request``
object, checked or not; the two ``# nosec B310`` markers point at this check.
"""

from __future__ import annotations

from typing import Any

import pytest

from script import check_ha_compatibility


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "http://raw.githubusercontent.com/home-assistant/core/dev/x.txt",
        "ftp://example.invalid/x.txt",
        "HTTPS-like://example.invalid/",
        "//example.invalid/no-scheme",
    ],
)
def test_a_non_https_url_is_refused_before_it_is_opened(
    url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The refusal happens before ``urlopen``; nothing is read from the URL."""

    opened: list[Any] = []

    def _record(request: Any, timeout: float | None = None) -> Any:
        opened.append(request)
        raise AssertionError("urlopen must not be reached for a non-https URL")

    monkeypatch.setattr(check_ha_compatibility.urllib.request, "urlopen", _record)

    with pytest.raises(ValueError, match="https"):
        check_ha_compatibility._fetch_url_content(url)

    assert opened == []


def test_an_https_url_reaches_urlopen(monkeypatch: pytest.MonkeyPatch) -> None:
    """The check does not refuse the scheme the checker actually uses."""

    class _Response:
        def __enter__(self) -> _Response:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def read(self) -> bytes:
            return b"aiohttp==3.0\n"

    opened: list[str] = []

    def _record(request: Any, timeout: float | None = None) -> _Response:
        opened.append(request.full_url)
        return _Response()

    monkeypatch.setattr(check_ha_compatibility.urllib.request, "urlopen", _record)
    url = "https://raw.githubusercontent.com/home-assistant/core/dev/x.txt"

    assert check_ha_compatibility._fetch_url_content(url) == "aiohttp==3.0\n"
    assert opened == [url]


@pytest.mark.parametrize(
    "url", ["https://pypi.org/pypi/homeassistant/json", "HTTPS://pypi.org/x"]
)
def test_require_https_accepts_https(url: str) -> None:
    """URL schemes are case-insensitive; ``urlparse`` lowercases them."""

    check_ha_compatibility._require_https(url)
