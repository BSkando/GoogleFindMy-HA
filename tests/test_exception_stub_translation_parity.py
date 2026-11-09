# tests/test_exception_stub_translation_parity.py
"""The ``conftest`` exception stubs accept translation metadata like the core.

The integration raises ``UpdateFailed`` with ``translation_domain``,
``translation_key`` and ``translation_placeholders``, and the setup wrapper
re-raises ``ConfigEntryNotReady`` from it. The suite runs almost every test
against the lightweight stubs in ``tests/conftest.py``, never against the core.
A stub that rejected those keyword arguments would fail every test that
builds one of them with a ``TypeError``; a stub that accepted them but rendered ``str()`` differently
would let a wrong setup message look right. This module therefore builds the
same exceptions from the stub and from the core and compares what each stores
and renders.

Comparison rule: no ``inspect.signature`` comparison. The core signature of
``UpdateFailed`` is ``(*args, retry_after=None, **kwargs)``, the translation
arguments live only in ``HomeAssistantError.__init__``, and
``ConfigEntryNotReady`` has no ``__init__`` of its own. The tests construct
instead and compare attributes and ``str()``.

The stub classes are bound at import time of this module, which is before the
``use_real_homeassistant_modules`` fixture swaps ``sys.modules``; the core
classes are imported inside the tests, after the swap.

``str()`` of a core exception that carries a key but no message looks the key
up in the translation cache of the running instance, so that case runs under
the ``hass`` fixture. No translations are loaded there, which is the least
favourable case of the core (the raw key comes back). The stub reproduces that
case on purpose: a wrapper that builds its text from such an exception then
shows the raw key in the stub world as well, and its test fails.
"""

from __future__ import annotations

import importlib.util
import sys
from importlib.metadata import version
from typing import TYPE_CHECKING, Any

import pytest
from awesomeversion import AwesomeVersion
from homeassistant.exceptions import ConfigEntryNotReady as StubConfigEntryNotReady
from homeassistant.exceptions import HomeAssistantError as StubHomeAssistantError
from homeassistant.helpers.update_coordinator import UpdateFailed as StubUpdateFailed

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

DOMAIN = "googlefindmy"
KEY = "parity_probe_key"
PLACEHOLDERS = {"count": "3"}
TRANSLATION_ATTRIBUTES = (
    "translation_domain",
    "translation_key",
    "translation_placeholders",
)

# First core on which a feature was measured present; older cores may lack it.
RETRY_AFTER_SINCE = "2026.1.3"  # absent on 2025.9.1
CLASS_DEFAULTS_SINCE = "2026.9.3"  # absent on 2026.1.3

_HAS_PLUGIN = (
    importlib.util.find_spec("pytest_homeassistant_custom_component") is not None
)


def _core_classes() -> tuple[type[Any], type[Any], type[Any]]:
    """Return ``HomeAssistantError``, ``UpdateFailed``, ``ConfigEntryNotReady`` of the core."""
    from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError
    from homeassistant.helpers.update_coordinator import UpdateFailed

    return HomeAssistantError, UpdateFailed, ConfigEntryNotReady


def _core_takes_retry_after() -> bool:
    """Probe ``UpdateFailed(retry_after=...)`` by construction.

    Home Assistant 2025.9.1, the floor in ``hacs.json``, defines
    ``UpdateFailed`` without an ``__init__`` of its own and rejects the
    keyword.
    """
    _, core_update_failed, _ = _core_classes()
    try:
        err = core_update_failed("text", retry_after=1.0)
    except TypeError:
        return False
    return getattr(err, "retry_after", None) == 1.0


def _require_core_feature(present: bool, feature: str, since: str) -> None:
    """Skip on a core older than ``since``; fail on a newer core without it.

    The stubs follow the newest core. A core that predates a feature is
    skipped with a visible reason; a core from ``since`` on that lacks it
    fails, so a renamed or removed field cannot drop a comparison silently.
    """
    if present:
        return
    installed = AwesomeVersion(version("homeassistant"))
    assert installed < AwesomeVersion(since), (
        f"{feature} is missing on core {installed}, measured present on {since}"
    )
    pytest.skip(f"core {installed} predates {feature} (measured present on {since})")


def test_missing_feature_fails_from_its_anchor_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A core at or above the anchor that lacks the feature fails, not skips."""
    monkeypatch.setattr(sys.modules[__name__], "version", lambda _name: "2026.1.3")
    try:
        _require_core_feature(False, "probe", "2026.1.3")
    except pytest.skip.Exception:
        pytest.fail("a core at the anchor without the feature was skipped")
    except AssertionError as err:
        assert "measured present on 2026.1.3" in str(err)
    else:
        pytest.fail("a core at the anchor without the feature passed")


def test_missing_feature_is_skipped_below_its_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A core below the anchor that lacks the feature is skipped with a reason."""
    monkeypatch.setattr(sys.modules[__name__], "version", lambda _name: "2025.9.1")
    with pytest.raises(pytest.skip.Exception, match="predates probe"):
        _require_core_feature(False, "probe", "2026.1.3")


def _stub_classes() -> tuple[type[Any], type[Any], type[Any]]:
    return StubHomeAssistantError, StubUpdateFailed, StubConfigEntryNotReady


def _translation_kwargs() -> dict[str, Any]:
    return {
        "translation_domain": DOMAIN,
        "translation_key": KEY,
        "translation_placeholders": dict(PLACEHOLDERS),
    }


def _stored(err: BaseException, *, with_retry: bool) -> dict[str, Any]:
    names = (
        *TRANSLATION_ATTRIBUTES,
        "generate_message",
        *(("retry_after",) if with_retry else ()),
    )
    return {name: getattr(err, name) for name in names}


def test_bound_classes_are_the_conftest_stubs() -> None:
    """Guard the premise: the module-level names are the stubs, not the core."""
    for klass in _stub_classes():
        assert not klass.__module__.startswith("homeassistant"), klass


def test_stub_hierarchy_matches_the_core(use_real_homeassistant_modules: None) -> None:
    """``UpdateFailed`` and ``ConfigEntryNotReady`` derive from ``HomeAssistantError``."""
    core_error, core_update_failed, core_not_ready = _core_classes()
    assert issubclass(core_update_failed, core_error)
    assert issubclass(core_not_ready, core_error)
    assert issubclass(StubUpdateFailed, StubHomeAssistantError)
    assert issubclass(StubConfigEntryNotReady, StubHomeAssistantError)


@pytest.mark.parametrize(
    ("index", "with_retry"),
    [(0, False), (1, False), (1, True), (2, False)],
    ids=[
        "HomeAssistantError",
        "UpdateFailed",
        "UpdateFailed-retry_after",
        "ConfigEntryNotReady",
    ],
)
def test_stub_stores_translation_arguments_like_the_core(
    use_real_homeassistant_modules: None, index: int, with_retry: bool
) -> None:
    """All translation arguments (and ``retry_after``) are accepted and stored alike."""
    if with_retry:
        _require_core_feature(
            _core_takes_retry_after(),
            "UpdateFailed(retry_after=...)",
            RETRY_AFTER_SINCE,
        )
    extra = {"retry_after": 30.0} if with_retry else {}
    core = _core_classes()[index]("text", **_translation_kwargs(), **extra)
    stub = _stub_classes()[index]("text", **_translation_kwargs(), **extra)

    assert _stored(stub, with_retry=with_retry) == _stored(core, with_retry=with_retry)
    assert stub.args == core.args


@pytest.mark.parametrize(
    "index",
    [0, 1, 2],
    ids=["HomeAssistantError", "UpdateFailed", "ConfigEntryNotReady"],
)
def test_stub_defaults_match_the_core(
    use_real_homeassistant_modules: None, index: int
) -> None:
    """Without translation arguments every attribute is ``None`` on both sides."""
    core = _core_classes()[index]("text")
    stub = _stub_classes()[index]("text")

    assert _stored(stub, with_retry=False) == _stored(core, with_retry=False)
    assert str(stub) == str(core) == "text"


def test_update_failed_retry_after_default_matches_the_core(
    use_real_homeassistant_modules: None,
) -> None:
    """Without ``retry_after`` both sides store ``None``."""
    _require_core_feature(
        _core_takes_retry_after(), "UpdateFailed(retry_after=...)", RETRY_AFTER_SINCE
    )
    _, core_update_failed, _ = _core_classes()
    assert (
        StubUpdateFailed("text").retry_after == core_update_failed("text").retry_after
    )


def _bare_pair() -> tuple[BaseException, BaseException]:
    """Instances of a stub and a core subclass that skip ``super().__init__``."""
    core_error = _core_classes()[0]

    class _CoreBare(core_error):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            Exception.__init__(self)

    class _StubBare(StubHomeAssistantError):
        def __init__(self) -> None:
            Exception.__init__(self)

    return _StubBare(), _CoreBare()


def test_class_default_of_generate_message_matches_the_core(
    use_real_homeassistant_modules: None,
) -> None:
    """A subclass that skips ``super().__init__`` reads ``generate_message`` alike."""
    stub, core = _bare_pair()
    assert stub.generate_message == core.generate_message  # type: ignore[attr-defined]


def test_class_defaults_of_translation_fields_match_the_core(
    use_real_homeassistant_modules: None,
) -> None:
    """A subclass that skips ``super().__init__`` reads the same field defaults."""
    core_error = _core_classes()[0]
    _require_core_feature(
        all(hasattr(core_error, name) for name in TRANSLATION_ATTRIBUTES),
        "class defaults of the translation fields",
        CLASS_DEFAULTS_SINCE,
    )
    stub, core = _bare_pair()
    assert {n: getattr(stub, n) for n in TRANSLATION_ATTRIBUTES} == {
        n: getattr(core, n) for n in TRANSLATION_ATTRIBUTES
    }


def test_update_failed_text_wins_over_the_key(
    use_real_homeassistant_modules: None,
) -> None:
    """(i) With a text argument ``str()`` is the text, key or not."""
    _, core_update_failed, _ = _core_classes()
    core = core_update_failed("server said no", **_translation_kwargs())
    stub = StubUpdateFailed("server said no", **_translation_kwargs())

    assert str(stub) == str(core) == "server said no"


def test_config_entry_not_ready_falls_back_to_its_cause(
    use_real_homeassistant_modules: None,
) -> None:
    """(iii) ``ConfigEntryNotReady`` without text renders ``str(__cause__)``."""
    _, core_update_failed, core_not_ready = _core_classes()
    core = core_not_ready()
    core.__cause__ = core_update_failed("cold start")
    stub = StubConfigEntryNotReady()
    stub.__cause__ = StubUpdateFailed("cold start")

    assert str(stub) == str(core) == "cold start"


@pytest.mark.skipif(
    not _HAS_PLUGIN, reason="needs pytest-homeassistant-custom-component"
)
@pytest.mark.asyncio
async def test_key_without_text_renders_the_key_without_cached_translations(
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(ii) Key but no text: the core returns the raw key when no translation is cached.

    ``use_real_homeassistant_modules`` re-imports ``homeassistant.core``, whose
    thread-local ``_hass`` is then empty, while the ``hass`` fixture registered
    itself in the module the plugin imported first. ``async_get_hass`` of the
    re-imported module would raise, so the instance is registered there too.
    Its translation cache is created on first use and holds nothing for this
    domain, which is the case under test.
    """
    from homeassistant import core as real_core

    monkeypatch.setattr(real_core._hass, "hass", hass)
    _, core_update_failed, _ = _core_classes()
    core = core_update_failed(**_translation_kwargs())
    stub = StubUpdateFailed(**_translation_kwargs())

    assert str(stub) == str(core) == KEY
