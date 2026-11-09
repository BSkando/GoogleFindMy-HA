# tests/test_setup_retry_reason_translation.py
"""A failed first refresh leaves a translatable setup-retry reason on the entry.

The integration card shows ``ConfigEntry.error_reason_translation_key`` when
the entry is in ``SETUP_RETRY``. Home Assistant sets it from the
``ConfigEntryNotReady`` that ``async_setup_entry`` raises, and the core copies
the key of an ``UpdateFailed`` cause into that exception during
``async_config_entry_first_refresh``. This test drives that chain with the
real ``DataUpdateCoordinator``, the real config-entry setup and the
production helper ``_initial_refresh_not_ready`` that the integration's
``async_setup_entry`` raises.

Home Assistant is built from a fresh import of
``pytest_homeassistant_custom_component.common`` after
``use_real_homeassistant_modules`` swapped in the real modules (the pattern of
``tests/test_services_permissions.py``): the plugin's own ``hass`` fixture
belongs to the copy imported when the plugin loaded, whose
``ConfigEntryNotReady`` is a different class than the one imported here.

``custom_components.googlefindmy`` itself stays bound to the stubs. The
helper reads ``ConfigEntryNotReady`` from its module globals, so the test
binds that one name to the core class through ``monkeypatch``; the helper's
code runs unchanged. The ``try`` around the call in ``async_setup_entry`` is
covered by ``tests/test_config_entry_startup.py`` in the stub world.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from collections.abc import AsyncIterator
from importlib.metadata import version
from types import ModuleType
from typing import TYPE_CHECKING, Any

import pytest
from awesomeversion import AwesomeVersion

if (
    importlib.util.find_spec("pytest_homeassistant_custom_component") is None
):  # pragma: no cover - optional dependency
    pytest.skip(
        "pytest-homeassistant-custom-component is required for this test",
        allow_module_level=True,
    )

pytest_plugins = ("pytest_homeassistant_custom_component",)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

pytestmark = pytest.mark.asyncio

DOMAIN = "googlefindmy"
KEY = "cold_start_empty_device_list"
COMMON_PARENT = "pytest_homeassistant_custom_component"
COMMON_CHILD = "common"
COMMON = f"{COMMON_PARENT}.{COMMON_CHILD}"


@pytest.fixture(autouse=True)
def _use_real_ha_modules(use_real_homeassistant_modules: None) -> None:
    """Activate real Home Assistant modules from the central conftest fixture."""


@pytest.fixture
def ha_common(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Import the plugin's ``common`` freshly against the real modules.

    Every key the import creates is handed to ``monkeypatch`` and removed on
    teardown, so later stub-world tests do not see this copy.
    """

    def _keys() -> set[str]:
        return {k for k in sys.modules if k == COMMON or k.startswith(f"{COMMON}.")}

    before = _keys()
    monkeypatch.delitem(sys.modules, COMMON, raising=False)
    parent = importlib.import_module(COMMON_PARENT)
    monkeypatch.setattr(
        parent, COMMON_CHILD, getattr(parent, COMMON_CHILD, None), raising=False
    )
    module = importlib.import_module(COMMON)
    for key in sorted(_keys() - before):
        created = sys.modules.pop(key)
        monkeypatch.setitem(sys.modules, key, created)
    return module


@pytest.fixture
async def hass(ha_common: ModuleType) -> AsyncIterator[HomeAssistant]:
    """Home Assistant built from the same module copy as the classes below."""

    from homeassistant.helpers import frame

    with ha_common.mock_storage():
        async with ha_common.async_test_home_assistant() as instance:
            frame.async_setup(instance)
            yield instance
            await instance.async_stop(force=True)


async def _setup_entry_with_cause(
    hass: HomeAssistant,
    ha_common: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    cause_kwargs: dict[str, Any],
) -> Any:
    """Set up an entry whose first refresh fails with ``UpdateFailed``."""

    from homeassistant.config_entries import ConfigFlow
    from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
    from homeassistant.helpers.update_coordinator import (
        DataUpdateCoordinator,
        UpdateFailed,
    )

    import custom_components.googlefindmy as integration

    monkeypatch.setattr(integration, "ConfigEntryNotReady", ConfigEntryNotReady)

    async def _update() -> None:
        raise UpdateFailed(
            "Cold start: empty device list; treating as transient.", **cause_kwargs
        )

    async def _setup(hass: HomeAssistant, entry: Any) -> bool:
        coordinator: DataUpdateCoordinator[None] = DataUpdateCoordinator(
            hass,
            integration._LOGGER,
            config_entry=entry,
            name=DOMAIN,
            update_method=_update,
        )
        try:
            await coordinator.async_config_entry_first_refresh()
        except ConfigEntryAuthFailed:
            raise
        except Exception as err:
            raise integration._initial_refresh_not_ready(err) from err
        return True

    ha_common.mock_integration(
        hass, ha_common.MockModule(DOMAIN, async_setup_entry=_setup)
    )
    # The core imports the config_flow platform before it sets up an entry.
    ha_common.mock_platform(hass, f"{DOMAIN}.config_flow", None)

    class _Flow(ConfigFlow):
        """Empty handler: the core looks one up for the version check."""

        VERSION = 1

    entry = ha_common.MockConfigEntry(domain=DOMAIN)
    entry.add_to_hass(hass)
    with ha_common.mock_config_flow(DOMAIN, _Flow):
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    return entry


# First core on which ``error_reason_translation_domain`` was measured
# present; 2025.9.1 and 2026.1.3 lack it and look the key up under the
# entry's own domain.
REASON_DOMAIN_SINCE = "2026.9.3"


def _has_reason_domain(entry: Any) -> bool:
    """Whether the core records the domain of the retry reason.

    A core from ``REASON_DOMAIN_SINCE`` on without the field fails, so a
    renamed field cannot drop the domain check silently.
    """
    if hasattr(entry, "error_reason_translation_domain"):
        return True
    installed = AwesomeVersion(version("homeassistant"))
    assert installed < AwesomeVersion(REASON_DOMAIN_SINCE), (
        f"error_reason_translation_domain is missing on core {installed}"
    )
    return False


async def test_missing_reason_domain_fails_from_its_anchor_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A core from the anchor on without the field fails; an older one passes."""
    module = sys.modules[__name__]
    monkeypatch.setattr(module, "version", lambda _name: REASON_DOMAIN_SINCE)
    with pytest.raises(AssertionError, match="error_reason_translation_domain"):
        _has_reason_domain(object())
    monkeypatch.setattr(module, "version", lambda _name: "2026.1.3")
    assert _has_reason_domain(object()) is False


async def test_setup_retry_carries_the_translation_key_of_the_cause(
    hass: HomeAssistant,
    ha_common: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The entry the frontend reads holds the cause's key and domain."""

    from homeassistant.config_entries import ConfigEntryState

    entry = await _setup_entry_with_cause(
        hass,
        ha_common,
        monkeypatch,
        {"translation_domain": DOMAIN, "translation_key": KEY},
    )

    assert entry.state is ConfigEntryState.SETUP_RETRY
    assert entry.error_reason_translation_key == KEY
    if _has_reason_domain(entry):
        assert entry.error_reason_translation_domain == DOMAIN
    assert entry.reason == (
        "Initial refresh failed: Cold start: empty device list; treating as transient."
    )


async def test_setup_retry_without_a_key_on_the_cause_has_no_key(
    hass: HomeAssistant,
    ha_common: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: without a key on the cause the entry carries none either."""

    from homeassistant.config_entries import ConfigEntryState

    entry = await _setup_entry_with_cause(hass, ha_common, monkeypatch, {})

    assert entry.state is ConfigEntryState.SETUP_RETRY
    assert entry.error_reason_translation_key is None
    if _has_reason_domain(entry):
        assert entry.error_reason_translation_domain is None
