.PHONY: bootstrap-doctoc clean clean-node-modules doctoc lint preflight test-single test-cov test-ha test-unload translation-check check-ha-compat install install-dev

PYTHON ?= python3
NPM ?= npm
POETRY ?= poetry
PYTEST_ARGS ?=
PYTEST_COV_FLAGS ?= --cov-report=term-missing
# Space-separated interpreters for `make preflight`, one per Home Assistant
# track you want the suite to run on. Empty by default: the paths are
# machine-local, so they belong on the command line, not in this file.
PREFLIGHT_PYTHONS ?=

# Remove DOCTOC_SENTINEL via `make clean` to force a DocToc reinstall when the cached dev dependency changes.
DOCTOC_SENTINEL := .bootstrap/doctoc-preinstall.stamp
NPM_CACHE ?= .npm-cache

clean:
	@$(PYTHON) script/clean_pycache.py
	@if [ -f "$(DOCTOC_SENTINEL)" ]; then \
		echo "[make clean] Removing DocToc bootstrap sentinel"; \
		rm -f "$(DOCTOC_SENTINEL)"; \
	fi

clean-node-modules:
	@$(PYTHON) script/clean_node_modules.py

lint:
	@$(POETRY) run ruff check . --fix

bootstrap-doctoc:
	@mkdir -p .bootstrap
	@echo "[make bootstrap-doctoc] Installing DocToc dev dependency (cached via $(NPM_CACHE))"
	@$(NPM) ci --prefer-offline --no-fund --no-audit --cache $(NPM_CACHE) --include=dev
	@touch $(DOCTOC_SENTINEL)

doctoc:
	@echo "[make doctoc] Regenerating AGENTS.md table of contents"
	@$(POETRY) run python script/precommit_hooks/update_doctoc.py AGENTS.md

install:
	@echo "[make install] Installing Poetry dependencies"
	@$(POETRY) install

install-dev:
	@echo "[make install-dev] Installing Poetry dependencies with dev and test groups"
	@$(POETRY) install --with dev,test

translation-check:
	@echo "[make translation-check] Checking for missing translation keys"
	@$(POETRY) run python -m script.translation_key_check

preflight:
	@echo "[make preflight] Running the local preflight S1-S8 (see script/AGENTS.md)"
	@$(PYTHON) script/local_verify.py --all \
		$(foreach interpreter,$(PREFLIGHT_PYTHONS),--python $(interpreter))

test-single:
	@echo "[make test-single] Running pytest -q $(PYTEST_ARGS) $(TEST)"
	@$(POETRY) run pytest -q $(PYTEST_ARGS) $(TEST)

test-cov:
	@echo "[make test-cov] Running pytest -q --cov with coverage"
	@bash -o pipefail -c "$(POETRY) run pytest -q --cov $(PYTEST_COV_FLAGS) $(PYTEST_ARGS) 2>&1 | tee pytest_output.log"

test-ha:
	@echo "[make test-ha] Running targeted Home Assistant regression smoke tests"
	@$(POETRY) run pytest -q $(PYTEST_ARGS) \
			tests/test_entity_recovery_manager.py \
			tests/test_homeassistant_callback_stub_helper.py
	@echo "[make test-ha] Executing full-suite coverage run (see pytest_output.log for details)"
	@bash -o pipefail -c "$(POETRY) run pytest -q --cov $(PYTEST_COV_FLAGS) $${PYTEST_ARGS:+$${PYTEST_ARGS} } 2>&1 | tee pytest_output.log"

test-unload:
	@echo "[make test-unload] Running parent unload rollback regression suite"
	@$(POETRY) run pytest -q $(PYTEST_ARGS) tests/test_unload_subentry_cleanup.py

check-ha-compat:
	@echo "[make check-ha-compat] Checking dependency compatibility with Home Assistant"
	@$(POETRY) run python script/check_ha_compatibility.py --verbose
