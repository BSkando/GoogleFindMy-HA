#!/usr/bin/env python3
"""Check dependency compatibility with Home Assistant constraints.

This script verifies that the integration's requirements are compatible
with Home Assistant's package constraints. It helps prevent dependency
conflicts when users install this integration alongside Home Assistant.

Features:
- Forward check: Verify requirements work with latest/specific HA version
- Backward check: Verify declared minimum HA version satisfies all requirements
- Find minimum: Discover the oldest HA version that satisfies all requirements

Usage:
    python script/check_ha_compatibility.py [--ha-version VERSION] [--verbose]
    python script/check_ha_compatibility.py --find-minimum
    python script/check_ha_compatibility.py --check-declared-minimum

Examples:
    # Check against latest HA version
    python script/check_ha_compatibility.py

    # Check against specific HA version
    python script/check_ha_compatibility.py --ha-version 2025.9.0

    # Find the minimum HA version that satisfies all requirements
    python script/check_ha_compatibility.py --find-minimum

    # Verify the declared minimum in manifest.json is correct
    python script/check_ha_compatibility.py --check-declared-minimum
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Sequence

try:
    from packaging.specifiers import SpecifierSet
    from packaging.version import InvalidVersion, Version
except ImportError:
    print("Error: 'packaging' library required. Install with: pip install packaging")
    sys.exit(1)


class Colors:
    """ANSI color codes for terminal output."""

    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def parse_requirement(req_str: str) -> tuple[str | None, str]:
    """Parse a requirement string into (name, specifier)."""
    match = re.match(r"^([a-zA-Z0-9_-]+)\s*(.*)$", req_str.strip())
    if not match:
        return None, ""
    name = match.group(1).lower().replace("-", "_")
    specifier = match.group(2).strip()
    return name, specifier


def get_pinned_version(specifier_str: str) -> Version | None:
    """Extract pinned version from specifier like ==1.0.0."""
    if not specifier_str:
        return None
    match = re.search(r"==\s*([0-9][0-9a-zA-Z._-]*)", specifier_str)
    if not match:
        return None
    try:
        return Version(match.group(1))
    except InvalidVersion:
        return None


def get_min_version(specifier_str: str) -> Version | None:
    """Extract minimum version from a specifier like >=1.0.0."""
    if not specifier_str:
        return None
    match = re.search(r">=\s*([0-9][0-9a-zA-Z._-]*)", specifier_str)
    if not match:
        return None
    try:
        return Version(match.group(1))
    except InvalidVersion:
        return None


def _require_https(url: str) -> None:
    """Refuse every URL whose scheme is not ``https`` before it is opened.

    ``urllib.request.urlopen`` also opens ``file:`` and ``ftp:`` URLs, and one
    of the URLs below interpolates the ``--ha-version`` argument.
    """
    scheme = urlparse(url).scheme
    if scheme != "https":
        raise ValueError(f"refusing to open {url!r}: only https URLs are fetched")


def fetch_ha_versions_from_pypi() -> list[str]:
    """Fetch all Home Assistant versions from PyPI.

    Returns list of stable versions (YYYY.M.P format) sorted newest to oldest.
    """
    url = "https://pypi.org/pypi/homeassistant/json"
    _require_https(url)
    req = urllib.request.Request(url, headers={"User-Agent": "ha-compat-check/1.0"})

    try:
        # The scheme was checked by _require_https above.
        with urllib.request.urlopen(req, timeout=30) as response:  # nosec B310
            data = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError) as e:
        print(f"{Colors.RED}Error fetching HA versions from PyPI: {e}{Colors.RESET}")
        return []

    versions = list(data.get("releases", {}).keys())
    stable_pattern = re.compile(r"^\d{4}\.\d+\.\d+$")
    stable_versions = [v for v in versions if stable_pattern.match(v)]
    stable_versions.sort(
        key=lambda v: tuple(int(p) for p in v.split(".")), reverse=True
    )
    return stable_versions


def _fetch_url_content(url: str) -> str | None:
    """Fetch content from URL, return None on 404 or error."""
    _require_https(url)
    headers = {"User-Agent": "ha-compat-check/1.0"}
    req = urllib.request.Request(url, headers=headers)
    try:
        # The scheme was checked by _require_https above.
        with urllib.request.urlopen(req, timeout=30) as response:  # nosec B310
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        if e.code == HTTPStatus.NOT_FOUND:
            return None
        raise
    except urllib.error.URLError:
        return None


def _parse_constraints_content(content: str) -> dict[str, str]:
    """Parse constraints file content into a dict."""
    constraints: dict[str, str] = {}
    for raw_line in content.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        name, spec = parse_requirement(stripped)
        if name:
            constraints[name] = spec
    return constraints


def fetch_ha_constraints(
    ha_version: str | None = None,
    silent: bool = False,
) -> dict[str, str] | None:
    """Fetch Home Assistant package constraints from GitHub."""
    if ha_version:
        url = f"https://raw.githubusercontent.com/home-assistant/core/{ha_version}/homeassistant/package_constraints.txt"
    else:
        url = "https://raw.githubusercontent.com/home-assistant/core/dev/homeassistant/package_constraints.txt"

    try:
        content = _fetch_url_content(url)
    except urllib.error.HTTPError as e:
        if not silent:
            print(f"{Colors.YELLOW}Warning: Could not fetch {url}: {e}{Colors.RESET}")
        return None

    if content is None:
        if not silent:
            print(f"{Colors.RED}Error: Could not fetch HA constraints{Colors.RESET}")
        return None

    return _parse_constraints_content(content)


def load_manifest(manifest_path: Path) -> dict:
    """Load manifest.json."""
    with open(manifest_path) as f:
        return json.load(f)


def load_manifest_requirements(manifest_path: Path) -> list[str]:
    """Load requirements from manifest.json."""
    manifest = load_manifest(manifest_path)
    return manifest.get("requirements", [])


def _extract_ha_version_from_pyproject(pyproject_path: Path) -> str | None:
    """Extract homeassistant version from pyproject.toml."""
    if not pyproject_path.exists():
        return None
    try:
        content = pyproject_path.read_text()
        match = re.search(r'homeassistant["\s]*>=\s*([0-9]+\.[0-9]+\.[0-9]+)', content)
        return match.group(1) if match else None
    except Exception:
        return None


def get_declared_ha_minimum(manifest_path: Path) -> str | None:
    """Get the declared minimum HA version.

    Note: The 'homeassistant' field is only valid for core integrations,
    not custom components. For custom components, we check pyproject.toml.
    """
    manifest = load_manifest(manifest_path)
    ha_version = manifest.get("homeassistant")
    if ha_version is None:
        pyproject_path = manifest_path.parent.parent.parent / "pyproject.toml"
        ha_version = _extract_ha_version_from_pyproject(pyproject_path)
    return ha_version


def check_requirements_against_constraints(
    requirements: Sequence[str],
    ha_constraints: dict[str, str],
) -> tuple[list[str], list[str]]:
    """Check if requirements are compatible with HA constraints."""
    errors: list[str] = []
    warnings: list[str] = []

    for req in requirements:
        name, our_spec = parse_requirement(req)
        if not name:
            continue
        ha_spec = ha_constraints.get(
            name, ha_constraints.get(name.replace("_", "-"), "")
        )
        if not ha_spec:
            continue
        _check_single_requirement(name, our_spec, ha_spec, errors, warnings)

    return errors, warnings


def _check_single_requirement(
    name: str,
    our_spec: str,
    ha_spec: str,
    errors: list[str],
    warnings: list[str],
) -> None:
    """Check a single requirement against HA constraint."""
    ha_pinned = get_pinned_version(ha_spec)
    our_min = get_min_version(our_spec)

    if ha_pinned and our_min and ha_pinned < our_min:
        errors.append(f"{name}: HA pins {ha_pinned}, we require >={our_min}")
        return

    if ha_pinned and our_spec and not our_min:
        try:
            if not SpecifierSet(our_spec).contains(ha_pinned):
                errors.append(
                    f"{name}: HA pins {ha_pinned}, incompatible with {our_spec}"
                )
        except Exception:
            warnings.append(f"{name}: Could not parse specifier {our_spec}")


def check_compatibility(
    requirements: Sequence[str],
    ha_constraints: dict[str, str],
    verbose: bool = False,
) -> tuple[list[str], list[str]]:
    """Check compatibility of requirements against HA constraints with output."""
    errors: list[str] = []
    warnings: list[str] = []

    print(f"\n{Colors.BOLD}Dependency Compatibility Check{Colors.RESET}")
    print("=" * 70)
    print(f"{'Package':<25} {'Our Requirement':<18} {'HA Constraint':<18} {'Status'}")
    print("-" * 70)

    for req in requirements:
        _check_and_print_requirement(req, ha_constraints, verbose, errors, warnings)

    print("-" * 70)
    return errors, warnings


def _check_and_print_requirement(
    req: str,
    ha_constraints: dict[str, str],
    verbose: bool,
    errors: list[str],
    warnings: list[str],
) -> None:
    """Check single requirement and print result."""
    name, our_spec = parse_requirement(req)
    if not name:
        return

    ha_spec = ha_constraints.get(name, ha_constraints.get(name.replace("_", "-"), ""))
    status, status_detail = _evaluate_requirement(
        name, our_spec, ha_spec, errors, warnings
    )

    if not ha_spec and verbose:
        status = f"{Colors.BLUE}N/A{Colors.RESET}"
        status_detail = "Not constrained by HA"

    if verbose or "OK" not in status:
        ha_display = ha_spec[:18] if ha_spec else "-"
        our_display = our_spec[:18] if our_spec else "-"
        print(f"{name:<25} {our_display:<18} {ha_display:<18} {status}")
        if status_detail:
            print(f"{'':>25} {status_detail}")


def _evaluate_requirement(
    name: str,
    our_spec: str,
    ha_spec: str,
    errors: list[str],
    warnings: list[str],
) -> tuple[str, str]:
    """Evaluate a requirement and return status tuple."""
    if not ha_spec:
        return f"{Colors.GREEN}OK{Colors.RESET}", ""

    ha_pinned = get_pinned_version(ha_spec)
    our_min = get_min_version(our_spec)

    if ha_pinned and our_min:
        if ha_pinned < our_min:
            errors.append(
                f"{name}: Home Assistant pins version {ha_pinned}, "
                f"but we require >={our_min}. Consider lowering minimum to >={ha_pinned}"
            )
            return (
                f"{Colors.RED}ERROR{Colors.RESET}",
                f"HA pins {ha_pinned}, we require >={our_min}",
            )
        return f"{Colors.GREEN}OK{Colors.RESET}", f"HA {ha_pinned} >= our {our_min}"

    if ha_pinned and our_spec:
        try:
            if not SpecifierSet(our_spec).contains(ha_pinned):
                errors.append(
                    f"{name}: HA pins {ha_pinned} which is incompatible with {our_spec}"
                )
                return (
                    f"{Colors.RED}CONFLICT{Colors.RESET}",
                    f"HA pins {ha_pinned}, incompatible with {our_spec}",
                )
        except Exception:
            warnings.append(f"{name}: Could not parse specifier {our_spec}")
            return f"{Colors.YELLOW}UNKNOWN{Colors.RESET}", ""

    return f"{Colors.GREEN}OK{Colors.RESET}", ""


def _test_ha_version(
    ha_version: str,
    requirements: Sequence[str],
    verbose: bool,
) -> tuple[bool, list[str]]:
    """Test if a specific HA version is compatible. Returns (compatible, errors)."""
    if verbose:
        print(f"  Testing HA {ha_version}...", end=" ", flush=True)

    constraints = fetch_ha_constraints(ha_version, silent=True)
    if constraints is None:
        if verbose:
            print(f"{Colors.YELLOW}constraints not found{Colors.RESET}")
        return False, []

    errors, _ = check_requirements_against_constraints(requirements, constraints)

    if verbose:
        if errors:
            print(f"{Colors.RED}incompatible{Colors.RESET}")
            for err in errors:
                print(f"      {err}")
        else:
            print(f"{Colors.GREEN}compatible{Colors.RESET}")

    return len(errors) == 0, errors


def find_minimum_ha_version(
    requirements: Sequence[str],
    verbose: bool = False,
    max_versions_to_check: int = 50,
) -> tuple[str | None, dict[str, str]]:
    """Find the oldest HA version that satisfies all requirements."""
    print(f"\n{Colors.BOLD}Finding Minimum Compatible HA Version{Colors.RESET}")
    print("=" * 70)

    print("Fetching HA versions from PyPI...", end=" ")
    all_versions = fetch_ha_versions_from_pypi()
    if not all_versions:
        print(f"{Colors.RED}failed{Colors.RESET}")
        return None, {}
    print(f"found {len(all_versions)} stable releases")

    versions_to_check = all_versions[:max_versions_to_check]
    print(f"Checking last {len(versions_to_check)} versions (newest to oldest)...")

    minimum_version: str | None = None
    last_compatible: str | None = None
    blocking_reqs: dict[str, str] = {}

    for ha_version in versions_to_check:
        compatible, errors = _test_ha_version(ha_version, requirements, verbose)
        if compatible:
            last_compatible = ha_version
            minimum_version = ha_version
        elif last_compatible:
            for err in errors:
                pkg = err.split(":")[0]
                if pkg not in blocking_reqs:
                    blocking_reqs[pkg] = f"requires HA >= {last_compatible}"

    return minimum_version, blocking_reqs


def check_declared_minimum(manifest_path: Path, verbose: bool = False) -> int:
    """Check if the declared minimum HA version is correct."""
    print(f"{Colors.BOLD}Checking Declared Minimum HA Version{Colors.RESET}")
    print("=" * 70)

    declared_min = get_declared_ha_minimum(manifest_path)
    requirements = load_manifest_requirements(manifest_path)

    if not declared_min:
        return _handle_missing_declaration(requirements, verbose)

    return _validate_declared_minimum(declared_min, requirements, verbose)


def _handle_missing_declaration(requirements: Sequence[str], verbose: bool) -> int:
    """Handle case where no minimum HA version is declared."""
    print(f"{Colors.YELLOW}Warning: No minimum HA version declared{Colors.RESET}")
    print("(Note: Custom integrations cannot use 'homeassistant' in manifest.json)")
    print()

    min_version, blocking = find_minimum_ha_version(requirements, verbose)
    if min_version:
        print(f"\n{Colors.CYAN}Suggested minimum: {min_version}{Colors.RESET}")
        if blocking:
            print("\nBlocking requirements:")
            for pkg, reason in blocking.items():
                print(f"  - {pkg}: {reason}")
        print(f'\nUpdate pyproject.toml dev deps: "homeassistant>={min_version}"')
    return 1


def _validate_declared_minimum(
    declared_min: str,
    requirements: Sequence[str],
    verbose: bool,
) -> int:
    """Validate that declared minimum satisfies all requirements."""
    print(f"Declared minimum: {Colors.BLUE}{declared_min}{Colors.RESET}")

    constraints = fetch_ha_constraints(declared_min)
    if constraints is None:
        print(
            f"{Colors.RED}Error: Could not fetch constraints for HA {declared_min}{Colors.RESET}"
        )
        return 1

    print(f"Loaded {len(constraints)} constraints from HA {declared_min}")
    errors, _ = check_requirements_against_constraints(requirements, constraints)

    if errors:
        return _handle_invalid_minimum(declared_min, requirements, errors)

    print(
        f"\n{Colors.GREEN}{Colors.BOLD}OK: Declared minimum {declared_min} is valid{Colors.RESET}"
    )
    print("All requirements are satisfied by this HA version.")

    if verbose:
        _check_if_minimum_can_be_lowered(declared_min, requirements)

    return 0


def _handle_invalid_minimum(
    declared_min: str,
    requirements: Sequence[str],
    errors: list[str],
) -> int:
    """Handle case where declared minimum is too old."""
    print(
        f"\n{Colors.RED}{Colors.BOLD}ERROR: Declared minimum {declared_min} is too old!{Colors.RESET}"
    )
    print(f"\nThe following requirements are NOT satisfied by HA {declared_min}:")
    for err in errors:
        print(f"  {Colors.RED}x{Colors.RESET} {err}")

    print(f"\n{Colors.CYAN}Searching for correct minimum...{Colors.RESET}")
    min_version, _ = find_minimum_ha_version(requirements, verbose=False)
    if min_version:
        print(f"\n{Colors.GREEN}Correct minimum should be: {min_version}{Colors.RESET}")
        print(f'\nUpdate pyproject.toml dev deps: "homeassistant>={min_version}"')
    return 1


def _check_if_minimum_can_be_lowered(
    declared_min: str, requirements: Sequence[str]
) -> None:
    """Check if the minimum version could be lowered."""
    print(f"\n{Colors.CYAN}Checking if minimum can be lowered...{Colors.RESET}")
    min_version, _ = find_minimum_ha_version(requirements, verbose=False)
    if not min_version or min_version == declared_min:
        return
    try:
        if Version(min_version) < Version(declared_min):
            print(f"Note: Minimum could potentially be lowered to {min_version}")
    except InvalidVersion:
        pass


def _resolve_manifest_path(manifest_arg: Path) -> Path | None:
    """Resolve manifest path, trying fallback locations."""
    if manifest_arg.exists():
        return manifest_arg
    script_dir = Path(__file__).parent.parent
    fallback = script_dir / "custom_components/googlefindmy/manifest.json"
    if fallback.exists():
        return fallback
    return None


def _run_find_minimum(args: argparse.Namespace) -> int:
    """Run the --find-minimum mode."""
    requirements = load_manifest_requirements(args.manifest)
    print(f"Manifest: {args.manifest}")
    print(f"Found {len(requirements)} integration requirements")

    min_version, blocking = find_minimum_ha_version(
        requirements, args.verbose, args.max_versions
    )
    print()

    if not min_version:
        print(f"{Colors.RED}Could not determine minimum HA version{Colors.RESET}")
        return 1

    print(
        f"{Colors.GREEN}{Colors.BOLD}Minimum compatible HA version: {min_version}{Colors.RESET}"
    )
    if blocking:
        print("\nRequirements that determine the minimum:")
        for pkg, reason in blocking.items():
            print(f"  - {pkg}: {reason}")

    return _compare_with_declared(args.manifest, min_version)


def _compare_with_declared(manifest_path: Path, min_version: str) -> int:
    """Compare found minimum with declared version."""
    declared = get_declared_ha_minimum(manifest_path)
    if not declared:
        print(
            f'\n{Colors.CYAN}Suggestion: Update pyproject.toml dev deps: "homeassistant>={min_version}"{Colors.RESET}'
        )
        return 0

    try:
        declared_v = Version(declared)
        min_v = Version(min_version)
        if declared_v < min_v:
            print(
                f"\n{Colors.RED}Warning: Declared minimum ({declared}) is too old!{Colors.RESET}"
            )
            print(f'Update pyproject.toml dev deps: "homeassistant>={min_version}"')
            return 1
        if declared_v > min_v:
            print(
                f"\n{Colors.CYAN}Note: Declared minimum ({declared}) could be lowered to {min_version}{Colors.RESET}"
            )
    except InvalidVersion:
        pass
    return 0


def _run_standard_check(args: argparse.Namespace) -> int:
    """Run standard compatibility check against specific or latest HA version."""
    ha_version_str = args.ha_version or "latest (dev branch)"
    print(
        f"Checking against Home Assistant: {Colors.BLUE}{ha_version_str}{Colors.RESET}"
    )
    print(f"Manifest: {args.manifest}")

    ha_constraints = fetch_ha_constraints(args.ha_version)
    if ha_constraints is None:
        return 1

    print(f"Loaded {len(ha_constraints)} HA package constraints")
    requirements = load_manifest_requirements(args.manifest)
    print(f"Found {len(requirements)} integration requirements")

    errors, warnings = check_compatibility(requirements, ha_constraints, args.verbose)
    return _print_check_summary(errors, warnings)


def _print_check_summary(errors: list[str], warnings: list[str]) -> int:
    """Print summary of compatibility check and return exit code."""
    print()
    if errors:
        print(
            f"{Colors.RED}{Colors.BOLD}FAILED:{Colors.RESET} {len(errors)} compatibility issue(s) found\n"
        )
        for error in errors:
            print(f"  {Colors.RED}x{Colors.RESET} {error}")
        print(
            "\nThese issues may prevent the integration from working with Home Assistant."
        )
        print("Consider updating the version bounds in manifest.json")
        return 1

    if warnings:
        print(
            f"{Colors.YELLOW}{Colors.BOLD}WARNING:{Colors.RESET} {len(warnings)} warning(s)"
        )
        for warning in warnings:
            print(f"  {Colors.YELLOW}!{Colors.RESET} {warning}")
        return 0

    print(
        f"{Colors.GREEN}{Colors.BOLD}PASSED:{Colors.RESET} All dependencies are compatible with Home Assistant"
    )
    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check dependency compatibility with Home Assistant",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          Check against latest HA (dev branch)
  %(prog)s --ha-version 2025.9.0    Check against specific version
  %(prog)s --find-minimum           Find oldest compatible HA version
  %(prog)s --check-declared-minimum Verify pyproject.toml minimum is correct
        """,
    )
    parser.add_argument(
        "--ha-version",
        help="Home Assistant version to check against (default: dev branch)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show all packages and detailed output",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("custom_components/googlefindmy/manifest.json"),
        help="Path to manifest.json",
    )
    parser.add_argument(
        "--find-minimum",
        action="store_true",
        help="Find the oldest HA version that satisfies all requirements",
    )
    parser.add_argument(
        "--check-declared-minimum",
        action="store_true",
        help="Verify the declared minimum HA version is correct",
    )
    parser.add_argument(
        "--max-versions",
        type=int,
        default=50,
        help="Maximum number of versions to check (default: 50)",
    )
    args = parser.parse_args()

    resolved_manifest = _resolve_manifest_path(args.manifest)
    if resolved_manifest is None:
        print(
            f"{Colors.RED}Error: manifest.json not found at {args.manifest}{Colors.RESET}"
        )
        return 1
    args.manifest = resolved_manifest

    print(
        f"{Colors.BOLD}Home Assistant Dependency Compatibility Checker{Colors.RESET}\n"
    )

    if args.check_declared_minimum:
        return check_declared_minimum(args.manifest, args.verbose)
    if args.find_minimum:
        return _run_find_minimum(args)
    return _run_standard_check(args)


if __name__ == "__main__":
    sys.exit(main())
