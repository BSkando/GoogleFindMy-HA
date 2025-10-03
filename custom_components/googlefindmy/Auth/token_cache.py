# custom_components/googlefindmy/Auth/token_cache.py
#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#

import json
import os
import asyncio
import threading
from typing import Dict, Optional

try:
    import aiofiles
except ImportError:
    aiofiles = None

SECRETS_FILE = 'secrets.json'

# --- Concurrency primitives ---
# 1) Per-process write lock for file + memory updates (works from sync paths).
_write_lock = threading.RLock()
# 2) Per-key async locks to deduplicate generator work (avoids thundering herd).
_async_key_locks: Dict[str, asyncio.Lock] = {}

def _get_async_key_lock(name: str) -> asyncio.Lock:
    # Not performance-critical; small, lock-free map check is fine
    lock = _async_key_locks.get(name)
    if lock is None:
        lock = asyncio.Lock()
        _async_key_locks[name] = lock
    return lock


# --- Internal sync helpers (single source of truth = in-memory cache) ---

_memory_cache: Dict[str, Optional[str]] = {}

def _get_secrets_file() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, SECRETS_FILE)

def _read_secrets_file_sync() -> Dict[str, Optional[str]]:
    """Read file contents atomically under the write lock and return a dict."""
    with _write_lock:
        secrets_file = _get_secrets_file()
        if not os.path.exists(secrets_file):
            return {}
        try:
            with open(secrets_file, 'r') as f:
                data = json.load(f)
                # normalize to dict
                return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError):
            return {}

def _write_secrets_file_sync_from_memory():
    """Persist the current in-memory cache under the write lock."""
    with _write_lock:
        secrets_file = _get_secrets_file()
        tmp = dict(_memory_cache)  # snapshot
        with open(secrets_file, 'w') as f:
            json.dump(tmp, f)

def _set_cached_value_sync(name: str, value: Optional[str]):
    """Update memory and write the full cache to disk atomically."""
    with _write_lock:
        if value is None:
            if name in _memory_cache:
                del _memory_cache[name]
        else:
            _memory_cache[name] = value
        # Write the entire memory cache to file so we never lose concurrent keys
        secrets_file = _get_secrets_file()
        with open(secrets_file, 'w') as f:
            json.dump(_memory_cache, f)


# --- Public API (sync) ---

def get_cached_value_or_set(name: str, generator: callable):
    existing_value = get_cached_value(name)
    if existing_value is not None:
        return existing_value

    # Generate synchronously and persist atomically
    value = generator()
    set_cached_value(name, value)
    return value

def get_cached_value(name: str):
    # Fast path: in-memory cache
    val = get_from_memory_cache(name)
    if val is not None:
        return val

    # Slow path: read from file and backfill memory
    data = _read_secrets_file_sync()
    if data:
        set_memory_cache(data)
    return data.get(name)

def set_cached_value(name: str, value: Optional[str]):
    _set_cached_value_sync(name, value)


# --- Public API (async) ---

async def async_get_cached_value_or_set(name: str, generator):
    # Fast path without lock
    existing_value = await async_get_cached_value(name)
    if existing_value is not None:
        return existing_value

    # Slow path with per-key lock to avoid thundering herd
    lock = _get_async_key_lock(name)
    async with lock:
        # Re-check after we acquired the lock
        existing_value = await async_get_cached_value(name)
        if existing_value is not None:
            return existing_value

        if asyncio.iscoroutinefunction(generator):
            value = await generator()
        else:
            value = generator()

        # Delegate to the sync writer under the process-wide write lock
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _set_cached_value_sync, name, value)
        return value

async def async_get_cached_value(name: str):
    # Fast path: in-memory cache
    val = get_from_memory_cache(name)
    if val is not None:
        return val

    # Read file (no write), then backfill memory
    secrets_file = _get_secrets_file()
    loop = asyncio.get_running_loop()
    exists = await loop.run_in_executor(None, os.path.exists, secrets_file)
    if not exists:
        return None

    # Prefer consistent read via executor to share the same lock/path
    data = await loop.run_in_executor(None, _read_secrets_file_sync)
    if data:
        set_memory_cache(data)
        return data.get(name)
    return None

async def async_set_cached_value(name: str, value: Optional[str]):
    # Perform the entire write in the executor using the same sync write lock
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _set_cached_value_sync, name, value)


# --- Bulk helpers / diagnostics ---

def save_oauth_token(token: str):
    """Save OAuth token to cache."""
    set_cached_value("oauth_token", token)

async def async_save_oauth_token(token: str):
    """Save OAuth token to cache (async version)."""
    await async_set_cached_value("oauth_token", token)

def load_oauth_token():
    """Load OAuth token from cache."""
    return get_cached_value("oauth_token")

async def async_load_oauth_token():
    """Load OAuth token from cache (async version)."""
    return await async_get_cached_value("oauth_token")

def get_all_cached_values() -> Dict[str, Optional[str]]:
    """
    Return a full snapshot from disk (source of truth) and refresh memory.
    This avoids returning a partial in-memory view.
    """
    data = _read_secrets_file_sync()
    if data:
        set_memory_cache(data)
    return data

async def async_get_all_cached_values() -> Dict[str, Optional[str]]:
    """Async variant of get_all_cached_values()."""
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, _read_secrets_file_sync)
    if data:
        set_memory_cache(data)
    return data


# --- In-memory cache management ---

def set_memory_cache(data: dict):
    """Set the in-memory cache for Home Assistant (avoids file I/O in event loop)."""
    global _memory_cache
    with _write_lock:
        _memory_cache = data.copy()

def get_from_memory_cache(key: str):
    """Get a value from the in-memory cache."""
    return _memory_cache.get(key)

async def async_load_cache_from_file():
    """Load the entire cache from file into memory (async)."""
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, _read_secrets_file_sync)
    set_memory_cache(data)
    return data
