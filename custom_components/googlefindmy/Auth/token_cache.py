#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#

import json
import os
import asyncio
try:
    import aiofiles
except ImportError:
    aiofiles = None

SECRETS_FILE = 'secrets.json'

# PATCH: Helper to normalize FCM credentials when stored as a JSON string.
def _normalize_fcm_credentials(value):
    """Normalize 'fcm_credentials' to a dict if it's a JSON string."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value

def get_cached_value_or_set(name: str, generator: callable):

    existing_value = get_cached_value(name)

    if existing_value is not None:
        return existing_value

    value = generator()
    set_cached_value(name, value)
    return value

async def async_get_cached_value_or_set(name: str, generator):

    existing_value = await async_get_cached_value(name)

    if existing_value is not None:
        return existing_value

    if asyncio.iscoroutinefunction(generator):
        value = await generator()
    else:
        value = generator()
    
    await async_set_cached_value(name, value)
    return value


def get_cached_value(name: str):
    # Check in-memory cache first (for Home Assistant)
    value = get_from_memory_cache(name)
    if value is not None:
        # PATCH: Normalize 'fcm_credentials' read from memory cache.
        if name == 'fcm_credentials':
            value = _normalize_fcm_credentials(value)
            # Keep memory cache consistent with normalized type.
            _memory_cache[name] = value
        return value
    
    # Fall back to synchronous file access (for non-async contexts)
    # This should only be used during initialization or in non-async contexts
    secrets_file = _get_secrets_file()

    if os.path.exists(secrets_file):
        with open(secrets_file, 'r') as file:
            try:
                data = json.load(file)
                value = data.get(name)
                if value:
                    # PATCH: Normalize 'fcm_credentials' read from file.
                    if name == 'fcm_credentials':
                        value = _normalize_fcm_credentials(value)
                        _memory_cache[name] = value
                    return value
            except json.JSONDecodeError:
                return None
    return None

async def async_get_cached_value(name: str):
    # Check in-memory cache first (for Home Assistant)
    value = get_from_memory_cache(name)
    if value is not None:
        # PATCH: Normalize 'fcm_credentials' read from memory cache (async path).
        if name == 'fcm_credentials':
            value = _normalize_fcm_credentials(value)
            _memory_cache[name] = value
        return value
    
    # Use async file operations if available
    secrets_file = _get_secrets_file()

    # Use asyncio to run the sync file check in executor
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, secrets_file)
    
    if exists:
        if aiofiles:
            async with aiofiles.open(secrets_file, 'r') as file:
                try:
                    content = await file.read()
                    data = json.loads(content)
                    value = data.get(name)
                    if value:
                        # PATCH: Normalize 'fcm_credentials' read from file (async path).
                        if name == 'fcm_credentials':
                            value = _normalize_fcm_credentials(value)
                        # Update in-memory cache
                        _memory_cache[name] = value
                        return value
                except json.JSONDecodeError:
                    return None
        else:
            # Fallback to sync in executor
            def read_file():
                with open(secrets_file, 'r') as file:
                    try:
                        data = json.load(file)
                        value = data.get(name)
                        if value:
                            return value
                    except json.JSONDecodeError:
                        return None
            value = await loop.run_in_executor(None, read_file)
            if value and name == 'fcm_credentials':
                # PATCH: Normalize after executor read.
                value = _normalize_fcm_credentials(value)
            if value:
                _memory_cache[name] = value
                return value
    return None


def set_cached_value(name: str, value: str):
    # Update in-memory cache immediately
    if value is None and name in _memory_cache:
        del _memory_cache[name]
    else:
        _memory_cache[name] = value
    
    # Synchronous file write (for non-async contexts)
    secrets_file = _get_secrets_file()

    if os.path.exists(secrets_file):
        with open(secrets_file, 'r') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                raise Exception("Could not read secrets file. Aborting.")
    else:
        data = {}
    
    # Handle None value to clear the cache entry
    if value is None and name in data:
        del data[name]
    else:
        data[name] = value
        
    with open(secrets_file, 'w') as file:
        json.dump(data, file)

async def async_set_cached_value(name: str, value: str):
    # Update in-memory cache immediately
    if value is None and name in _memory_cache:
        del _memory_cache[name]
    else:
        _memory_cache[name] = value
    
    # Async file operations
    secrets_file = _get_secrets_file()
    
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, secrets_file)

    if exists:
        if aiofiles:
            async with aiofiles.open(secrets_file, 'r') as file:
                try:
                    content = await file.read()
                    data = json.loads(content)
                except json.JSONDecodeError:
                    raise Exception("Could not read secrets file. Aborting.")
        else:
            # Fallback to sync in executor
            def read_file():
                with open(secrets_file, 'r') as file:
                    return json.load(file)
            try:
                data = await loop.run_in_executor(None, read_file)
            except json.JSONDecodeError:
                raise Exception("Could not read secrets file. Aborting.")
    else:
        data = {}
    
    # Handle None value to clear the cache entry
    if value is None and name in data:
        del data[name]
    else:
        data[name] = value
    
    if aiofiles:
        async with aiofiles.open(secrets_file, 'w') as file:
            await file.write(json.dumps(data))
    else:
        # Fallback to sync in executor
        def write_file():
            with open(secrets_file, 'w') as file:
                json.dump(data, file)
        await loop.run_in_executor(None, write_file)


def _get_secrets_file():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, SECRETS_FILE)


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


def get_all_cached_values():
    """Get all cached values for debugging."""
    # Return from memory cache if available
    if _memory_cache:
        # PATCH: Normalize 'fcm_credentials' in memory snapshot.
        data = _memory_cache.copy()
        if 'fcm_credentials' in data:
            data['fcm_credentials'] = _normalize_fcm_credentials(data['fcm_credentials'])
            _memory_cache['fcm_credentials'] = data['fcm_credentials']
        return data
    
    secrets_file = _get_secrets_file()
    
    if os.path.exists(secrets_file):
        with open(secrets_file, 'r') as file:
            try:
                data = json.load(file)
                # PATCH: Normalize 'fcm_credentials' read from file.
                if 'fcm_credentials' in data:
                    data['fcm_credentials'] = _normalize_fcm_credentials(data['fcm_credentials'])
                    _memory_cache['fcm_credentials'] = data['fcm_credentials']
                return data
            except json.JSONDecodeError:
                return {}
    return {}

async def async_get_all_cached_values():
    """Get all cached values for debugging (async version)."""
    # Return from memory cache if available
    if _memory_cache:
        # PATCH: Normalize 'fcm_credentials' in memory snapshot (async path).
        data = _memory_cache.copy()
        if 'fcm_credentials' in data:
            data['fcm_credentials'] = _normalize_fcm_credentials(data['fcm_credentials'])
            _memory_cache['fcm_credentials'] = data['fcm_credentials']
        return data
    
    secrets_file = _get_secrets_file()
    
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, secrets_file)
    
    if exists:
        if aiofiles:
            async with aiofiles.open(secrets_file, 'r') as file:
                try:
                    content = await file.read()
                    data = json.loads(content)
                    # PATCH: Normalize 'fcm_credentials' read from file (async path).
                    if 'fcm_credentials' in data:
                        data['fcm_credentials'] = _normalize_fcm_credentials(data['fcm_credentials'])
                    # Update memory cache
                    set_memory_cache(data)
                    return data
                except json.JSONDecodeError:
                    return {}
        else:
            # Fallback to sync in executor
            def read_file():
                with open(secrets_file, 'r') as file:
                    try:
                        data = json.load(file)
                        return data
                    except json.JSONDecodeError:
                        return {}
            data = await loop.run_in_executor(None, read_file)
            if data and 'fcm_credentials' in data:
                # PATCH: Normalize after executor read.
                data['fcm_credentials'] = _normalize_fcm_credentials(data['fcm_credentials'])
            set_memory_cache(data or {})
            return data
    return {}

# Global variable to store in-memory secrets for Home Assistant
_memory_cache = {}

def set_memory_cache(data: dict):
    """Set the in-memory cache for Home Assistant (avoids file I/O in event loop)."""
    global _memory_cache
    _memory_cache = data.copy()

def get_from_memory_cache(key: str):
    """Get a value from the in-memory cache."""
    return _memory_cache.get(key)

async def async_load_cache_from_file():
    """Load the entire cache from file into memory (async)."""
    secrets_file = _get_secrets_file()
    
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, secrets_file)
    
    if exists:
        if aiofiles:
            async with aiofiles.open(secrets_file, 'r') as file:
                try:
                    content = await file.read()
                    data = json.loads(content)
                    # PATCH: Normalize 'fcm_credentials' during bulk load.
                    if 'fcm_credentials' in data:
                        data['fcm_credentials'] = _normalize_fcm_credentials(data['fcm_credentials'])
                    set_memory_cache(data)
                    return data
                except json.JSONDecodeError:
                    return {}
        else:
            # Fallback to sync in executor
            def read_file():
                with open(secrets_file, 'r') as file:
                    try:
                        data = json.load(file)
                        return data
                    except json.JSONDecodeError:
                        return {}
            data = await loop.run_in_executor(None, read_file)
            if data and 'fcm_credentials' in data:
                # PATCH: Normalize after executor read.
                data['fcm_credentials'] = _normalize_fcm_credentials(data['fcm_credentials'])
            set_memory_cache(data or {})
            return data
    return {}
