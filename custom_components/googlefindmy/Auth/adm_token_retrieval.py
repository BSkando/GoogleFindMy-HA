#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#

from custom_components.googlefindmy.Auth.token_retrieval import request_token, async_request_token
from custom_components.googlefindmy.Auth.username_provider import get_username
from custom_components.googlefindmy.Auth.token_cache import get_cached_value_or_set, async_get_cached_value, async_set_cached_value

def _generate_adm_token(username):
    """Generate a new ADM token."""
    return request_token(username, "android_device_manager")

async def _async_generate_adm_token(username):
    """Generate a new ADM token asynchronously."""
    return await async_request_token(username, "android_device_manager")

def get_adm_token(username):
    """Get ADM token from cache or generate a new one."""
    return get_cached_value_or_set(f'adm_token_{username}', lambda: _generate_adm_token(username))

async def async_get_adm_token(username):
    """Get ADM token from cache or generate a new one asynchronously."""
    cache_key = f'adm_token_{username}'

    # Try to get from cache first
    cached_token = await async_get_cached_value(cache_key)
    if cached_token:
        return cached_token

    # Generate new token if not in cache
    new_token = await _async_generate_adm_token(username)
    if new_token:
        await async_set_cached_value(cache_key, new_token)

    return new_token


if __name__ == '__main__':
    print(get_adm_token(get_username()))