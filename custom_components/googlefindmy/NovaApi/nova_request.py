#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#

import binascii
import requests
import aiohttp
from bs4 import BeautifulSoup

from custom_components.googlefindmy.Auth.aas_token_retrieval import get_aas_token
from custom_components.googlefindmy.Auth.adm_token_retrieval import get_adm_token, async_get_adm_token
from custom_components.googlefindmy.Auth.username_provider import get_username


def nova_request(api_scope, hex_payload):
    url = "https://android.googleapis.com/nova/" + api_scope

    # Try to get ADM token from cache first, then generate if needed
    from custom_components.googlefindmy.Auth.token_cache import get_cached_value, get_all_cached_values
    from custom_components.googlefindmy.Auth.username_provider import get_username
    import logging
    
    _logger = logging.getLogger(__name__)
    username = get_username()
    
    # Debug: Print all available cached values
    all_cached = get_all_cached_values()
    _logger.debug(f"Available cached tokens: {list(all_cached.keys())}")
    _logger.debug(f"Username: {username}")
    
    # Check if we have a cached ADM token (from secrets.json)
    android_device_manager_oauth_token = get_cached_value(f'adm_token_{username}')
    _logger.debug(f"ADM token for {username}: {'Found' if android_device_manager_oauth_token else 'Not found'}")
    if android_device_manager_oauth_token:
        _logger.debug(f"ADM token preview: {android_device_manager_oauth_token[:20]}...")
    
    if not android_device_manager_oauth_token:
        # Try alternative token names that might be in secrets.json
        android_device_manager_oauth_token = get_cached_value('adm_token')
        _logger.debug(f"Generic ADM token: {'Found' if android_device_manager_oauth_token else 'Not found'}")
        
    if not android_device_manager_oauth_token:
        # Look for ANY adm_token in the cache
        for key, value in all_cached.items():
            if key.startswith('adm_token_') and '@' in key:
                android_device_manager_oauth_token = value
                extracted_username = key.replace('adm_token_', '')
                _logger.debug(f"Found ADM token for {extracted_username}, using it")
                # Update the username for future use
                username = extracted_username
                break
        
    if not android_device_manager_oauth_token:
        # Fall back to generating ADM token
        try:
            _logger.debug("Attempting to generate new ADM token...")
            android_device_manager_oauth_token = get_adm_token(username)
            _logger.debug(f"Generated ADM token: {'Success' if android_device_manager_oauth_token else 'Failed'}")
        except Exception as e:
            _logger.error(f"ADM token generation failed: {e}")
            raise RuntimeError(
                f"Failed to get Android Device Manager token: {e}. "
                "Please ensure you have run the authentication script on a machine with Chrome "
                "and configured the integration with the generated tokens."
            )
    
    if not android_device_manager_oauth_token:
        raise ValueError("No ADM token available - please reconfigure authentication")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Authorization": "Bearer " + android_device_manager_oauth_token,
        "Accept-Language": "en-US",
        "User-Agent": "fmd/20006320; gzip"
    }

    payload = binascii.unhexlify(hex_payload)

    _logger.debug(f"Making Nova API request to: {url}")
    _logger.debug(f"Request headers: {headers}")
    _logger.debug(f"Payload length: {len(payload)} bytes")
    
    response = requests.post(url, headers=headers, data=payload)
    
    _logger.debug(f"Nova API response status: {response.status_code}")
    _logger.debug(f"Response content length: {len(response.content)} bytes")
    
    if response.status_code == 200:
        result_hex = response.content.hex()
        _logger.debug(f"Nova API success - returning {len(result_hex)} characters of hex data")
        return result_hex
    elif response.status_code == 401:
        # Token expired - clear cached ADM token and retry once
        _logger.warning(f"Got 401 Unauthorized - ADM token likely expired, refreshing...")
        from custom_components.googlefindmy.Auth.token_cache import set_cached_value
        
        # Clear the expired ADM token
        set_cached_value(f'adm_token_{username}', None)
        
        # Generate new ADM token
        try:
            _logger.debug("Generating fresh ADM token...")
            android_device_manager_oauth_token = get_adm_token(username)
            
            if android_device_manager_oauth_token:
                # Retry request with new token
                headers["Authorization"] = "Bearer " + android_device_manager_oauth_token
                _logger.debug("Retrying Nova API request with fresh token...")
                retry_response = requests.post(url, headers=headers, data=payload)
                
                if retry_response.status_code == 200:
                    result_hex = retry_response.content.hex()
                    _logger.debug(f"Nova API success after token refresh - returning {len(result_hex)} characters")
                    return result_hex
                else:
                    _logger.error(f"Nova API still failed after token refresh: status={retry_response.status_code}")
                    raise RuntimeError(f"Nova API request failed after token refresh with status {retry_response.status_code}")
            else:
                _logger.error("Failed to generate new ADM token")
                raise RuntimeError("Failed to refresh ADM token after 401 error")
                
        except Exception as e:
            _logger.error(f"Token refresh failed: {e}")
            raise RuntimeError(f"Failed to refresh ADM token: {e}")
    else:
        soup = BeautifulSoup(response.text, 'html.parser')
        error_message = soup.get_text() if soup else response.text
        _logger.error(f"Nova API failed: status={response.status_code}, error='{error_message[:200]}...'")
        raise RuntimeError(f"Nova API request failed with status {response.status_code}: {error_message}")


async def async_nova_request(api_scope, hex_payload):
    """Async version of nova_request for Home Assistant compatibility."""
    url = "https://android.googleapis.com/nova/" + api_scope

    # Try to get ADM token from cache first, then generate if needed
    from custom_components.googlefindmy.Auth.token_cache import async_get_cached_value, async_get_all_cached_values
    from custom_components.googlefindmy.Auth.username_provider import username_string
    import logging
    
    _logger = logging.getLogger(__name__)
    
    # Use async methods to avoid blocking the event loop
    username = await async_get_cached_value(username_string)
    if not username:
        username = "user@example.com"  # fallback
    
    # Check if we have a cached ADM token (from secrets.json) - use async version
    android_device_manager_oauth_token = await async_get_cached_value(f'adm_token_{username}')
    _logger.debug(f"ADM token for {username}: {'Found' if android_device_manager_oauth_token else 'Not found'}")
    
    if not android_device_manager_oauth_token:
        # Try alternative token names that might be in secrets.json
        android_device_manager_oauth_token = await async_get_cached_value('adm_token')
        _logger.debug(f"Generic ADM token: {'Found' if android_device_manager_oauth_token else 'Not found'}")
        
    if not android_device_manager_oauth_token:
        # Look for ANY adm_token in the cache - use async version
        all_cached = await async_get_all_cached_values()
        for key, value in all_cached.items():
            if key.startswith('adm_token_') and '@' in key:
                android_device_manager_oauth_token = value
                extracted_username = key.replace('adm_token_', '')
                _logger.debug(f"Found ADM token for {extracted_username}, using it")
                # Update the username for future use
                username = extracted_username
                break
        
    if not android_device_manager_oauth_token:
        # Fall back to generating ADM token
        try:
            _logger.debug("Attempting to generate new ADM token...")
            android_device_manager_oauth_token = get_adm_token(username)
            _logger.debug(f"Generated ADM token: {'Success' if android_device_manager_oauth_token else 'Failed'}")
        except Exception as e:
            _logger.error(f"ADM token generation failed: {e}")
            raise RuntimeError(
                f"Failed to get Android Device Manager token: {e}. "
                "Please ensure you have run the authentication script on a machine with Chrome "
                "and configured the integration with the generated tokens."
            )
    
    if not android_device_manager_oauth_token:
        raise ValueError("No ADM token available - please reconfigure authentication")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Authorization": "Bearer " + android_device_manager_oauth_token,
        "Accept-Language": "en-US",
        "User-Agent": "fmd/20006320; gzip"
    }

    payload = binascii.unhexlify(hex_payload)

    _logger.debug(f"Making async Nova API request to: {url}")
    _logger.debug(f"Payload length: {len(payload)} bytes")
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=payload) as response:
            content = await response.read()
            
            _logger.debug(f"Nova API response status: {response.status}")
            _logger.debug(f"Response content length: {len(content)} bytes")
            
            if response.status == 200:
                result_hex = content.hex()
                _logger.debug(f"Nova API success - returning {len(result_hex)} characters of hex data")
                return result_hex
            elif response.status == 401:
                # Token expired - clear cached ADM token and retry once
                _logger.warning(f"Got 401 Unauthorized - ADM token likely expired, refreshing...")
                from custom_components.googlefindmy.Auth.token_cache import set_cached_value
                
                # Clear the expired ADM token
                set_cached_value(f'adm_token_{username}', None)
                
                # Generate new ADM token
                try:
                    _logger.debug("Generating fresh ADM token...")
                    android_device_manager_oauth_token = await async_get_adm_token(username)
                    
                    if android_device_manager_oauth_token:
                        # Retry request with new token
                        headers["Authorization"] = "Bearer " + android_device_manager_oauth_token
                        _logger.debug("Retrying Nova API request with fresh token...")
                        
                        async with session.post(url, headers=headers, data=payload) as retry_response:
                            retry_content = await retry_response.read()
                            
                            if retry_response.status == 200:
                                result_hex = retry_content.hex()
                                _logger.debug(f"Nova API success after token refresh - returning {len(result_hex)} characters")
                                return result_hex
                            else:
                                _logger.error(f"Nova API still failed after token refresh: status={retry_response.status}")
                                raise RuntimeError(f"Nova API request failed after token refresh with status {retry_response.status}")
                    else:
                        _logger.error("Failed to generate new ADM token")
                        raise RuntimeError("Failed to refresh ADM token after 401 error")
                        
                except Exception as e:
                    _logger.error(f"Token refresh failed: {e}")
                    raise RuntimeError(f"Failed to refresh ADM token: {e}")
            else:
                error_text = await response.text()
                soup = BeautifulSoup(error_text, 'html.parser')
                error_message = soup.get_text() if soup else error_text
                _logger.error(f"Nova API failed: status={response.status}, error='{error_message[:200]}...'")
                raise RuntimeError(f"Nova API request failed with status {response.status}: {error_message}")


if __name__ == '__main__':
    print(get_aas_token())