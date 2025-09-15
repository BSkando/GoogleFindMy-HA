#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#

import gpsoauth
import asyncio

from custom_components.googlefindmy.Auth.aas_token_retrieval import get_aas_token

def request_token(username, scope, play_services = False):

    aas_token = get_aas_token()
    # Use a hardcoded android_id instead of FcmReceiver to avoid ChromeDriver
    # Android ID should be a large integer (16 hex digits)
    android_id = 0x38918a453d071993
    request_app = 'com.google.android.gms' if play_services else 'com.google.android.apps.adm'

    try:
        auth_response = gpsoauth.perform_oauth(
            username, aas_token, android_id,
            service='oauth2:https://www.googleapis.com/auth/' + scope,
            app=request_app,
            client_sig='38918a453d07199354f8b19af05ec6562ced5788')
        
        if not auth_response:
            raise ValueError("No response from gpsoauth.perform_oauth")
            
        if 'Auth' not in auth_response:
            raise KeyError(f"'Auth' not found in response: {auth_response}")
            
        token = auth_response['Auth']
        return token
    except Exception as e:
        raise RuntimeError(f"Failed to get auth token for scope '{scope}': {e}")

async def async_request_token(username, scope, play_services = False):
    """Async wrapper for request_token to avoid blocking I/O in event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, request_token, username, scope, play_services)