#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#
from binascii import unhexlify

from custom_components.googlefindmy.Auth.token_cache import get_cached_value_or_set
from custom_components.googlefindmy.KeyBackup.shared_key_flow import request_shared_key_flow


def _retrieve_shared_key():
    print("""[SharedKeyRetrieval] You need to log in again to access end-to-end encrypted keys to decrypt location reports.
> This script will now open Google Chrome on your device. 
> Make that you allow Python (or PyCharm) to control Chrome (macOS only).
    """)

    # Check if we're running in a non-interactive environment (like Home Assistant)
    import sys
    try:
        if not sys.stdin.isatty():
            raise EOFError("Non-interactive environment detected")
        # Press enter to continue
        input("[SharedKeyRetrieval] Press 'Enter' to continue...")
    except EOFError:
        # Running in non-interactive mode, look for alternative keys
        from custom_components.googlefindmy.Auth.token_cache import get_all_cached_values
        
        all_cached = get_all_cached_values()
        print(f"[SharedKeyRetrieval] Available cached keys: {list(all_cached.keys())}")
        
        # Try to find any key that might work as a shared key
        # Look for keys in FCM credentials that might serve as shared keys
        fcm_creds = all_cached.get('fcm_credentials', {})
        if isinstance(fcm_creds, str):
            import json
            try:
                fcm_creds = json.loads(fcm_creds)
            except Exception:
                fcm_creds = {}
        if 'keys' in fcm_creds and 'private' in fcm_creds['keys']:
            print("[SharedKeyRetrieval] Using FCM private key as shared key fallback")
            # Use first 32 bytes of the private key as shared key
            import base64
            private_key_b64 = fcm_creds['keys']['private']
            
            # Add proper Base64 padding if missing
            missing_padding = len(private_key_b64) % 4
            if missing_padding:
                private_key_b64 += '=' * (4 - missing_padding)
            
            try:
                private_key_der = base64.b64decode(private_key_b64)
            except Exception as decode_error:
                print(f"[SharedKeyRetrieval] Failed to decode FCM private key: {decode_error}")
                raise RuntimeError(f"Failed to decode FCM private key for shared key fallback: {decode_error}")
            # Extract a 32-byte key from the private key
            return private_key_der[-32:].hex()  # Use last 32 bytes
        
        raise RuntimeError("No suitable shared key found in cache")

    shared_key = request_shared_key_flow()
    return shared_key


def get_shared_key() -> bytes:
    # First try to get the cached shared key directly
    from custom_components.googlefindmy.Auth.token_cache import get_cached_value, get_all_cached_values
    
    shared_key_hex = get_cached_value('shared_key')
    if shared_key_hex:
        print(f"[SharedKeyRetrieval] Found cached shared key: {len(shared_key_hex)} chars")
        return unhexlify(shared_key_hex)
    
    print("[SharedKeyRetrieval] No shared_key found in cache, debugging...")
    
    # Debug: check what keys are actually available
    all_cached = get_all_cached_values()
    available_keys = list(all_cached.keys())
    print(f"[SharedKeyRetrieval] Available keys in cache: {available_keys}")
    
    if 'shared_key' in all_cached:
        shared_key_hex = all_cached['shared_key']
        print(f"[SharedKeyRetrieval] Found shared_key via get_all_cached_values: {len(shared_key_hex)} chars")
        return unhexlify(shared_key_hex)
    
    print("[SharedKeyRetrieval] shared_key definitely not found, trying to generate...")
    return unhexlify(get_cached_value_or_set('shared_key', _retrieve_shared_key))


if __name__ == '__main__':
    print(get_shared_key())
