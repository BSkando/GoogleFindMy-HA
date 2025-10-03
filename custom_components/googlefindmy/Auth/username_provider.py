# custom_components/googlefindmy/Auth/username_provider.py
#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#
import logging
from custom_components.googlefindmy.Auth.token_cache import get_cached_value

_LOGGER = logging.getLogger(__name__)

username_string = 'username'


def get_username():
    username = get_cached_value(username_string)
    if username is not None:
        return username

    # Fail fast instead of using a placeholder that would cause PERMISSION_DENIED later.
    _LOGGER.error(
        "No Google username configured in cache key '%s'. Please configure the account in the UI.",
        username_string,
    )
    raise RuntimeError(
        "Google username is not configured. Open the integration UI and set the account."
    )


if __name__ == '__main__':
    get_username()
