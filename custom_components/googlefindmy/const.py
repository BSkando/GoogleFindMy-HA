"""Constants for Google Find My Device integration."""

DOMAIN = "googlefindmy"

# Configuration keys
CONF_OAUTH_TOKEN = "oauth_token"

# Update interval
UPDATE_INTERVAL = 60  # seconds

# Services
SERVICE_LOCATE_DEVICE = "locate_device"
SERVICE_PLAY_SOUND = "play_sound"
SERVICE_LOCATE_EXTERNAL = "locate_device_external"

# Google Home Filter defaults
DEFAULT_GOOGLE_HOME_FILTER_ENABLED = True
DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS = "nest,google,home,mini,hub,display,chromecast,speaker"
GOOGLE_HOME_SPAM_THRESHOLD_MINUTES = 15