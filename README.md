# Google FindMy Device (Find Hub) - Home Assistant Integration <img src="https://github.com/BSkando/GoogleFindMy-HA/blob/main/icon.png" width="30"> 

A comprehensive Home Assistant custom integration for Google's FindMy Device network, enabling real-time(ish) tracking and control of FindMy devices directly within Home Assistant!

>[!NOTE]
>**This is a true integration! No docker containers, external systems, or scripts required (other than for initial authentication)!**

<img src="https://github.com/BSkando/GoogleFindMy-HA/blob/main/icon.png" width="30"> [![GitHub Repo stars](https://img.shields.io/github/stars/BSkando/GoogleFindMy-HA?style=for-the-badge&logo=github)](https://github.com/BSkando/GoogleFindMy-HA) [![Home Assistant Community Forum](https://img.shields.io/badge/Home%20Assistant-Community%20Forum-blue?style=for-the-badge&logo=home-assistant)](https://community.home-assistant.io/t/google-findmy-find-hub-integration/931136) [![Buy me a coffee](https://img.shields.io/badge/Coffee-Addiction!-yellow?style=for-the-badge&logo=buy-me-a-coffee)](https://www.buymeacoffee.com/bskando) <img src="https://github.com/BSkando/GoogleFindMy-HA/blob/main/icon.png" width="30">

## Features 

- 🗺️ **Real-time Device Tracking**: Track Google FindMy devices with location data, sourced from the FindMy network
- ⏱️ **Configurable Polling**: Flexible polling intervals with rate limit protection
- 🔔 **Sound Button Entity**: Devices include button entity that plays a sound on supported devices
- ✅ **Attribute grading system**: Best location data is selected automatically based on recency, accuracy, and source of data
- 📍 **Historical Map-View**: Each tracker has a filterable Map-View that shows tracker movement with location data
- 📋 **Statistic Entity**: Detailed statistics for monitoring integration performance
- ❣️ **More to come!**

## Installation

### HACS (Recommended)
1. Add this repository to HACS as a custom repository
2. Install "Google Find My Device" from HACS
3. Restart Home Assistant
4. Add the integration through the UI

### Manual Installation
1. Download this repository
2. Copy the `googlefindmy` folder to `custom_components/`
3. Restart Home Assistant
4. Add the integration through the UI

## First-Time Setup

>[!IMPORTANT]
>**Authentication is a 2-part process.  One part requires use of a python script to obtain a secrets.json file, which will contain all necessary keys for authentication!  This is currently the *ONLY* way to authenticate to the FindMy network.**

### Authentication Part 1 (External Steps)
1. Navigate to [GoogleFindMyTools](https://github.com/leonboe1/GoogleFindMyTools?tab=readme-ov-file#how-to-use) repository and follow the directions on "How to use" the main.py script.
3. Complete the authentication process to generate `Auth/secrets.json`
4. Copy the entire contents of the secrets.json file.
    - Specifically, open the file in a text editor, select all, and copy.

### Authentication Part 2 (Home Assistant Steps)  
5. Add the integration to your Home Assistant install.
6. In Home Assistant, paste the copied text from secrets.json when prompted.
7. After completing authentication and adding devices, RESTART Home Assistant!

### Problems with Authentication?
>[!NOTE]
>Recently, some have had issues with the script from the repository above.  If you follow all the steps in Leon's repository and are unable to get through the main.py sequence due to errors, please try using my modification of the script [BACKUP:GoogleFindMyTools](https://github.com/BSkando/GoogleFindMyTools)

## Configuration Options

Accessible via the ⚙️ cogwheel button on the main Google Find My Device Integration page.

| **Option** | **Default** | **Units** | **Description** |
| :---: | :---: | :---: | --- |
| tracked_devices | - | - | Select which devices from your account are tracked with the integration. |
| location_poll_interval | 300 | seconds | How often the integration runs a poll cycle for all devices |
| device_poll_delay | 5 | seconds | How much time to wait between polling devices during a poll cycle |
| min_accuract_threshold | 100 | meters | Distance beyond which location data will be rejected from writing to logbook/recorder |
| movement_threshold | 50 | meters | Distance a device must travel to show an update in device location |
| google_home_filter_enabled | true | toggle | Enables/disables Google Home device location update filtering |
| google_home_filter_keywords | various | text input | Keywords, separated by commas, that are used in filtering out location data from Google Home devices |
| enable_stats_entities | true | toggle | Enables/disables "Google Find My Integration" statistics entity, which displays various useful statistics, including when polling is active |
| map_vew_token_expiration | false | toggle | Enables/disables expiration of generated API token for accessing recorder history, used in Map View location data queries |

## Services (Actions)

The integration provides a couple of Home Assistant Actions for use with automations.  Note that Device ID is different than Entity ID.  Device ID is a long, alpha-numeric value that can be obtained from the Device info pages.

| Action | Attribute | Description |
| :---: | :---: | --- |
| googlefindmy.locate_device | Device ID | Request fresh location data for a specific device. |
| googlefindmy.play_sound | Device ID | Play a sound on a specific device for location assistance.  Devices must be capable of playing a sound.  Most devices should be compatible. |
| googlefindmy.refresh_device_urls | - | Refreshes all device Map View URLs.  Useful if you are having problems with accessing Map View pages. |

## Troubleshooting

### No Location Data
- Check if devices have moved recently (Find My devices may not update GPS when stationary)
- Check battery levels (low battery may disable GPS reporting)

### FCM Connection Problems
- Extended timeout allows up to 60 seconds for device response
- Check firewall settings for Firebase Cloud Messaging
- Review FCM debug logs for connection details

### Rate Limiting
The integration respects Google's rate limits by:
- Sequential device polling (one device at a time)
- Configurable delays between requests
- Minimum poll interval enforcement
- Automatic retry with exponential backoff

## Privacy and Security

- All location data uses Google's end-to-end encryption
- Authentication tokens are securely cached
- No location data is transmitted to third parties
- Local processing of all GPS coordinates

## Contributing

Contributions are welcome and encouraged! 

To contrubuted, please:
1. Fork the repository
2. Create a feature branch
3. Test thoroughly with your Find My devices
4. Submit a pull request with detailed description

## Credits

- Böttger, L. (2024). GoogleFindMyTools [Computer software]. https://github.com/leonboe1/GoogleFindMyTools
- Firebase Cloud Messaging integration. https://github.com/home-assistant/mobile-apps-fcm-push

## Special thanks to some amazing contributors!

- @DominicWindisch
- @suka97
- @jleinenbach

## Disclaimer

This integration is not affiliated with Google. Use at your own risk and in compliance with Google's Terms of Service. The developers are not responsible for any misuse or issues arising from the use of this integration.
