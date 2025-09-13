# Google Find My Device - Home Assistant Integration
<img width="256" height="256" alt="icon@2x" src="https://github.com/user-attachments/assets/e0660442-c327-440a-99be-02785dc2daa0" />

A comprehensive Home Assistant custom integration for Google's Find My Device network, enabling real-time tracking and control of Find My devices directly within Home Assistant.

**This is a true integration! No scripts, docker containers, or external systems required (other than for initial authentication)!**

## Features

- **Real-time Device Tracking**: Track Google Find My devices with fresh GPS location data
- **Configurable Polling**: Flexible polling intervals with rate limit compliance
- **Sound Button Entity**: Devices include a button entity that plays a sound on devices that support playing sound
- **Attribute grading system**: Location data is selected automatically based on 3 major attributes: 1) Accuracy 2) Recency 3) Comes from your device or the network.

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

## Configuration

### Authentication Setup
1. Run [GoogleFindMyTools](https://github.com/leonboe1/GoogleFindMyTools) on a machine with Chrome
  **MUST PERFORM CRITICAL STEPS BELOW!!!**
2. Complete the authentication process to generate `Auth/secrets.json`
3. Copy the entire contents of the secrets.json file.  Specifically, open the file in a text editor, select all, and copy.
4. In Home Assistant, paste the copied text from secrets.json when prompted.
5. After completing authentication and adding devices, RESTART Home Assistant!

#### **CRITICAL AUTHENTICATION STEPS:** 
**When running main.py, there are 2 steps to the authentication process.  BOTH must be followed!**
1. Run main.py per the instructions in the repo above.  You will get your first authentication step and open a Chrome window.
![mainpy1](https://github.com/user-attachments/assets/dad8b94b-c9c7-4499-a516-f3c8e3498388)
2. After you authenticate the first time, you should see a list of your devices, type in a number of one of your devices and type 'Enter'.  Once you see the location info and error message, you can close the terminal and continue to step 2. above.
![mainpy2](https://github.com/user-attachments/assets/e36e602c-081f-495e-a2b5-8627fa04420c)

## Configuration Options

- **Location Poll Interval**: How often to request fresh location data (default: 5 minutes, minimum: 2 minutes)
- **Device Poll Delay**: Delay between individual device polls (default: 5 seconds)
- **Accuracy Threshold**: Minimum GPS accuracy to accept (default: 100 meters)

### Services

The integration provides several services:

#### `googlefindmy.locate_device`
Request fresh location data for a specific device.

#### `googlefindmy.play_sound`
Play a sound on a specific device for location assistance.

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

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Test thoroughly with your Find My devices
4. Submit a pull request with detailed description

## Credits

- Böttger, L. (2024). GoogleFindMyTools [Computer software]. https://github.com/leonboe1/GoogleFindMyTools
- Firebase Cloud Messaging integration. https://github.com/home-assistant/mobile-apps-fcm-push

## Disclaimer

This integration is not affiliated with Google. Use at your own risk and in compliance with Google's Terms of Service. The developers are not responsible for any misuse or issues arising from the use of this integration.
