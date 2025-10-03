import asyncio
import base64
import binascii

from custom_components.googlefindmy.Auth.token_cache import set_cached_value, get_cached_value

class FcmReceiver:

    _instance = None
    _listening = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(FcmReceiver, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True

        # Initialize attributes first to prevent attribute errors
        self.credentials = None
        self.location_update_callbacks = []
        self.pc = None
        self._listening = False

        # Define Firebase project configuration
        project_id = "google.com:api-project-289722593072"
        app_id = "1:289722593072:android:3cfcf5bc359f0308"
        api_key = "AIzaSyD_gko3P392v6how2H7UpdeXQ0v2HLettc"
        message_sender_id = "289722593072"

        try:
            # Lazy import to avoid protobuf conflicts
            try:
                from custom_components.googlefindmy.Auth.firebase_messaging import FcmRegisterConfig, FcmPushClient
            except ImportError as e:
                print(f"[FCMReceiver] Import error: {e}")
                from .firebase_messaging import FcmRegisterConfig, FcmPushClient

            fcm_config = FcmRegisterConfig(
                project_id=project_id,
                app_id=app_id,
                api_key=api_key,
                messaging_sender_id=message_sender_id,
                bundle_id="com.google.android.apps.adm",
            )

            self.credentials = get_cached_value('fcm_credentials')
            # PATCH: Normalize credentials if stored as JSON string to ensure consistent downstream type.
            if isinstance(self.credentials, str):
                try:
                    import json
                    self.credentials = json.loads(self.credentials)
                except Exception:
                    # Leave as-is; read-path hardening handles non-dict values elsewhere.
                    pass
            self.pc = FcmPushClient(self._on_notification, fcm_config, self.credentials, self._on_credentials_updated)
        except Exception as e:
            print(f"[FCMReceiver] Initialization error: {e}")
            # Ensure attributes exist even if initialization fails
            if not hasattr(self, 'credentials'):
                self.credentials = None
            if not hasattr(self, 'pc'):
                self.pc = None


    def register_for_location_updates(self, callback):
        try:
            if not hasattr(self, '_listening'):
                self._listening = False
                
            if not self._listening:
                try:
                    # Try to use existing event loop
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # If loop is already running, we can't use run_until_complete
                        raise RuntimeError("Event loop is already running")
                    loop.run_until_complete(self._register_for_fcm_and_listen())
                except RuntimeError:
                    # No event loop or loop is running, create a new one
                    asyncio.run(self._register_for_fcm_and_listen())

            self.location_update_callbacks.append(callback)

            if self.credentials and 'fcm' in self.credentials and 'registration' in self.credentials['fcm']:
                return self.credentials['fcm']['registration']['token']
            else:
                raise RuntimeError("FCM credentials not available after registration")
        except AttributeError as e:
            raise RuntimeError(f"FcmReceiver not properly initialized: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to register for location updates: {e}")


    def get_fcm_token(self):
        """Get FCM token without registering for callbacks."""
        try:
            if self.credentials and 'fcm' in self.credentials and 'registration' in self.credentials['fcm']:
                return self.credentials['fcm']['registration']['token']
            else:
                # Try to initialize credentials if not already done
                if self.credentials is None:
                    try:
                        # Try to use existing event loop
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            # If loop is already running, we can't use run_until_complete
                            raise RuntimeError("Event loop is already running")
                        loop.run_until_complete(self._register_for_fcm())
                    except RuntimeError:
                        # No event loop or loop is running, create a new one
                        asyncio.run(self._register_for_fcm())
                
                if self.credentials and 'fcm' in self.credentials and 'registration' in self.credentials['fcm']:
                    return self.credentials['fcm']['registration']['token']
                else:
                    raise RuntimeError("FCM credentials not available after registration attempt")
        except Exception as e:
            raise RuntimeError(f"Failed to get FCM token: {e}")

    def stop_listening(self):
        if self.pc:
            try:
                # Try to use existing event loop
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # If loop is already running, we can't use run_until_complete
                    raise RuntimeError("Event loop is already running")
                loop.run_until_complete(self.pc.stop())
            except RuntimeError:
                # No event loop or loop is running, create a new one
                asyncio.run(self.pc.stop())
        self._listening = False


    def get_android_id(self):

        if self.credentials is None:
            try:
                # Try to use existing event loop
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # If loop is already running, we can't use run_until_complete
                    raise RuntimeError("Event loop is already running")
                loop.run_until_complete(self._register_for_fcm_and_listen())
            except RuntimeError:
                # No event loop or loop is running, create a new one
                asyncio.run(self._register_for_fcm_and_listen())

        if self.credentials and 'gcm' in self.credentials and 'android_id' in self.credentials['gcm']:
            return self.credentials['gcm']['android_id']
        else:
            raise RuntimeError("FCM credentials not available or missing android_id")


    # Define a callback function for handling notifications
    def _on_notification(self, obj, notification, data_message):

        # Check if the payload is present
        if 'data' in obj and 'com.google.android.apps.adm.FCM_PAYLOAD' in obj['data']:

            # Decode the base64 string
            base64_string = obj['data']['com.google.android.apps.adm.FCM_PAYLOAD']
            decoded_bytes = base64.b64decode(base64_string)

            # print("[FCMReceiver] Decoded FMDN Message:", decoded_bytes.hex())

            # Convert to hex string
            hex_string = binascii.hexlify(decoded_bytes).decode('utf-8')

            for callback in self.location_update_callbacks:
                callback(hex_string)
        else:
            print("[FCMReceiver] Payload not found in the notification.")


    def _on_credentials_updated(self, creds):
        # PATCH: Normalize credentials if provided as JSON string; ensures consistent dict type for downstream consumers.
        creds_to_store = creds
        if isinstance(creds_to_store, str):
            try:
                import json
                creds_to_store = json.loads(creds_to_store)
            except Exception:
                # If parsing fails, store raw value; read-path hardening must cope with it.
                pass

        self.credentials = creds_to_store

        # Also store to disk
        set_cached_value('fcm_credentials', self.credentials)
        print("[FCMReceiver] Credentials updated.")


    async def _register_for_fcm(self):
        fcm_token = None

        # Register or check in with FCM and get the FCM token
        while fcm_token is None:
            try:
                fcm_token = await self.pc.checkin_or_register()
            except Exception as e:
                await self.pc.stop()
                print("[FCMReceiver] Failed to register with FCM. Retrying...")
                await asyncio.sleep(5)


    async def _register_for_fcm_and_listen(self):
        await self._register_for_fcm()
        await self.pc.start()
        self._listening = True
        print("[FCMReceiver] Listening for notifications. This can take a few seconds...")


if __name__ == "__main__":
    receiver = FcmReceiver()
    print(receiver.get_android_id())
