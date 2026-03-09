import json
import os
import re
import requests
import threading
import time
import traceback
import uuid
from librespot.audio.decoders import AudioQuality
from librespot.core import Session
from librespot.zeroconf import ZeroconfServer
from ..otsconfig import config, cache_dir
from ..runtimedata import get_logger, account_pool, pending, download_queue, pending_lock
from ..utils import make_call, conv_list_format

logger = get_logger("api.spotify")
BASE_URL = "https://api.spotify.com/v1"

# Cache for album track IDs to avoid repeated API calls and race conditions
_album_track_ids_cache = {}
_album_track_ids_cache_lock = threading.Lock()

# Cache for Spotify Web API client-credentials token
_spotify_app_token = {"access_token": None, "expires_at": 0}
_spotify_app_token_lock = threading.Lock()
_spotify_app_token_cred_index = -1
_spotify_app_credential_backoff = {}
_spotify_app_credential_backoff_lock = threading.Lock()


def _normalize_spotify_client_credentials(raw_credentials):
    normalized = []
    if not isinstance(raw_credentials, list):
        return normalized
    for entry in raw_credentials:
        if not isinstance(entry, dict):
            continue
        client_id = (entry.get("clientId") or entry.get("client_id") or "").strip()
        client_secret = (entry.get("clientSecret") or entry.get("client_secret") or "").strip()
        if client_id and client_secret:
            normalized.append(
                {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "source": "config_list",
                }
            )
    return normalized


def _load_spotify_client_credentials():
    env_client_id = os.environ.get("ONTHESPOT_SPOTIFY_CLIENT_ID", "").strip()
    env_client_secret = os.environ.get("ONTHESPOT_SPOTIFY_CLIENT_SECRET", "").strip()
    if env_client_id or env_client_secret:
        if not env_client_id or not env_client_secret:
            logger.info("Spotify app creds missing in env; both client ID and secret are required.")
            return []
        return [
            {
                "client_id": env_client_id,
                "client_secret": env_client_secret,
                "source": "env",
            }
        ]

    cfg_credentials = _normalize_spotify_client_credentials(
        config.get("spotify_client_credentials", [])
    )
    if cfg_credentials:
        return cfg_credentials

    cfg_client_id = config.get("spotify_client_id", "").strip()
    cfg_client_secret = config.get("spotify_client_secret", "").strip()
    if cfg_client_id and cfg_client_secret:
        return [
            {
                "client_id": cfg_client_id,
                "client_secret": cfg_client_secret,
                "source": "config_single",
            }
        ]
    return []


def _remove_spotify_client_credential(client_id, client_secret):
    credentials = config.get("spotify_client_credentials", [])
    if not isinstance(credentials, list):
        return False

    def _matches(entry):
        if not isinstance(entry, dict):
            return False
        entry_id = (entry.get("clientId") or entry.get("client_id") or "").strip()
        entry_secret = (entry.get("clientSecret") or entry.get("client_secret") or "").strip()
        return entry_id == client_id and entry_secret == client_secret

    filtered = [entry for entry in credentials if not _matches(entry)]
    if len(filtered) == len(credentials):
        return False

    config.set("spotify_client_credentials", filtered)
    config.save()
    logger.warning("Removed invalid Spotify client credential ending with %s.", _mask_value(client_id))
    return True


def _select_next_spotify_credential(credentials):
    global _spotify_app_token_cred_index
    if not credentials:
        return None
    now = time.time()
    for _ in range(len(credentials)):
        _spotify_app_token_cred_index = (_spotify_app_token_cred_index + 1) % len(credentials)
        candidate = credentials[_spotify_app_token_cred_index]
        if not _is_spotify_credential_rate_limited(candidate.get("client_id"), now):
            return candidate
    return None


def _mask_value(value, visible=6):
    if not value:
        return ""
    if len(value) <= visible:
        return value
    return f"...{value[-visible:]}"


def _is_spotify_credential_rate_limited(client_id, now=None):
    if not client_id:
        return False
    now = now or time.time()
    with _spotify_app_credential_backoff_lock:
        until = _spotify_app_credential_backoff.get(client_id)
        if not until:
            return False
        if until <= now:
            _spotify_app_credential_backoff.pop(client_id, None)
            return False
        return True


def _backoff_spotify_credential(client_id, seconds=None):
    if not client_id:
        return
    if seconds is None:
        seconds = int(config.get("spotify_client_backoff_seconds", 3600))
    until = time.time() + seconds
    with _spotify_app_credential_backoff_lock:
        _spotify_app_credential_backoff[client_id] = until
    logger.warning(
        "Backing off Spotify client_id=%s for %ss due to rate limit.",
        _mask_value(client_id),
        seconds,
    )


def _spotify_get_app_access_token(force_rotate=False):
    global _spotify_app_token_cred_index
    credentials = _load_spotify_client_credentials()
    if not credentials:
        logger.info("Spotify app creds missing; skipping client-credentials token.")
        return None

    now = time.time()
    max_retry_after = config.get("api_retry_max_retry_after", 10)
    default_delay = config.get("api_retry_default_delay", 1)

    with _spotify_app_token_lock:
        if (
            not force_rotate
            and _spotify_app_token["access_token"]
            and _spotify_app_token["expires_at"] > now + 30
        ):
            ttl = int(_spotify_app_token["expires_at"] - now)
            logger.info("Spotify app token cache hit (ttl=%ss).", ttl)
            return _spotify_app_token["access_token"]

        attempts = 0
        while credentials and attempts < len(credentials):
            cred = _select_next_spotify_credential(credentials)
            if cred is None:
                logger.warning("All Spotify client credentials are in backoff.")
                break

            client_id = cred["client_id"]
            client_secret = cred["client_secret"]
            logger.info(
                "Requesting Spotify app token with client_id=%s",
                _mask_value(client_id),
            )
            try:
                resp = requests.post(
                    "https://accounts.spotify.com/api/token",
                    data={"grant_type": "client_credentials"},
                    auth=(client_id, client_secret),
                    timeout=10,
                )
            except requests.RequestException as e:
                logger.error("Spotify app token request failed: %s", e)
                attempts += 1
                continue

            if resp.status_code == 200:
                data = resp.json()
                access_token = data.get("access_token")
                expires_in = int(data.get("expires_in", 0))
                if not access_token:
                    logger.error("Spotify app token response missing access_token.")
                    attempts += 1
                    continue

                _spotify_app_token["access_token"] = access_token
                _spotify_app_token["expires_at"] = now + expires_in
                _spotify_app_token["client_id"] = client_id
                logger.info("Spotify app token refreshed (expires_in=%ss).", expires_in)
                return access_token

            if resp.status_code in (400, 401) and "invalid_client" in resp.text:
                if cred.get("source") == "config_list":
                    if _remove_spotify_client_credential(client_id, client_secret):
                        credentials = [
                            entry
                            for entry in credentials
                            if entry.get("client_id") != client_id
                        ]
                        attempts = 0
                        if _spotify_app_token_cred_index >= len(credentials):
                            _spotify_app_token_cred_index = -1
                        continue
                logger.error(
                    "Spotify app token request invalid credentials for client_id=%s",
                    _mask_value(client_id),
                )
                _backoff_spotify_credential(client_id)
                attempts += 1
                continue

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait_seconds = (
                        int(retry_after) if retry_after is not None else default_delay
                    )
                except ValueError:
                    wait_seconds = default_delay
                if wait_seconds > max_retry_after and len(credentials) <= 1:
                    logger.warning(
                        "Spotify app token rate limited with Retry-After=%ss; exceeds max %ss.",
                        wait_seconds,
                        max_retry_after,
                    )
                    return None
                logger.warning(
                    "Spotify app token rate limited (Retry-After=%ss). Rotating credentials.",
                    wait_seconds,
                )
                _backoff_spotify_credential(client_id)
                attempts += 1
                if len(credentials) <= 1:
                    time.sleep(max(0, wait_seconds))
                continue

            logger.error(
                "Spotify app token request error: %s - %s",
                resp.status_code,
                resp.text,
            )
            attempts += 1

    return None


def _spotify_get_public_api_headers(token, context, force_rotate=False):
    app_token = _spotify_get_app_access_token(force_rotate=force_rotate)
    if app_token:
        logger.info("Spotify %s using app token.", context)
        return {"Authorization": f"Bearer {app_token}"}, "app"

    logger.info("Spotify %s using session token.", context)
    return {"Authorization": f"Bearer {token.tokens().get('user-read-email')}"}, "session"


def _spotify_make_call_with_headers(
    url,
    token,
    context,
    headers,
    auth_source,
    params=None,
    skip_cache=False,
    text=False,
    use_ssl=False,
    max_retry_after=None,
    wait_for_rate_limit=False,
):
    refresh_headers = None
    if auth_source == "app":
        current_client_id = _spotify_app_token.get("client_id")
        def refresh_headers():
            _backoff_spotify_credential(current_client_id)
            new_headers, new_source = _spotify_get_public_api_headers(
                token, context, force_rotate=True
            )
            if new_source != "app":
                return None
            return new_headers
    elif auth_source == "session":
        def refresh_headers():
            try:
                from ..accounts import get_account_token
                rotated_token = get_account_token("spotify", rotate=True)
                if rotated_token is None:
                    logger.warning(
                        "Spotify %s session refresh failed: no rotated account token available.",
                        context,
                    )
                    return None
                rotated_access_token = rotated_token.tokens().get("user-read-email")
                if not rotated_access_token:
                    logger.warning(
                        "Spotify %s session refresh failed: rotated token missing user-read-email scope token.",
                        context,
                    )
                    return None
                logger.warning(
                    "Spotify %s session token rotated after rate-limit response.",
                    context,
                )
                return {"Authorization": f"Bearer {rotated_access_token}"}
            except Exception as e:
                logger.warning(
                    "Spotify %s session refresh failed during rate-limit handling: %s",
                    context,
                    e,
                )
                return None

    return make_call(
        url,
        params=params,
        headers=headers,
        skip_cache=skip_cache,
        text=text,
        use_ssl=use_ssl,
        refresh_headers=refresh_headers,
        max_retry_after=max_retry_after,
        wait_for_rate_limit=wait_for_rate_limit,
    )


def _spotify_extract_year(value):
    if not value:
        return None
    match = re.search(r'(\d{4})', str(value))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def spotify_get_playlist_updated_year(token, headers, auth_source, playlist_id, tracks_total, context):
    """Get playlist last modified year based on most recently added track.
    
    Optimized: Only fetches the LAST track's added_at (1 API call instead of 2).
    The most recent addition is the best indicator of when playlist was updated.
    """
    if not isinstance(tracks_total, int) or tracks_total <= 0:
        return None

    # Fetch only the LAST track (most recent addition)
    resp = _spotify_make_call_with_headers(
        f"{BASE_URL}/playlists/{playlist_id}/tracks",
        token,
        context,
        headers,
        auth_source,
        params={
            'offset': str(max(0, tracks_total - 1)),
            'limit': '1',
            'fields': 'items(added_at)'
        },
        skip_cache=False,  # Cache this to avoid repeated calls for same playlist
    )
    
    if not resp or not resp.get('items'):
        return None
    
    last_added_at = resp['items'][0].get('added_at')
    return str(_spotify_extract_year(last_added_at)) if _spotify_extract_year(last_added_at) else None

def clear_album_track_ids_cache():
    """Clear the album track IDs cache to free memory."""
    with _album_track_ids_cache_lock:
        count = len(_album_track_ids_cache)
        _album_track_ids_cache.clear()
        logger.info(f"Cleared album track IDs cache ({count} entries)")


class MirrorSpotifyPlayback:
    def __init__(self):
        self.thread = None
        self.is_running = False

    def start(self):
        if self.thread is None:
            logger.info('Starting SpotifyMirrorPlayback')
            self.is_running = True
            self.thread = threading.Thread(target=self.run)
            self.thread.start()
        else:
            logger.warning('SpotifyMirrorPlayback is already running.')

    def stop(self):
        if self.thread is not None:
            logger.info('Stopping SpotifyMirrorPlayback')
            self.is_running = False
            self.thread.join()
            self.thread = None
        else:
            logger.warning('SpotifyMirrorPlayback is not running.')

    def run(self):
        # Circular Import
        from ..accounts import get_account_token
        while self.is_running:
            time.sleep(5)
            try:
                token = get_account_token('spotify').tokens()
            except (AttributeError, IndexError):
                # Account pool hasn't been filled yet
                continue
            url = f"{BASE_URL}/me/player/currently-playing"
            try:
                resp = requests.get(url, headers={"Authorization": f"Bearer {token.get('user-read-currently-playing')}"})
            except:
                logger.info("Session Expired, reinitializing...")
                parsing_index = config.get('active_account_number')
                spotify_re_init_session(account_pool[parsing_index])
                token = account_pool[parsing_index]['login']['session']
                continue
            if resp.status_code == 200:
                data = resp.json()
                if data['currently_playing_type'] == 'track':
                    item_id = data['item']['id']
                    if item_id not in pending and item_id not in download_queue:
                        parent_category = 'track'
                        playlist_name = ''
                        playlist_by = ''
                        if data['context'] is not None:
                            if data['context'].get('type') == 'playlist':
                                match = re.search(r'spotify:playlist:(\w+)', data['context']['uri'])
                                if match:
                                    playlist_id = match.group(1)
                                else:
                                    continue
                                token = get_account_token('spotify')
                                playlist_name, playlist_by = spotify_get_playlist_data(token, playlist_id)
                                parent_category = 'playlist'
                            elif data['context'].get('type') == 'collection':
                                playlist_name = 'Liked Songs'
                                playlist_by = 'me'
                                parent_category = 'playlist'
                            elif data['context'].get('type') in ('album', 'artist'):
                                parent_category = 'album'
                        # Use item id to prevent duplicates
                        #local_id = format_local_id(item_id)
                        with pending_lock:
                            pending[item_id] = {
                                'local_id': item_id,
                                'item_service': 'spotify',
                                'item_type': 'track',
                                'item_id': item_id,
                                'parent_category': parent_category,
                                'playlist_name': playlist_name,
                                'playlist_by': playlist_by,
                                'playlist_number': '?'
                            }
                        logger.info(f'Mirror Spotify Playback added track to download queue: https://open.spotify.com/track/{item_id}')
                        continue
                else:
                    logger.info('Spotify API does not return enough data to parse currently playing episodes.')
                    continue
            else:
                continue


def spotify_new_session():
    os.makedirs(os.path.join(cache_dir(), 'sessions'), exist_ok=True)

    uuid_uniq = str(uuid.uuid4())
    session_json_path = os.path.join(os.path.join(cache_dir(), 'sessions'),
                 f"ots_login_{uuid_uniq}.json")

    CLIENT_ID: str = "65b708073fc0480ea92a077233ca87bd"
    ZeroconfServer._ZeroconfServer__default_get_info_fields['clientID'] = CLIENT_ID
    zs_builder = ZeroconfServer.Builder()
    zs_builder.device_name = 'OnTheSpot'
    zs_builder.conf.stored_credentials_file = session_json_path
    zs = zs_builder.create()
    logger.info("Zeroconf login service started")

    while True:
        time.sleep(1)
        if zs.has_valid_session():
            logger.info(f"Grabbed {zs._ZeroconfServer__session} for {zs._ZeroconfServer__session.username()}")
            if zs._ZeroconfServer__session.username() in config.get('accounts'):
                logger.info("Account already exists")
                return False
            else:
                # I wish there was a way to get credentials without saving to
                # a file and parsing it but not currently sure how.
                try:
                    with open(session_json_path, 'r') as file:
                        zeroconf_login = json.load(file)
                except FileNotFoundError as e:
                    logger.error(f"Error: {str(e)} The file {session_json_path} was not found.\nTraceback: {traceback.format_exc()}")
                except json.JSONDecodeError as e:
                    logger.error(f"Error: {str(e)} Failed to decode JSON from the file.\nTraceback: {traceback.format_exc()}")
                except Exception as e:
                    logger.error(f"Unknown Error: {str(e)}\nTraceback: {traceback.format_exc()}")
                cfg_copy = config.get('accounts').copy()
                new_user = {
                    "uuid": uuid_uniq,
                    "service": "spotify",
                    "active": True,
                    "login": {
                        "username": zeroconf_login["username"],
                        "credentials": zeroconf_login["credentials"],
                        "type": zeroconf_login["type"],
                    }
                }
                zs.close()
                cfg_copy.append(new_user)
                config.set('accounts', cfg_copy)
                config.save()
                logger.info("New account added to config.")
                return True


def spotify_login_user(account):
    try:
        # I'd prefer to use 'Session.Builder().stored(credentials).create but
        # I can't get it to work, loading from credentials file instead.
        uuid = account['uuid']
        username = account['login']['username']

        session_dir = os.path.join(cache_dir(), "sessions")
        os.makedirs(session_dir, exist_ok=True)
        session_json_path = os.path.join(session_dir, f"ots_login_{uuid}.json")
        try:
            with open(session_json_path, 'w') as file:
                json.dump(account['login'], file)
            logger.info(f"Login information for '{username[:4]}*******' written to {session_json_path}")
        except IOError as e:
            logger.error(f"Error writing to file {session_json_path}: {str(e)}\nTraceback: {traceback.format_exc()}")

        config = Session.Configuration.Builder().set_stored_credential_file(session_json_path).build()
        # For some reason initialising session as None prevents premature application exit
        session = None
        try:
            session = Session.Builder(conf=config).stored_file(session_json_path).create()
        except Exception:
            time.sleep(3)
            session = Session.Builder(conf=config).stored_file(session_json_path).create()
        logger.debug("Session created")
        logger.info(f"Login successful for user '{username[:4]}*******'")
        account_type = session.get_user_attribute("type")
        bitrate = "160k"
        if account_type == "premium":
            bitrate = "320k"
        account_pool.append({
            "uuid": uuid,
            "username": username,
            "service": "spotify",
            "status": "active",
            "account_type": account_type,
            "bitrate": bitrate,
            "login": {
                "session": session,
                "session_path": session_json_path,
            }
        })
        return True
    except Exception as e:
        logger.error(f"Unknown Exception: {str(e)}\nTraceback: {traceback.format_exc()}")
        account_pool.append({
            "uuid": uuid,
            "username": username,
            "service": "spotify",
            "status": "error",
            "account_type": "N/A",
            "bitrate": "N/A",
            "login": {
                "session": "",
                "session_path": "",
            }
        })
        return False


def spotify_re_init_session(account, max_retries=4, force=False):
    """
    NUCLEAR OPTION: Completely destroy and recreate a Spotify session from scratch.

    This aggressively cleans up ALL session state and forces a completely fresh session.
    Librespot sessions go stale and this ensures we start with a clean slate.

    RATE LIMITED: Won't reconnect more than once per 30 seconds unless forced.

    Args:
        account: Account dictionary containing session information
        max_retries: Maximum number of connection retry attempts (default: 4)
        force: If True, bypass rate limiting and force reconnection
    """
    import gc
    import time

    session_json_path = os.path.join(cache_dir(), "sessions", f"ots_login_{account['uuid']}.json")
    username = account.get('username', 'unknown')

    # Rate limiting: Don't reconnect more than once per 30 seconds
    MIN_RECONNECT_INTERVAL = 30  # seconds
    last_reconnect = account.get('last_reconnect_time', 0)
    time_since_last = time.time() - last_reconnect

    if not force and time_since_last < MIN_RECONNECT_INTERVAL:
        logger.info(
            f"Rate limit: Session for {username} was reconnected {time_since_last:.1f}s ago, "
            f"skipping reconnection (min interval: {MIN_RECONNECT_INTERVAL}s)"
        )
        # Return existing session without reconnecting
        return account.get('login', {}).get('session')

    logger.info(f"NUCLEAR SESSION RESET for account {username} (UUID: {account['uuid']})")

    # STEP 1: Aggressively destroy old session
    old_session = account.get('login', {}).get('session')
    if old_session:
        try:
            # Try to close it
            old_session.close()
            logger.debug(f"Closed old session for {username}")
        except Exception as e:
            logger.warning(f"Error closing old session (will force cleanup anyway): {e}")

        # Delete ALL references to force cleanup
        try:
            if 'session' in account.get('login', {}):
                del account['login']['session']
            del old_session
            logger.debug(f"Deleted old session references for {username}")
        except Exception as e:
            logger.warning(f"Error deleting session references: {e}")

    # STEP 2: Force garbage collection to clean up librespot internals
    gc.collect()
    logger.debug("Forced garbage collection")

    # STEP 3: Small delay to ensure everything is cleaned up
    time.sleep(0.5)

    # STEP 4: Create completely fresh session from stored credentials with retry logic
    session = None
    last_error = None

    for attempt in range(max_retries):
        try:
            if attempt > 0:
                # Exponential backoff: 2s, 4s, 8s, 16s
                delay = 2 ** attempt
                logger.info(f"Retry {attempt}/{max_retries-1} for {username} after {delay}s delay...")
                time.sleep(delay)

            logger.info(f"Creating fresh session for {username}... (attempt {attempt + 1}/{max_retries})")

            # Build completely new config
            session_config = Session.Configuration.Builder().set_stored_credential_file(session_json_path).build()

            # Create completely new session - this establishes a new TCP connection
            session = Session.Builder(conf=session_config).stored_file(session_json_path).create()

            # Verify session is actually working
            try:
                account_type = session.get_user_attribute("type")
                logger.info(f"✓ NEW SESSION ACTIVE for {username} (type: {account_type})")
            except Exception as verify_err:
                logger.error(f"New session created but verification failed: {verify_err}")
                raise RuntimeError(f"Session verification failed: {verify_err}")

            # Update account with fresh session
            account['login']['session_path'] = session_json_path
            account['login']['session'] = session
            account['status'] = 'active'
            account['account_type'] = account_type
            account['bitrate'] = "320k" if account_type == "premium" else "160k"
            account['last_reconnect_time'] = time.time()  # Track reconnection time for rate limiting

            logger.info(f"✓ NUCLEAR SESSION RESET SUCCESSFUL for {username} (bitrate: {account['bitrate']})")
            return session

        except (ConnectionRefusedError, ConnectionError, OSError, TimeoutError) as e:
            last_error = e
            error_type = type(e).__name__
            logger.warning(f"Connection error during session creation (attempt {attempt + 1}/{max_retries}): {error_type} - {e}")

            # Clean up failed session attempt
            if session:
                try:
                    session.close()
                except:
                    pass
                session = None

            # If this was the last attempt, we'll raise the error below
            if attempt == max_retries - 1:
                logger.error(f"✗ All {max_retries} connection attempts failed for {username}")
                break

        except Exception as e:
            # For non-connection errors, fail immediately
            last_error = e
            logger.error(f'✗ NUCLEAR SESSION RESET FAILED for {username}: {e}')
            account['status'] = 'error'
            raise RuntimeError(f"Failed to recreate session for {username}: {e}")

    # If we get here, all retries failed
    account['status'] = 'error'
    raise RuntimeError(f"Failed to recreate session for {username} after {max_retries} attempts: {last_error}")


def spotify_warm_session(account, track_id):
    from librespot.audio.decoders import AudioQuality, VorbisOnlyAudioQuality
    from librespot.metadata import TrackId

    token = account.get('login', {}).get('session')
    if not token or isinstance(token, str):
        raise RuntimeError("Session missing or invalid")
    if not track_id:
        raise RuntimeError("No track ID available for warm-up")

    audio_key = TrackId.from_base62(track_id)
    # librespot's AudioQuality enum differs across versions. Prefer "MEDIUM" if
    # present (older forks), otherwise fall back to "NORMAL" (current upstream).
    quality = getattr(AudioQuality, "MEDIUM", None) or getattr(AudioQuality, "NORMAL", None)
    if quality is None:
        # Defensive fallback: keep the warm-up from crashing even if the enum changes again.
        quality = AudioQuality.HIGH
    stream = token.content_feeder().load(
        audio_key,
        VorbisOnlyAudioQuality(quality),
        False,
        None,
    )
    try:
        if hasattr(stream, "close"):
            stream.close()
        elif hasattr(stream, "stream") and hasattr(stream.stream, "close"):
            stream.stream.close()
    except Exception:
        pass
    return True


def spotify_get_token(parsing_index):
    """
    Get Spotify session token.

    LAZY RECONNECTION: Only recreates session when it's actually invalid,
    not proactively. Sessions can stay healthy for hours if properly managed.

    The old 30-second cooldown was counter-productive - it caused:
    - Unnecessary Spotify API load → rate limiting
    - TCP connection churn → connection resets
    - More problems than it solved

    Sessions go stale from INACTIVITY (30-60 min), not age.
    """
    username = account_pool[parsing_index].get('username', 'unknown')

    try:
        token = account_pool[parsing_index]['login']['session']
        # Check if session is a valid object (not an empty string from failed login)
        if not token or isinstance(token, str):
            raise AttributeError("Session is empty or invalid")
        logger.debug(f"Using existing session for {username}")
        return token

    except (OSError, AttributeError, KeyError) as e:
        # Session missing or invalid - recreate ONLY when needed
        logger.info(f'Session invalid for {username} ({e}), recreating...')
        spotify_re_init_session(account_pool[parsing_index])
        token = account_pool[parsing_index]['login']['session']
        return token


def spotify_get_artist_album_ids(token, artist_id, _retry=False):
    logger.info(f"Getting album ids for artist: '{artist_id}'")
    items = []
    offset = 0
    limit = 50
    while True:
        headers = {}
        try:
            headers, auth_source = _spotify_get_public_api_headers(token, "artist albums")
        except (RuntimeError, OSError) as e:
            if _retry:
                logger.error(f"Failed to get token after retry for artist {artist_id}: {e}")
                raise
            logger.warning(f"Token retrieval failed for artist album IDs, attempting session reconnect: {e}")
            # Re-initialize the session
            parsing_index = config.get('active_account_number')
            spotify_re_init_session(account_pool[parsing_index])
            # Get the new token
            new_token = account_pool[parsing_index]['login']['session']
            # Retry with the new token
            return spotify_get_artist_album_ids(new_token, artist_id, _retry=True)

        url = f'{BASE_URL}/artists/{artist_id}/albums?include_groups=album%2Csingle&limit={limit}&offset={offset}' #%2Cappears_on%2Ccompilation
        artist_data = _spotify_make_call_with_headers(
            url,
            token,
            "artist albums",
            headers,
            auth_source,
        )
        if artist_data is None:
            logger.error("Spotify artist albums request failed (%s) for %s", auth_source, artist_id)
            return []

        offset += limit
        items.extend(artist_data['items'])

        if artist_data['total'] <= offset:
            break

    item_ids = []
    for album in items:
        item_ids.append(album['id'])
    return item_ids


def spotify_get_playlist_data(token, playlist_id, _retry=False):
    logger.info(f"Get playlist data for playlist: {playlist_id}")
    
    # Use app token for playlist data (higher rate limits)
    try:
        headers, auth_source = _spotify_get_public_api_headers(token, "playlist data")
        logger.info(f"Using {auth_source} token for playlist data")
    except (RuntimeError, OSError) as e:
        if _retry:
            logger.error(f"Failed to get token after retry for playlist {playlist_id}: {e}")
            raise
        logger.warning(f"Token retrieval failed for playlist data, attempting session reconnect: {e}")
        parsing_index = config.get('active_account_number')
        spotify_re_init_session(account_pool[parsing_index])
        new_token = account_pool[parsing_index]['login']['session']
        return spotify_get_playlist_data(new_token, playlist_id, _retry=True)

    # Enable caching to avoid rate limits on repeated requests
    resp = _spotify_make_call_with_headers(
        f'{BASE_URL}/playlists/{playlist_id}',
        token,
        "playlist data",
        headers,
        auth_source,
        skip_cache=False,
    )
    # Get highest quality playlist image (first image is highest quality)
    image_url = ''
    if resp.get('images') and len(resp['images']) > 0:
        image_url = resp['images'][0].get('url', '')
        logger.info(f"Playlist image URL: {image_url}")
    else:
        logger.warning(f"No playlist images found for playlist {playlist_id}")
    return resp['name'], resp['owner']['display_name'], image_url


def spotify_get_lyrics(token, item_id, item_type, metadata, filepath, _retry=False):
    if config.get('download_lyrics'):
        lyrics = []
        try:
            if item_type == "track":
                url = f'https://spclient.wg.spotify.com/color-lyrics/v2/track/{item_id}?format=json&market=from_token'
            elif item_type == "episode":
                url = f"https://spclient.wg.spotify.com/transcript-read-along/v2/episode/{item_id}?format=json&market=from_token"

            headers = {}
            headers['app-platform'] = 'WebPlayer'
            try:
                headers['Authorization'] = f'Bearer {token.tokens().get("user-read-email")}'
            except (RuntimeError, OSError) as e:
                if _retry:
                    logger.error(f"Failed to get token after retry for lyrics {item_id}: {e}")
                    raise
                logger.warning(f"Token retrieval failed for lyrics, attempting session reconnect: {e}")
                # Re-initialize the session
                parsing_index = config.get('active_account_number')
                spotify_re_init_session(account_pool[parsing_index])
                # Get the new token
                new_token = account_pool[parsing_index]['login']['session']
                # Retry with the new token
                return spotify_get_lyrics(new_token, item_id, item_type, metadata, filepath, _retry=True)

            headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'

            resp = make_call(url, headers=headers)
            if resp == None:
                logger.info(f"Failed to find lyrics for {item_type}: {item_id}")
                return None

            if not config.get('only_download_plain_lyrics'):
                if config.get("embed_branding"):
                    lyrics.append('[re:OnTheSpot]')

                for key in metadata.keys():
                    value = metadata[key]
                    if key in ['title', 'track_title', 'tracktitle'] and config.get("embed_name"):
                        title = value
                        lyrics.append(f'[ti:{title}]')
                    elif key == 'artists' and config.get("embed_artist"):
                        artist = value
                        lyrics.append(f'[ar:{artist}]')
                    elif key in ['album_name', 'album'] and config.get("embed_album"):
                        album = value
                        lyrics.append(f'[al:{album}]')
                    elif key in ['writers'] and config.get("embed_writers"):
                        author = value
                        lyrics.append(f'[au:{author}]')

                if item_type == "track":
                    lyrics.append(f'[by:{resp["lyrics"]["provider"]}]')

                if config.get("embed_length"):
                    length_value = metadata.get('length')
                    try:
                        l_ms = int(length_value)
                    except (TypeError, ValueError):
                        l_ms = None
                        logger.debug(f"Skipping embed_length for {item_id}: invalid length '{length_value}'")
                    if l_ms is not None:
                        if round((l_ms/1000)/60) < 10:
                            digit="0"
                        else:
                            digit=""
                        lyrics.append(f'[length:{digit}{round((l_ms/1000)/60)}:{round((l_ms/1000)%60)}]\n')

            default_length = len(lyrics)

            if item_type == "track":
                if resp["lyrics"]["syncType"] == "LINE_SYNCED":
                    for line in resp["lyrics"]["lines"]:
                        minutes, seconds = divmod(int(line['startTimeMs']) / 1000, 60)
                        if not config.get('only_download_plain_lyrics'):
                            lyrics.append(f'[{minutes:0>2.0f}:{seconds:05.2f}] {line["words"]}')
                        else:
                            lyrics.append(line["words"])
                elif resp["lyrics"]["syncType"] == "UNSYNCED" and not config.get("only_download_synced_lyrics"):
                    lyrics = [line['words'] for line in resp['lyrics']['lines']]

            elif item_type == "episode":
                if resp["timeSyncedStatus"] == "SYLLABLE_SYNCED":
                    for line in resp["section"]:
                        try:
                            minutes, seconds = divmod(int(line['startMs']) / 1000, 60)
                            lyrics.append(f'[{minutes:0>2.0f}:{seconds:05.2f}] {line["text"]["sentence"]["text"]}')
                        except KeyError as e:
                            logger.debug(f"Invalid line: {str(e)} likely title, skipping..")
                else:
                    logger.info("Unsynced episode lyrics, please open a bug report.")

        except (KeyError, IndexError) as e:
            logger.error(f'KeyError/Index Error. Failed to get lyrics for {item_id}: {str(e)}\nTraceback: {traceback.format_exc()}')

        merged_lyrics = '\n'.join(lyrics)

        if lyrics:
            logger.debug(lyrics)
            if len(lyrics) <= default_length:
                return False
            if config.get('save_lrc_file'):
                with open(filepath + '.lrc', 'w', encoding='utf-8') as f:
                    f.write(merged_lyrics)
            if config.get('embed_lyrics'):
                if item_type == "track":
                    return {"lyrics": merged_lyrics, "language": resp['lyrics']['language']}
                if item_type == "episode":
                    return {"lyrics": merged_lyrics}
            else:
                return True
    else:
        return False


def spotify_get_playlist_items(token, playlist_id, _retry=False):
    """
    Get playlist items with MINIMAL data (just IDs and types).
    For playlists, we fetch metadata per-track during download to avoid API hammering.
    """
    logger.info(f"Getting items in playlist: '{playlist_id}'")
    
    # Cache debug flag ONCE to avoid config deadlocks inside loop
    debug_enabled = config.get('debug_playlist_flow', False)
    
    if debug_enabled:
        logger.info(f"[DEBUG FLOW] spotify_get_playlist_items: starting, token type: {type(token)}")
    
    items = []
    offset = 0
    limit = 100

    while True:
        url = f'{BASE_URL}/playlists/{playlist_id}/tracks?additional_types=track%2Cepisode&offset={offset}&limit={limit}&fields=items(track(id,type,name,artists(name),external_urls(spotify),is_playable)),total'
        
        # Use app token if available (much higher rate limits than session token)
        try:
            headers, auth_source = _spotify_get_public_api_headers(token, "playlist items")
            if debug_enabled:
                logger.info(f"[DEBUG FLOW] Using {auth_source} token for playlist request")
        except (RuntimeError, OSError) as e:
            if _retry:
                logger.error(f"Failed to get token after retry for playlist items {playlist_id}: {e}")
                raise
            logger.warning(f"Token retrieval failed for playlist items, attempting session reconnect: {e}")
            parsing_index = config.get('active_account_number')
            spotify_re_init_session(account_pool[parsing_index])
            new_token = account_pool[parsing_index]['login']['session']
            return spotify_get_playlist_items(new_token, playlist_id, _retry=True)

        if debug_enabled:
            logger.info(f"[DEBUG FLOW] Calling make_call() for offset={offset}, limit={limit} (minimal fields)")

        # Use cache for playlists to avoid rate limiting (playlists don't change that often)
        # First request per session will be cached, subsequent requests use cache
        resp = _spotify_make_call_with_headers(
            url,
            token,
            "playlist items",
            headers,
            auth_source,
            skip_cache=False,
        )
        
        if resp is None:
            logger.error(f"Failed to get playlist items for {playlist_id} at offset {offset}")
            raise RuntimeError(f"Playlist API call failed for {playlist_id}")

        offset += limit
        items.extend(resp['items'])

        if resp['total'] <= offset:
            break
    
    logger.info(f"Retrieved {len(items)} minimal items from playlist {playlist_id}")
    return items


def spotify_get_liked_songs(token, _retry=False):
    logger.info("Getting liked songs")
    items = []
    offset = 0
    limit = 50

    while True:
        url = f'{BASE_URL}/me/tracks?offset={offset}&limit={limit}'
        headers = {}
        
        # Get token with timeout protection
        token_error = None
        token_result = None
        
        def get_token_with_timeout():
            nonlocal token_result, token_error
            try:
                token_result = token.tokens().get('user-library-read')
            except Exception as e:
                token_error = e
        
        import threading
        token_thread = threading.Thread(target=get_token_with_timeout, daemon=True)
        token_thread.start()
        token_thread.join(timeout=10)
        
        if token_thread.is_alive():
            logger.error(f"Token retrieval hung for liked songs - session is dead")
            if _retry:
                raise RuntimeError("Token retrieval timed out after retry")
            logger.warning("Attempting session reconnect after token timeout...")
            parsing_index = config.get('active_account_number')
            spotify_re_init_session(account_pool[parsing_index])
            new_token = account_pool[parsing_index]['login']['session']
            return spotify_get_liked_songs(new_token, _retry=True)
        
        if token_error:
            if _retry:
                logger.error(f"Failed to get token after retry for liked songs: {token_error}")
                raise token_error
            logger.warning(f"Token retrieval failed for liked songs, attempting session reconnect: {token_error}")
            parsing_index = config.get('active_account_number')
            spotify_re_init_session(account_pool[parsing_index])
            new_token = account_pool[parsing_index]['login']['session']
            return spotify_get_liked_songs(new_token, _retry=True)
        
        headers['Authorization'] = f"Bearer {token_result}"

        # Use cache to avoid rate limiting on repeated requests
        resp = make_call(url, headers=headers, skip_cache=False)

        offset += limit
        items.extend(resp['items'])

        if resp['total'] <= offset:
            break
    return items


def spotify_get_your_episodes(token, _retry=False):
    logger.info("Getting your episodes")
    items = []
    offset = 0
    limit = 50

    while True:
        headers = {}
        
        # Get token with timeout protection
        token_error = None
        token_result = None
        
        def get_token_with_timeout():
            nonlocal token_result, token_error
            try:
                token_result = token.tokens().get('user-library-read')
            except Exception as e:
                token_error = e
        
        import threading
        token_thread = threading.Thread(target=get_token_with_timeout, daemon=True)
        token_thread.start()
        token_thread.join(timeout=10)
        
        if token_thread.is_alive():
            logger.error(f"Token retrieval hung for your episodes - session is dead")
            if _retry:
                raise RuntimeError("Token retrieval timed out after retry")
            logger.warning("Attempting session reconnect after token timeout...")
            parsing_index = config.get('active_account_number')
            spotify_re_init_session(account_pool[parsing_index])
            new_token = account_pool[parsing_index]['login']['session']
            return spotify_get_your_episodes(new_token, _retry=True)
        
        if token_error:
            if _retry:
                logger.error(f"Failed to get token after retry for your episodes: {token_error}")
                raise token_error
            logger.warning(f"Token retrieval failed for your episodes, attempting session reconnect: {token_error}")
            parsing_index = config.get('active_account_number')
            spotify_re_init_session(account_pool[parsing_index])
            new_token = account_pool[parsing_index]['login']['session']
            return spotify_get_your_episodes(new_token, _retry=True)
        
        headers['Authorization'] = f"Bearer {token_result}"

        url = f'{BASE_URL}/me/episodes?offset={offset}&limit={limit}'

        # Use cache to avoid rate limiting
        resp = make_call(url, headers=headers, skip_cache=False)

        offset += limit
        items.extend(resp['items'])

        if resp['total'] <= offset:
            break
    return items


def spotify_get_album_track_ids(token, album_id, _retry=False):
    # Check cache first (thread-safe)
    with _album_track_ids_cache_lock:
        if album_id in _album_track_ids_cache:
            logger.debug(f"Using cached track IDs for album: {album_id}")
            return _album_track_ids_cache[album_id]
    
    # Not in cache, fetch from API
    logger.info(f"Getting tracks from album: {album_id}")
    tracks = []
    offset = 0
    limit = 50

    while True:
        url=f'{BASE_URL}/albums/{album_id}/tracks?offset={offset}&limit={limit}'
        headers = {}
        try:
            headers, auth_source = _spotify_get_public_api_headers(token, "album tracks")
        except (RuntimeError, OSError) as e:
            if _retry:
                logger.error(f"Failed to get token after retry for album {album_id}: {e}")
                raise
            logger.warning(f"Token retrieval failed for album track IDs, attempting session reconnect: {e}")
            # Re-initialize the session
            parsing_index = config.get('active_account_number')
            spotify_re_init_session(account_pool[parsing_index])
            # Get the new token
            new_token = account_pool[parsing_index]['login']['session']
            # Retry with the new token
            return spotify_get_album_track_ids(new_token, album_id, _retry=True)

        resp = _spotify_make_call_with_headers(
            url,
            token,
            "album tracks",
            headers,
            auth_source,
            skip_cache=True,
        )
        if resp is None:
            logger.error("Spotify album tracks request failed (%s) for %s", auth_source, album_id)
            return []

        offset += limit
        tracks.extend(resp['items'])

        if resp['total'] <= offset:
            break

    item_ids = []
    for track in tracks:
        if track:
            item_ids.append(track['id'])
    
    logger.info(f"Album {album_id} has {len(item_ids)} tracks with IDs: {item_ids[:5]}{'...' if len(item_ids) > 5 else ''}")
    
    # Store in cache (thread-safe)
    with _album_track_ids_cache_lock:
        _album_track_ids_cache[album_id] = item_ids
    
    return item_ids


def spotify_get_album_tracks_with_metadata(token, album_id, _retry=False):
    """
    Get album tracks with full track objects for optimization.
    Returns list of track objects that can be cached to avoid individual API calls.
    """
    logger.info(f"Getting tracks with metadata from album: {album_id}")
    tracks = []
    offset = 0
    limit = 50

    # First get album data for shared metadata
    headers = {}
    try:
        headers, auth_source = _spotify_get_public_api_headers(token, "album metadata")
    except (RuntimeError, OSError) as e:
        if _retry:
            logger.error(f"Failed to get token after retry for album {album_id}: {e}")
            raise
        logger.warning(f"Token retrieval failed for album metadata, attempting session reconnect: {e}")
        parsing_index = config.get('active_account_number')
        spotify_re_init_session(account_pool[parsing_index])
        new_token = account_pool[parsing_index]['login']['session']
        return spotify_get_album_tracks_with_metadata(new_token, album_id, _retry=True)
    
    # Get full album data (includes album-level metadata)
    album_data = _spotify_make_call_with_headers(
        f'{BASE_URL}/albums/{album_id}',
        token,
        "album metadata",
        headers,
        auth_source,
    )
    if not album_data:
        logger.error("Failed to get album data for %s", album_id)
        return []

    # Get all tracks from album
    while True:
        url = f'{BASE_URL}/albums/{album_id}/tracks?offset={offset}&limit={limit}'
        resp = _spotify_make_call_with_headers(
            url,
            token,
            "album metadata",
            headers,
            auth_source,
            skip_cache=True,
        )
        if resp is None:
            logger.error("Spotify album tracks request failed for %s", album_id)
            break

        offset += limit
        
        # Enrich track objects with album data
        for track in resp['items']:
            if track:
                # Add album data to track object (Spotify's simplified track from /albums/.../tracks doesn't include album data)
                track['album'] = {
                    'id': album_id,
                    'name': album_data.get('name', ''),
                    'album_type': album_data.get('album_type', ''),
                    'total_tracks': album_data.get('total_tracks', 0),
                    'release_date': album_data.get('release_date', ''),
                    'images': album_data.get('images', []),
                    'artists': album_data.get('artists', [])
                }
                tracks.append(track)

        if resp['total'] <= offset:
            break
    
    logger.info(f"Album {album_id} has {len(tracks)} tracks with enriched metadata")
    return tracks


def spotify_get_search_results(
    token,
    search_term,
    content_types,
    _retry=False,
    _force_session=False,
    _app_rotated=False,
):
    logger.info(f"Get search result for term '{search_term}'")

    headers = {}
    auth_source = "session"
    if _force_session:
        logger.info("Spotify search forcing session token fallback.")
        try:
            headers = {"Authorization": f"Bearer {token.tokens().get('user-read-email')}"}
            auth_source = "session"
        except (RuntimeError, OSError, ConnectionError, Exception) as e:
            if _retry:
                logger.error(f"Failed to get session token after retry: {e}")
                raise
            logger.warning(f"Session token retrieval failed, attempting session reconnect: {e}")
            try:
                parsing_index = config.get('active_account_number')
                spotify_re_init_session(account_pool[parsing_index])
                new_token = account_pool[parsing_index]['login']['session']
                return spotify_get_search_results(
                    new_token, search_term, content_types, _retry=True, _force_session=True
                )
            except Exception as reconnect_error:
                logger.error(f"Failed to reconnect and search with session token: {reconnect_error}")
                raise
    else:
        try:
            headers, auth_source = _spotify_get_public_api_headers(token, "search")
        except (RuntimeError, OSError, ConnectionError, Exception) as e:
            if _retry:
                logger.error(f"Failed to get token after retry: {e}")
                raise
            logger.warning(f"Token retrieval failed, attempting session reconnect: {e}")
            try:
                parsing_index = config.get('active_account_number')
                spotify_re_init_session(account_pool[parsing_index])
                new_token = account_pool[parsing_index]['login']['session']
                return spotify_get_search_results(new_token, search_term, content_types, _retry=True)
            except Exception as reconnect_error:
                logger.error(f"Failed to reconnect and search: {reconnect_error}")
                raise

    params = {}
    params['limit'] = config.get("max_search_results")
    params['offset'] = '0'
    params['q'] = search_term
    params['type'] = ",".join(c_type for c_type in content_types)
    params['market'] = config.get("spotify_search_market") or "BE"
    logger.info(
        "Spotify search request auth_source=%s types=%s limit=%s",
        auth_source,
        params['type'],
        params['limit'],
    )

    data = _spotify_make_call_with_headers(
        f"{BASE_URL}/search",
        token,
        "search",
        headers,
        auth_source,
        params=params,
        skip_cache=True,
        max_retry_after=90 if auth_source == "session" else None,
        wait_for_rate_limit=(auth_source == "session"),
    )
    if data is None:
        if auth_source == "app" and not _force_session and not _app_rotated:
            logger.warning("Spotify search failed using app token; rotating app credentials and retrying.")
            try:
                rotated_headers, rotated_source = _spotify_get_public_api_headers(
                    token, "search", force_rotate=True
                )
                if rotated_source == "app":
                    data = _spotify_make_call_with_headers(
                        f"{BASE_URL}/search",
                        token,
                        "search",
                        rotated_headers,
                        rotated_source,
                        params=params,
                        skip_cache=True,
                    )
                    if data is not None:
                        auth_source = "app"
            except Exception as e:
                logger.warning(f"Failed to rotate app credentials for search retry: {e}")
        if auth_source == "app" and not _force_session:
            logger.warning("Spotify search failed using app token; retrying with session token.")
            return spotify_get_search_results(
                token,
                search_term,
                content_types,
                _retry=_retry,
                _force_session=True,
                _app_rotated=True,
            )
        logger.error("Spotify search failed (auth_source=%s).", auth_source)
        return []
    if isinstance(data, dict) and data.get("error"):
        logger.error(f"Spotify search error payload: {data.get('error')}")
        return []

    search_results = []
    playlist_year_cache = {}
    for key in data.keys():
        bucket = data.get(key, {})
        items = bucket.get("items") if isinstance(bucket, dict) else None
        if not items:
            continue
        for item in items:
            if not item:
                logger.warning("Spotify search returned empty item in %s bucket.", key)
                continue
            try:
                item_type = item['type']
                if item_type == "track":
                    album = item.get('album') or {}
                    images = album.get('images') or []
                    item_name = f"{config.get('explicit_label') if item.get('explicit') else ''} {item['name']}"
                    item_by = f"{config.get('metadata_separator').join([artist['name'] for artist in item.get('artists', [])])}"
                    item_thumbnail_url = images[-1]["url"] if images else ""
                elif item_type == "album":
                    rel_year = re.search(r'(\d{4})', item.get('release_date', '')).group(1)
                    images = item.get('images') or []
                    item_name = f"[Y:{rel_year}] [T:{item['total_tracks']}] {item['name']}"
                    item_by = f"{config.get('metadata_separator').join([artist['name'] for artist in item.get('artists', [])])}"
                    item_thumbnail_url = images[-1]["url"] if images else ""
                elif item_type == "playlist":
                    tracks_total = item.get('tracks', {}).get('total')
                    if isinstance(tracks_total, int):
                        playlist_id = item['id']
                        # Fetch last modified year (optimized: 1 API call per playlist)
                        if playlist_id in playlist_year_cache:
                            rel_year = playlist_year_cache[playlist_id]
                        else:
                            rel_year = spotify_get_playlist_updated_year(
                                token,
                                headers,
                                auth_source,
                                playlist_id,
                                tracks_total,
                                "search",
                            )
                            playlist_year_cache[playlist_id] = rel_year
                        item_name = f"[Y:{rel_year or '????'}] [T:{tracks_total}] {item['name']}"
                    else:
                        item_name = f"{item['name']}"
                    owner = item.get('owner') or {}
                    images = item.get('images') or []
                    item_by = f"{owner.get('display_name', '')}"
                    item_thumbnail_url = images[-1]["url"] if images else ""
                elif item_type == "artist":
                    images = item.get('images') or []
                    item_name = item['name']
                    if f"{'/'.join(item.get('genres', []))}" != "":
                        item_name = item['name'] + f"  |  GENERES: {'/'.join(item.get('genres', []))}"
                    item_by = f"{item['name']}"
                    item_thumbnail_url = images[-1]["url"] if images else ""
                elif item_type == "show":
                    images = item.get('images') or []
                    item_name = f"{config.get('explicit_label') if item.get('explicit') else ''} {item['name']}"
                    item_by = f"{item.get('publisher', '')}"
                    item_thumbnail_url = images[-1]["url"] if images else ""
                    item_type = "podcast"
                elif item_type == "episode":
                    images = item.get('images') or []
                    item_name = f"{config.get('explicit_label') if item.get('explicit') else ''} {item['name']}"
                    item_by = ""
                    item_thumbnail_url = images[-1]["url"] if images else ""
                    item_type = "podcast_episode"
                elif item_type == "audiobook":
                    images = item.get('images') or []
                    item_name = f"{config.get('explicit_label') if item.get('explicit') else ''} {item['name']}"
                    item_by = f"{item.get('publisher', '')}"
                    item_thumbnail_url = images[-1]["url"] if images else ""
                else:
                    logger.warning("Spotify search returned unsupported item type: %s", item_type)
                    continue
            except Exception as e:
                logger.error("Spotify search item parse failed for %s: %s", item.get('id', 'unknown'), e)
                continue

            search_results.append({
                'item_id': item['id'],
                'item_name': item_name,
                'item_by': item_by,
                'item_type': item_type,
                'item_service': "spotify",
                'item_url': item['external_urls']['spotify'],
                'item_thumbnail_url': item_thumbnail_url
            })
    return search_results


def spotify_get_item_by_id(token, item_id, item_type, _retry=False):
    """
    Fetch a single Spotify item by ID and return it as a search result.

    Args:
        token: Spotify session token
        item_id: The 22-character Spotify ID
        item_type: Type of item (track, album, playlist, artist, episode, show)

    Returns:
        List containing a single search result dict, or empty list on error
    """
    logger.info(f"Get item by ID: {item_type}/{item_id}")

    headers = {}
    auth_source = "session"
    try:
        headers, auth_source = _spotify_get_public_api_headers(token, "item by id")
    except (RuntimeError, OSError, ConnectionError, Exception) as e:
        if _retry:
            logger.error(f"Failed to get token after retry for item {item_type}/{item_id}: {e}")
            return []
        logger.warning(f"Token retrieval failed for item {item_type}/{item_id}, attempting session reconnect: {e}")
        try:
            parsing_index = config.get('active_account_number')
            spotify_re_init_session(account_pool[parsing_index])
            new_token = account_pool[parsing_index]['login']['session']
            return spotify_get_item_by_id(new_token, item_id, item_type, _retry=True)
        except Exception as retry_error:
            logger.error(f"Failed to reconnect session for item {item_type}/{item_id}: {retry_error}")
            return []

    # Map internal types to API endpoints
    endpoint_type = item_type
    if item_type == 'podcast':
        endpoint_type = 'show'
    elif item_type == 'podcast_episode':
        endpoint_type = 'episode'

    try:
        # Fetch item data from Spotify API
        item = _spotify_make_call_with_headers(
            f"{BASE_URL}/{endpoint_type}s/{item_id}",
            token,
            "item by id",
            headers,
            auth_source,
            skip_cache=True,
        )
        if not item:
            logger.error("Failed to fetch %s %s (%s)", item_type, item_id, auth_source)
            return []

        # Format the result based on item type (same as search results)
        if item_type == "track":
            item_name = f"{config.get('explicit_label') if item.get('explicit') else ''} {item['name']}"
            item_by = f"{config.get('metadata_separator').join([artist['name'] for artist in item['artists']])}"
            item_thumbnail_url = item['album']['images'][-1]["url"] if len(item['album']['images']) > 0 else ""
        elif item_type == "album":
            rel_year = re.search(r'(\d{4})', item['release_date']).group(1) if item.get('release_date') else '????'
            item_name = f"[Y:{rel_year}] [T:{item['total_tracks']}] {item['name']}"
            item_by = f"{config.get('metadata_separator').join([artist['name'] for artist in item['artists']])}"
            item_thumbnail_url = item['images'][-1]["url"] if len(item['images']) > 0 else ""
        elif item_type == "playlist":
            tracks_total = item.get('tracks', {}).get('total')
            if isinstance(tracks_total, int):
                rel_year = spotify_get_playlist_updated_year(
                    token,
                    headers,
                    auth_source,
                    item['id'],
                    tracks_total,
                    "item by id",
                )
                item_name = f"[Y:{rel_year or '????'}] [T:{tracks_total}] {item['name']}"
            else:
                item_name = f"{item['name']}"
            item_by = f"{item['owner']['display_name']}"
            item_thumbnail_url = item['images'][-1]["url"] if len(item['images']) > 0 else ""
        elif item_type == "artist":
            item_name = item['name']
            if f"{'/'.join(item.get('genres', []))}" != "":
                item_name = item['name'] + f"  |  GENERES: {'/'.join(item['genres'])}"
            item_by = f"{item['name']}"
            item_thumbnail_url = item['images'][-1]["url"] if len(item['images']) > 0 else ""
        elif item_type in ["show", "podcast"]:
            item_name = f"{config.get('explicit_label') if item.get('explicit') else ''} {item['name']}"
            item_by = f"{item['publisher']}"
            item_thumbnail_url = item['images'][-1]["url"] if len(item['images']) > 0 else ""
            item_type = "podcast"
        elif item_type in ["episode", "podcast_episode"]:
            item_name = f"{config.get('explicit_label') if item.get('explicit') else ''} {item['name']}"
            item_by = ""
            item_thumbnail_url = item['images'][-1]["url"] if len(item['images']) > 0 else ""
            item_type = "podcast_episode"
        elif item_type == "audiobook":
            item_name = f"{config.get('explicit_label') if item.get('explicit') else ''} {item['name']}"
            item_by = f"{item['publisher']}"
            item_thumbnail_url = item['images'][-1]["url"] if len(item['images']) > 0 else ""
        else:
            logger.error(f"Unknown item type: {item_type}")
            return []

        return [{
            'item_id': item['id'],
            'item_name': item_name,
            'item_by': item_by,
            'item_type': item_type,
            'item_service': "spotify",
            'item_url': item['external_urls']['spotify'],
            'item_thumbnail_url': item_thumbnail_url
        }]

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error fetching item {item_type}/{item_id}: {e}")
        return []
    except (KeyError, ValueError, AttributeError) as e:
        logger.error(f"Data parsing error for item {item_type}/{item_id}: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching item {item_type}/{item_id}: {e}")
        return []


def spotify_extract_metadata_from_cached_track(track_obj, playlist_number=None):
    """
    Extract metadata from a cached track object (from playlist/liked songs response).
    This avoids making individual API calls for tracks we already have data for.
    
    Note: This provides basic metadata. Full metadata (with audio features, credits)
    requires additional API calls via spotify_get_track_metadata.
    """
    from ..utils import conv_list_format
    
    info = {}
    
    # Basic track info (already in track object)
    info['title'] = track_obj.get('name', '')
    info['item_id'] = track_obj.get('id', '')
    info['item_url'] = track_obj.get('external_urls', {}).get('spotify', '')
    info['length'] = str(track_obj.get('duration_ms', ''))
    info['explicit'] = track_obj.get('explicit', False)
    info['isrc'] = track_obj.get('external_ids', {}).get('isrc', '')
    info['is_playable'] = track_obj.get('is_playable', True)
    
    # Artists
    artists = [artist.get('name') for artist in track_obj.get('artists', [])]
    info['artists'] = conv_list_format(artists)
    
    # Album info (already in track object)
    album = track_obj.get('album', {})
    info['album_name'] = album.get('name', '')
    info['album_type'] = album.get('album_type', '')
    info['total_tracks'] = album.get('total_tracks', 0)
    info['release_year'] = album.get('release_date', '').split('-')[0] if album.get('release_date') else ''
    
    # Album artists
    album_artists = album.get('artists', [])
    if album_artists:
        info['album_artists'] = album_artists[0].get('name', '')
    
    # Image
    images = album.get('images', [])
    info['image_url'] = images[0].get('url', '') if images else ''
    
    # Track number - use playlist position if provided, otherwise use album track number
    info['track_number'] = playlist_number if playlist_number else track_obj.get('track_number')
    info['disc_number'] = track_obj.get('disc_number', 1)
    
    # These require additional API calls, so we set defaults
    info['genre'] = ''
    info['label'] = ''
    info['copyright'] = ''
    info['total_discs'] = 1
    
    logger.debug(f"Extracted cached metadata for track: {info['title']} by {info['artists']}")
    return info


def spotify_get_track_metadata(token, item_id, _retry=False, album_lock=None):
    headers = {}
    try:
        headers, auth_source = _spotify_get_public_api_headers(token, "track metadata")
    except (RuntimeError, OSError) as e:
        if _retry:
            logger.error(f"Failed to get token after retry for track {item_id}: {e}")
            raise
        logger.warning(f"Token retrieval failed for track metadata, attempting session reconnect: {e}")
        # Re-initialize the session
        parsing_index = config.get('active_account_number')
        spotify_re_init_session(account_pool[parsing_index])
        # Get the new token
        new_token = account_pool[parsing_index]['login']['session']
        # Retry with the new token
        return spotify_get_track_metadata(new_token, item_id, _retry=True)

    if auth_source == "session":
        market_param = "from_token"
        track_data = _spotify_make_call_with_headers(
            f'{BASE_URL}/tracks?ids={item_id}&market={market_param}',
            token,
            "track metadata",
            headers,
            auth_source,
        )
    else:
        market_param = "none"
        track_data = _spotify_make_call_with_headers(
            f'{BASE_URL}/tracks?ids={item_id}',
            token,
            "track metadata",
            headers,
            auth_source,
        )
        if not track_data or track_data.get('tracks', [{}])[0].get('is_playable') is False:
            market_param = "EU"
            track_data = _spotify_make_call_with_headers(
                f'{BASE_URL}/tracks?ids={item_id}&market={market_param}',
                token,
                "track metadata",
                headers,
                auth_source,
            )
            if not track_data or track_data.get('tracks', [{}])[0].get('is_playable') is False:
                logger.info("Spotify track metadata EU unavailable for %s; falling back to US", item_id)
                market_param = "US"
                track_data = _spotify_make_call_with_headers(
                    f'{BASE_URL}/tracks?ids={item_id}&market={market_param}',
                    token,
                    "track metadata",
                    headers,
                    auth_source,
                )
    if not track_data:
        logger.error(
            "Spotify track metadata request failed (%s, market=%s) for %s",
            auth_source,
            market_param,
            item_id,
        )
        return {}
    album_data = _spotify_make_call_with_headers(
        f"{BASE_URL}/albums/{track_data.get('tracks', [])[0].get('album', {}).get('id')}",
        token,
        "track metadata",
        headers,
        auth_source,
    )
    artist_data = _spotify_make_call_with_headers(
        f"{BASE_URL}/artists/{track_data.get('tracks', [])[0].get('artists', [])[0].get('id')}",
        token,
        "track metadata",
        headers,
        auth_source,
    )
    
    # Lock ONLY the album track ID fetch (critical section for duplicate prevention)
    if album_lock:
        album_lock.acquire()
    try:
        album_track_ids = spotify_get_album_track_ids(token, track_data.get('tracks', [])[0].get('album', {}).get('id'))
    finally:
        if album_lock:
            album_lock.release()
    try:
        track_audio_data = _spotify_make_call_with_headers(
            f'{BASE_URL}/audio-features/{item_id}',
            token,
            "track metadata",
            headers,
            auth_source,
        )
    except Exception:
        track_audio_data = ''
    session_headers = None
    try:
        session_headers = {"Authorization": f"Bearer {token.tokens().get('user-read-email')}"}
    except (RuntimeError, OSError) as e:
        logger.warning("Spotify credits request missing session token: %s", e)

    try:
        credits_headers = session_headers or headers
        credits_data = make_call(
            f'https://spclient.wg.spotify.com/track-credits-view/v0/experimental/{item_id}/credits',
            headers=credits_headers,
        )
    except Exception:
        credits_data = ''

    # Artists
    artists = []
    for data in track_data.get('tracks', [{}])[0].get('artists', []):
        artists.append(data.get('name'))
    artists = conv_list_format(artists)

    # Track Number
    track_number = None
    if album_track_ids:
        for i, track_id in enumerate(album_track_ids):
            if track_id == str(item_id):
                track_number = i + 1
                logger.info(f"Track {item_id} found at position {track_number} in album track list")
                break
    if not track_number:
        track_number = track_data.get('tracks', [{}])[0].get('track_number')
        logger.warning(f"Track {item_id} not found in album track list, using API track_number: {track_number}")

    info = {}
    info['artists'] = artists
    info['album_name'] = track_data.get('tracks', [{}])[0].get('album', {}).get("name", '')
    info['album_type'] = album_data.get('album_type')
    info['album_artists'] = album_data.get('artists', [{}])[0].get('name')
    info['title'] = track_data.get('tracks', [{}])[0].get('name')

    try:
        info['image_url'] = track_data.get('tracks', [{}])[0].get('album', {}).get('images', [{}])[0].get('url')
    except IndexError:
        info['image_url'] = ''
        logger.info('Invalid thumbnail')

    info['release_year'] = track_data.get('tracks', [{}])[0].get('album', {}).get('release_date').split("-")[0]
    #info['track_number'] = track_data.get('tracks', [{}])[0].get('track_number')
    info['track_number'] = track_number
    info['total_tracks'] = track_data.get('tracks', [{}])[0].get('album', {}).get('total_tracks')
    info['disc_number'] = track_data.get('tracks', [{}])[0].get('disc_number')
    info['total_discs'] = sorted([trk.get('disc_number', 0) for trk in album_data.get('tracks', {}).get('items', [])])[-1] if 'tracks' in album_data else 1
    info['genre'] = conv_list_format(artist_data.get('genres', []))
    info['label'] = album_data.get('label')
    info['copyright'] = conv_list_format([holder.get('text') for holder in album_data.get('copyrights', [])])
    info['explicit'] = track_data.get('tracks', [{}])[0].get('explicit', False)
    info['isrc'] = track_data.get('tracks', [{}])[0].get('external_ids', {}).get('isrc')
    info['length'] = str(track_data.get('tracks', [{}])[0].get('duration_ms'))
    info['item_url'] = track_data.get('tracks', [{}])[0].get('external_urls', {}).get('spotify')
    #info['popularity'] = track_data.get('tracks', [{}])[0].get('popularity')
    info['item_id'] = track_data.get('tracks', [{}])[0].get('id')
    track_entry = track_data.get('tracks', [{}])[0]
    if 'is_playable' in track_entry and track_entry.get('is_playable') is False:
        info['is_playable'] = False
    else:
        info['is_playable'] = True

    if credits_data:
        credits = {}
        for credit_block in credits_data.get('roleCredits', []):
            role_title = credit_block.get('roleTitle').lower()
            credits[role_title] = [
                artist.get('name') for artist in credit_block.get('artists', [])
            ]
        info['performers'] = conv_list_format([item for item in credits.get('performers', []) if isinstance(item, str)])
        info['producers'] = conv_list_format([item for item in credits.get('producers', []) if isinstance(item, str)])
        info['writers'] = conv_list_format([item for item in credits.get('writers', []) if isinstance(item, str)])

    if track_audio_data:
        key_mapping = {
            0: "C",
            1: "C♯/D♭",
            2: "D",
            3: "D♯/E♭",
            4: "E",
            5: "F",
            6: "F♯/G♭",
            7: "G",
            8: "G♯/A♭",
            9: "A",
            10: "A♯/B♭",
            11: "B"
        }
        info['bpm'] = str(track_audio_data.get('tempo'))
        info['key'] = str(key_mapping.get(track_audio_data.get('key'), ''))
        info['time_signature'] = track_audio_data.get('time_signature')
        info['acousticness'] = track_audio_data.get('acousticness')
        info['danceability'] = track_audio_data.get('danceability')
        info['energy'] = track_audio_data.get('energy')
        info['instrumentalness'] = track_audio_data.get('instrumentalness')
        info['liveness'] = track_audio_data.get('liveness')
        info['loudness'] = track_audio_data.get('loudness')
        info['speechiness'] = track_audio_data.get('speechiness')
        info['valence'] = track_audio_data.get('valence')
    return info


def spotify_get_podcast_episode_metadata(token, episode_id, _retry=False):
    logger.info(f"Get episode info for episode by id '{episode_id}'")
    headers = {}
    try:
        headers['Authorization'] = f"Bearer {token.tokens().get('user-read-email')}"
    except (RuntimeError, OSError) as e:
        if _retry:
            logger.error(f"Failed to get token after retry for episode {episode_id}: {e}")
            raise
        logger.warning(f"Token retrieval failed for episode metadata, attempting session reconnect: {e}")
        # Re-initialize the session
        parsing_index = config.get('active_account_number')
        spotify_re_init_session(account_pool[parsing_index])
        # Get the new token
        new_token = account_pool[parsing_index]['login']['session']
        # Retry with the new token
        return spotify_get_podcast_episode_metadata(new_token, episode_id, _retry=True)

    episode_data = make_call(f"{BASE_URL}/episodes/{episode_id}", headers=headers)
    show_episode_ids = spotify_get_podcast_episode_ids(token, episode_data.get('show', {}).get('id'))
    # I believe audiobook ids start with a 7 but to verify you can use https://api.spotify.com/v1/audiobooks/{id}
    # the endpoint could possibly be used to mark audiobooks in genre but it doesn't really provide any additional
    # metadata compared to show_data beyond abridged and unabridged.

    track_number = ''
    for index, episode in enumerate(show_episode_ids):
        if episode == episode_id:
            track_number = index + 1
            break

    copyrights = []
    for copyright in episode_data.get('show', {}).get('copyrights', []):
        text = copyright.get('text')
        copyrights.append(text)

    info = {}
    info['album_name'] = episode_data.get('show', {}).get('name')
    info['title'] = episode_data.get('name')
    info['image_url'] = episode_data.get('images', [{}])[0].get('url')
    info['release_year'] = episode_data.get('release_date').split('-')[0]
    info['track_number'] = track_number
    # Not accurate
    #info['total_tracks'] = episode_data.get('show', {}).get('total_episodes', 0)
    info['total_tracks'] = len(show_episode_ids)
    info['artists'] = conv_list_format([episode_data.get('show', {}).get('publisher')])
    info['album_artists'] = conv_list_format([episode_data.get('show', {}).get('publisher')])
    info['language'] = conv_list_format(episode_data.get('languages', []))
    description = episode_data.get('description')
    info['description'] = str(description if description else episode_data.get('show', {}).get('description', ""))
    info['copyright'] = conv_list_format(copyrights)
    info['length'] = str(episode_data.get('duration_ms'))
    info['explicit'] = episode_data.get('explicit')
    info['is_playable'] = episode_data.get('is_playable')
    info['item_url'] = episode_data.get('external_urls', {}).get('spotify')
    info['item_id'] = episode_data.get('id')

    return info


def spotify_get_podcast_episode_ids(token, show_id, _retry=False):
    logger.info(f"Getting show episodes: {show_id}'")
    episodes = []
    offset = 0
    limit = 50

    while True:
        url = f'{BASE_URL}/shows/{show_id}/episodes?offset={offset}&limit={limit}'
        headers = {}
        try:
            headers['Authorization'] = f"Bearer {token.tokens().get('user-read-email')}"
        except (RuntimeError, OSError) as e:
            if _retry:
                logger.error(f"Failed to get token after retry for podcast episode IDs {show_id}: {e}")
                raise
            logger.warning(f"Token retrieval failed for podcast episode IDs, attempting session reconnect: {e}")
            # Re-initialize the session
            parsing_index = config.get('active_account_number')
            spotify_re_init_session(account_pool[parsing_index])
            # Get the new token
            new_token = account_pool[parsing_index]['login']['session']
            # Retry with the new token
            return spotify_get_podcast_episode_ids(new_token, show_id, _retry=True)

        resp = make_call(url, headers=headers)

        offset += limit
        episodes.extend(resp['items'])

        if resp['total'] <= offset:
            break

    item_ids = []
    for episode in episodes:
        if episode:
            item_ids.append(episode['id'])
    return item_ids
