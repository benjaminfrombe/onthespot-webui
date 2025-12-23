import os
from datetime import timedelta
# Required for librespot-python
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'
import argparse
import json
import shutil
import subprocess
import sys
import threading
import time
import traceback
from flask import Flask, jsonify, render_template, redirect, request, send_file, url_for, flash, Response, session
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_socketio import SocketIO, emit
from .accounts import FillAccountPool, get_account_token
from .api.apple_music import apple_music_get_track_metadata, apple_music_add_account, apple_music_login_user
from .api.bandcamp import bandcamp_get_track_metadata, bandcamp_login_user
from .api.deezer import deezer_get_track_metadata, deezer_add_account, deezer_login_user
from .api.qobuz import qobuz_get_track_metadata, qobuz_add_account, qobuz_login_user
from .api.soundcloud import soundcloud_get_track_metadata, soundcloud_add_account, soundcloud_login_user
from .api.spotify import MirrorSpotifyPlayback, spotify_new_session, spotify_get_track_metadata, spotify_get_podcast_episode_metadata, spotify_login_user
from .api.tidal import tidal_get_track_metadata, tidal_login_user
from .api.youtube_music import youtube_music_get_track_metadata, youtube_music_add_account, youtube_music_login_user
from .api.crunchyroll import crunchyroll_get_episode_metadata, crunchyroll_add_account, crunchyroll_login_user
from .api.generic import generic_add_account, generic_login_user
try:
    from .api.plex import plex_api
    PLEX_AVAILABLE = True
except Exception as e:
    print(f"WARNING: Failed to import Plex API: {e}")
    PLEX_AVAILABLE = False
    plex_api = None
from .downloader import DownloadWorker, RetryWorker
from .otsconfig import cache_dir, config_dir, config
from .parse_item import parsingworker, parse_url
from .runtimedata import get_logger, account_pool, pending, download_queue, download_queue_lock, pending_lock, parsing, parsing_lock, register_worker, set_worker_restart_callback, set_watchdog_restart_callback, system_notifications, system_notifications_lock, album_download_locks, album_download_locks_lock
from . import runtimedata
from .search import get_search_results
from .utils import format_bytes

logger = get_logger("web")
_restart_lock = threading.Lock()
_restart_in_progress = False


def _cache_download_queue_to_disk():
    """Persist the current download queue so the new process can resume it."""
    import json
    cached_path = os.path.join(cache_dir(), 'cached_download_queue.json')
    # Never block forever here: hard restart must remain possible even if the queue lock is stuck.
    lock_acquired = download_queue_lock.acquire(timeout=3)
    if not lock_acquired:
        raise TimeoutError("Timed out acquiring download_queue_lock while caching queue")
    try:
        # Save essential metadata to preserve playlist context
        items_to_cache = []
        for item in download_queue.values():
            items_to_cache.append({
                'item_url': item.get('item_url'),
                'item_service': item.get('item_service'),
                'item_type': item.get('item_type'),
                'item_id': item.get('item_id'),
                'parent_category': item.get('parent_category'),
                'playlist_name': item.get('playlist_name'),
                'playlist_by': item.get('playlist_by'),
                'playlist_number': item.get('playlist_number'),
                'playlist_total': item.get('playlist_total')
            })
        with open(cached_path, 'w') as file:
            json.dump(items_to_cache, file, indent=2)
    finally:
        download_queue_lock.release()
    return cached_path


def trigger_hard_restart(reason):
    """
    Spawn a fresh web process and exit.

    Mirrors the manual Restart button behaviour so automatic restarts
    triggered by repeated download failures get the same recovery path.
    """
    global _restart_in_progress
    with _restart_lock:
        if _restart_in_progress:
            logger.warning("Hard restart already in progress, skipping duplicate trigger")
            return
        _restart_in_progress = True

    logger.error(f"🔄 HARD RESTART INITIATED: {reason}")
    logger.error("="*80)

    # Notify web clients so the UI can show a reconnecting state immediately.
    try:
        if 'socketio' in globals():
            socketio.emit('hard_restart', {'reason': reason}, namespace='/')
            # Give the event a brief chance to flush before process exit.
            time.sleep(0.15)
    except Exception as e:
        logger.error(f"Failed to emit hard_restart websocket event: {e}")

    try:
        cached_path = _cache_download_queue_to_disk()
        logger.info(f"Cached download queue to {cached_path}")
    except Exception as e:
        # Caching is best-effort; do not let it prevent restart.
        logger.error(f"Failed to cache download queue before restart: {e}")

    try:
        subprocess.Popen([sys.executable, '-m', 'onthespot.web'] + (sys.argv[1:]))
    except Exception as e:
        logger.error(f"Failed to spawn replacement process: {e}")
        with _restart_lock:
            _restart_in_progress = False
        return

    os._exit(0)
os.environ['FLASK_ENV'] = 'production'
web_resources = os.path.join(config.app_root, 'resources', 'web')
app = Flask('OnTheSpot', template_folder=web_resources, static_folder=web_resources)
app.secret_key = config.get("web_secret_key")
REMEMBER_DURATION = timedelta(days=90)
app.config['REMEMBER_COOKIE_DURATION'] = REMEMBER_DURATION
app.config['PERMANENT_SESSION_LIFETIME'] = REMEMBER_DURATION
login_manager = LoginManager()
login_manager.init_app(app)

# Initialize SocketIO for real-time updates
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')


class QueueWorker(threading.Thread):
    def __init__(self):
        super().__init__()
        self.is_running = True

    def run(self):
        logger.info("QueueWorker started")
        while self.is_running:
            try:
                # Wait if batch parse is in progress to allow complete queue population
                with runtimedata.batch_parse_lock:
                    is_parsing = runtimedata.batch_parse_in_progress
                
                if is_parsing:
                    with pending_lock:
                        pending_count = len(pending)
                    logger.info(f"QueueWorker waiting: batch parse in progress, {pending_count} items in pending queue")
                    time.sleep(0.5)
                    continue
                
                if pending:
                    # Set flag to prevent downloads during batch processing (keeps downloads paused until ALL pending items processed)
                    runtimedata.set_batch_queue_processing_flag(True)
                    
                    # Process pending items in batches to avoid long blocking for huge playlists
                    BATCH_SIZE = 50  # Process 50 items at a time
                    with pending_lock:
                        # Get first BATCH_SIZE items
                        all_items = list(pending.items())
                        # Sort by playlist number to maintain order
                        all_items.sort(key=lambda x: int(x[1].get('playlist_number', 0) or 0))
                        
                        items_to_process = all_items[:BATCH_SIZE]
                        # Remove processed items from pending
                        for local_id, _ in items_to_process:
                            del pending[local_id]
                        
                        remaining = len(pending)
                    
                    logger.info(f"QueueWorker processing {len(items_to_process)} items from pending queue ({remaining} remaining)")
                    
                    for local_id, item in items_to_process:
                        try:
                            logger.debug(f"QueueWorker processing item: {local_id} (service: {item['item_service']}, type: {item['item_type']})")
                            token = get_account_token(item['item_service'])
                            
                            # For Spotify albums, use album lock to serialize track_number lookups
                            album_lock_ctx = None
                            if item['item_service'] == "spotify" and item.get('parent_category') == 'album' and item.get('parent_id'):
                                album_key = f"{item['item_service']}:{item.get('parent_id')}"
                                with album_download_locks_lock:
                                    if album_key not in album_download_locks:
                                        album_download_locks[album_key] = threading.Lock()
                                    album_lock_ctx = album_download_locks[album_key]
                            
                            if album_lock_ctx:
                                item_metadata = globals()[f"{item['item_service']}_get_{item['item_type']}_metadata"](token, item['item_id'], album_lock=album_lock_ctx)
                            else:
                                item_metadata = globals()[f"{item['item_service']}_get_{item['item_type']}_metadata"](token, item['item_id'])
                            if item_metadata:
                                # Preserve playlist context from pending item
                                playlist_total = item.get('playlist_total')
                                logger.debug(f"QueueWorker: Processing {local_id}, pending item keys: {list(item.keys())}, playlist_total={playlist_total}, playlist_number={item.get('playlist_number')}")
                                
                                with download_queue_lock:
                                    download_queue[local_id] = {
                                        'local_id': local_id,
                                        'available': True,
                                        "item_service": item["item_service"],
                                        "item_type": item["item_type"],
                                        'item_id': item['item_id'],
                                        'item_status': 'Waiting',
                                        "file_path": None,
                                        "item_name": item_metadata["title"],
                                        "item_by": item_metadata["artists"],
                                        'parent_category': item['parent_category'],
                                        'playlist_name': item.get('playlist_name'),
                                        'playlist_by': item.get('playlist_by'),
                                        'playlist_number': item.get('playlist_number'),
                                        'playlist_total': playlist_total,
                                        'track_number': item_metadata.get('track_number'),
                                        'album_name': item_metadata.get('album_name'),
                                        'item_album_name': item_metadata.get('album_name'),
                                        'item_thumbnail': item_metadata["image_url"],
                                        'item_url': item_metadata["item_url"],
                                        'progress': 0,
                                        'last_update_time': time.time()
                                    }
                        except Exception as e:
                            logger.error(f"Error processing {local_id}: {str(e)}\nTraceback: {traceback.format_exc()}")
                            with pending_lock:
                                pending[local_id] = item
                    
                    logger.info(f"QueueWorker finished processing batch, {len(download_queue)} items now in download queue")
                    
                    # Only clear flag when ALL pending items are processed
                    if remaining == 0:
                        runtimedata.set_batch_queue_processing_flag(False)
                        logger.info("All pending items processed, downloads can now start")
                else:
                    time.sleep(0.2)
            except Exception as e:
                logger.error(f"Unknown Exception in QueueWorker: {str(e)}\nTraceback: {traceback.format_exc()}")

    def stop(self):
        logger.info('Stopping Queue Worker')
        self.is_running = False
        self.join(timeout=5)


class WatchdogWorker(threading.Thread):
    """
    Worker that monitors for stuck batch operation flags and stuck downloads.
    Checks every 30 seconds for:
    1. Batch operation flags that have been stuck longer than timeout
    2. Downloads stuck in "Downloading" state without progress updates
    """
    def __init__(self):
        super().__init__()
        self.is_running = True

    def run(self):
        logger.info('WatchdogWorker started')
        stuck_timeout = 30  # Consider a download stuck after 30 seconds without updates
        
        while self.is_running:
            try:
                time.sleep(30)  # Check every 30 seconds
                
                # Check and clear stuck flags
                from .runtimedata import check_and_clear_stuck_flags
                if check_and_clear_stuck_flags():
                    logger.warning("Watchdog cleared stuck flags - workers should resume")
                
                # Check for stuck downloads
                stuck_detected = False
                lock_acquired = download_queue_lock.acquire(timeout=2)
                if not lock_acquired:
                    logger.error("⚠️ WATCHDOG ALERT: download_queue_lock could not be acquired (possible deadlock)")
                    stuck_detected = True
                else:
                    try:
                        current_time = time.time()
                        for local_id, item in download_queue.items():
                            # Check if item has been in "Downloading" state for too long without updates
                            if item['item_status'] == 'Downloading':
                                last_update = item.get('last_update_time', 0)
                                if last_update > 0 and current_time - last_update > stuck_timeout:
                                    logger.error(f"⚠️ WATCHDOG ALERT: Download stuck for {int(current_time - last_update)}s without progress: {item.get('item_name', 'Unknown')} (ID: {local_id})")
                                    stuck_detected = True
                                    break
                    finally:
                        download_queue_lock.release()
                
                if stuck_detected:
                    logger.error("🚨 WATCHDOG: Triggering hard restart due to stuck download...")
                    trigger_hard_restart("watchdog detected stuck download")
                    
            except Exception as e:
                logger.error(f"Error in WatchdogWorker: {e}\nTraceback: {traceback.format_exc()}")

    def stop(self):
        logger.info('Stopping Watchdog Worker')
        self.is_running = False
        self.join(timeout=5)


class ParsingCleanupWorker(threading.Thread):
    """
    Worker that periodically cleans up stuck items in the parsing dict to prevent memory leaks.
    Checks every 5 minutes and removes items older than 10 minutes.
    """
    def __init__(self):
        super().__init__()
        self.is_running = True
        self.CLEANUP_INTERVAL = 300  # Check every 5 minutes
        self.MAX_AGE = 600  # Remove items older than 10 minutes

    def run(self):
        logger.info('ParsingCleanupWorker started')
        while self.is_running:
            try:
                time.sleep(self.CLEANUP_INTERVAL)
                
                # Check parsing dict size and clean up old entries
                with parsing_lock:
                    if len(parsing) > 100:  # Only clean if dict is getting large
                        logger.warning(f"Parsing dict has {len(parsing)} items, performing cleanup")
                        # In real implementation, we'd track timestamps. For now, just limit size
                        if len(parsing) > 1000:
                            logger.error(f"Parsing dict has {len(parsing)} items! Clearing to prevent memory leak")
                            parsing.clear()
                        
            except Exception as e:
                logger.error(f"Error in ParsingCleanupWorker: {str(e)}\nTraceback: {traceback.format_exc()}")

    def stop(self):
        logger.info('Stopping ParsingCleanup Worker')
        self.is_running = False
        self.join(timeout=5)


class AutoClearWorker(threading.Thread):
    """
    Worker that automatically clears completed downloads after all items are done.
    Waits 60 seconds after the queue is fully complete before clearing.
    """
    def __init__(self):
        super().__init__()
        self.is_running = True
        self.last_all_done_time = None
        self.CLEAR_DELAY_SECONDS = 60

    def run(self):
        logger.info('AutoClearWorker started')
        while self.is_running:
            try:
                time.sleep(5)  # Check every 5 seconds

                with download_queue_lock:
                    if not download_queue:
                        # Queue is empty, reset timer
                        self.last_all_done_time = None
                        continue

                    # Check if all items are in a "done" state
                    all_done = True
                    all_successful = True
                    for local_id, item in download_queue.items():
                        status = item.get("item_status", "")
                        if status not in ("Downloaded", "Already Exists", "Cancelled", "Unavailable", "Deleted", "Failed"):
                            # Found an item that's still in progress
                            all_done = False
                            break
                        # Check if this item failed (for Plex scan decision)
                        if status in ("Failed", "Cancelled", "Unavailable"):
                            all_successful = False

                    if all_done:
                        # All items are done
                        if self.last_all_done_time is None:
                            # First time we've noticed everything is done
                            self.last_all_done_time = time.time()
                            logger.info(f"All downloads complete. Will auto-clear in {self.CLEAR_DELAY_SECONDS} seconds...")
                            
                            # Trigger Plex library scan if enabled (only if all successful)
                            if config.get('plex_auto_scan', False) and all_successful:
                                try:
                                    from .api.plex import plex_api
                                    logger.info("Triggering Plex library scan after download completion...")
                                    result = plex_api.scan_library()
                                    if result.get('success'):
                                        logger.info("Plex library scan triggered successfully")
                                        # Add notification for web UI
                                        with system_notifications_lock:
                                            system_notifications.append({
                                                'timestamp': time.time(),
                                                'message': 'Added to Plex',
                                                'type': 'success'
                                            })
                                    else:
                                        logger.warning(f"Plex library scan failed: {result.get('error')}")
                                        with system_notifications_lock:
                                            system_notifications.append({
                                                'timestamp': time.time(),
                                                'message': f"Plex library scan failed: {result.get('error')}",
                                                'type': 'error'
                                            })
                                except Exception as e:
                                    logger.error(f"Failed to trigger Plex library scan: {e}")
                                    with system_notifications_lock:
                                        system_notifications.append({
                                            'timestamp': time.time(),
                                            'message': f"Failed to trigger Plex library scan: {str(e)}",
                                            'type': 'error'
                                        })
                        else:
                            # Check if enough time has passed
                            elapsed = time.time() - self.last_all_done_time
                            if elapsed >= self.CLEAR_DELAY_SECONDS:
                                # Time to clear!
                                logger.info("Auto-clearing completed downloads...")
                                keys_to_delete = []
                                for local_id, item in download_queue.items():
                                    if item["item_status"] in ("Downloaded", "Already Exists", "Cancelled", "Unavailable", "Deleted"):
                                        keys_to_delete.append(local_id)

                                for key in keys_to_delete:
                                    del download_queue[key]

                                logger.info(f"Auto-cleared {len(keys_to_delete)} items from download queue")
                                self.last_all_done_time = None
                    else:
                        # Not all items are done, reset timer
                        if self.last_all_done_time is not None:
                            logger.debug("New downloads detected, resetting auto-clear timer")
                        self.last_all_done_time = None

            except Exception as e:
                logger.error(f"Error in AutoClearWorker: {str(e)}\nTraceback: {traceback.format_exc()}")
                time.sleep(5)

    def stop(self):
        logger.info('Stopping AutoClear Worker')
        self.is_running = False
        self.join(timeout=5)


class WebSocketBroadcaster(threading.Thread):
    """Thread that broadcasts download queue updates via WebSocket"""
    def __init__(self):
        super().__init__()
        self.is_running = True
        self.daemon = True

    def run(self):
        logger.info('WebSocketBroadcaster started')
        while self.is_running:
            try:
                time.sleep(0.1)  # Broadcast 10 times per second for real-time updates
                
                with download_queue_lock:
                    queue_data = dict(download_queue)
                
                # Emit queue update to all connected clients
                socketio.emit('queue_update', queue_data, namespace='/')
                
            except Exception as e:
                logger.error(f"Error in WebSocketBroadcaster: {str(e)}")
                time.sleep(1)

    def stop(self):
        logger.info('Stopping WebSocket Broadcaster')
        self.is_running = False


class User(UserMixin):
    def __init__(self, id, is_admin=False, is_plex_user=False):
        self.id = id
        self.is_admin = is_admin
        self.is_plex_user = is_plex_user

@login_manager.user_loader
def load_user(user_id):
    # Load user with their role from session
    is_admin = session.get('is_admin', False)
    is_plex_user = session.get('is_plex_user', False)
    return User(user_id, is_admin=is_admin, is_plex_user=is_plex_user)


login_manager.login_view = "/login"


def admin_required(f):
    """Decorator to require admin access"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('login'))
        if not current_user.is_admin:
            flash('Access denied. Admin privileges required.')
            return redirect(url_for('search'))
        return f(*args, **kwargs)
    return decorated_function


@app.route('/login', methods=['GET', 'POST'])
def login():
    if not config.get('use_webui_login') or not config.get('webui_username'):
        user = User('guest', is_admin=True)  # Guest has admin access
        login_user(user, remember=True, duration=REMEMBER_DURATION)
        session.permanent = True
        session['is_admin'] = True
        session['is_plex_user'] = False
        return redirect(url_for('search'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username == config.get('webui_username') and password == config.get('webui_password'):
            user = User(username, is_admin=True)  # Admin credentials = admin access
            login_user(user, remember=True, duration=REMEMBER_DURATION)
            session.permanent = True
            session['is_admin'] = True
            session['is_plex_user'] = False
            return redirect(url_for('search'))
        flash('Invalid credentials, please try again.')

    # Load config for template
    config_path = os.path.join(config_dir(), 'otsconfig.json')
    with open(config_path, 'r') as config_file:
        config_data = json.load(config_file)
    return render_template('login.html', config=config_data)


@app.route('/api/auth/plex', methods=['POST'])
def auth_plex():
    """Authenticate user with Plex account"""
    if not PLEX_AVAILABLE or not plex_api:
        return jsonify(success=False, error='Plex API not available'), 500

    try:
        data = request.json
        auth_token = data.get('authToken')

        if not auth_token:
            return jsonify(success=False, error='Auth token required'), 400

        # Validate token with Plex.tv and get user info
        import requests
        headers = {
            'Accept': 'application/json',
            'X-Plex-Token': auth_token,
            'X-Plex-Client-Identifier': 'onthespot-music-downloader'
        }

        response = requests.get('https://plex.tv/users/account.json', headers=headers)
        response.raise_for_status()
        plex_user = response.json()['user']

        # Check if Plex login is enabled in config
        if not config.get('use_plex_login', False):
            return jsonify(success=False, error='Plex login is not enabled'), 403

        username = plex_user.get('username', plex_user.get('email', 'plex_user'))
        plex_id = plex_user.get('id')

        # Check if user has access to the Plex server (if enabled)
        is_admin = False
        if config.get('require_plex_server_access', True):
            try:
                server_url = config.get('plex_server_url', 'http://127.0.0.1:32400')
                owner_plex_token = config.get('plex_auth_token')

                if not owner_plex_token or not server_url:
                    logger.warning("Plex server access check enabled but server not configured")
                    return jsonify(success=False, error='Plex server not configured. Contact administrator.'), 403

                # Method 1: Check if user can access the server directly with their token
                has_access = False
                try:
                    test_headers = {
                        'Accept': 'application/json',
                        'X-Plex-Token': auth_token
                    }
                    test_response = requests.get(f"{server_url}/identity",
                                                headers=test_headers,
                                                timeout=5)

                    if test_response.status_code == 200:
                        has_access = True
                        logger.info(f"Plex user {username} can access server directly with their token")

                        # Check if they're the owner by comparing tokens
                        if auth_token == owner_plex_token:
                            is_admin = True
                            logger.info(f"Plex user {username} is the server owner (token match)")
                    else:
                        logger.debug(f"Direct server access test failed: HTTP {test_response.status_code}")
                except Exception as e:
                    logger.debug(f"Direct server access test failed: {str(e)}")

                # Method 2: If direct access failed, check shared users list (for server owners checking who has access)
                if not has_access:
                    try:
                        owner_headers = {
                            'Accept': 'application/json',
                            'X-Plex-Token': owner_plex_token,
                            'X-Plex-Client-Identifier': 'onthespot-music-downloader'
                        }

                        # First verify if owner_plex_token is valid by getting account info
                        owner_response = requests.get('https://plex.tv/users/account.json',
                                                     headers=owner_headers,
                                                     timeout=5)

                        if owner_response.status_code == 200:
                            owner_user = owner_response.json()['user']
                            owner_id = owner_user.get('id')

                            # Check if authenticating user IS the owner
                            if plex_id == owner_id:
                                has_access = True
                                is_admin = True
                                logger.info(f"Plex user {username} is the server owner (ID match)")
                            else:
                                # Get list of shared users
                                users_response = requests.get('https://plex.tv/api/users',
                                                            headers=owner_headers,
                                                            timeout=10)

                                if users_response.status_code == 200:
                                    import xml.etree.ElementTree as ET
                                    root = ET.fromstring(users_response.text)

                                    for user_elem in root.findall('.//User'):
                                        user_id = user_elem.get('id')
                                        if user_id and int(user_id) == plex_id:
                                            has_access = True
                                            logger.info(f"Plex user {username} found in shared users list")
                                            break
                        else:
                            logger.warning(f"Could not verify owner token: HTTP {owner_response.status_code}")

                    except Exception as e:
                        logger.warning(f"Shared users check failed: {str(e)}")

                if not has_access:
                    logger.warning(f"Plex user {username} (ID: {plex_id}) does not have access to the server")
                    logger.info(f"Server URL checked: {server_url}")
                    return jsonify(success=False, error='You do not have access to this Plex server'), 403

            except Exception as e:
                logger.error(f"Error checking Plex server access: {str(e)}")
                logger.exception("Full traceback:")
                return jsonify(success=False, error=f'Server access verification failed: {str(e)}'), 500

        # Create user session
        user = User(username, is_admin=is_admin, is_plex_user=True)
        login_user(user, remember=True, duration=REMEMBER_DURATION)
        session.permanent = True
        session['is_admin'] = is_admin
        session['is_plex_user'] = True

        logger.info(f"User logged in via Plex: {username} (admin: {is_admin})")
        return jsonify(success=True, user={'username': username, 'email': plex_user.get('email'), 'is_admin': is_admin})

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to validate Plex token: {str(e)}")
        return jsonify(success=False, error='Invalid Plex token'), 401
    except Exception as e:
        logger.error(f"Error during Plex authentication: {str(e)}")
        return jsonify(success=False, error=str(e)), 500


@app.route('/api/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route('/icons/<path:filename>')
def serve_icons(filename):
    icon_path = os.path.join(config.app_root, 'resources', 'icons', filename)
    return send_file(icon_path)


@app.route('/')
@login_required
def index():
    return redirect(url_for('search'))


@app.route('/search')
@login_required
def search():
    config_path = os.path.join(config_dir(), 'otsconfig.json')
    with open(config_path, 'r') as config_file:
        config_data = json.load(config_file)
    return render_template('search.html', config=config_data, user=current_user)


@app.route('/download_queue')
@login_required
def download_queue_page():
    config_path = os.path.join(config_dir(), 'otsconfig.json')
    with open(config_path, 'r') as config_file:
        config_data = json.load(config_file)
    if socketio.async_mode == "threading":
        socketio_transports = ["polling"]
    else:
        socketio_transports = ["websocket", "polling"]
    return render_template('download_queue.html', config=config_data, user=current_user, socketio_transports=socketio_transports)


@app.route('/settings')
@admin_required
def settings():
    config_path = os.path.join(config_dir(), 'otsconfig.json')
    with open(config_path, 'r') as config_file:
        config_data = json.load(config_file)
    return render_template('settings.html', config=config_data, account_pool=account_pool)


@app.route('/about')
@login_required
def about():
    return render_template('about.html', version=config.get("version"), statistics=f"{config.get('total_downloaded_items')} / {format_bytes(config.get('total_downloaded_data'))}", user=current_user)


@app.route('/api/search_results')
@login_required
def search_results():
    query = request.args.get('q')

    # Read filter states from URL parameters, default to only albums
    content_types = []
    if request.args.get('tracks') == 'true':
        content_types.append('track')
    if request.args.get('playlists') == 'true':
        content_types.append('playlist')
    if request.args.get('albums') == 'true':
        content_types.append('album')
    if request.args.get('artists') == 'true':
        content_types.append('artist')
    if request.args.get('podcasts') == 'true':
        content_types.append('show')
    if request.args.get('episodes') == 'true':
        content_types.append('episode')
    if request.args.get('audiobooks') == 'true':
        content_types.append('audiobook')

    # If no filters specified, default to albums only
    if not content_types:
        content_types = ['album']

    results = get_search_results(query, content_types=content_types)

    # Handle special return values
    if results is True:
        # URL was parsed and added to queue
        return jsonify({
            'success': True,
            'message': 'Item is being parsed and will be added to the download queue shortly.',
            'results': []
        })
    elif results is False:
        return jsonify({
            'success': False,
            'message': 'Invalid item, please check your query or account settings.',
            'results': []
        })
    elif results is None:
        return jsonify({
            'success': False,
            'message': 'You need to login to at least one account to use this feature.',
            'results': []
        })
    elif isinstance(results, dict) and results.get('error'):
        retry_after = results.get('retry_after')
        message = results.get('error', 'Search failed.')
        if retry_after:
            message = f"{message} Retry after {retry_after}s."
        return jsonify({
            'success': False,
            'message': message,
            'results': []
        })
    elif isinstance(results, list):
        # Return search results array (including Spotify ID lookups)
        return jsonify({
            'success': True,
            'results': results
        })
    else:
        # Unexpected return type
        return jsonify({
            'success': False,
            'message': 'Unexpected error occurred.',
            'results': []
        })


@app.route('/api/clear_items', methods=['POST'])
@login_required
def clear_items():
    keys_to_delete = []
    for local_id, item in download_queue.items():
        if item["item_status"] in ("Downloaded", "Already Exists", "Cancelled", "Unavailable", "Deleted"):
            keys_to_delete.append(local_id)
    for key in keys_to_delete:
        del download_queue[key]
    return jsonify(success=True)


@app.route('/api/cancel_items', methods=['POST'])
@login_required
def cancel_items():
    """Cancel all pending and waiting items, and mark downloading items for cancellation."""
    # Clear parsing queue
    with parsing_lock:
        parsing.clear()
    
    # Clear pending queue
    with pending_lock:
        pending.clear()
    
    # Cancel all items in download queue that are waiting or downloading
    with download_queue_lock:
        for local_id, item in download_queue.items():
            if item["item_status"] in ("Waiting", "Downloading"):
                download_queue[local_id]['item_status'] = 'Cancelled'
    
    return jsonify(success=True)


@app.route('/api/retry_items', methods=['POST'])
@login_required
def retry_items():
    # First, check if there are any failed downloads
    has_failed_downloads = False
    with download_queue_lock:
        for local_id, item in download_queue.items():
            if item["item_status"] in ("Failed", "Cancelled"):
                has_failed_downloads = True
                break

    # If there are failed downloads, force reconnect all Spotify accounts
    if has_failed_downloads:
        logger.info("Found failed downloads - forcing Spotify account reconnection before retry")
        from .api.spotify import spotify_re_init_session

        reconnected_count = 0
        for account_idx, account in enumerate(account_pool):
            if account.get('service') == 'spotify' and account.get('login', {}).get('session'):
                try:
                    logger.info(f"Reconnecting Spotify account {account_idx}: {account.get('username', 'unknown')}")
                    spotify_re_init_session(account)
                    account['last_session_time'] = time.time()
                    reconnected_count += 1
                except Exception as e:
                    logger.error(f"Failed to reconnect Spotify account {account_idx}: {e}")

        if reconnected_count > 0:
            logger.info(f"Successfully reconnected {reconnected_count} Spotify account(s) - now retrying failed downloads")

    # Now retry the failed/cancelled downloads
    with download_queue_lock:
        for local_id, item in download_queue.items():
            if item["item_status"] in ("Failed", "Cancelled"):
                download_queue[local_id]['item_status'] = 'Waiting'
                download_queue[local_id]['available'] = True
    return jsonify(success=True)


@app.route('/api/restart', methods=['POST'])
@login_required
def restart():
    trigger_hard_restart("manual restart via /api/restart")
    return jsonify(success=True)


@app.route('/api/download_queue')
@login_required
def get_items():
    with download_queue_lock:
        return Response(json.dumps(download_queue, sort_keys=True), mimetype='application/json')

@app.route('/api/cancel/<path:local_id>', methods=['POST'])
@login_required
def cancel_item(local_id):
    with download_queue_lock:
        if local_id in download_queue:
            download_queue[local_id]['item_status'] = 'Cancelled'
    return jsonify(success=True)


@app.route('/api/retry/<path:local_id>', methods=['POST'])
@login_required
def retry_item(local_id):
    with download_queue_lock:
        if local_id in download_queue:
            download_queue[local_id]['item_status'] = 'Waiting'
            download_queue[local_id]['available'] = True
    return jsonify(success=True)


@app.route('/api/download/<path:local_id>')
@login_required
def download_media(local_id):
    if local_id != 'logs':
        return send_file(download_queue[local_id]['file_path'], as_attachment=True)
    else:
        return send_file(os.path.join(cache_dir(), "logs", config.session_uuid, "onthespot.log"), as_attachment=True)

@app.route('/api/parse_url/<path:url>', methods=['POST'])
@login_required
def parse_download(url):
    parse_url(url)
    return jsonify(success=True)

@app.route('/api/notifications', methods=['GET'])
@login_required
def get_notifications():
    """Fetch and clear pending notifications"""
    with system_notifications_lock:
        notifications = list(system_notifications)
        system_notifications.clear()
    return jsonify(notifications=notifications)


@app.route('/api/delete/<path:local_id>', methods=['DELETE'])
@login_required
def delete_media(local_id):
    os.remove(download_queue[local_id]['file_path'])
    download_queue[local_id]['item_status'] = 'Deleted'
    return jsonify(success=True)


@app.route('/api/update_settings', methods=['POST'])
@login_required
def update_settings():
    data = request.json
    for key, value in data.items():
        if isinstance(value, str) and value.isdigit():
            value = int(value)
        config.set(key, value)
        config.save()
    return jsonify(success=True)


@app.route('/api/add_account', methods=['POST'])
@login_required
def add_account():
    account = request.get_json()
    if account['service'] == 'apple_music':
        apple_music_add_account(account['password'])
    elif account['service'] == "bandcamp":
        bandcamp_add_account()
    elif account['service'] == "deezer":
        deezer_add_account(account['password'])
    elif account['service'] == "qobuz":
        qobuz_add_account(account['email'], account['password'])
    elif account['service'] == "soundcloud":
        soundcloud_add_account(account['password'])
    elif account['service'] == "youtube_music":
        youtube_music_add_account()
    elif account['service'] == "crunchyroll":
        crunchyroll_add_account(account['email'], account['password'])
    elif account['service'] == "generic":
        generic_add_account()
    config.set('active_account_number', config.get('active_account_number') + 1)
    config.save()
    return jsonify(success=True)


@app.route('/api/remove_account/<path:uuid>', methods=['DELETE'])
@login_required
def remove_account(uuid):
    accounts = config.get('accounts').copy()
    for i, account in enumerate(accounts):
        if account['uuid'] == uuid:
            account_number = i
    del account_pool[account_number]
    del accounts[account_number]
    config.set('accounts', accounts)
    config.set('active_account_number', 0)
    config.save()
    return jsonify(success=True)


@app.route('/api/clear_cache', methods=['DELETE'])
@login_required
def clear_cache():
    shutil.rmtree(os.path.join(cache_dir(), "reqcache"))
    shutil.rmtree(os.path.join(cache_dir(), "logs"))
    # Clear in-memory album track IDs cache
    from .api.spotify import clear_album_track_ids_cache
    clear_album_track_ids_cache()
    return jsonify(success=True)


# Plex API routes
@app.route('/plex_playlists')
@login_required
def plex_playlists():
    """Display the playlist import page"""
    logger.info("=== Plex playlists route accessed ===")
    if not PLEX_AVAILABLE:
        return jsonify(success=False, error="Plex API not available"), 500
    logger.debug("=== Loading Plex playlists page ===")
    config_path = os.path.join(config_dir(), 'otsconfig.json')
    with open(config_path, 'r') as config_file:
        config_data = json.load(config_file)

    # Get m3u directory from config
    m3u_formatter = config_data.get('m3u_path_formatter', './m3u/')
    logger.debug(f"M3U formatter from config: {m3u_formatter}")

    # Remove any trailing / and filename patterns
    m3u_directory = m3u_formatter.rsplit(os.path.sep, 1)[0] if os.path.sep in m3u_formatter else m3u_formatter.rsplit('/', 1)[0] if '/' in m3u_formatter else 'M3U'
    logger.debug(f"M3U directory (relative): {m3u_directory}")

    # Expand the path to absolute path - always join with audio_download_path if it's a relative path
    if not os.path.isabs(m3u_directory):
        m3u_directory = os.path.join(config_data.get('audio_download_path', './downloads'), m3u_directory)

    logger.debug(f"M3U directory (absolute): {m3u_directory}")
    logger.debug(f"Directory exists: {os.path.exists(m3u_directory)}")

    # List all m3u files
    playlists = []
    if os.path.exists(m3u_directory):
        files = os.listdir(m3u_directory)
        logger.debug(f"Files in directory: {files}")
        for filename in files:
            if filename.lower().endswith('.m3u') or filename.lower().endswith('.m3u8'):
                full_path = os.path.join(m3u_directory, filename)
                playlists.append({
                    'name': filename,
                    'path': full_path
                })
                logger.debug(f"Added playlist: {filename} -> {full_path}")

    logger.info(f"Found {len(playlists)} playlists in {m3u_directory}")
    return render_template('plex_playlists.html', config=config_data, playlists=playlists, m3u_directory=m3u_directory, user=current_user)


@app.route('/api/plex/import_playlist', methods=['POST'])
@login_required
def plex_import_playlist():
    """Import a playlist to Plex"""
    logger.debug("=== Plex import playlist API called ===")
    data = request.json
    playlist_path = data.get('playlist_path')

    logger.debug(f"Received playlist path: {playlist_path}")

    if not playlist_path:
        logger.error("No playlist path provided")
        return jsonify(success=False, error='No playlist path provided')

    if not os.path.exists(playlist_path):
        logger.error(f"Playlist file not found: {playlist_path}")
        return jsonify(success=False, error='Playlist file not found')

    logger.info(f"Attempting to import playlist: {playlist_path}")
    logger.debug(f"Plex config - Server: {plex_api.server_url}, Library: {plex_api.library_section_id}, Token: {'SET' if plex_api.auth_token else 'NOT SET'}")

    result = plex_api.upload_playlist(playlist_path)
    logger.debug(f"Import result: {result}")
    return jsonify(**result)


@app.route('/api/plex/delete_playlist', methods=['DELETE'])
@login_required
def plex_delete_playlist():
    """Delete a playlist file"""
    logger.debug("=== Plex delete playlist API called ===")
    data = request.json
    playlist_path = data.get('playlist_path')

    logger.debug(f"Received playlist path to delete: {playlist_path}")

    if not playlist_path:
        logger.error("No playlist path provided")
        return jsonify(success=False, error='No playlist path provided')

    if not os.path.exists(playlist_path):
        logger.error(f"Playlist file not found: {playlist_path}")
        return jsonify(success=False, error='Playlist file not found')

    try:
        os.remove(playlist_path)
        logger.info(f"Successfully deleted playlist: {playlist_path}")
        return jsonify(success=True)
    except Exception as e:
        logger.error(f"Failed to delete playlist {playlist_path}: {str(e)}")
        return jsonify(success=False, error=str(e))


def main():
    config.migration()
    print(f'OnTheSpot Version: {config.get("version")}')
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Host IP address')
    parser.add_argument('--port', type=int, default=5000, help='Port number')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()

    fill_account_pool = FillAccountPool(
        finished_callback=lambda: logger.info("Finished filling account pool."),
        progress_callback=lambda message, status: logger.info(f"{message} {'Success' if status else 'Failed'}")
    )

    fill_account_pool.start()

    thread = threading.Thread(target=parsingworker)
    thread.daemon = True
    thread.start()

    def start_workers():
        """Start all worker threads and register them"""
        logger.info("Starting worker threads...")

        for _ in range(config.get('maximum_queue_workers')):
            queue_worker = QueueWorker()
            queue_worker.start()
            register_worker(queue_worker)

        for _ in range(config.get('maximum_download_workers')):
            download_worker = DownloadWorker()
            download_worker.start()
            register_worker(download_worker)

        if config.get('enable_retry_worker'):
            retryworker = RetryWorker()
            retryworker.start()
            register_worker(retryworker)

        # Start watchdog worker
        watchdog_worker = WatchdogWorker()
        watchdog_worker.start()
        register_worker(watchdog_worker)
        
        # Start parsing cleanup worker (memory leak prevention)
        parsing_cleanup_worker = ParsingCleanupWorker()
        parsing_cleanup_worker.start()
        register_worker(parsing_cleanup_worker)
        
        # Start auto-clear worker
        autoclear_worker = AutoClearWorker()
        autoclear_worker.start()
        register_worker(autoclear_worker)
        
        # Start WebSocket broadcaster for real-time updates
        ws_broadcaster = WebSocketBroadcaster()
        ws_broadcaster.start()
        register_worker(ws_broadcaster)

        logger.info("All workers started and registered")

    def restart_workers():
        """Soft restart of worker threads - keeps web server running"""
        from .runtimedata import kill_all_workers
        logger.warning("⚠️ SOFT RESTART: Restarting worker threads due to consecutive download failures")
        logger.info("Web server will remain available during worker restart")
        
        try:
            # Stop all workers
            kill_all_workers()
            
            # Wait a moment for cleanup
            time.sleep(1)
            
            # Restart workers
            start_workers()
            
            logger.info("✅ Worker threads successfully restarted")
        except Exception as e:
            logger.error(f"Failed to restart workers: {e}")
            logger.error("Attempting hard restart as fallback...")
            trigger_hard_restart("soft restart failed")
    
    def watchdog_hard_restart():
        """Hard restart triggered by watchdog for stuck system"""
        trigger_hard_restart("watchdog detected stuck flags (30s timeout)")

    # Register the restart callbacks
    set_worker_restart_callback(restart_workers)  # Soft restart for failure threshold
    set_watchdog_restart_callback(watchdog_hard_restart)  # Hard restart for stuck flags

    # Start initial workers
    start_workers()

    fill_account_pool.join()  # Wait for account pool to finish loading

    # Check account pool status and retry failed accounts before loading cached queue
    max_retries = 3
    retry_count = 0
    while retry_count < max_retries:
        failed_accounts = [acc for acc in account_pool if acc.get('active') and acc.get('status') != 'active']
        if not failed_accounts:
            break
        
        logger.warning(f"Found {len(failed_accounts)} failed accounts, attempting re-initialization (attempt {retry_count + 1}/{max_retries})")
        for account in failed_accounts:
            service = account['service']
            try:
                logger.info(f"Re-initializing account {account.get('uuid', 'unknown')} ({service})")
                valid_login = globals()[f"{service}_login_user"](account)
                if valid_login:
                    logger.info(f"Successfully re-initialized account {account.get('uuid', 'unknown')}")
                else:
                    logger.error(f"Failed to re-initialize account {account.get('uuid', 'unknown')}")
            except Exception as e:
                logger.error(f"Exception during account re-initialization: {e}")
        
        retry_count += 1
        if retry_count < max_retries and any(acc.get('status') != 'active' for acc in account_pool if acc.get('active')):
            time.sleep(2)  # Wait before retry
    
    # Log final account status
    active_accounts = [acc for acc in account_pool if acc.get('status') == 'active']
    logger.info(f"Account pool ready: {len(active_accounts)} active accounts out of {len(account_pool)} total")

    if config.get('mirror_spotify_playback'):
        mirrorplayback = MirrorSpotifyPlayback()
        mirrorplayback.start()

    # Try JSON format first (new format with metadata)
    cached_file_json = os.path.join(cache_dir(), 'cached_download_queue.json')
    cached_file_txt = os.path.join(cache_dir(), 'cached_download_queue.txt')
    
    if os.path.isfile(cached_file_json):
        import json
        logger.info(f'Found cached download queue at {cached_file_json}, restoring items with metadata...')
        try:
            with open(cached_file_json, 'r') as file:
                cached_items = json.load(file)
            
            # Add items back to parsing queue with preserved metadata
            from .utils import format_local_id
            for item_data in cached_items:
                local_id = format_local_id(item_data.get('item_id'))
                with parsing_lock:
                    parsing[item_data.get('item_id')] = {
                        'item_url': item_data.get('item_url'),
                        'item_service': item_data.get('item_service'),
                        'item_type': item_data.get('item_type'),
                        'item_id': item_data.get('item_id'),
                        'parent_category': item_data.get('parent_category'),
                        'playlist_name': item_data.get('playlist_name'),
                        'playlist_by': item_data.get('playlist_by'),
                        'playlist_number': item_data.get('playlist_number'),
                        'playlist_total': item_data.get('playlist_total')
                    }
            logger.info(f'Restored {len(cached_items)} items from cached queue')
            os.remove(cached_file_json)
        except Exception as e:
            logger.error(f'Failed to restore cached queue from JSON: {e}')
    elif os.path.isfile(cached_file_txt):
        # Fallback to old format (plain URLs)
        logger.info(f'Found old format cached download queue at {cached_file_txt}, parsing as URLs...')
        get_search_results(cached_file_txt)
        os.remove(cached_file_txt)

    # Log registered routes for debugging
    logger.info("=== Registered Routes ===")
    for rule in app.url_map.iter_rules():
        logger.info(f"  {rule.rule} -> {rule.endpoint}")
    logger.info("========================")

    # Use socketio.run instead of app.run for WebSocket support
    socketio.run(app, host=args.host, port=args.port, debug=args.debug, allow_unsafe_werkzeug=True)


if __name__ == '__main__':
    main()
