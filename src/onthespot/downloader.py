import glob
import re
import requests
import subprocess
import threading
import time
import traceback
import os
import queue
from librespot.audio.decoders import AudioQuality, VorbisOnlyAudioQuality
from librespot.metadata import TrackId, EpisodeId
from yt_dlp import YoutubeDL
from .accounts import get_account_token
from .api.apple_music import apple_music_get_track_metadata, apple_music_get_decryption_key, apple_music_get_lyrics, apple_music_get_webplayback_info
from .api.bandcamp import bandcamp_get_track_metadata
from .api.deezer import deezer_get_track_metadata, get_song_info_from_deezer_website, genurlkey, calcbfkey, decryptfile
from .api.qobuz import qobuz_get_track_metadata, qobuz_get_file_url
from .api.soundcloud import soundcloud_get_track_metadata
from .api.spotify import spotify_get_track_metadata, spotify_get_podcast_episode_metadata, spotify_get_lyrics
from .api.tidal import tidal_get_track_metadata, tidal_get_lyrics, tidal_get_file_url
from .api.youtube_music import youtube_music_get_track_metadata
from .api.crunchyroll import crunchyroll_get_episode_metadata, crunchyroll_get_decryption_key, crunchyroll_get_mpd_info, crunchyroll_close_stream
from .api.generic import generic_get_track_metadata
from .otsconfig import config
from .runtimedata import get_logger, download_queue, download_queue_lock, account_pool, temp_download_path, increment_failure_count, reset_failure_count, album_download_locks, album_download_locks_lock
from . import runtimedata
from .utils import format_item_path, convert_audio_format, embed_metadata, set_music_thumbnail, fix_mp3_metadata, add_to_m3u_file, strip_metadata, convert_video_format

logger = get_logger("downloader")

# Shared helper to compute the final output path with the expected extension.
def build_final_file_path(base_path, item_type, default_format, item_service=None):
    """
    Determine the final output path (with extension) for an item.
    Keeps the logic in one place so playlist writing and renaming stay in sync.
    """
    # If conversion is enabled, use the configured output format (predictable)
    if not config.get('raw_media_download'):
        if item_type == "track":
            return base_path + "." + config.get("track_file_format")
        if item_type == "podcast_episode":
            return base_path + "." + config.get("podcast_file_format")
        return base_path

    # Raw downloads need the service's native/container format
    if not default_format:
        default_map = {
            "spotify": ".ogg",
            "deezer": ".mp3",
            "soundcloud": ".mp3",
            "youtube_music": ".m4a",
            "apple_music": ".m4a",
            "bandcamp": ".mp3",
            "qobuz": ".flac",
            "tidal": ".flac",
        }
        default_format = default_map.get(item_service)

    if default_format and not default_format.startswith("."):
        default_format = "." + default_format

    if default_format:
        return base_path + default_format

    logger.warning("Default format not set for raw download; using base path without extension")
    return base_path


class RetryWorker:
    def __init__(self):
        self.thread = threading.Thread(target=self.run)
        self.is_running = True


    def start(self):
        logger.info('Starting Retry Worker')
        self.thread.start()


    def run(self):
        from .runtimedata import get_consecutive_failures
        while self.is_running:
            if download_queue:
                # First, check if there are any failed downloads
                has_failed_downloads = False
                failed_count = 0
                with download_queue_lock:
                    for local_id in download_queue.keys():
                        if download_queue[local_id]['item_status'] == "Failed":
                            has_failed_downloads = True
                            failed_count += 1

                # If failures are accumulating, back off to let hard restart happen
                current_failure_count = get_consecutive_failures()
                if current_failure_count >= 3:
                    logger.warning(f"High consecutive failure count ({current_failure_count}), backing off to allow hard restart")
                    time.sleep(10)
                    continue

                # If there are failed downloads, force reconnect all Spotify accounts
                if has_failed_downloads:
                    logger.info(f"Found {failed_count} failed downloads - forcing Spotify account reconnection before retry")
                    from .api.spotify import spotify_re_init_session

                    reconnected_count = 0
                    for account_idx, account in enumerate(account_pool):
                        if account.get('service') == 'spotify' and account.get('login', {}).get('session'):
                            try:
                                logger.info(f"Reconnecting Spotify account {account_idx}: {account.get('username', 'unknown')}")
                                # Force reconnection to ensure failed downloads get fresh sessions
                                spotify_re_init_session(account, force=True)
                                reconnected_count += 1
                            except Exception as e:
                                logger.error(f"Failed to reconnect Spotify account {account_idx}: {e}")

                    if reconnected_count > 0:
                        logger.info(f"Successfully reconnected {reconnected_count} Spotify account(s) - now retrying failed downloads")
                    elif failed_count > 0:
                        logger.warning(f"Could not reconnect any accounts, but have {failed_count} failed downloads")

                # Now retry the failed downloads
                with download_queue_lock:
                    for local_id in download_queue.keys():
                        if download_queue[local_id]['item_status'] == "Failed":
                            logger.debug(f'Retrying : {local_id}')
                            download_queue[local_id]['item_status'] = "Waiting"
            if config.get('retry_worker_delay') > 0:
                time.sleep(config.get('retry_worker_delay') * 60)
            continue


    def stop(self):
        logger.info('Stopping Retry Worker')
        self.is_running = False
        self.thread.join()


class DownloadWorker:
    def __init__(self):
        self.thread = threading.Thread(target=self.run)
        self.is_running = True


    def start(self):
        logger.info('Starting Download Worker')
        self.thread.start()


    def readd_item_to_download_queue(self, item):
        """Re-add item to download queue - optimized to avoid delete/recreate"""
        with download_queue_lock:
            try:
                local_id = item['local_id']
                # Just mark as available instead of delete-recreate
                if local_id in download_queue:
                    download_queue[local_id]['available'] = True
            except (KeyError):
                # Item likely cleared from queue
                return


    def update_progress(self, item, status, progress_value):
        """Update progress for web interface"""
        # Always update the item's progress for web interface
        item['progress'] = progress_value


    def yt_dlp_progress_hook(self, item, d):
        progress = item.get('progress', 0)
        progress_str = re.search(r'(\d+\.\d+)%', d['_percent_str'])
        if progress_str:
            updated_progress_value = round(float(progress_str.group(1))) - 1
            if updated_progress_value >= progress:
                self.update_progress(item, "Downloading", updated_progress_value)
        if item['item_status'] == 'Cancelled':
            raise Exception("Download cancelled by user.")


    def _find_account_index(self, service, token):
        """Find the index of the account in account_pool that matches the given token"""
        for idx, account in enumerate(account_pool):
            if account.get('service') == service:
                account_token = account.get('login', {}).get('session')
                if account_token is token:
                    return idx
        return None

    def _get_available_accounts(self, service):
        """Get list of available account indices for a service"""
        indices = []
        for idx, account in enumerate(account_pool):
            if account.get('service') == service and account.get('active', True):
                indices.append(idx)
        return indices

    def _validate_spotify_session(self, token, account_idx):
        """
        Validate that a Spotify session is still healthy before using it.

        Sessions can go stale after being idle for ~1 hour as Spotify disconnects
        inactive clients. Using a stale session results in incomplete downloads
        that create corrupted .ogg files, causing FFmpeg exit 183 errors.

        This proactively checks session health and reinitializes if needed.
        """
        from .api.spotify import spotify_re_init_session

        try:
            # Quick health check - this will fail if session is stale
            user_type = token.get_user_attribute("type")
            logger.debug(f"Session validation passed for account {account_idx} (user type: {user_type})")
            return token
        except Exception as e:
            logger.warning(f"Session validation failed for account {account_idx}: {e}")
            logger.info(f"Reinitializing stale session for account {account_idx}...")
            try:
                spotify_re_init_session(account_pool[account_idx])
                new_token = account_pool[account_idx]['login']['session']
                logger.info(f"Session successfully reinitialized for account {account_idx}")
                return new_token
            except Exception as reinit_err:
                logger.error(f"Session reinitialization failed for account {account_idx}: {reinit_err}")
                raise RuntimeError(f"Cannot use account {account_idx}: session is stale and reinitialization failed")


    def _try_get_spotify_stream(self, item, item_id, item_type, token, quality, tried_accounts=None):
        """
        Try to get a Spotify stream, with fallback to other accounts if one fails.
        Returns (stream, token, account_index) on success, raises exception on complete failure.
        """
        from .api.spotify import spotify_re_init_session
        
        if tried_accounts is None:
            tried_accounts = set()
        
        if item_type == "track":
            audio_key = TrackId.from_base62(item_id)
        elif item_type == "podcast_episode":
            audio_key = EpisodeId.from_base62(item_id)
        
        # Find current account index
        current_account_idx = self._find_account_index('spotify', token)
        available_accounts = self._get_available_accounts('spotify')
        
        max_retries_per_account = 2
        
        # Try current account first
        if current_account_idx is not None and current_account_idx not in tried_accounts:
            for attempt in range(max_retries_per_account):
                try:
                    stream = token.content_feeder().load(audio_key, VorbisOnlyAudioQuality(quality), False, None)
                    logger.info(f"Successfully got stream from account index {current_account_idx}")
                    return stream, token, current_account_idx
                except (RuntimeError, OSError, queue.Empty) as e:
                    error_str = str(e)
                    error_type = type(e).__name__

                    # queue.Empty means audio key fetch timed out - always retry
                    # Other errors check if they're in the known retryable list
                    is_retryable = (error_type == 'Empty' or
                                   any(x in error_str for x in ['Bad file descriptor', 'Cannot get alternative track',
                                                                 'Unable to', 'Failed fetching audio key']))

                    if is_retryable:
                        if attempt < max_retries_per_account - 1:
                            logger.warning(f"Download stream failed ({error_type}, attempt {attempt + 1}) on account {current_account_idx}, reconnecting session: {e}")
                            try:
                                # Force reconnection to fix stream issues immediately
                                spotify_re_init_session(account_pool[current_account_idx], force=True)
                                token = account_pool[current_account_idx]['login']['session']
                                # Refresh quality check with new token
                                if token.get_user_attribute("type") == "premium" and item_type == 'track':
                                    quality = AudioQuality.VERY_HIGH
                                logger.info("Session reconnected successfully, retrying...")
                            except Exception as reinit_err:
                                logger.error(f"Session reinit failed for account {current_account_idx}: {reinit_err}")
                                break  # Try next account
                        else:
                            logger.warning(f"Max retries reached for account {current_account_idx}")
                            break  # Try next account
                    else:
                        raise
            
            tried_accounts.add(current_account_idx)
        
        # Try other available accounts
        for account_idx in available_accounts:
            if account_idx in tried_accounts:
                continue
            
            logger.info(f"Trying fallback account index {account_idx}")
            tried_accounts.add(account_idx)
            
            try:
                # Get token from this account
                fallback_token = account_pool[account_idx]['login']['session']
                
                # Check quality for this account
                fallback_quality = AudioQuality.HIGH
                if fallback_token.get_user_attribute("type") == "premium" and item_type == 'track':
                    fallback_quality = AudioQuality.VERY_HIGH
                
                for attempt in range(max_retries_per_account):
                    try:
                        stream = fallback_token.content_feeder().load(audio_key, VorbisOnlyAudioQuality(fallback_quality), False, None)
                        logger.info(f"Successfully got stream from fallback account index {account_idx}")
                        return stream, fallback_token, account_idx
                    except (RuntimeError, OSError, queue.Empty) as e:
                        error_str = str(e)
                        error_type = type(e).__name__

                        # queue.Empty means audio key fetch timed out - always retry
                        is_retryable = (error_type == 'Empty' or
                                       any(x in error_str for x in ['Bad file descriptor', 'Cannot get alternative track',
                                                                     'Unable to', 'Failed fetching audio key']))

                        if is_retryable:
                            if attempt < max_retries_per_account - 1:
                                logger.warning(f"Fallback account {account_idx} stream failed ({error_type}, attempt {attempt + 1}), reconnecting: {e}")
                                try:
                                    # Force reconnection to fix fallback account stream issues
                                    spotify_re_init_session(account_pool[account_idx], force=True)
                                    fallback_token = account_pool[account_idx]['login']['session']
                                    logger.info(f"Fallback account {account_idx} reconnected, retrying...")
                                except Exception as reinit_err:
                                    logger.error(f"Fallback account {account_idx} reinit failed: {reinit_err}")
                                    break
                            else:
                                logger.warning(f"Max retries reached for fallback account {account_idx}")
                                break
                        else:
                            raise
            except Exception as e:
                logger.error(f"Fallback account {account_idx} failed completely: {e}")
                continue
        
        # All accounts exhausted
        raise RuntimeError(f"Failed to load audio stream after trying {len(tried_accounts)} account(s)")

    def _ensure_playlist_entry(self, item, item_metadata, base_path, default_format, final_path=None):
        """
        Track completed playlist item for deferred M3U writing.
        M3U will be written when all playlist items are complete.
        Returns the final file path used for the entry.
        """
        final_path = final_path or build_final_file_path(base_path, item['item_type'], default_format, item_service=item.get('item_service'))

        if not final_path:
            return None

        # Set file_path for tracking
        item['file_path'] = final_path

        return final_path


    def run(self):
        last_heartbeat = time.time()
        heartbeat_interval = 60  # Log every 60 seconds
        
        while self.is_running:
            try:
                # Periodic heartbeat logging
                if time.time() - last_heartbeat > heartbeat_interval:
                    with runtimedata.batch_queue_processing_lock:
                        is_processing = runtimedata.batch_queue_processing
                    logger.info(f"DownloadWorker heartbeat: batch_processing={is_processing}, queue_size={len(download_queue)}")
                    last_heartbeat = time.time()
                
                try:
                    # Wait if QueueWorker is batch processing items into download queue
                    with runtimedata.batch_queue_processing_lock:
                        is_batch_processing = runtimedata.batch_queue_processing
                    
                    if is_batch_processing:
                        time.sleep(0.2)
                        continue
                    
                    if download_queue:
                        with download_queue_lock:
                            # Sort queue by album to group album tracks together
                            # Priority: album_name, track_number, then insertion order
                            available_items = [
                                (local_id, item) for local_id, item in download_queue.items()
                                if item['available'] and item['item_status'] == 'Waiting'
                            ]
                            
                            if not available_items:
                                time.sleep(0.2)
                                continue
                            
                            # Sort by playlist number (if playlist item), then album name, then track number
                            available_items.sort(key=lambda x: (
                                int(x[1].get('playlist_number', 0) or 0) if x[1].get('parent_category') == 'playlist' else 999999,  # Playlist items first, by number
                                x[1].get('album_name') or '\uffff',  # Then group by album
                                int(x[1].get('track_number') or 9999),  # Then by track number
                                x[0]  # Maintain insertion order as tiebreaker
                            ))
                            
                            # Get the first available item
                            local_id, item = available_items[0]
                            download_queue[local_id]['available'] = False
                    else:
                        time.sleep(0.2)
                        continue

                    item_service = item['item_service']
                    item_type = item['item_type']
                    item_id = item['item_id']

                    if item['item_status'] in (
                        "Cancelled",
                        "Failed",
                        "Unavailable",
                        "Downloaded",
                        "Already Exists",
                        "Deleted"
                    ):
                        time.sleep(0.2)
                        self.readd_item_to_download_queue(item)
                        continue
                except (RuntimeError, OSError, StopIteration):
                    time.sleep(0.2)
                    continue

                item['item_status'] = "Downloading"
                self.update_progress(item, "Downloading", 1)
                
                logger.info(f"Starting download for track ID: {item_id} from service: {item_service}")

                token = get_account_token(item_service, rotate=config.get("rotate_active_account_number"))
                # Get account index for failure tracking
                account_index = self._find_account_index(item_service, token) if token else None

                # For Spotify albums, acquire album lock BEFORE metadata fetch to serialize track_number lookup
                album_lock = None
                album_key = None
                if item_service == "spotify" and item.get('parent_category') == 'album' and item.get('parent_id'):
                    album_key = f"{item_service}:{item.get('parent_id')}"  # Use parent_id (album_id) for shared lock
                    with album_download_locks_lock:
                        if album_key not in album_download_locks:
                            album_download_locks[album_key] = threading.Lock()
                        album_lock = album_download_locks[album_key]
                    album_lock.acquire()
                    logger.debug(f"Acquired album lock for metadata fetch: {album_key}")

                try:
                    item_metadata = globals()[f"{item_service}_get_{item_type}_metadata"](token, item_id)

                    # album number shim from enumerated items, i hate youtube
                    if item_service == 'youtube_music' and item.get('parent_category') == 'album':
                        item_metadata.update({'track_number': item['playlist_number']})

                    # Use playlist position for playlists instead of album track number
                    if item.get('parent_category') == 'playlist':
                        item_metadata.update({'track_number': item['playlist_number']})

                    item_path = format_item_path(item, item_metadata)
                    
                    # Release album lock after metadata is fetched (will reacquire for stream)
                    if album_lock:
                        album_lock.release()
                        album_lock = None
                        logger.debug(f"Released album lock after metadata fetch")
                except (Exception, KeyError) as e:
                    if album_lock:
                        album_lock.release()
                        album_lock = None
                    logger.error(f"Failed to fetch metadata for '{item_id}', Error: {str(e)}\nTraceback: {traceback.format_exc()}")
                    item['item_status'] = "Failed"
                    self.update_progress(item, "Failed", 0)
                    increment_failure_count(account_index)  # Track failure for worker restart
                    self.readd_item_to_download_queue(item)
                    continue

                temp_file_path = ''
                file_path = ''
                final_file_path = None
                if item_service != 'generic':
                    if item_type in ['track', 'podcast_episode']:
                        dl_root = config.get("audio_download_path")
                    elif item_type in ['movie', 'episode']:
                        dl_root = config.get("video_download_path")
                    if temp_download_path:
                        dl_root = temp_download_path[0]
                    file_path = os.path.join(dl_root, item_path)
                    directory, file_name = os.path.split(file_path)

                    # Additional verification of path length limits
                    name, ext = os.path.splitext(file_name)
                    MAX_PATH_LENGTH = 260
                    available_length = MAX_PATH_LENGTH - len(os.path.join(directory, ''))
                    if len(file_name) > available_length:
                        trim_length = available_length - len(ext)
                        name = name[:trim_length]
                        file_name = name + ext
                        file_path = os.path.join(directory, file_name)
                    
                    temp_file_path = os.path.join(directory, '~' + file_name)

                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    logger.info(f"DEBUG full file_path: {file_path}")

                    # Skip download if file exists under different extension
                    file_directory = os.path.dirname(file_path)
                    target_filename = os.path.basename(file_path)

                    logger.info(f"Checking for existing files matching: '{target_filename}.*' in {file_directory}")

                    # Check if directory exists first to avoid exception overhead
                    if os.path.isdir(file_directory):
                        # Use set for O(1) lookup instead of list
                        audio_exts = {".mp3", ".flac", ".ogg", ".m4a", ".opus", ".wav", ".aac", ".wma"}
                        subtitle_exts = {".lrc", ".ass", ".srt", ".vtt"}

                        # Use scandir instead of listdir - avoids separate stat calls
                        for entry in os.scandir(file_directory):
                            # entry.is_file() uses cached stat info from scandir
                            if not entry.is_file():
                                continue

                            # Skip subtitle/lyrics files
                            if any(entry.name.endswith(ext) for ext in subtitle_exts):
                                continue

                            # Check if file matches target (strip audio extension)
                            # Use os.path.splitext for cleaner extension handling
                            entry_base, entry_ext = os.path.splitext(entry.name)
                            if entry_ext.lower() not in audio_exts:
                                # Not an audio file, use full name
                                entry_base = entry.name

                            if entry_base == target_filename:
                                logger.info(f"MATCH FOUND! File '{entry.name}' matches target '{target_filename}' - Skipping download and metadata rewrite")
                                item['file_path'] = entry.path

                                # Set status to Already Exists first
                                if item['item_status'] in ('Downloading', 'Setting Thumbnail', 'Adding To M3U', 'Getting Lyrics'):
                                    self.update_progress(item, "Already Exists", 100)
                                item['item_status'] = 'Already Exists'
                                
                                # M3U - track after status is set
                                if config.get('create_m3u_file') and item.get('parent_category') == 'playlist' and not item.get('_m3u_written'):
                                    try:
                                        add_to_m3u_file(item, item_metadata)
                                        item['_m3u_written'] = True
                                    except Exception as m3u_error:
                                        logger.error(f"Failed to add item to M3U file: {str(m3u_error)}\nTraceback: {traceback.format_exc()}")
                                        logger.warning("M3U write failed, but file download was successful and will not be deleted")
                                logger.info(f"File already exists (found as {entry.name}), Skipping download for track by id '{item_id}'")
                                reset_failure_count(account_index)  # Reset failure counter since file exists
                                
                                # Track completed playlist item (if not already tracked above)
                                if config.get('create_m3u_file') and item.get('parent_category') == 'playlist' and not item.get('_m3u_written'):
                                    try:
                                        add_to_m3u_file(item, item_metadata)
                                    except Exception as m3u_error:
                                        logger.error(f"Failed to track existing playlist item for M3U: {str(m3u_error)}")
                                
                                time.sleep(0.2)
                                item['progress'] = 100
                                self.readd_item_to_download_queue(item)
                                break

                if item['item_status'] == 'Already Exists':
                    continue

                if not item_metadata['is_playable']:
                    logger.error(f"Track is unavailable, track id '{item_id}'")
                    item['item_status'] = 'Unavailable'
                    self.update_progress(item, "Unavailable", 0)
                    self.readd_item_to_download_queue(item)
                    continue

                try:
                    # Audio
                    if item_service == "spotify":

                        default_format = ".ogg"
                        temp_file_path += default_format
                        final_file_path = self._ensure_playlist_entry(item, item_metadata, file_path, default_format, final_file_path) or final_file_path

                        # Session recreation is now handled automatically in spotify_get_token()
                        # The token we received is already from a fresh session
                        logger.debug(f"Using session token for Spotify download")

                        quality = AudioQuality.HIGH
                        bitrate = "160k"
                        if token.get_user_attribute("type") == "premium" and item_type == 'track':
                            quality = AudioQuality.VERY_HIGH
                            bitrate = "320k"

                        # Retry loop for handling stalls and connection resets
                        max_download_retries = 3
                        download_retry_count = 0
                        download_successful = False

                        while download_retry_count < max_download_retries and not download_successful:
                            stream = None  # Initialize for finally block
                            try:
                                # For Spotify albums, reacquire album lock for stream acquisition
                                # For non-album items, no lock needed
                                if item.get('parent_category') == 'album' and item.get('parent_id'):
                                    stream_album_key = f"{item_service}:{item.get('parent_id')}"
                                    with album_download_locks_lock:
                                        if stream_album_key not in album_download_locks:
                                            album_download_locks[stream_album_key] = threading.Lock()
                                        stream_album_lock = album_download_locks[stream_album_key]
                                    
                                    # Acquire album lock for stream initialization
                                    logger.debug(f"Reacquiring album lock for stream: {stream_album_key}")
                                    stream_album_lock.acquire()
                                    try:
                                        logger.debug(f"Acquired album lock, getting stream: {stream_album_key}")
                                        # Get stream (with account fallback)
                                        stream, token, _ = self._try_get_spotify_stream(item, item_id, item_type, token, quality)
                                        logger.debug(f"Stream acquired, released lock: {stream_album_key}")
                                    finally:
                                        stream_album_lock.release()
                                else:
                                    # Non-album items don't need locking
                                    stream, token, _ = self._try_get_spotify_stream(item, item_id, item_type, token, quality)

                                # Validate stream is working with initial test read
                                stall_timeout = config.get("download_stall_timeout")
                                test_data = None
                                test_error = None
                                
                                def test_stream_read():
                                    nonlocal test_data, test_error
                                    try:
                                        # Try to read first chunk to verify stream is alive
                                        test_data = stream.input_stream.stream().read(1024)
                                    except Exception as e:
                                        test_error = e
                                
                                test_thread = threading.Thread(target=test_stream_read, daemon=True)
                                test_thread.start()
                                test_thread.join(timeout=5)  # 5 second timeout for initial stream validation
                                
                                if test_thread.is_alive():
                                    raise Exception("Stream validation failed: initial read blocked (session likely dead)")
                                
                                if test_error:
                                    raise test_error
                                
                                if test_data is None:
                                    raise Exception("Stream validation failed: no data returned")

                                total_size = stream.input_stream.size
                                downloaded = len(test_data)  # Account for test read
                                last_progress_time = time.time()

                                with open(temp_file_path, 'wb') as file:
                                    # Write initial test data
                                    file.write(test_data)
                                    
                                    consecutive_empty_reads = 0
                                    MAX_CONSECUTIVE_EMPTY = 3

                                    while downloaded < total_size:
                                        if item['item_status'] == 'Cancelled':
                                           raise Exception("Download cancelled by user.")

                                        # Check for stalled download (no progress for stall_timeout seconds)
                                        time_since_progress = time.time() - last_progress_time
                                        if time_since_progress > stall_timeout:
                                            raise Exception(f"Download stalled (no progress for {time_since_progress:.1f}s), reconnecting...")

                                        # Read with timeout to catch hanging reads
                                        read_start_time = time.time()
                                        data = None
                                        read_error = None

                                        def read_with_timeout():
                                            nonlocal data, read_error
                                            try:
                                                data = stream.input_stream.stream().read(config.get("download_chunk_size"))
                                            except Exception as e:
                                                read_error = e

                                        read_thread = threading.Thread(target=read_with_timeout, daemon=True)
                                        read_thread.start()
                                        read_thread.join(timeout=stall_timeout)

                                        if read_thread.is_alive():
                                            raise Exception(f"Download stalled (read blocked for >{stall_timeout}s), reconnecting...")

                                        if read_error:
                                            raise read_error

                                        if data is None:
                                            raise Exception("Read operation failed to return data")

                                        if len(data) != 0:
                                            downloaded += len(data)
                                            file.write(data)
                                            progress_pct = int((downloaded / total_size) * 100)
                                            self.update_progress(item, "Downloading", progress_pct)
                                            last_progress_time = time.time()
                                            consecutive_empty_reads = 0  # Reset on successful read
                                        else:
                                            # Empty read - check if we're actually done or if stream died
                                            consecutive_empty_reads += 1
                                            download_pct = (downloaded / total_size * 100) if total_size > 0 else 0

                                            if download_pct < 95:
                                                # We're not close to done - stream probably died
                                                if consecutive_empty_reads >= MAX_CONSECUTIVE_EMPTY:
                                                    raise RuntimeError(
                                                        f"Stream died prematurely: only {downloaded}/{total_size} bytes "
                                                        f"({download_pct:.1f}%) downloaded before stream returned empty. "
                                                        f"Session likely stale - will retry with fresh session."
                                                    )
                                                # Brief pause and try again
                                                time.sleep(0.1)
                                                continue
                                            else:
                                                # We're close to done, empty read might be normal end-of-stream
                                                if consecutive_empty_reads >= MAX_CONSECUTIVE_EMPTY:
                                                    logger.debug(f"Stream ended at {download_pct:.1f}% complete after {consecutive_empty_reads} empty reads")
                                                    break
                                                time.sleep(0.05)
                                                continue

                                    # Ensure all data is flushed to disk before closing
                                    file.flush()
                                    os.fsync(file.fileno())

                                # CRITICAL: Verify ACTUAL file size on disk, not just the counter
                                actual_file_size = os.path.getsize(temp_file_path) if os.path.exists(temp_file_path) else 0

                                # Minimum acceptable: 90% of expected OR at least 50KB for any audio
                                min_acceptable = max(int(total_size * 0.90), 50000) if total_size > 0 else 50000

                                logger.debug(f"Download validation: actual={actual_file_size}, expected={total_size}, counter={downloaded}, min={min_acceptable}")

                                if actual_file_size < min_acceptable:
                                    # File is definitely corrupted - clean it up
                                    try:
                                        os.remove(temp_file_path)
                                        logger.debug(f"Removed corrupted temp file: {temp_file_path}")
                                    except:
                                        pass
                                    raise RuntimeError(
                                        f"Download verification FAILED: actual file size {actual_file_size} bytes "
                                        f"is below minimum {min_acceptable} bytes (expected ~{total_size}). "
                                        f"Stream likely died - will retry with fresh session."
                                    )

                                # Also validate OGG header for Spotify files
                                try:
                                    with open(temp_file_path, 'rb') as f:
                                        header = f.read(4)
                                        if header != b'OggS':
                                            raise RuntimeError(
                                                f"Downloaded file has invalid OGG header (got {header!r}). "
                                                f"File is corrupted - will retry."
                                            )
                                except IOError as e:
                                    raise RuntimeError(f"Cannot read downloaded file for validation: {e}")

                                logger.info(f"Download verified: {actual_file_size} bytes, valid OGG header")

                                # Mark as successful - stream cleanup will happen in finally block
                                download_successful = True
                                logger.info(f"Download completed successfully after {download_retry_count + 1} attempt(s)")

                            except (RuntimeError, OSError, Exception) as e:
                                error_str = str(e)
                                # Check if this is a stall, connection, or stream death error
                                if any(x in error_str.lower() for x in [
                                    'stalled', 'timeout', 'timed out', 'connection reset', 'failed reading packet',
                                    'broken pipe', 'bad file descriptor', 'incomplete download',
                                    'stream died', 'verification failed', 'corrupted', 'invalid ogg header'
                                ]):
                                    download_retry_count += 1
                                    if download_retry_count < max_download_retries:
                                        logger.warning(f"Download interrupted (attempt {download_retry_count}/{max_download_retries}): {error_str}")
                                        logger.info(f"Forcing session recreation and retrying download...")

                                        # FORCE session recreation on stream death/corruption
                                        current_account_idx = self._find_account_index('spotify', token)
                                        if current_account_idx is not None:
                                            logger.info(f"Forcing fresh session for account {current_account_idx} after stream failure")
                                            from .api.spotify import spotify_re_init_session
                                            try:
                                                # Force reconnection even if within rate limit window for corrupted streams
                                                spotify_re_init_session(account_pool[current_account_idx], force=True)
                                                token = account_pool[current_account_idx]['login']['session']
                                            except Exception as session_err:
                                                logger.error(f"Failed to recreate session: {session_err}")

                                        # Clean up partial file (stream cleanup in finally block)
                                        if os.path.exists(temp_file_path):
                                            try:
                                                os.remove(temp_file_path)
                                            except Exception:
                                                pass

                                        # Wait a bit before retrying
                                        time.sleep(2)
                                        continue
                                    else:
                                        logger.error(f"Max download retries ({max_download_retries}) reached, giving up")
                                        raise
                                else:
                                    # Not a recoverable error, re-raise immediately
                                    raise

                            finally:
                                # CRITICAL: Always clean up stream to prevent file descriptor leaks
                                # This runs whether download succeeds, fails, or is cancelled
                                if stream is not None:
                                    try:
                                        stream_obj = stream.input_stream.stream()
                                        stream_obj.close()
                                        logger.debug("Closed Spotify stream in finally block")
                                    except Exception as stream_err:
                                        logger.warning(f"Error closing stream in finally: {stream_err}")

                                    try:
                                        del stream.input_stream
                                        del stream
                                    except Exception as del_err:
                                        logger.warning(f"Error deleting stream references: {del_err}")

                                    # Force garbage collection to clean up librespot resources
                                    import gc
                                    gc.collect()

                    elif item_service == 'deezer':
                        song = get_song_info_from_deezer_website(token, item['item_id'])

                        song_quality = 1
                        song_format = 'MP3_128'
                        bitrate = "128k"
                        default_format = ".mp3"
                        if int(song.get("FILESIZE_FLAC")) > 0:
                            song_quality = 9
                            song_format ='FLAC'
                            bitrate = "1411k"
                            default_format = ".flac"
                        elif int(song.get("FILESIZE_MP3_320")) > 0:
                            song_quality = 3
                            song_format = 'MP3_320'
                            bitrate = "320k"
                        elif int(song.get("FILESIZE_MP3_256")) > 0:
                            song_quality = 5
                            song_format = 'MP3_256'
                            bitrate = "256k"
                        temp_file_path += default_format

                        headers = {
                            'Origin': 'https://www.deezer.com',
                            'Accept-Encoding': 'utf-8',
                            'Referer': 'https://www.deezer.com/login',
                        }

                        # Retry loop for handling stalls and connection resets
                        max_download_retries = 3
                        download_retry_count = 0
                        download_successful = False

                        while download_retry_count < max_download_retries and not download_successful:
                            try:
                                track_data = token['session'].post(
                                    "https://media.deezer.com/v1/get_url",
                                    json={
                                        'license_token': token['license_token'],
                                        'media': [{
                                            'type': "FULL",
                                            'formats': [
                                                { 'cipher': "BF_CBC_STRIPE", 'format': song_format }
                                            ]
                                        }],
                                        'track_tokens': [song["TRACK_TOKEN"]]
                                    },
                                    headers = headers
                                ).json()

                                try:
                                    logger.debug(track_data)
                                    url = track_data['data'][0]['media'][0]['sources'][0]['url']
                                except KeyError as e:
                                    # Fallback to lowest quality
                                    logger.error(f'Unable to select Deezer quality, falling back to 128kbps. Error: {str(e)}\nTraceback: {traceback.format_exc()}')
                                    song_quality = 1
                                    song_format = 'MP3_128'
                                    bitrate = "128k"
                                    default_format = ".mp3"
                                    urlkey = genurlkey(song["SNG_ID"], song["MD5_ORIGIN"], song["MEDIA_VERSION"], song_quality)
                                    url = "https://e-cdns-proxy-%s.dzcdn.net/mobile/1/%s" % (song["MD5_ORIGIN"][0], urlkey.decode())

                                final_file_path = self._ensure_playlist_entry(item, item_metadata, file_path, default_format, final_file_path) or final_file_path

                                stall_timeout = config.get("download_stall_timeout")
                                # Timeout tuple: (connect timeout, read timeout)
                                # Both set to stall_timeout to ensure requests don't hang
                                request_timeout = (stall_timeout, stall_timeout)

                                file = requests.get(url, stream=True, timeout=request_timeout)

                                if file.status_code == 200:
                                    total_size = int(file.headers.get('content-length', 0))
                                    downloaded = 0
                                    data_chunks = b''
                                    last_progress_time = time.time()

                                    for data in file.iter_content(chunk_size=config.get("download_chunk_size")):
                                        # Check for stalled download
                                        if time.time() - last_progress_time > stall_timeout:
                                            raise Exception(f"Download stalled (no progress for {stall_timeout}s), reconnecting...")

                                        downloaded += len(data)
                                        data_chunks += data

                                        if downloaded != total_size:
                                            if item['item_status'] == 'Cancelled':
                                                raise Exception("Download cancelled by user.")
                                            progress_pct = int((downloaded / total_size) * 100)
                                            self.update_progress(item, "Downloading", progress_pct)
                                            last_progress_time = time.time()  # Update progress time when data received

                                    key = calcbfkey(song["SNG_ID"])

                                    self.update_progress(item, "Decrypting", 99)
                                    with open(temp_file_path, "wb") as fo:
                                        decryptfile(data_chunks, key, fo)
                                        # Ensure all data is flushed to disk before closing
                                        fo.flush()
                                        os.fsync(fo.fileno())

                                    download_successful = True
                                    logger.info(f"Deezer download completed successfully after {download_retry_count + 1} attempt(s)")

                                else:
                                    logger.info(f"Deezer download attempts failed: {file.status_code}")
                                    item['item_status'] = "Failed"
                                    self.update_progress(item, "Failed", 0)
                                    self.readd_item_to_download_queue(item)
                                    break

                            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                                # Timeout or connection errors are always recoverable
                                download_retry_count += 1
                                if download_retry_count < max_download_retries:
                                    logger.warning(f"Deezer download connection/timeout error (attempt {download_retry_count}/{max_download_retries}): {e}")
                                    logger.info(f"Reconnecting and retrying Deezer download...")
                                    # Wait a bit before retrying
                                    time.sleep(2)
                                    continue
                                else:
                                    logger.error(f"Max Deezer download retries ({max_download_retries}) reached, giving up")
                                    raise
                            except Exception as e:
                                error_str = str(e)
                                # Check if this is a stall or connection error
                                if any(x in error_str.lower() for x in ['stalled', 'timeout', 'timed out', 'connection reset', 'failed reading packet', 'broken pipe', 'connection aborted', 'incomplete download']):
                                    download_retry_count += 1
                                    if download_retry_count < max_download_retries:
                                        logger.warning(f"Deezer download interrupted (attempt {download_retry_count}/{max_download_retries}): {error_str}")
                                        logger.info(f"Reconnecting and retrying Deezer download...")
                                        # Wait a bit before retrying
                                        time.sleep(2)
                                        continue
                                    else:
                                        logger.error(f"Max Deezer download retries ({max_download_retries}) reached, giving up")
                                        raise
                                else:
                                    # Not a recoverable error, re-raise immediately
                                    raise

                    elif item_service in ("soundcloud", "youtube_music"):
                        item_url = item_metadata['item_url']
                        ydl_opts = {}
                        if item_service == "soundcloud":
                            if token['oauth_token']:
                                ydl_opts['format'] = 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio'
                                ydl_opts['username'] = 'oauth'
                                ydl_opts['password'] = token['oauth_token']
                            else:
                                default_format = ".mp3"
                                bitrate = "128k"
                                ydl_opts['format'] = 'bestaudio[ext=mp3]'
                        elif item_service == "youtube_music":
                            default_format = '.m4a'
                            bitrate = "128k"
                            ydl_opts['format'] = 'bestaudio[ext=m4a]'
                        ydl_opts['quiet'] = True
                        ydl_opts['no_warnings'] = True
                        ydl_opts['noprogress'] = True
                        ydl_opts['extract_audio'] = True
                        ydl_opts['outtmpl'] = temp_file_path
                        ydl_opts['progress_hooks'] = [lambda d: self.yt_dlp_progress_hook(item, d)]
                        with YoutubeDL(ydl_opts) as video:
                            if item_service == "soundcloud" and token['oauth_token']:
                                info_dict = video.extract_info(item_url)
                                bitrate = f"{info_dict.get('abr')}k"
                                default_format = f".{info_dict.get('audio_ext')}"
                            final_file_path = self._ensure_playlist_entry(item, item_metadata, file_path, default_format, final_file_path) or final_file_path
                            video.download(item_url)

                    elif item_service in ("bandcamp", "qobuz", "tidal"):
                        if item_service in ("qobuz", "tidal"):
                            default_format = '.flac'
                            bitrate = "1411k"
                            file_url = globals()[f"{item_service}_get_file_url"](token, item_id)
                        elif item_service == 'bandcamp':
                            default_format = '.mp3'
                            bitrate = "128k"
                            file_url = item_metadata['file_url']

                        final_file_path = self._ensure_playlist_entry(item, item_metadata, file_path, default_format, final_file_path) or final_file_path

                        # Retry loop for handling stalls and connection resets
                        max_download_retries = 3
                        download_retry_count = 0
                        download_successful = False

                        while download_retry_count < max_download_retries and not download_successful:
                            try:
                                stall_timeout = config.get("download_stall_timeout")
                                # Timeout tuple: (connect timeout, read timeout)
                                # Both set to stall_timeout to ensure requests don't hang
                                request_timeout = (stall_timeout, stall_timeout)

                                response = requests.get(file_url, stream=True, timeout=request_timeout)
                                total_size = int(response.headers.get('Content-Length', 0))
                                downloaded = 0
                                data_chunks = b''
                                last_progress_time = time.time()
                                with open(temp_file_path, 'wb') as file:
                                    for data in response.iter_content(chunk_size=config.get("download_chunk_size", 1024)):
                                        # Check for stalled download
                                        if time.time() - last_progress_time > stall_timeout:
                                            raise Exception(f"Download stalled (no progress for {stall_timeout}s), reconnecting...")

                                        if data:
                                            downloaded += len(data)
                                            data_chunks += data
                                            file.write(data)

                                            if total_size > 0 and downloaded != total_size:
                                                if item['item_status'] == 'Cancelled':
                                                    raise Exception("Download cancelled by user.")
                                                progress_pct = int((downloaded / total_size) * 100)
                                                self.update_progress(item, "Downloading", progress_pct)
                                                last_progress_time = time.time()  # Update progress time when data received

                                    # Ensure all data is flushed to disk before closing
                                    file.flush()
                                    os.fsync(file.fileno())

                                download_successful = True
                                logger.info(f"{item_service} download completed successfully after {download_retry_count + 1} attempt(s)")

                            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                                # Timeout or connection errors are always recoverable
                                download_retry_count += 1
                                if download_retry_count < max_download_retries:
                                    logger.warning(f"{item_service} download connection/timeout error (attempt {download_retry_count}/{max_download_retries}): {e}")
                                    logger.info(f"Reconnecting and retrying {item_service} download...")
                                    # Clean up partial file
                                    if os.path.exists(temp_file_path):
                                        try:
                                            os.remove(temp_file_path)
                                        except Exception:
                                            pass
                                    # Wait a bit before retrying
                                    time.sleep(2)
                                    continue
                                else:
                                    logger.error(f"Max {item_service} download retries ({max_download_retries}) reached, giving up")
                                    raise
                            except Exception as e:
                                error_str = str(e)
                                # Check if this is a stall or connection error
                                if any(x in error_str.lower() for x in ['stalled', 'timeout', 'timed out', 'connection reset', 'failed reading packet', 'broken pipe', 'connection aborted', 'incomplete download']):
                                    download_retry_count += 1
                                    if download_retry_count < max_download_retries:
                                        logger.warning(f"{item_service} download interrupted (attempt {download_retry_count}/{max_download_retries}): {error_str}")
                                        logger.info(f"Reconnecting and retrying {item_service} download...")
                                        # Clean up partial file
                                        if os.path.exists(temp_file_path):
                                            try:
                                                os.remove(temp_file_path)
                                            except Exception:
                                                pass
                                        # Wait a bit before retrying
                                        time.sleep(2)
                                        continue
                                    else:
                                        logger.error(f"Max {item_service} download retries ({max_download_retries}) reached, giving up")
                                        raise
                                else:
                                    # Not a recoverable error, re-raise immediately
                                    raise

                    elif item_service == "apple_music":
                        default_format = '.m4a'
                        bitrate = "256k"
                        webplayback_info = apple_music_get_webplayback_info(token, item_id)

                        stream_url = None
                        for asset in webplayback_info["assets"]:
                            if asset["flavor"] == "28:ctrp256":
                                stream_url = asset["URL"]

                        if not stream_url:
                            logger.error(f'Apple music playback info invalid: {webplayback_info}')
                            continue

                        decryption_key = apple_music_get_decryption_key(token, stream_url, item_id)
                        final_file_path = self._ensure_playlist_entry(item, item_metadata, file_path, default_format, final_file_path) or final_file_path

                        ydl_opts = {}
                        ydl_opts['quiet'] = True
                        ydl_opts['no_warnings'] = True
                        ydl_opts['outtmpl'] = temp_file_path
                        ydl_opts['allow_unplayable_formats'] = True
                        ydl_opts['fixup'] = 'never'
                        ydl_opts['allowed_extractors'] = ['generic']
                        ydl_opts['noprogress'] = True
                        ydl_opts['progress_hooks'] = [lambda d: self.yt_dlp_progress_hook(item, d)]
                        with YoutubeDL(ydl_opts) as video:
                            video.download(stream_url)

                        self.update_progress(item, "Decrypting", 99)

                        decrypted_temp_file_path = temp_file_path + '.m4a'
                        command = [
                            config.get('_ffmpeg_bin_path'),
                            "-loglevel", "error",
                            "-y",
                            "-decryption_key", decryption_key,
                            "-i", temp_file_path,
                            "-c", "copy",
                            "-movflags",
                            "+faststart",
                            decrypted_temp_file_path
                        ]
                        if os.name == 'nt':
                            subprocess.check_call(command, shell=False, creationflags=subprocess.CREATE_NO_WINDOW)
                        else:
                            subprocess.check_call(command, shell=False)

                        if os.path.exists(temp_file_path):
                            os.remove(temp_file_path)
                        os.rename(decrypted_temp_file_path, temp_file_path)

                    # Video
                    elif item_service == "crunchyroll":
                        ydl_opts = {}
                        ydl_opts['quiet'] = True
                        ydl_opts['no_warnings'] = True
                        ydl_opts['allow_unplayable_formats'] = True
                        ydl_opts['fixup'] = 'never'
                        ydl_opts['allowed_extractors'] = ['generic']
                        ydl_opts['noprogress'] = True
                        ydl_opts['progress_hooks'] = [lambda d: self.yt_dlp_progress_hook(item, d)]

                        # Extract preferred language
                        encrypted_files = []
                        video_files = []
                        subtitle_formats = []
                        for version in item_metadata['versions']:
                            if version['audio_locale'] in config.get('preferred_audio_language').replace(' ', '').split(',') or config.get('download_all_available_audio'):
                                try:
                                    mpd_url, stream_token, audio_locale, headers, versions, additional_subtitle_formats = crunchyroll_get_mpd_info(token, version['guid'])
                                    subtitle_formats += additional_subtitle_formats
                                    decryption_key = crunchyroll_get_decryption_key(token, version['guid'], mpd_url, stream_token)
                                except Exception as e:
                                    logger.error(e)
                                    continue

                                token = get_account_token(item_service)
                                headers['Authorization'] = f'Bearer {token}'
                                ydl_opts['http_headers'] = headers
                                ydl_opts['outtmpl'] = temp_file_path + f' - {version["audio_locale"]}.%(ext)s.%(ext)s'

                                self.update_progress(item, "Downloading Video", 1)
                                ydl_video_opts = ydl_opts
                                ydl_video_opts['format'] = (f'(bestvideo[height<={config.get("preferred_video_resolution")}][ext=mp4]/bestvideo)')
                                with YoutubeDL(ydl_video_opts) as video:
                                    encrypted_files.append({
                                        'path': video.prepare_filename(video.extract_info(mpd_url, download=False)),
                                        'type': 'video',
                                        'decryption_key': decryption_key,
                                        'language': version['audio_locale']
                                    })
                                    video.download(mpd_url)

                                token = get_account_token(item_service)
                                headers['Authorization'] = f'Bearer {token}'
                                ydl_opts['http_headers'] = headers

                                self.update_progress(item, "Downloading Audio", 1)
                                ydl_audio_opts = ydl_opts
                                ydl_audio_opts['format'] = ('(bestaudio[ext=m4a]/bestaudio)')
                                with YoutubeDL(ydl_audio_opts) as audio:
                                    encrypted_files.append({
                                        'path': audio.prepare_filename(audio.extract_info(mpd_url, download=False)),
                                        'decryption_key': decryption_key,
                                        'type': 'audio',
                                        'language': version['audio_locale']
                                    })
                                    audio.download(mpd_url)

                                crunchyroll_close_stream(token, item_id, stream_token)

                                # Download Chapters
                                if not config.get('raw_media_download') and config.get('download_chapters'):
                                    self.update_progress(item, "Downloading Chapters", 1)
                                    chapter_file = temp_file_path + f' - {version["audio_locale"]}.txt'
                                    if not os.path.exists(chapter_file):
                                        stall_timeout = config.get("download_stall_timeout")
                                        resp = requests.get(f'https://static.crunchyroll.com/skip-events/production/{version["guid"]}.json', timeout=(stall_timeout, stall_timeout))
                                        if resp.status_code == 200:
                                            chapter_data = resp.json()
                                            with open(chapter_file, 'w', encoding='utf-8') as file:
                                                file.write(';FFMETADATA1\n')
                                                for entry in ['intro', 'credits']:
                                                    if chapter_data.get(entry):
                                                        file.write(f"[CHAPTER]\nTIMEBASE=1/1\nSTART={chapter_data[entry].get('start')}\nEND={chapter_data[entry].get('end')}\ntitle={entry.title()}\nlanguage={version['audio_locale']}\n")
                                            video_files.append({
                                                'path': chapter_file,
                                                'type': 'chapter',
                                                'format': 'txt',
                                                'language': version['audio_locale']
                                            })

                        self.update_progress(item, "Decrypting", 99)

                        for encrypted_file in encrypted_files:
                            decrypted_temp_file_path = os.path.splitext(encrypted_file['path'])[0]
                            video_files.append({
                                "path": decrypted_temp_file_path,
                                "format": os.path.splitext(encrypted_file['path'])[1],
                                "type": encrypted_file['type'],
                                "language": encrypted_file.get('language')
                            })

                            command = [
                                config.get('_ffmpeg_bin_path'),
                                "-loglevel", "error",
                                "-y",
                                "-decryption_key", encrypted_file['decryption_key'],
                                "-i", encrypted_file['path'],
                                "-c", "copy",
                                "-movflags",
                                "+faststart",
                                decrypted_temp_file_path
                            ]
                            if os.name == 'nt':
                                subprocess.check_call(command, shell=False, creationflags=subprocess.CREATE_NO_WINDOW)
                            else:
                                subprocess.check_call(command, shell=False)

                            if os.path.exists(encrypted_file['path']):
                                os.remove(encrypted_file['path'])

                        # Download Subtitles
                        if config.get("download_subtitles"):
                            item['item_status'] = 'Downloading Subtitles'
                            self.update_progress(item, "Downloading Subtitles", 99)

                            finished_sub_langs = []
                            for subtitle_format in subtitle_formats:
                                lang = subtitle_format['language']
                                if lang in finished_sub_langs:
                                    continue
                                finished_sub_langs.append(lang)
                                if lang in config.get('preferred_subtitle_language').split(',') or config.get('download_all_available_subtitles'):
                                    subtitle_file = temp_file_path + f' - {lang}.{subtitle_format["extension"]}'
                                    if not os.path.exists(subtitle_file):
                                        stall_timeout = config.get("download_stall_timeout")
                                        subtitle_data = requests.get(subtitle_format['url'], timeout=(stall_timeout, stall_timeout)).text
                                        with open(subtitle_file, 'w', encoding='utf-8') as file:
                                            file.write(subtitle_data)
                                    video_files.append({
                                        'path': subtitle_file,
                                        'type': 'subtitle',
                                        'format': subtitle_format['extension'],
                                        'language': lang
                                    })

                    elif item_service == 'generic':
                        temp_file_path = ''
                        ydl_opts = {}
                        ydl_opts['format'] = (f'(bestvideo[height<={config.get("preferred_video_resolution")}][ext=mp4]+bestaudio[ext=m4a])/'
                                            f'(bestvideo[height<={config.get("preferred_video_resolution")}]+bestaudio)/'
                                            f'best')
                        ydl_opts['quiet'] = True
                        ydl_opts['no_warnings'] = True
                        ydl_opts['noprogress'] = True
                        ydl_opts['outtmpl'] = config.get('video_download_path') + os.path.sep + '%(title)s.%(ext)s'
                        ydl_opts['ffmpeg_location'] = config.get('_ffmpeg_bin_path')
                        ydl_opts['postprocessors'] = [{
                            'key': 'FFmpegMetadata',
                        }]
                        ydl_opts['progress_hooks'] = [lambda d: self.yt_dlp_progress_hook(item, d)]
                        with YoutubeDL(ydl_opts) as video:
                            item['file_path'] = video.prepare_filename(video.extract_info(item_id, download=False))
                            video.download(item_id)

                except RuntimeError as e:
                    error_str = str(e).lower()
                    # Session/auth errors are more serious - count them more heavily
                    is_session_error = any(x in error_str for x in [
                        'session', 'auth', 'failed to load audio stream', 
                        'cannot use account', 'stale'
                    ])
                    
                    logger.info(f"Download failed: {item}, Error: {str(e)}\nTraceback: {traceback.format_exc()}")
                    item['item_status'] = 'Failed'
                    self.update_progress(item, "Failed", 0)
                    
                    # Track failures - session errors count double to trigger restart faster
                    if is_session_error:
                        logger.warning("Session-related error detected, incrementing failure count twice")
                        increment_failure_count(account_index)  # Count twice for session errors
                    increment_failure_count(account_index)
                    
                    self.readd_item_to_download_queue(item)
                    continue

                if item_service != 'generic':
                    item['progress'] = 99
                    # Audio Formatting
                    if item_type in ('track', 'podcast_episode'):
                        # Lyrics
                        if item_service in ("apple_music", "spotify", "tidal") and config.get('download_lyrics'):
                            item['item_status'] = 'Getting Lyrics'
                            self.update_progress(item, "Getting Lyrics", 99)
                            extra_metadata = globals()[f"{item_service}_get_lyrics"](token, item_id, item_type, item_metadata, file_path)
                            if isinstance(extra_metadata, dict):
                                item_metadata.update(extra_metadata)

                        if not final_file_path:
                            final_file_path = build_final_file_path(file_path, item_type, default_format, item_service=item_service)
                        file_path = final_file_path

                        os.rename(temp_file_path, file_path)
                        item['file_path'] = file_path

                        # Small delay for filesystem sync (helps in edge cases)
                        time.sleep(0.1)

                        # Validate file after download
                        if not os.path.exists(file_path):
                            raise RuntimeError(f"File disappeared after rename: {file_path}")
                        file_size = os.path.getsize(file_path)
                        if file_size == 0:
                            raise RuntimeError(f"File is empty after download: {file_path}")
                        logger.debug(f"File validated after download: {file_path} ({file_size} bytes)")

                        # Convert file format and embed metadata
                        if not config.get('raw_media_download'):
                            item['item_status'] = 'Converting'
                            self.update_progress(item, "Converting", 99)

                            if config.get('use_custom_file_bitrate'):
                                bitrate = config.get("file_bitrate")
                            convert_audio_format(file_path, bitrate, default_format)

                            # Override metadata for playlist tracks
                            if item.get('parent_category') == 'playlist':
                                item_metadata['album'] = item.get('playlist_name', item_metadata.get('album'))
                                item_metadata['album_name'] = item.get('playlist_name', item_metadata.get('album'))
                                item_metadata['album_artists'] = 'Various Artists'
                                item_metadata['album_type'] = 'compilation'  # This triggers compilation=1 in embed_metadata
                                item_metadata['disc_number'] = 1
                                item_metadata['total_discs'] = 1
                                item_metadata['parent_category'] = 'playlist'  # Pass to set_music_thumbnail for cover.jpg handling
                                # Use playlist cover instead of track's album cover
                                if item.get('playlist_image_url'):
                                    item_metadata['playlist_image_url'] = item.get('playlist_image_url')
                                logger.info(f"Playlist track: setting album to '{item_metadata['album']}', album_artist='Various Artists', disc=1/1, and album_type='compilation'")

                            embed_metadata(item, item_metadata)

                            # Thumbnail
                            logger.debug(f"Checking thumbnail settings: save_album_cover={config.get('save_album_cover')}, embed_cover={config.get('embed_cover')}")
                            if config.get('save_album_cover') or config.get('embed_cover'):
                                item['item_status'] = 'Setting Thumbnail'
                                self.update_progress(item, "Setting Thumbnail", 99)
                                set_music_thumbnail(file_path, item_metadata)
                            else:
                                logger.info("Skipping thumbnail: both save_album_cover and embed_cover are disabled")

                            if os.path.splitext(file_path)[1] == '.mp3':
                                fix_mp3_metadata(file_path)
                        else:
                            if config.get('save_album_cover'):
                                item['item_status'] = 'Setting Thumbnail'
                                self.update_progress(item, "Setting Thumbnail", 99)
                                set_music_thumbnail(file_path, item_metadata)

                        # M3U
                        if config.get('create_m3u_file') and item.get('parent_category') == 'playlist' and not item.get('_m3u_written'):
                            item['item_status'] = 'Adding To M3U'
                            self.update_progress(item, "Adding To M3U", 99)
                            final_file_path = self._ensure_playlist_entry(item, item_metadata, file_path, default_format, final_file_path)

                    # Video Formatting
                    elif item_type in ('movie', 'episode'):
                        for file in video_files:
                            final_path = file['path'].replace('~', '')
                            os.rename(file['path'], final_path)
                            file['path'] = final_path

                        # Small delay for filesystem sync
                        time.sleep(0.1)
                        logger.debug(f"Video files validated after download")

                        if not config.get("raw_media_download"):
                            item['item_status'] = 'Converting'
                            self.update_progress(item, "Converting", 99)
                            if item_type == "episode":
                                output_format = config.get("show_file_format")
                            elif item_type == "movie":
                                output_format = config.get("movie_file_format")
                            convert_video_format(item, file_path, output_format, video_files, item_metadata)
                            item['file_path'] = file_path + '.' + output_format
                        else:
                            item['file_path'] = file_path + '.mp4'

                item['item_status'] = 'Downloaded'
                logger.info("Item Successfully Downloaded")
                item['progress'] = 100
                self.update_progress(item, "Downloaded", 100)
                reset_failure_count(account_index)  # Reset failure counter on successful download
                
                # Track completed playlist item and write M3U if playlist is complete
                if config.get('create_m3u_file') and item.get('parent_category') == 'playlist':
                    try:
                        add_to_m3u_file(item, item_metadata)
                    except Exception as m3u_error:
                        logger.error(f"Failed to track playlist item for M3U: {str(m3u_error)}\nTraceback: {traceback.format_exc()}")
                
                try:
                    config.set('total_downloaded_data', config.get('total_downloaded_data') + os.path.getsize(item['file_path']))
                    config.set('total_downloaded_items', config.get('total_downloaded_items') + 1)
                    config.save()
                except Exception:
                    pass
                time.sleep(config.get("download_delay"))
                self.readd_item_to_download_queue(item)
                continue
            except Exception as e:
                error_str = str(e).lower()
                # Session/auth errors are more serious - count them more heavily
                is_session_error = any(x in error_str for x in [
                    'session', 'auth', 'failed to load audio stream', 
                    'cannot use account', 'stale'
                ])
                
                logger.error(f"Unknown Exception: {str(e)}\nTraceback: {traceback.format_exc()}")
                if item['item_status'] != "Cancelled":
                    item['item_status'] = "Failed"
                    self.update_progress(item, "Failed", 0)
                    
                    # Track failures - session errors count double to trigger restart faster
                    if is_session_error:
                        logger.warning("Session-related error detected, incrementing failure count twice")
                        increment_failure_count(account_index)  # Count twice for session errors
                    increment_failure_count(account_index)
                else:
                    self.update_progress(item, "Cancelled", 0)

                time.sleep(config.get("download_delay"))
                self.readd_item_to_download_queue(item)

                # Robust cleanup: remove all related files including temp files with ~ prefix
                cleanup_paths = []

                # Add known file paths
                if temp_file_path:
                    cleanup_paths.append(temp_file_path)
                if file_path:
                    cleanup_paths.append(file_path)
                if isinstance(item.get('file_path'), str):
                    cleanup_paths.append(item['file_path'])

                # Also find any temp files with ~ prefix in the directory
                if file_path:
                    directory = os.path.dirname(file_path)
                    basename = os.path.basename(file_path)
                    # Look for temp files like ~basename*
                    temp_pattern = os.path.join(directory, "~" + basename.split('.')[0] + "*")
                    temp_files = glob.glob(temp_pattern)
                    cleanup_paths.extend(temp_files)
                    if temp_files:
                        logger.info(f"Found {len(temp_files)} temp files to clean up: {temp_files}")

                # Remove all identified files
                for cleanup_path in set(cleanup_paths):  # Use set to avoid duplicates
                    if cleanup_path and os.path.exists(cleanup_path):
                        try:
                            os.remove(cleanup_path)
                            logger.debug(f"Cleaned up file after error: {cleanup_path}")
                        except Exception as cleanup_err:
                            logger.warning(f"Could not remove file {cleanup_path}: {cleanup_err}")

                continue


    def stop(self):
        logger.info('Stopping Download Worker')
        self.is_running = False
        self.thread.join()
