import re
import time
import traceback
from .accounts import get_account_token
from .api.apple_music import apple_music_get_album_track_ids, apple_music_get_artist_album_ids, apple_music_get_playlist_data
from .api.bandcamp import bandcamp_get_album_track_ids, bandcamp_get_artist_album_ids
from .api.deezer import deezer_parse_url, deezer_get_album_track_ids, deezer_get_artist_album_ids, deezer_get_playlist_data
from .api.qobuz import qobuz_get_album_track_ids, qobuz_get_artist_album_ids, qobuz_get_label_album_ids, qobuz_get_playlist_data
from .api.soundcloud import soundcloud_parse_url,  soundcloud_get_artist_album_ids, soundcloud_get_album_track_ids, soundcloud_get_playlist_data
from .api.spotify import spotify_get_album_track_ids, spotify_get_artist_album_ids, spotify_get_playlist_items, spotify_get_playlist_data, spotify_get_liked_songs, spotify_get_your_episodes, spotify_get_podcast_episode_ids
from .api.tidal import tidal_get_album_track_ids, tidal_get_artist_album_ids, tidal_get_playlist_data, tidal_get_mix_data
from .api.youtube_music import youtube_music_get_channel_track_ids, youtube_music_get_playlist_data
from .api.generic import generic_get_track_metadata
from .api.crunchyroll import crunchyroll_get_show_episode_ids
from .runtimedata import account_pool, get_logger, parsing, download_queue, pending, parsing_lock, pending_lock
import onthespot.runtimedata as runtimedata
from .utils import format_local_id
from .otsconfig import config

logger = get_logger('parse_item')

def _debug_log(message):
    """Debug logging only when debug_playlist_flow is enabled"""
    if config.get('debug_playlist_flow', False):
        logger.info(f"[DEBUG FLOW] {message}")

# Audio
APPLE_MUSIC_URL_REGEX = re.compile(r'https?://music.apple.com/([a-z]{2})/(?P<type>album|playlist|artist)(?:/(?P<title>[-a-z0-9]+))?/(?P<id>[\w.-]+)(?:\?i=(?P<track_id>\d+))?(?:&.*)?$')
BANDCAMP_URL_REGEX = re.compile(r'https?://[a-z0-9-]+.bandcamp.com(?:/(?P<type>track|album|music)/[a-z0-9-]+)?')
DEEZER_URL_REGEX = re.compile(r'https?://www.deezer.com/(?:[a-z]{2}/)?(?P<type>album|playlist|track|artist)/(?P<id>\d+)')
DEEZER_SHARE_URL_REGEX = re.compile(r'https?://link.deezer.com/s/([-a-z0-9]+)')
QOBUZ_URL_REGEX = re.compile(r"https?://(www.|play.|open.)?qobuz.com/(?:[a-z]{2}-[a-z]{2}/)?(?P<type>album|playlist|artist|track|label|interpreter)(?:/[^/]+)?(?:/[^/]+)?/(?P<id>[-a-z0-9]+)")
SOUNDCLOUD_URL_REGEX = re.compile(r"https?://(m.)?soundcloud.com/[-\w:/]+")
SPOTIFY_URL_REGEX = re.compile(r"https?://open.spotify.com/(intl-([a-zA-Z]+)/|)(?P<type>track|album|artist|playlist|episode|show)/(?P<id>[0-9a-zA-Z]{22})(\?si=.+?)?$")
TIDAL_URL_REGEX = re.compile(r"https?://(www.|listen.)?tidal.com/(browse/)?(?P<type>album|track|artist|playlist|mix)/(?P<id>[-a-z0-9]+)")
YOUTUBE_MUSIC_URL_REGEX = re.compile(r"https?://music.youtube.com/(watch\?v=(?P<video_id>[a-zA-Z0-9_-]+)|channel/(?P<channel_id>[a-zA-Z0-9_-]+)|playlist\?list=(?P<playlist_id>[a-zA-Z0-9_-]+))")
# Video
#YOUTUBE_URL_REGEX = re.compile(r"https?://(www.|music.)?youtube.com/(watch\?v=(?P<video_id>[a-zA-Z0-9_-]+)|channel/(?P<channel_id>[a-zA-Z0-9_-]+)|playlist\?list=(?P<playlist_id>[a-zA-Z0-9_-]+))")
CRUNCHYROLL_URL_REGEX = re.compile(r"https?://(www.)?crunchyroll.com/(?P<type>watch|series)/(musicvideo/)?(?P<id>[-A-Z0-9]+)/(?P<title>[-a-z0-9]+)")

def parse_url(url):
    _debug_log(f"parse_url called with: {url}")
    
    # Audio
    if re.match(APPLE_MUSIC_URL_REGEX, url):
        match = re.search(APPLE_MUSIC_URL_REGEX, url)
        item_id = match.group("id")
        item_type = match.group("type")
        item_service = 'apple_music'
        if match.group("track_id"):
            item_id = match.group("track_id")
            item_type = 'track'

    elif re.match(BANDCAMP_URL_REGEX, url):
        match = re.search(BANDCAMP_URL_REGEX, url)
        item_id = url
        item_service = 'bandcamp'
        if not match.group("type") or match.group("type") == 'music':
            item_type = 'artist'
        else:
            item_type = match.group("type")

    elif re.match(DEEZER_URL_REGEX, url):
        match = re.search(DEEZER_URL_REGEX, url)
        item_id = match.group("id")
        item_type = match.group("type")
        item_service = 'deezer'

    elif re.match(DEEZER_SHARE_URL_REGEX, url):
        deezer_parse_url(url)
        return True

    elif re.match(QOBUZ_URL_REGEX, url):
        match = re.search(QOBUZ_URL_REGEX, url)
        item_id = match.group("id")
        item_type = match.group("type")
        item_service = 'qobuz'
        if item_type == 'interpreter':
            item_type = 'artist'

    elif re.match(SOUNDCLOUD_URL_REGEX, url):
        token = get_account_token('soundcloud')
        item_type, item_id = soundcloud_parse_url(url, token)
        item_service = "soundcloud"

    elif re.match(SPOTIFY_URL_REGEX, url):
        match = re.search(SPOTIFY_URL_REGEX, url)
        item_id = match.group("id")
        item_type = match.group("type")
        item_service = "spotify"
        if item_type == 'episode':
            item_type = 'podcast_episode'
        elif item_type == 'show':
            item_type = 'podcast'
    elif url == 'https://open.spotify.com/collection/tracks':
        item_id = None
        item_type = 'liked_songs'
        item_service = "spotify"
    elif url == 'https://open.spotify.com/collection/your-episodes':
        item_id = None
        item_type = 'your_episodes'
        item_service = "spotify"

    elif re.match(TIDAL_URL_REGEX, url):
        match = re.search(TIDAL_URL_REGEX, url)
        if match:
            item_service = 'tidal'
            item_type = match.group('type')
            item_id = match.group('id')

    elif re.match(YOUTUBE_MUSIC_URL_REGEX, url):
        match = re.search(YOUTUBE_MUSIC_URL_REGEX, url)
        if match:
            item_service = 'youtube_music'
            if match.group('video_id'):
                item_type = 'track'
                item_id = match.group('video_id')
            if match.group('channel_id'):
                item_type = 'artist'
                item_id = match.group('channel_id')
            if match.group('playlist_id'):
                item_type = 'playlist'
                item_id = match.group('playlist_id')

    # Video
    elif re.match(CRUNCHYROLL_URL_REGEX, url):
        match = re.search(CRUNCHYROLL_URL_REGEX, url)
        if match:
            item_service = 'crunchyroll'
            item_id = match.group('id') + '/' + match.group('title')
            if match.group('type') == 'watch':
                item_type = 'episode'
            elif match.group('type') == 'series':
                item_type = 'show'
            if match.group('id').startswith('MV'):
                # Music Video
                pass
    else:
        # Check if generic account is in account pool and if so
        # parse url using yt-dlp, else return 'Invalid Url'.
        try:
            generic_enabled = False
            for account in account_pool:
                if account['service'] == 'generic':
                    generic_enabled = True
            if generic_enabled:
                logger.info(f'Unable to parse url falling back to yt-dlp: {url}')
                # Check if yt-dlp can parse track
                item_metadata = generic_get_track_metadata('', url)
                # Returns false if playlist, currently not supported
                if item_metadata:
                    item_service = 'generic'
                    item_type = 'track'
                    item_id = url
                else:
                    return True
            else:
                logger.info(f'Invalid Url: {url}')
                return False
        except Exception as e:
            logger.info(f'Error Possibly Invalid Url: {url}, "{e}"')
            return False
    
    _debug_log(f"Parsed URL: service={item_service}, type={item_type}, id={item_id}")
    
    with parsing_lock:
        parsing[item_id] = {
            'item_url': url,
            'item_service': item_service,
            'item_type': item_type,
            'item_id': item_id
        }
        _debug_log(f"Added to parsing dict: {item_id}, total parsing items: {len(parsing)}")


def parsingworker():
    _debug_log("parsingworker thread started")
    while True:
        if parsing:
            try:
                with parsing_lock:
                    parsing_count = len(parsing)
                _debug_log(f"parsingworker: {parsing_count} items in parsing queue")
                
                item_id = next(iter(parsing))
                with parsing_lock:
                    item = parsing.pop(item_id)
                
                _debug_log(f"parsingworker picked up: {item}")
                logger.info(f"Parsing: {item}")

                current_service = item['item_service']
                current_type = item['item_type']
                current_id = item['item_id']
                current_url = item['item_url']
                
                _debug_log(f"Getting token for service: {current_service}")
                token = get_account_token(current_service)

                # Check if token is valid
                if token is None:
                    logger.error(f"Failed to get valid token for {current_service}. Re-queuing item for retry: {current_url}")
                    _debug_log(f"Token is None for {current_service}, re-queuing")
                    # Put the item back into parsing queue for retry
                    with parsing_lock:
                        parsing[item_id] = item
                    time.sleep(5)  # Wait before retrying to give accounts time to initialize
                    continue

                _debug_log(f"Processing: service={current_service}, type={current_type}, id={current_id}")

                if current_service == "spotify":
                    if current_type == "playlist":
                        # For playlists: minimal fetch, instant queue, lazy metadata
                        # This avoids API hammering and provides instant user feedback
                        _debug_log(f"Handling Spotify playlist: {current_id}")
                        
                        # NO batch_parse flag for playlists (we add directly to download_queue)
                        try:
                            logger.info(f"Starting to parse playlist: {current_id}")
                            _debug_log(f"Calling spotify_get_playlist_items for: {current_id}")
                            items = spotify_get_playlist_items(token, current_id)
                            _debug_log(f"Got {len(items)} minimal items from spotify_get_playlist_items")
                            
                            _debug_log(f"Calling spotify_get_playlist_data for: {current_id}")
                            playlist_name, playlist_by, playlist_image_url = spotify_get_playlist_data(token, current_id)
                            _debug_log(f"Playlist data: name='{playlist_name}', by='{playlist_by}'")
                            
                            total_items = len(items)
                            logger.info(f"Playlist '{playlist_name}' has {total_items} items, adding to download queue with MINIMAL data (metadata will be fetched per-track during download)")
                            _debug_log(f"About to add {total_items} items DIRECTLY to download_queue")
                            
                            added_count = 0
                            skipped_count = 0
                            
                            # Add items DIRECTLY to download_queue with minimal info
                            # Metadata will be fetched by DownloadWorker right before downloading each track
                            with download_queue_lock:
                                for index, item in enumerate(items):
                                    try:
                                        track_obj = item.get('track')
                                        if track_obj is None:
                                            logger.warning(f"Skipping None track at position {index + 1} in playlist {playlist_name}")
                                            skipped_count += 1
                                            continue
                                        
                                        item_id = track_obj.get('id')
                                        if not item_id:
                                            logger.warning(f"Skipping track with no ID at position {index + 1} in playlist {playlist_name}")
                                            skipped_count += 1
                                            continue
                                        
                                        item_type = track_obj.get('type', 'track')
                                        local_id = format_local_id(item_id)
                                        
                                        # Add to download_queue with MINIMAL info
                                        # Metadata will be fetched just-in-time by DownloadWorker
                                        download_queue[local_id] = {
                                            'local_id': local_id,
                                            'available': True,
                                            'item_service': 'spotify',
                                            'item_type': item_type,
                                            'item_id': item_id,
                                            'item_status': 'Waiting',
                                            'file_path': None,
                                            'item_name': f'Track {index + 1}',  # Placeholder, will be updated
                                            'item_by': '',  # Will be fetched during download
                                            'parent_category': 'playlist',
                                            'playlist_name': playlist_name,
                                            'playlist_by': playlist_by,
                                            'playlist_number': str(index + 1),
                                            'playlist_total': total_items,
                                            'item_thumbnail': playlist_image_url,  # Use playlist cover as placeholder
                                            'item_url': '',  # Will be fetched during download
                                            'progress': 0,
                                            'last_update_time': time.time(),
                                            'needs_metadata_fetch': True  # Flag for DownloadWorker
                                        }
                                        added_count += 1
                                        
                                        if config.get('debug_playlist_flow', False) and (index < 3 or index >= total_items - 1):
                                            _debug_log(f"Added track {index + 1}/{total_items} to download_queue (ID: {item_id})")
                                            
                                    except (TypeError, KeyError) as e:
                                        logger.error(f'Error processing track at index {index} in playlist: {e}')
                                        skipped_count += 1
                                        continue
                            
                            logger.info(f"Instantly added {added_count} items from playlist '{playlist_name}' to download_queue (skipped: {skipped_count})")
                            _debug_log(f"Download queue now has {len(download_queue)} items total")
                            
                            # Download playlist cover after adding all items
                            if playlist_image_url and config.get('save_album_cover'):
                                import requests
                                from PIL import Image
                                from io import BytesIO
                                import os
                                from .utils import sanitize_data
                                
                                try:
                                    # Calculate playlist directory from formatter
                                    playlist_path_template = config.get('playlist_path_formatter')
                                    # Extract just the directory part (everything before the last /)
                                    dir_template = playlist_path_template.rsplit('/', 1)[0] if '/' in playlist_path_formatter else ''
                                    
                                    # Format with available variables
                                    playlist_dir = dir_template.format(
                                        playlist_name=sanitize_data(playlist_name),
                                        playlist_owner=sanitize_data(playlist_by),
                                        playlist_by=sanitize_data(playlist_by)
                                    )
                                    
                                    # Get full path
                                    dl_root = config.get('audio_download_path')
                                    full_playlist_dir = os.path.join(dl_root, playlist_dir)
                                    os.makedirs(full_playlist_dir, exist_ok=True)
                                    
                                    cover_path = os.path.join(full_playlist_dir, 'cover.jpg')
                                    logger.info(f"Downloading playlist cover for '{playlist_name}' to: {cover_path}")
                                    img = Image.open(BytesIO(requests.get(playlist_image_url).content))
                                    if img.mode != 'RGB':
                                        img = img.convert('RGB')
                                    img.save(cover_path, format='JPEG')
                                    logger.info(f"Saved playlist cover: {cover_path}")
                                except Exception as e:
                                    logger.error(f"Failed to save playlist cover: {e}")
                        except Exception as e:
                            logger.error(f"Exception parsing playlist {current_id}: {e}\nTraceback: {traceback.format_exc()}")
                            # Don't re-raise - playlist is already in download_queue
                            logger.warning(f"Playlist {current_id} had errors but items are queued")
                        
                        # No finally block needed - we don't set batch_parse flag for playlists
                        continue
                    elif current_type == "liked_songs":
                        # Set batch parse flag
                        runtimedata.set_batch_parse_flag(True)
                        
                        try:
                            logger.info("Starting to parse liked_songs")
                            tracks = spotify_get_liked_songs(token)
                            total_tracks = len(tracks)
                            logger.info(f"Liked Songs has {total_tracks} items, adding to pending queue...")
                            for index, track in enumerate(tracks):
                                track_obj = track.get('track')
                                if not track_obj:
                                    logger.warning(f"Skipping None track at position {index + 1} in Liked Songs")
                                    continue
                                
                                item_id = track_obj.get('id')
                                if not item_id:
                                    logger.warning(f"Skipping track with no ID at position {index + 1} in Liked Songs")
                                    continue
                                
                                local_id = format_local_id(item_id)
                                with pending_lock:
                                    pending[local_id] = {
                                        'local_id': local_id,
                                        'item_service': 'spotify',
                                        'item_type': 'track',
                                        'item_id': item_id,
                                        'parent_category': 'playlist',
                                        'playlist_name': 'Liked Songs',
                                        'playlist_by': 'me',
                                        'playlist_number': str(index + 1),
                                        'playlist_total': total_tracks,
                                        # Cache the full track object from liked songs response
                                        'cached_track_data': track_obj
                                    }
                            logger.info(f"Finished adding {total_tracks} items from Liked Songs to pending queue")
                        except Exception as e:
                            logger.error(f"Exception parsing liked_songs: {e}\nTraceback: {traceback.format_exc()}")
                            raise
                        finally:
                            # Always clear batch parse flag
                            runtimedata.set_batch_parse_flag(False)
                            logger.info("Cleared batch_parse_in_progress flag for liked_songs")
                        
                        continue
                    elif current_type == "your_episodes":
                        # Set batch parse flag
                        runtimedata.set_batch_parse_flag(True)
                        
                        try:
                            logger.info("Starting to parse your_episodes")
                            tracks = spotify_get_your_episodes(token)
                            total_tracks = len(tracks)
                            logger.info(f"Your Episodes has {total_tracks} items, adding to pending queue...")
                            for index, track in enumerate(tracks):
                                episode_obj = track.get('episode')
                                if not episode_obj:
                                    logger.warning(f"Skipping None episode at position {index + 1} in Your Episodes")
                                    continue
                                
                                item_id = episode_obj.get('id')
                                if not item_id:
                                    logger.warning(f"Skipping episode with no ID at position {index + 1} in Your Episodes")
                                    continue
                                    
                                local_id = format_local_id(item_id)
                                with pending_lock:
                                    pending[local_id] = {
                                        'local_id': local_id,
                                        'item_service': 'spotify',
                                        'item_type': 'podcast_episode',
                                        'item_id': item_id,
                                        'parent_category': 'playlist',
                                        'playlist_name': 'Your Episodes',
                                        'playlist_by': 'me',
                                        'playlist_number': str(index + 1),
                                        'playlist_total': total_tracks
                                    }
                            logger.info(f"Finished adding {total_tracks} items from Your Episodes to pending queue")
                        except Exception as e:
                            logger.error(f"Exception parsing your_episodes: {e}\nTraceback: {traceback.format_exc()}")
                            raise
                        finally:
                            # Always clear batch parse flag
                            runtimedata.set_batch_parse_flag(False)
                            logger.info("Cleared batch_parse_in_progress flag for your_episodes")
                        
                        continue

                if current_service == 'youtube_music' and current_type == 'artist':
                    # Set batch parse flag
                    runtimedata.set_batch_parse_flag(True)
                    
                    try:
                        logger.info(f"Starting to parse artist: {current_id}")
                        track_ids = youtube_get_channel_track_ids(token, current_id)
                        total_items = len(track_ids)
                        logger.info(f"Artist has {total_items} items, adding to pending queue...")
                        for track_id in track_ids:
                            local_id = format_local_id(track_id)
                            with pending_lock:
                                pending[local_id] = {
                                    'local_id': local_id,
                                    'item_service': current_service,
                                    'item_type': 'track',
                                    'item_id': track_id,
                                    'parent_category': 'album'
                                    }
                        logger.info(f"Finished adding {total_items} items from artist to pending queue")
                    except Exception as e:
                        logger.error(f"Exception parsing artist {current_id}: {e}\nTraceback: {traceback.format_exc()}")
                        raise
                    finally:
                        # Always clear batch parse flag
                        runtimedata.set_batch_parse_flag(False)
                        logger.info(f"Cleared batch_parse_in_progress flag for artist {current_id}")
                    
                    continue

                if current_type in ["track", "podcast_episode", "movie", "episode"]:
                    local_id = format_local_id(item_id)
                    
                    # Preserve playlist context if it exists (from cached queue)
                    parent_category = item.get('parent_category', current_type)
                    playlist_name = item.get('playlist_name')
                    playlist_by = item.get('playlist_by')
                    playlist_number = item.get('playlist_number')
                    playlist_total = item.get('playlist_total')
                    
                    with pending_lock:
                        pending[local_id] = {
                            'local_id': local_id,
                            'item_service': current_service,
                            'item_type': current_type,
                            'item_id': item_id,
                            'parent_category': parent_category,
                            'playlist_name': playlist_name,
                            'playlist_by': playlist_by,
                            'playlist_number': playlist_number,
                            'playlist_total': playlist_total
                            }
                    continue

                elif current_type in ["podcast", "audiobook"]:
                    # Set batch parse flag
                    runtimedata.set_batch_parse_flag(True)
                    
                    try:
                        logger.info(f"Starting to parse {current_type}: {current_id}")
                        item_ids = globals()[f"{current_service}_get_{current_type}_episode_ids"](token, current_id)
                        total_items = len(item_ids)
                        logger.info(f"{current_type} has {total_items} items, adding to pending queue...")
                        for item_id in item_ids:
                            local_id = format_local_id(item_id)
                            with pending_lock:
                                pending[local_id] = {
                                    'local_id': local_id,
                                    'item_service': current_service,
                                    'item_type': 'podcast_episode',
                                    'item_id': item_id,
                                    'parent_category': current_type
                                    }
                        logger.info(f"Finished adding {total_items} items from {current_type} to pending queue")
                    except Exception as e:
                        logger.error(f"Exception parsing {current_type} {current_id}: {e}\nTraceback: {traceback.format_exc()}")
                        raise
                    finally:
                        # Always clear batch parse flag
                        runtimedata.set_batch_parse_flag(False)
                        logger.info(f"Cleared batch_parse_in_progress flag for {current_type} {current_id}")
                    
                    continue

                elif current_type in ["album", "playlist", "mix"]:
                    # Set batch parse flag
                    runtimedata.set_batch_parse_flag(True)
                    
                    try:
                        playlist_name = ''
                        playlist_by = ''
                        track_objects = []  # Store full track objects for optimization
                        
                        if current_type == "album" and current_service == "spotify":
                            logger.info(f"Starting to parse album with metadata: {current_id}")
                            from .api.spotify import spotify_get_album_tracks_with_metadata
                            track_objects = spotify_get_album_tracks_with_metadata(token, current_id)
                            track_ids = [t['id'] for t in track_objects] if track_objects else []
                        elif current_type == "album":
                            logger.info(f"Starting to parse album: {current_id}")
                            track_ids = globals()[f"{current_service}_get_{current_type}_track_ids"](token, current_id)
                        else:
                            logger.info(f"Starting to parse {current_type}: {current_id}")
                            playlist_name, playlist_by, track_ids = globals()[f"{current_service}_get_{current_type}_data"](token, current_id)
                        
                        if current_type == 'mix':
                            current_type = 'playlist'
                        if current_service == 'youtube' and not playlist_by:
                            current_type = 'album'

                        total_items = len(track_ids)
                        logger.info(f"{current_type} has {total_items} items, adding to pending queue...")
                        
                        for index, track_id in enumerate(track_ids):
                            local_id = format_local_id(track_id)
                            
                            # Find corresponding track object if we have it
                            cached_track = None
                            if track_objects:
                                cached_track = next((t for t in track_objects if t['id'] == track_id), None)
                            
                            pending_item = {
                                'local_id': local_id,
                                'item_service': current_service,
                                'item_type': 'track',
                                'item_id': track_id,
                                'parent_category': current_type,
                                'parent_id': current_id if current_type == 'album' else '',
                                'playlist_name': playlist_name,
                                'playlist_by': playlist_by,
                                'playlist_number': str(index + 1),
                                'playlist_total': total_items
                            }
                            
                            # Add cached track data if available (optimization!)
                            if cached_track:
                                pending_item['cached_track_data'] = cached_track
                            
                            with pending_lock:
                                pending[local_id] = pending_item
                        
                        logger.info(f"Finished adding {total_items} items from {current_type} to pending queue")
                    except Exception as e:
                        logger.error(f"Exception parsing {current_type} {current_id}: {e}\nTraceback: {traceback.format_exc()}")
                        raise
                    finally:
                        # Always clear batch parse flag
                        runtimedata.set_batch_parse_flag(False)
                        logger.info(f"Cleared batch_parse_in_progress flag for {current_type} {current_id}")
                    
                    continue

                elif current_type in ["artist", "label"]:
                    item_ids = globals()[f"{current_service}_get_{current_type}_album_ids"](token, current_id)
                    for item_id in item_ids:
                        local_id = format_local_id(item_id)
                        with parsing_lock:
                            parsing[item_id] = {
                                'item_url': '',
                                'item_service': current_service,
                                'item_type': 'album',
                                'item_id': item_id
                            }

                elif current_type in ['show', 'season']:
                    item_ids = globals()[f"{current_service}_get_{current_type}_episode_ids"](token, current_id)
                    for item_id in item_ids:
                        local_id = format_local_id(item_id)
                        with pending_lock:
                            pending[local_id] = {
                                'local_id': local_id,
                                'item_service': current_service,
                                'item_type': 'episode',
                                'item_id': item_id,
                                'parent_category': current_type
                                }
                    continue
            except Exception as e:
                logger.error(f"Unknown Exception: {str(e)}\nTraceback: {traceback.format_exc()}")
                continue
        else:
            time.sleep(0.2)
