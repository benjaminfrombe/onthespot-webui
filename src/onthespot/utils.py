import base64
import json
import os
import platform
import requests
import ssl
import subprocess
import time
from hashlib import md5
from io import BytesIO
from PIL import Image
from mutagen.flac import Picture
from mutagen.id3 import ID3, ID3NoHeaderError, WOAS, USLT, TCMP, COMM
from mutagen.oggvorbis import OggVorbis
import music_tag
from .otsconfig import config
from .runtimedata import get_logger, pending, download_queue

logger = get_logger("utils")


class SSLAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, ssl_context, *args, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        context = self.ssl_context
        return super().init_poolmanager(*args, ssl_context=context, **kwargs)


def make_call(url, params=None, headers=None, session=None, skip_cache=False, text=False, use_ssl=False):
    if not skip_cache:
        request_key = md5(f'{url}'.encode()).hexdigest()
        req_cache_file = os.path.join(config.get('_cache_dir'), 'reqcache', request_key + '.json')
        os.makedirs(os.path.dirname(req_cache_file), exist_ok=True)
        if os.path.isfile(req_cache_file):
            logger.debug(f'URL "{url}" cache found! HASH: {request_key}')
            try:
                with open(req_cache_file, 'r', encoding='utf-8') as cf:
                    if text:
                        return cf.read()
                    json_data = json.load(cf)
                return json_data
            except json.JSONDecodeError:
                logger.error(f'URL "{url}" cache has invalid data')
                return None
        logger.debug(f'URL "{url}" has cache miss! HASH: {request_key}; Fetching data')

    if session is None:
        session = requests.Session()

    if use_ssl:
        ctx = ssl.create_default_context()
        ctx.verify_mode = ssl.CERT_REQUIRED
        session.mount('https://', SSLAdapter(ssl_context=ctx))

    response = session.get(url, headers=headers, params=params)

    if response.status_code == 200:
        if not skip_cache:
            with open(req_cache_file, 'w', encoding='utf-8') as cf:
                cf.write(response.text)
        if text:
            return response.text
        return json.loads(response.text)
    else:
        logger.info(f"Request status error {response.status_code}: {url}")
        return None


def format_local_id(item_id):
    suffix = 0
    local_id = f"{item_id}-{suffix}"
    while local_id in download_queue or local_id in pending:
        suffix += 1
        local_id = f"{item_id}-{suffix}"
    return local_id


def is_latest_release():
    url = "https://api.github.com/repos/justin025/onthespot/releases/latest"
    response = requests.get(url)
    if response.status_code == 200:
        current_version = config.get("version").replace('v', '').replace('.', '')
        latest_version = response.json()['name'].replace('v', '').replace('.', '')
        if int(latest_version) > int(current_version):
            logger.info(f"Update Available: {int(latest_version)} > {int(current_version)}")
            return False
    return True


def open_item(item):
    if platform.system() == 'Windows':
        os.startfile(item)
    elif platform.system() == 'Darwin':  # For MacOS
        subprocess.Popen(['open', item])
    else:  # For Linux and other Unix-like systems
        subprocess.Popen(['xdg-open', item])


def sanitize_data(value):
    if value is None:
        return ''
    char = config.get("illegal_character_replacement")
    if os.name == 'nt':
        illegal_chars = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
        for illegal_char in illegal_chars:
            value = value.replace(illegal_char, char)
        while value.endswith('.') or value.endswith(' '):
            value = value[:-1]
    else:
        value = value.replace('/', char)
    return value


def translate(string):
    try:
        response = requests.get(
            f"https://translate.googleapis.com/translate_a/single?dj=1&dt=t&dt=sp&dt=ld&dt=bd&client=dict-chrome-ex&sl=auto&tl={config.get('language')}&q={string}"
        )
        return response.json()["sentences"][0]["trans"]
    except (requests.exceptions.RequestException, KeyError, IndexError):
        return string


def conv_list_format(items):
    if len(items) == 0:
        return ''
    return (config.get('metadata_separator')).join(items)


def format_item_path(item, item_metadata):
    if config.get("translate_file_path"):
        name = translate(item_metadata.get('title'))
        album = translate(item_metadata.get('album_name'))
    else:
        name = item_metadata.get('title')
        album = item_metadata.get('album_name')

    if item['parent_category'] == 'playlist' and config.get("use_playlist_path"):
        path = config.get("playlist_path_formatter")
    elif item['item_type'] == 'track':
        path = config.get("track_path_formatter")
    elif item['item_type'] == 'podcast_episode':
        path = config.get("podcast_path_formatter")
    elif item['item_type'] == 'movie':
        path = config.get("movie_path_formatter")
    elif item['item_type'] == 'episode':
        path = config.get("show_path_formatter")

    # Calculate dynamic padding for playlist numbers based on total items
    playlist_number = item.get('playlist_number', '')
    if playlist_number:
        playlist_total = item.get('playlist_total')
        if playlist_total:
            # Determine padding width based on total count
            padding_width = len(str(playlist_total))
            playlist_number = str(playlist_number).zfill(padding_width)
            logger.debug(f"Playlist number padding: {item.get('playlist_number')} -> {playlist_number} (total: {playlist_total}, width: {padding_width})")
        else:
            playlist_number = str(playlist_number)
            logger.warning(f"Missing playlist_total for item {item.get('item_id')}, using unpadded number: {playlist_number}")
    playlist_number = sanitize_data(playlist_number)
    
    # Calculate dynamic padding for track numbers
    # For playlists, use playlist_total for padding; for albums, use total_tracks
    track_number = item_metadata.get('track_number', 1)
    
    if item.get('parent_category') == 'playlist' and item.get('playlist_total'):
        # For playlists, use playlist_total to determine padding
        playlist_total = item.get('playlist_total')
        padding_width = len(str(playlist_total))
        track_number = str(track_number).zfill(padding_width)
        logger.debug(f"Playlist track number padding: {item_metadata.get('track_number')} -> {track_number} (playlist_total: {playlist_total}, width: {padding_width})")
    elif config.get('use_double_digit_path_numbers'):
        # For albums, use total_tracks for padding
        total_tracks = item_metadata.get('total_tracks', 1)
        padding_width = max(2, len(str(total_tracks)))
        track_number = str(track_number).zfill(padding_width)
        logger.debug(f"Album track number padding: {item_metadata.get('track_number')} -> {track_number} (total_tracks: {total_tracks}, width: {padding_width})")
    else:
        track_number = str(track_number)
    
    item_path = path.format(
        # Universal
        service=sanitize_data(item.get('item_service')).title(),
        service_id=str(item_metadata.get('item_id')),
        name=sanitize_data(name),
        year=sanitize_data(item_metadata.get('release_year')),
        explicit=sanitize_data(str(config.get('explicit_label')) if item_metadata.get('explicit') else ''),

        # Audio
        artist=sanitize_data(item_metadata.get('artists')),
        album=sanitize_data(album),
        album_artist=sanitize_data(item_metadata.get('album_artists')),
        album_type=item_metadata.get('album_type', 'single').title(),
        disc_number=item_metadata.get('disc_number', 1) if not config.get('use_double_digit_path_numbers') else str(item_metadata.get('disc_number', 1)).zfill(max(2, len(str(item_metadata.get('total_discs', 1))))),
        track_number=track_number,
        genre=sanitize_data(item_metadata.get('genre')),
        label=sanitize_data(item_metadata.get('label')),
        trackcount=item_metadata.get('total_tracks', 1) if not config.get('use_double_digit_path_numbers') else str(item_metadata.get('total_tracks', 1)).zfill(max(2, len(str(item_metadata.get('total_tracks', 1))))),
        disccount=item_metadata.get('total_discs', 1) if not config.get('use_double_digit_path_numbers') else str(item_metadata.get('total_discs', 1)).zfill(max(2, len(str(item_metadata.get('total_discs', 1))))),
        isrc=str(item_metadata.get('isrc')),
        playlist_name=sanitize_data(item.get('playlist_name')),
        playlist_owner=sanitize_data(item.get('playlist_by')),
        playlist_number=playlist_number,

        # Show
        show_name=sanitize_data(item_metadata.get('show_name')),
        season_number=item_metadata.get('season_number', 1) if not config.get('use_double_digit_path_numbers') else str(item_metadata.get('season_number', 1)).zfill(2),
        episode_number=item_metadata.get('episode_number', 1) if not config.get('use_double_digit_path_numbers') else str(item_metadata.get('episode_number', 1)).zfill(2),
    )

    return item_path


def convert_audio_format(filename, bitrate, default_format):
    if os.path.isfile(os.path.abspath(filename)):
        target_path = os.path.abspath(filename)
        file_name = os.path.basename(target_path)
        filetype = os.path.splitext(file_name)[1]
        file_stem = os.path.splitext(file_name)[0]

        temp_name = os.path.join(os.path.dirname(target_path), "~" + file_stem + filetype)

        # Robust cleanup of existing temp file (may be corrupted from previous failed attempt)
        if os.path.isfile(temp_name):
            try:
                os.remove(temp_name)
                logger.debug(f"Removed existing temp file before conversion: {temp_name}")
                time.sleep(0.05)  # Give filesystem time to sync
            except (OSError, PermissionError) as e:
                logger.error(f"Failed to remove corrupted temp file {temp_name}: {e}")
                # Try to force remove with different strategy
                try:
                    if os.path.exists(temp_name):
                        os.chmod(temp_name, 0o666)  # Ensure writable
                        os.remove(temp_name)
                        logger.info(f"Successfully removed temp file after chmod: {temp_name}")
                except Exception as e2:
                    logger.error(f"Could not remove temp file even after chmod: {e2}")
                    raise RuntimeError(f"Cannot proceed: corrupted temp file exists and cannot be removed: {temp_name}")

        # Validate source file before attempting conversion
        if os.path.getsize(filename) == 0:
            raise RuntimeError(f"Cannot convert: source file is empty: {filename}")

        os.rename(filename, temp_name)

        # Add small delay to ensure file is fully accessible after rename
        time.sleep(0.1)

        # Validate file is actually readable before FFmpeg processing
        # This catches Docker volume sync issues early
        try:
            with open(temp_name, 'rb') as test_file:
                # Try to read first few bytes to ensure file is accessible
                header = test_file.read(1024)
                if len(header) == 0:
                    raise RuntimeError(f"File is empty or unreadable: {temp_name}")
            logger.debug(f"File readability validated before FFmpeg: {temp_name}")
        except (OSError, IOError) as e:
            raise RuntimeError(f"File not readable before FFmpeg conversion (Docker volume sync issue?): {temp_name}: {e}")

        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            try:
                # Clean up output file if it exists from a previous failed attempt
                # This is critical for Docker environments where partial files may persist
                if os.path.isfile(filename):
                    try:
                        os.remove(filename)
                        logger.debug(f"Removed existing output file before FFmpeg conversion: {filename}")
                        # Small delay to ensure filesystem sync, especially for Docker volumes
                        time.sleep(0.05)
                    except OSError as e:
                        logger.warning(f"Could not remove existing output file (attempt {attempt + 1}): {e}")
                        if attempt < max_retries - 1:
                            time.sleep(0.5 * (2 ** attempt))
                            continue
                        else:
                            raise

                # Prepare default parameters
                # Existing command initialization
                command = [config.get('_ffmpeg_bin_path'), '-y', '-i', temp_name]

                # Set log level based on environment variable
                if int(os.environ.get('SHOW_FFMPEG_OUTPUT', 0)) == 0:
                    command += ['-loglevel', 'error', '-hide_banner', '-nostats']

                # Check if media format is service default

                if filetype == default_format and config.get('use_custom_file_bitrate'):
                    command += ['-b:a', bitrate]
                elif filetype == default_format:
                    command += ['-c:a', 'copy']
                else:
                    command += [
                        #'-f', filetype.split('.')[1],
                        '-ac', '2',
                        '-ar', f'{config.get("file_hertz") if filetype != ".opus" else 48000}',
                        '-b:a', bitrate
                        ]

                # Add user defined parameters
                for param in config.get('ffmpeg_args'):
                    command.append(param)

                # Add output parameter at last
                command += [filename]
                logger.debug(
                    f'Converting media with ffmpeg. Built commandline {command}'
                    )
                # Run subprocess with CREATE_NO_WINDOW flag on Windows
                if os.name == 'nt':
                    subprocess.check_call(command, shell=False, creationflags=subprocess.CREATE_NO_WINDOW)
                else:
                    subprocess.check_call(command, shell=False)
                os.remove(temp_name)
                break  # Success, exit retry loop
            except subprocess.CalledProcessError as e:
                last_error = e
                # Log detailed error information
                logger.error(f"FFmpeg failed with exit code {e.returncode}: {e}")

                # Exit code 183 typically means file corruption or access issues
                if e.returncode == 183:
                    logger.error(f"FFmpeg exit 183 detected - likely file corruption or access issue with {temp_name}")

                if attempt < max_retries - 1:
                    # Wait before retrying (longer delays for Docker volume sync issues)
                    # Start at 1s instead of 0.5s to give Docker volumes more time to sync
                    wait_time = 1.0 * (2 ** attempt)  # 1s, 2s, 4s progression
                    logger.warning(f"FFmpeg conversion failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")

                    # Clean up partial output before retry
                    if os.path.isfile(filename):
                        try:
                            os.remove(filename)
                            logger.debug(f"Removed partial output file before retry: {filename}")
                        except Exception as cleanup_err:
                            logger.warning(f"Could not remove partial output file: {cleanup_err}")

                    time.sleep(wait_time)
                else:
                    # Final attempt failed, thorough cleanup and raise
                    logger.error(f"All {max_retries} FFmpeg conversion attempts failed for {temp_name}")

                    for cleanup_file in [temp_name, filename]:
                        if os.path.isfile(cleanup_file):
                            try:
                                os.remove(cleanup_file)
                                logger.debug(f"Cleaned up file after final failure: {cleanup_file}")
                            except Exception as cleanup_err:
                                logger.error(f"Could not clean up {cleanup_file}: {cleanup_err}")

                    raise RuntimeError(f"Failed to convert audio file after {max_retries} attempts (corrupted or invalid data): {e}")


def convert_video_format(item, output_path, output_format, video_files, item_metadata):
    target_path = os.path.abspath(output_path)
    file_name = os.path.basename(target_path)
    filetype = os.path.splitext(file_name)[1]
    file_stem = os.path.splitext(file_name)[0]

    temp_file_path = os.path.join(os.path.dirname(target_path), "~" + file_stem + filetype) + '.' + output_format

    # Clean up temp file if it exists from previous failed attempt
    if os.path.isfile(temp_file_path):
        os.remove(temp_file_path)

    # Prepare default parameters
    # Existing command initialization
    command = [config.get('_ffmpeg_bin_path'), '-y']

    current_type = ''
    format_map = []
    for map_index, file in enumerate(video_files):
        if current_type != file["type"]:
            i = 0
            current_type = file["type"]
        command += ['-i', file['path']]

        if current_type != 'chapter':
            format_map += ['-map', f'{map_index}:{current_type[:1]}']
            if file.get('language'):
                language = file.get('language')
                format_map += [f'-metadata:s:{current_type[:1]}:{i}', f'title={file.get("language")}']
                format_map += [f'-metadata:s:{current_type[:1]}:{i}', f'language={file.get("language")[:2]}']

        i += 1

    format_map += [f'-metadata', f'title={item_metadata.get("title")}']
    #format_map += [f'-metadata', f'genre={item_metadata.get("genre")}']
    format_map += [f'-metadata', f'copyright={item_metadata.get("copyright")}']
    format_map += [f'-metadata', f'description={item_metadata.get("description")}']
    #format_map += [f'-metadata', f'year={item_metadata.get("release_year")}']
    # TV Show Specific Tags
    if item['item_type'] == 'episode':
        format_map += [f'-metadata', f'show={item_metadata.get("show_name")}']
        format_map += [f'-metadata', f'episode_id={item_metadata.get("episode_number")}']
        format_map += [f'-metadata', f'tvsn={item_metadata.get("season_number")}']

    command += format_map

    # Set log level based on environment variable
    if int(os.environ.get('SHOW_FFMPEG_OUTPUT', 0)) == 0:
        command += ['-loglevel', 'error', '-hide_banner', '-nostats']

    # Add user defined parameters
    for param in config.get('ffmpeg_args'):
        command.append(param)

    command += ['-c', 'copy']
    if output_format == 'mp4':
        command += ['-c:s', 'mov_text']

    # Add output parameter at last
    command += [temp_file_path]
    logger.debug(
        f'Converting media with ffmpeg. Built commandline {command}'
        )
    try:
        # Run subprocess with CREATE_NO_WINDOW flag on Windows
        if os.name == 'nt':
            subprocess.check_call(command, shell=False, creationflags=subprocess.CREATE_NO_WINDOW)
        else:
            subprocess.check_call(command, shell=False)

        for file in video_files:
            if os.path.exists(file['path']):
                os.remove(file['path'])

        os.rename(temp_file_path, output_path + '.' + output_format)
    except subprocess.CalledProcessError as e:
        # Clean up temp output file and input files
        if os.path.isfile(temp_file_path):
            os.remove(temp_file_path)
        raise RuntimeError(f"Failed to convert video file: {e}")


def embed_metadata(item, metadata):
    logger.info(f"embed_metadata() called for file: {item.get('file_path')}")
    if os.path.isfile(os.path.abspath(item['file_path'])):
        target_path = os.path.abspath(item['file_path'])
        file_name = os.path.basename(target_path)
        filetype = os.path.splitext(file_name)[1]
        file_stem = os.path.splitext(file_name)[0]
        logger.info(f"Embedding metadata for {filetype} file: {file_name}")

        temp_name = os.path.join(os.path.dirname(target_path), "~" + file_stem + filetype)

        if os.path.isfile(temp_name):
            os.remove(temp_name)

        os.rename(item['file_path'], temp_name)
        # Prepare default parameters
        # Existing command initialization
        command = [config.get('_ffmpeg_bin_path'), '-y', '-i', temp_name]

        if int(os.environ.get('SHOW_FFMPEG_OUTPUT', 0)) == 0:
            command += ['-loglevel', 'error', '-hide_banner', '-nostats']

        command += ['-c:a', 'copy']

        # Append metadata
        if config.get("embed_branding"):
            branding = "Downloaded by OnTheSpot, https://github.com/justin025/onthespot"
            if filetype == '.mp3':
                # Incorrectly embedded to TXXX:TCMP, patch sent upstream
                command += ['-metadata', 'COMM={}'.format(branding)]
            else:
                command += ['-metadata', 'comment={}'.format(branding)]

        if config.get("embed_service_id"):
            command += ['-metadata', f'{item["item_service"]}id={item["item_id"]}']

        for key in metadata.keys():
            value = metadata[key]

            if key == 'artists' and config.get("embed_artist"):
                command += ['-metadata', 'artist={}'.format(value)]

            elif key in ['album_name', 'album'] and config.get("embed_album"):
                command += ['-metadata', 'album={}'.format(value)]

            elif key in ['album_artists'] and config.get("embed_albumartist"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TPE2={}'.format(value)]
                else:
                    command += ['-metadata', 'album_artist={}'.format(value)]

            elif key in ['title', 'track_title', 'tracktitle'] and config.get("embed_name"):
                command += ['-metadata', 'title={}'.format(value)]

            elif key in ['year', 'release_year'] and config.get("embed_year"):
                command += ['-metadata', 'date={}'.format(value)]

            elif key in ['discnumber', 'disc_number', 'disknumber', 'disk_number'] and config.get("embed_discnumber"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TPOS={}/{}'.format(value, metadata['total_discs'])]
                else:
                    command += ['-metadata', 'disc={}/{}'.format(value, metadata['total_discs'])]

            elif key in ['track_number', 'tracknumber'] and config.get("embed_tracknumber"):
                command += ['-metadata', 'track={}/{}'.format(value, metadata.get('total_tracks'))]

            elif key == 'genre' and config.get("embed_genre"):
                command += ['-metadata', 'genre={}'.format(value)]

            elif key == 'performers' and config.get("embed_performers"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TPE1={}'.format(value)]
                else:
                    command += ['-metadata', 'performer={}'.format(value)]

            elif key == 'producers' and config.get("embed_producers"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TIPL={}'.format(value)]
                else:
                    command += ['-metadata', 'producer={}'.format(value)]

            elif key == 'writers' and config.get("embed_writers"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TEXT={}'.format(value)]
                else:
                    command += ['-metadata', 'author={}'.format(value)]

            elif key == 'label' and config.get("embed_label"):
                if filetype == '.mp3':
                    command += ['-metadata', 'publisher={}'.format(value)]

            elif key == 'copyright' and config.get("embed_copyright"):
                command += ['-metadata', 'copyright={}'.format(value)]

            elif key == 'description' and config.get("embed_description"):
                if filetype == '.mp3':
                    # Incorrectly embedded to TXXX:COMM, patch sent upstream
                    command += ['-metadata', 'COMM={}'.format(value)]
                else:
                    command += ['-metadata', 'comment={}'.format(value)]

            elif key == 'language' and config.get("embed_language"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TLAN={}'.format(value)]
                else:
                    command += ['-metadata', 'language={}'.format(value)]

            elif key == 'isrc' and config.get("embed_isrc"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TSRC={}'.format(value)]
                else:
                    command += ['-metadata', 'isrc={}'.format(value)]

            elif key == 'length' and config.get("embed_length"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TLEN={}'.format(value)]
                else:
                    command += ['-metadata', 'length={}'.format(value)]

            elif key == 'bpm' and config.get("embed_bpm"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TBPM={}'.format(value)]
                elif filetype == '.m4a':
                    command += ['-metadata', 'tmpo={}'.format(value)]
                else:
                    command += ['-metadata', 'bpm={}'.format(value)]

            elif key == 'key' and config.get("embed_key"):
                if filetype == '.mp3':
                    command += ['-metadata', 'TKEY={}'.format(value)]
                else:
                    command += ['-metadata', 'initialkey={}'.format(value)]

            elif key == 'album_type' and config.get("embed_compilation"):
                if filetype == '.mp3':
                    # Incorrectly embedded to TXXX:TCMP, patch sent upstream
                    command += ['-metadata', 'TCMP={}'.format(int(value == 'compilation'))]
                else:
                    command += ['-metadata', 'compilation={}'.format(int(value == 'compilation'))]

            elif key == 'item_url' and config.get("embed_url"):
                if filetype == '.mp3':
                    # Incorrectly embedded to TXXX:WOAS, patch sent upstream
                    command += ['-metadata', 'WOAS={}'.format(value)]
                else:
                    command += ['-metadata', 'website={}'.format(value)]

            elif key == 'lyrics' and config.get("embed_lyrics"):
                if filetype == '.mp3':
                    # Incorrectly embedded to TXXX:USLT, patch sent upstream
                    command += ['-metadata', 'USLT={}'.format(value)]
                else:
                    command += ['-metadata', 'lyrics={}'.format(value)]

            elif key == 'explicit' and config.get("embed_explicit"):
                if filetype == '.mp3':
                    command += ['-metadata', 'ITUNESADVISORY={}'.format(value)]
                else:
                    command += ['-metadata', 'explicit={}'.format(value)]

            elif key == 'upc' and config.get("embed_upc"):
                command += ['-metadata', 'upc={}'.format(value)]

            elif key == 'time_signature' and config.get("embed_timesignature"):
                command += ['-metadata', 'timesignature={}'.format(value)]

            elif key == 'acousticness' and config.get("embed_acousticness"):
                command += ['-metadata', 'acousticness={}'.format(value)]

            elif key == 'danceability' and config.get("embed_danceability"):
                command += ['-metadata', 'danceability={}'.format(value)]

            elif key == 'instrumentalness' and config.get("embed_instrumentalness"):
                command += ['-metadata', 'instrumentalness={}'.format(value)]

            elif key == 'liveness' and config.get("embed_liveness"):
                command += ['-metadata', 'liveness={}'.format(value)]

            elif key == 'loudness' and config.get("embed_loudness"):
                command += ['-metadata', 'loudness={}'.format(value)]

            elif key == 'speechiness' and config.get("embed_speechiness"):
                command += ['-metadata', 'speechiness={}'.format(value)]

            elif key == 'energy' and config.get("embed_energy"):
                command += ['-metadata', 'energy={}'.format(value)]

            elif key == 'valence' and config.get("embed_valence"):
                command += ['-metadata', 'valence={}'.format(value)]

        # Add output parameter at last
        command += [item['file_path']]
        logger.info(f'Embedding metadata with ffmpeg for: {file_name}')
        logger.debug(f'Embed metadata with ffmpeg. Built commandline {command}')
        try:
            # Run subprocess with CREATE_NO_WINDOW flag on Windows
            if os.name == 'nt':
                subprocess.check_call(command, shell=False, creationflags=subprocess.CREATE_NO_WINDOW)
            else:
                subprocess.check_call(command, shell=False)
            logger.info(f'Successfully embedded metadata in: {file_name}')
            os.remove(temp_name)
        except subprocess.CalledProcessError as e:
            # Clean up both temp input and potentially corrupted output files
            if os.path.isfile(temp_name):
                os.remove(temp_name)
            if os.path.isfile(item['file_path']):
                os.remove(item['file_path'])
            raise RuntimeError(f"Failed to embed metadata: {e}")
    else:
        logger.error(f"embed_metadata() called but file does not exist: {item.get('file_path')}")


def set_music_thumbnail(filename, metadata):
    # For playlist tracks, use playlist cover URL if available
    image_url = metadata.get('playlist_image_url') if metadata.get('parent_category') == 'playlist' else metadata.get('image_url')
    
    if image_url:
        target_path = os.path.abspath(filename)
        file_name = os.path.basename(target_path)
        filetype = os.path.splitext(file_name)[1]
        file_stem = os.path.splitext(file_name)[0]

        temp_name = os.path.join(os.path.dirname(target_path), "~" + file_stem + filetype)

        # For playlists, save as cover.jpg in playlist directory (only once)
        # For albums/tracks, use configured format
        if metadata.get('parent_category') == 'playlist':
            image_path = os.path.join(os.path.dirname(filename), 'cover.jpg')
        else:
            image_path = os.path.join(os.path.dirname(filename), 'cover')
            image_path += "." + config.get("album_cover_format")

        # Fetch thumbnail only if it doesn't exist (avoid re-downloading for each playlist track)
        if not os.path.isfile(image_path):
            logger.info(f"Fetching playlist cover image" if metadata.get('parent_category') == 'playlist' else f"Fetching item thumbnail")
            img = Image.open(BytesIO(requests.get(image_url).content))
            buf = BytesIO()
            if img.mode != 'RGB':
                img = img.convert('RGB')
            # Force JPG for playlists, use config for others
            img_format = 'JPEG' if metadata.get('parent_category') == 'playlist' else config.get("album_cover_format")
            img.save(buf, format=img_format)
            buf.seek(0)
            with open(image_path, 'wb') as cover:
                cover.write(buf.read())
            logger.info(f"Saved cover image: {image_path}")
        else:
            logger.info(f"Cover image already exists: {image_path}")

        if not config.get('raw_media_download'):
            # I have no idea why music tag manages to display covers
            # in file explorer but raw mutagen and ffmpeg do not.
            if config.get('embed_cover') and config.get('windows_10_explorer_thumbnails'):
                with open(image_path, 'rb') as image_file:
                    image_data = image_file.read()
                tags = music_tag.load_file(filename)
                tags['artwork'] = image_data
                tags.save()

            elif config.get('embed_cover') and filetype not in ('.wav', '.ogg'):
                if os.path.isfile(temp_name):
                    os.remove(temp_name)

                os.rename(filename, temp_name)

                command = [config.get('_ffmpeg_bin_path'), '-y', '-i', temp_name]

                # Set log level based on environment variable
                if int(os.environ.get('SHOW_FFMPEG_OUTPUT', 0)) == 0:
                    command += ['-loglevel', 'error', '-hide_banner', '-nostats']

                # Windows equivilant of argument list too long
                #if filetype == '.ogg':
                #    #with open(image_path, "rb") as image_file:
                #    #    base64_image = base64.b64encode(image_file.read()).decode('utf-8')
                #    #
                #    # Argument list too long, downscale the image instead
                #
                #    with Image.open(image_path) as img:
                #        new_size = (250, 250) # 250 seems to be the max
                #        img = img.resize(new_size, Image.Resampling.LANCZOS)
                #        with BytesIO() as output:
                #            img.save(output, format=config.get("album_cover_format"))
                #            output.seek(0)
                #            base64_image = base64.b64encode(output.read()).decode('utf-8')
                #
                #    # METADATA_BLOCK_PICTURE is a better supported format but I don't know how to write it
                #    command += [
                #        "-c", "copy", "-metadata", f"coverart={base64_image}", "-metadata", f"coverartmime=image/{config.get('album_cover_format')}"
                #        ]
                #else:
                command += [
                    '-i', image_path, '-map', '0:a', '-map', '1:v', '-c', 'copy', '-disposition:v:0', 'attached_pic',
                    '-metadata:s:v', 'title=Cover', '-metadata:s:v', 'comment=Cover (front), -id3v2_version 1'
                    ]

                command += [filename]
                logger.debug(
                    f'Setting thumbnail with ffmpeg. Built commandline {command}'
                    )
                try:
                    if os.name == 'nt':
                        subprocess.check_call(command, shell=False, creationflags=subprocess.CREATE_NO_WINDOW)
                    else:
                        subprocess.check_call(command, shell=False)
                    os.remove(temp_name)
                except subprocess.CalledProcessError as e:
                    # Clean up both temp input and potentially corrupted output files
                    if os.path.isfile(temp_name):
                        os.remove(temp_name)
                    if os.path.isfile(filename):
                        os.remove(filename)
                    raise RuntimeError(f"Failed to set thumbnail: {e}")

            elif config.get('embed_cover') and filetype == '.ogg':
                with open(image_path, 'rb') as image_file:
                    image_data = image_file.read()
                tags = OggVorbis(filename)
                logger.info(f"OGG tags before adding cover: {list(tags.keys())}")
                picture = Picture()
                picture.data = image_data
                picture.type = 3
                picture.desc = "Cover"
                picture.mime = f"image/{config.get('album_cover_format')}"
                picture_data = picture.write()
                encoded_data = base64.b64encode(picture_data)
                vcomment_value = encoded_data.decode("ascii")
                tags["metadata_block_picture"] = [vcomment_value]
                logger.info(f"OGG tags after adding cover: {list(tags.keys())}")
                tags.save()
                logger.info(f"OGG cover art embedded successfully for: {file_name}")

            if os.path.exists(temp_name):
                os.remove(temp_name)

        if not config.get('save_album_cover') and os.path.exists(image_path):
            os.remove(image_path)

def fix_mp3_metadata(filename):
    id3 = ID3(filename)
    if 'TXXX:WOAS' in id3:
        id3['WOAS'] = WOAS(url=id3['TXXX:WOAS'].text[0])
        del id3['TXXX:WOAS']
    if 'TXXX:USLT' in id3:
        id3.add(USLT(encoding=3, lang=u'und', desc=u'desc', text=id3['TXXX:USLT'].text[0]))
        del id3['TXXX:USLT']
    if 'TXXX:COMM' in id3:
        id3['COMM'] = COMM(encoding=3, lang='und', text=id3['TXXX:COMM'].text[0])
        del id3['TXXX:COMM']
    if 'TXXX:comment' in id3:
        del id3['TXXX:comment']
    if 'TXXX:TCMP' in id3:
        id3['TCMP'] = TCMP(encoding=3, text=id3['TXXX:TCMP'].text[0])
        del id3['TXXX:TCMP']
    id3.save()


def _get_playlist_cache_path(playlist_name, playlist_by):
    """Get the path to the playlist completion cache file."""
    from .otsconfig import cache_dir
    safe_name = sanitize_data(f"{playlist_name}_{playlist_by}")
    return os.path.join(cache_dir(), f'playlist_cache_{safe_name}.json')


def _load_playlist_cache(playlist_name, playlist_by):
    """Load completed playlist items from cache."""
    import json
    cache_path = _get_playlist_cache_path(playlist_name, playlist_by)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load playlist cache: {e}")
    return {'completed_items': [], 'total_expected': None}


def _save_playlist_cache(playlist_name, playlist_by, cache_data):
    """Save completed playlist items to cache."""
    import json
    from .otsconfig import cache_dir
    cache_path = _get_playlist_cache_path(playlist_name, playlist_by)
    os.makedirs(cache_dir(), exist_ok=True)
    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save playlist cache: {e}")


def _add_completed_playlist_item(item, item_metadata):
    """Track a completed playlist item in cache."""
    if not item.get('playlist_name'):
        return
    
    cache_data = _load_playlist_cache(item['playlist_name'], item['playlist_by'])
    
    # Add this item to completed list (avoid duplicates)
    item_entry = {
        'item_id': item.get('item_id'),
        'file_path': item.get('file_path'),
        'playlist_number': item.get('playlist_number'),
        'metadata': {
            'length': item_metadata.get('length'),
            'title': item_metadata.get('title'),
            'artists': item_metadata.get('artists'),
            'album_name': item_metadata.get('album_name'),
            'album_artists': item_metadata.get('album_artists'),
            'album_type': item_metadata.get('album_type'),
            'release_year': item_metadata.get('release_year'),
            'disc_number': item_metadata.get('disc_number'),
            'track_number': item_metadata.get('track_number'),
            'genre': item_metadata.get('genre'),
            'label': item_metadata.get('label'),
            'explicit': item_metadata.get('explicit'),
            'total_tracks': item_metadata.get('total_tracks'),
            'total_discs': item_metadata.get('total_discs'),
            'isrc': item_metadata.get('isrc'),
        },
        'item_service': item.get('item_service'),
        'item_id_full': item.get('item_id')
    }
    
    # Remove any existing entry with same item_id
    cache_data['completed_items'] = [
        i for i in cache_data['completed_items'] 
        if i.get('item_id') != item.get('item_id')
    ]
    cache_data['completed_items'].append(item_entry)
    
    _save_playlist_cache(item['playlist_name'], item['playlist_by'], cache_data)


def _check_and_write_playlist_m3u(playlist_name, playlist_by, download_queue):
    """Check if playlist is complete and write M3U if so."""
    if not playlist_name:
        return False
    
    # Count total items in this playlist from download queue
    from .runtimedata import download_queue_lock
    with download_queue_lock:
        playlist_items = [
            item for item in download_queue.values()
            if (item.get('parent_category') == 'playlist' and 
                item.get('playlist_name') == playlist_name and
                item.get('playlist_by') == playlist_by)
        ]
        
        total_items = len(playlist_items)
        completed_items = [item for item in playlist_items if item.get('item_status') in ('Downloaded', 'Already Exists')]
        completed_count = len(completed_items)
        
        # Check if all are completed
        all_complete = completed_count == total_items and total_items > 0
        
        logger.info(f"Playlist '{playlist_name}' status: {completed_count}/{total_items} complete")
        
        if not all_complete:
            pending_items = [(item.get('item_id'), item.get('item_status')) for item in playlist_items if item.get('item_status') not in ('Downloaded', 'Already Exists')]
            pending_statuses = [status for _, status in pending_items]
            logger.info(f"Playlist '{playlist_name}' not yet complete. Pending statuses: {set(pending_statuses)}")
            logger.info(f"Pending items: {pending_items}")
            return False
    
    # All items complete - write M3U from cache
    logger.info(f"Playlist '{playlist_name}' complete! Writing M3U file...")
    cache_data = _load_playlist_cache(playlist_name, playlist_by)
    
    if not cache_data['completed_items']:
        logger.warning(f"No completed items in cache for playlist '{playlist_name}'")
        return False
    
    # Generate M3U file
    path = config.get("m3u_path_formatter")
    m3u_file = path.format(
        playlist_name=sanitize_data(playlist_name),
        playlist_owner=sanitize_data(playlist_by),
    )
    m3u_file += "." + config.get("m3u_format")
    dl_root = config.get("audio_download_path")
    m3u_path = os.path.join(dl_root, m3u_file)
    
    os.makedirs(os.path.dirname(m3u_path), exist_ok=True)
    
    # Sort by playlist_number
    sorted_items = sorted(
        cache_data['completed_items'],
        key=lambda x: int(x.get('playlist_number', 999)) if x.get('playlist_number') else 999
    )
    
    # Write M3U file
    with open(m3u_path, 'w', encoding='utf-8') as f:
        f.write("#EXTM3U\n")
        
        for item_entry in sorted_items:
            metadata = item_entry['metadata']
            
            EXTINF = config.get('extinf_label').format(
                service=item_entry.get('item_service', '').title(),
                service_id=str(item_entry.get('item_id_full', '')),
                artist=metadata.get('artists', ''),
                album=metadata.get('album_name', ''),
                album_artist=metadata.get('album_artists', ''),
                album_type=metadata.get('album_type', 'single').title(),
                name=metadata.get('title', ''),
                year=metadata.get('release_year', ''),
                disc_number=metadata.get('disc_number', 1) if not config.get('use_double_digit_path_numbers') else str(metadata.get('disc_number', 1)).zfill(2),
                track_number=metadata.get('track_number', 1) if not config.get('use_double_digit_path_numbers') else str(metadata.get('track_number', 1)).zfill(2),
                genre=metadata.get('genre', ''),
                label=metadata.get('label', ''),
                explicit=str(config.get('explicit_label')) if metadata.get('explicit') else '',
                trackcount=metadata.get('total_tracks', 1) if not config.get('use_double_digit_path_numbers') else str(metadata.get('total_tracks', 1)).zfill(2),
                disccount=metadata.get('total_discs', 1) if not config.get('use_double_digit_path_numbers') else str(metadata.get('total_discs', 1)).zfill(2),
                isrc=str(metadata.get('isrc', '')),
                playlist_name=playlist_name,
                playlist_owner=playlist_by,
                playlist_number=item_entry.get('playlist_number', ''),
            ).replace(config.get('metadata_separator'), config.get('extinf_separator'))
            
            try:
                ext_length = round(int(metadata.get('length', 0))/1000)
            except Exception:
                ext_length = '-1'
            
            f.write(f"#EXTINF:{ext_length}, {EXTINF}\n")
            f.write(f"{item_entry['file_path']}\n")
    
    logger.info(f"M3U file written: {m3u_path} ({len(sorted_items)} tracks)")
    
    # Clean up cache file
    try:
        cache_path = _get_playlist_cache_path(playlist_name, playlist_by)
        if os.path.exists(cache_path):
            os.remove(cache_path)
            logger.debug(f"Removed playlist cache: {cache_path}")
    except Exception as e:
        logger.error(f"Failed to remove playlist cache: {e}")
    
    return True


def add_to_m3u_file(item, item_metadata):
    """Add item to playlist cache and write M3U if playlist is complete."""
    if not item.get('playlist_name') or item.get('parent_category') != 'playlist':
        return
    
    logger.info(f"Tracking completed playlist item: {item.get('item_id')} for playlist '{item.get('playlist_name')}'")
    
    # Add to cache
    _add_completed_playlist_item(item, item_metadata)
    
    # Check if playlist is complete and write M3U if so
    from .runtimedata import download_queue
    _check_and_write_playlist_m3u(item.get('playlist_name'), item.get('playlist_by'), download_queue)


def legacy_add_to_m3u_file(item, item_metadata):
    """Original immediate M3U writing (kept for backward compatibility)."""
    logger.info(f"Adding {item['file_path']} to m3u")

    path = config.get("m3u_path_formatter")

    m3u_file = path.format(
        playlist_name=sanitize_data(item['playlist_name']),
        playlist_owner=sanitize_data(item['playlist_by']),
    )

    m3u_file += "." + config.get("m3u_format")
    dl_root = config.get("audio_download_path")
    m3u_path = os.path.join(dl_root, m3u_file)

    os.makedirs(os.path.dirname(m3u_path), exist_ok=True)

    if not os.path.exists(m3u_path):
        with open(m3u_path, 'w', encoding='utf-8') as m3u_file:
            m3u_file.write("#EXTM3U\n")

    EXTINF = config.get('extinf_label').format(
        service=item.get('item_service').title(),
        service_id=str(item.get('item_id')),
        artist=item_metadata.get('artists'),
        album=item_metadata.get('album_name'),
        album_artist=item_metadata.get('album_artists'),
        album_type=item_metadata.get('album_type', 'single').title(),
        name=item_metadata.get('title'),
        year=item_metadata.get('release_year'),
        disc_number=item_metadata.get('disc_number', 1) if not config.get('use_double_digit_path_numbers') else str(item_metadata.get('disc_number', 1)).zfill(2),
        track_number=item_metadata.get('track_number', 1) if not config.get('use_double_digit_path_numbers') else str(item_metadata.get('track_number', 1)).zfill(2),
        genre=item_metadata.get('genre'),
        label=item_metadata.get('label'),
        explicit=str(config.get('explicit_label')) if item_metadata.get('explicit') else '',
        trackcount=item_metadata.get('total_tracks', 1) if not config.get('use_double_digit_path_numbers') else str(item_metadata.get('total_tracks', 1)).zfill(2),
        disccount=item_metadata.get('total_discs', 1) if not config.get('use_double_digit_path_numbers') else str(item_metadata.get('total_discs', 1)).zfill(2),
        isrc=str(item_metadata.get('isrc')),
        playlist_name=item.get('playlist_name'),
        playlist_owner=item.get('playlist_by'),
        playlist_number=item.get('playlist_number'),
    ).replace(config.get('metadata_separator'), config.get('extinf_separator'))

    # Check if the item is already in the M3U file
    with open(m3u_path, 'r', encoding='utf-8') as m3u_file:
        try:
            ext_length = round(int(item_metadata['length'])/1000)
        except Exception:
            ext_length = '-1'
        m3u_item_header = f"#EXTINF:{ext_length}, {EXTINF}"
        m3u_contents = m3u_file.readlines()

        # Check both header and file path to ensure the entry is complete and correct
        already_exists = False
        for i in range(len(m3u_contents) - 1):
            if m3u_contents[i].strip() == m3u_item_header:
                # Check if the next line contains the file path
                if m3u_contents[i + 1].strip() == item['file_path']:
                    already_exists = True
                    logger.info(f"{item['file_path']} already exists in the M3U file.")
                    break

        if not already_exists:
            with open(m3u_path, 'a', encoding='utf-8') as m3u_file:
                m3u_file.write(f"{m3u_item_header}\n{item['file_path']}\n")


def force_write_all_playlist_m3us():
    """Force write M3U files for all cached playlists (useful for cleanup/recovery)."""
    from .otsconfig import cache_dir
    from .runtimedata import download_queue
    import json
    
    cache_files = [f for f in os.listdir(cache_dir()) if f.startswith('playlist_cache_') and f.endswith('.json')]
    
    for cache_file in cache_files:
        try:
            cache_path = os.path.join(cache_dir(), cache_file)
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            if cache_data.get('completed_items'):
                # Extract playlist name/by from first item
                first_item = cache_data['completed_items'][0]
                playlist_name = first_item.get('metadata', {}).get('playlist_name')
                playlist_by = first_item.get('metadata', {}).get('playlist_owner')
                
                if playlist_name:
                    logger.info(f"Force writing M3U for cached playlist: {playlist_name}")
                    _check_and_write_playlist_m3u(playlist_name, playlist_by, download_queue)
        except Exception as e:
            logger.error(f"Failed to process playlist cache {cache_file}: {e}")


def strip_metadata(item):
    if os.path.isfile(os.path.abspath(item['file_path'])):
        target_path = os.path.abspath(item['file_path'])
        file_name = os.path.basename(target_path)
        filetype = os.path.splitext(file_name)[1]
        file_stem = os.path.splitext(file_name)[0]

        temp_name = os.path.join(os.path.dirname(target_path), "~" + file_stem + filetype)

        if os.path.isfile(temp_name):
            os.remove(temp_name)

        os.rename(item['file_path'], temp_name)
        # Prepare default parameters
        # Existing command initialization
        command = [config.get('_ffmpeg_bin_path'), '-y', '-i', temp_name]

        if int(os.environ.get('SHOW_FFMPEG_OUTPUT', 0)) == 0:
            command += ['-loglevel', 'error', '-hide_banner', '-nostats']

        command += ['-map', '0:a', '-map_metadata', '-1', '-c:a', 'copy']

        # Add output parameter at last
        command += [item['file_path']]
        logger.debug(
            f'Strip metadata with ffmpeg. Built commandline {command}'
            )
        try:
            # Run subprocess with CREATE_NO_WINDOW flag on Windows
            if os.name == 'nt':
                subprocess.check_call(command, shell=False, creationflags=subprocess.CREATE_NO_WINDOW)
            else:
                subprocess.check_call(command, shell=False)
            os.remove(temp_name)
        except subprocess.CalledProcessError as e:
            # Clean up both temp input and potentially corrupted output files
            if os.path.isfile(temp_name):
                os.remove(temp_name)
            if os.path.isfile(item['file_path']):
                os.remove(item['file_path'])
            raise RuntimeError(f"Failed to strip metadata: {e}")


def format_bytes(size):
    units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024
        index += 1
    return f"{size:.2f} {units[index]}"
