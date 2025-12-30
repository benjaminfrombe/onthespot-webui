from time import sleep
import threading
from .api.apple_music import apple_music_login_user, apple_music_get_token
from .api.bandcamp import bandcamp_login_user
from .api.deezer import deezer_login_user, deezer_get_token
from .api.qobuz import qobuz_login_user, qobuz_get_token
from .api.soundcloud import soundcloud_login_user, soundcloud_get_token
from .api.spotify import spotify_login_user, spotify_get_token
from .api.tidal import tidal_login_user, tidal_get_token
from .api.youtube_music import youtube_music_login_user
from .api.generic import generic_login_user
from .api.crunchyroll import crunchyroll_login_user, crunchyroll_get_token
from .otsconfig import config
from .runtimedata import get_logger, account_pool

logger = get_logger("accounts")


class FillAccountPool(threading.Thread):
    def __init__(self, finished_callback=None, progress_callback=None):
        super().__init__()
        self.finished_callback = finished_callback
        self.progress_callback = progress_callback

    def run(self):
        accounts = config.get('accounts')
        for account in accounts:
            service = account['service']
            if not account['active']:
                continue

            if self.progress_callback:
                self.progress_callback(f'Attempting to create session for {account["uuid"]}...', True)

            valid_login = globals()[f"{service}_login_user"](account)
            if valid_login:
                if self.progress_callback:
                    self.progress_callback(f'Session created for {account["uuid"]}!', True)
                continue
            else:
                if self.progress_callback:
                    self.progress_callback(f'Login failed for {account["uuid"]}!', True)
                    sleep(0.5)
                continue

        if self.finished_callback:
            self.finished_callback()


def get_account_token(item_service, rotate=False):
    if item_service in ('bandcamp', 'youtube_music', 'generic'):
        return
    parsing_index = config.get('active_account_number')
    
    def has_required_session(index):
        if item_service != 'spotify':
            return True
        return bool(account_pool[index].get('login', {}).get('session'))
    
    # Try the primary account first if not rotating and it's active
    if item_service == account_pool[parsing_index]['service'] and not rotate:
        if account_pool[parsing_index].get('status') == 'active' and has_required_session(parsing_index):
            return globals()[f"{item_service}_get_token"](parsing_index)
        else:
            if account_pool[parsing_index].get('status') != 'active':
                logger.debug(f"Primary account at index {parsing_index} is not active (status: {account_pool[parsing_index].get('status')}), searching for alternative")
            else:
                logger.debug(f"Primary account at index {parsing_index} missing session, searching for alternative")
    
    # Search for any active account of the requested service
    for i in range(parsing_index + 1, parsing_index + len(account_pool) + 1):
        index = i % len(account_pool)
        if item_service == account_pool[index]['service']:
            # Check if the account is active before using it
            if account_pool[index].get('status') != 'active':
                logger.debug(f"Skipping account at index {index} (status: {account_pool[index].get('status')})")
                continue
            if not has_required_session(index):
                logger.debug(f"Skipping account at index {index} (missing session)")
                continue
            if config.get("rotate_active_account_number"):
                logger.debug(f"Returning {account_pool[index]['service']} account number {index}: {account_pool[index]['uuid']}")
                config.set('active_account_number', index)
                config.save()
            else:
                logger.info(f"Using alternative {account_pool[index]['service']} account at index {index}: {account_pool[index]['uuid']}")
            return globals()[f"{item_service}_get_token"](index)
    
    # No active account found
    logger.error(f"No active account found for service: {item_service}")
    return None
