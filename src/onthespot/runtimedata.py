import linecache
import logging
import os
import sys
import tracemalloc
from functools import wraps
from logging.handlers import RotatingFileHandler
from threading import Lock
from collections import deque
from .otsconfig import config

log_formatter = logging.Formatter(
    '[%(asctime)s :: %(name)s :: %(pathname)s -> %(lineno)s:%(funcName)20s() :: %(levelname)s] -> %(message)s'
)
log_handler = RotatingFileHandler(config.get("_log_file"),
                                  mode='a',
                                  maxBytes=(5 * 1024 * 1024),
                                  backupCount=2,
                                  encoding='utf-8',
                                  delay=0)
stdout_handler = logging.StreamHandler(sys.stdout)
log_handler.setFormatter(log_formatter)
stdout_handler.setFormatter(log_formatter)

account_pool = []
temp_download_path = []
parsing = {}
pending = {}
download_queue = {}
parsing_lock = Lock()
pending_lock = Lock()
download_queue_lock = Lock()

# System notifications for web UI
system_notifications = deque(maxlen=200)
system_notifications_lock = Lock()

# Album download locks to prevent concurrent downloads from same album
album_download_locks = {}
album_download_locks_lock = Lock()


def get_or_create_album_lock(album_key, prune_threshold=128):
    """Get or create the lock for an album_key. Caller does NOT need to hold
    album_download_locks_lock — this function manages it. When the dict grows
    past prune_threshold, drops any locks that are currently free
    (no thread holds them) to avoid unbounded growth."""
    with album_download_locks_lock:
        lock = album_download_locks.get(album_key)
        if lock is None:
            if len(album_download_locks) >= prune_threshold:
                stale = []
                for k, l in album_download_locks.items():
                    if l.acquire(blocking=False):
                        try:
                            stale.append(k)
                        finally:
                            l.release()
                for k in stale:
                    album_download_locks.pop(k, None)
            from threading import Lock as _Lock
            lock = _Lock()
            album_download_locks[album_key] = lock
        return lock

# Batch parsing state (for playlists/albums that add multiple items)
batch_parse_in_progress = False
batch_parse_lock = Lock()
batch_parse_start_time = None  # Track when flag was set

# Timeout for batch operations (in seconds)
# Increased to allow Spotify rate limit retries to complete (retry-after can be 40-60s)
BATCH_OPERATION_TIMEOUT = 90

# Worker management
worker_threads = []
worker_threads_lock = Lock()

init_tray = False


def set_init_tray(value):
    global init_tray
    init_tray = value


def get_init_tray():
    return init_tray


loglevel = int(os.environ.get("LOG_LEVEL", 20))


def get_logger(name):
    logger = logging.getLogger(name)
    logger.addHandler(log_handler)
    logger.addHandler(stdout_handler)
    logger.setLevel(loglevel)
    return logger


logger_ = get_logger("runtimedata")


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger_.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception

def log_function_memory(wrap_func):
    tracemalloc.start()
    top_limit = 10
    def display_top(snapshot, snapshot_log_prefix, key_type='lineno'):
        snapshot = snapshot.filter_traces((
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        ))
        top_stats = snapshot.statistics(key_type)

        logger_.debug(f"{snapshot_log_prefix} Top {top_limit} lines")
        for index, stat in enumerate(top_stats[:top_limit], 1):
            frame = stat.traceback[0]
            logger_.debug("#%s: %s:%s: %.1f KiB"
                % (index, frame.filename, frame.lineno, stat.size / 1024))
            line = linecache.getline(frame.filename, frame.lineno).strip()
            if line:
                logger_.debug(f"{snapshot_log_prefix} -- {line}"  )

        other = top_stats[top_limit:]
        if other:
            size = sum(stat.size for stat in other)
            logger_.debug("%s other: %.1f KiB" % (len(other), size / 1024))
        total = sum(stat.size for stat in top_stats)
        logger_.debug("Total allocated size: %.1f KiB" % (total / 1024))

    @wraps(wrap_func)
    def snapshot_function_call(*args, **kwargs):
        prefix = f"{wrap_func.__name__}: "
        before_func = tracemalloc.take_snapshot()
        logger_.debug(f"Snapshotting before {wrap_func.__name__} call")
        ret_val = wrap_func(*args, **kwargs)
        display_top(before_func, prefix)
        logger_.debug(f"Snapshotting after {wrap_func.__name__} call")
        after_func = tracemalloc.take_snapshot()
        display_top(after_func, prefix)
        top_stats = after_func.compare_to(before_func, 'lineno')
        logger_.debug(f"{prefix} Top {top_limit} differences")
        for stat in top_stats[:10]:
            logger_.debug(f"{prefix}{stat}")
        return ret_val
    return snapshot_function_call


def register_worker(worker):
    """Register a worker thread for management"""
    with worker_threads_lock:
        worker_threads.append(worker)
        logger_.debug(f"Registered worker: {worker.__class__.__name__}, total workers: {len(worker_threads)}")


def kill_all_workers():
    """Kill all registered worker threads"""
    global worker_threads
    logger_.warning("Killing all worker threads...")
    
    # Log queue status before killing workers
    with pending_lock:
        pending_count = len(pending)
    logger_.info(f"Pending queue has {pending_count} items before worker restart")

    with worker_threads_lock:
        import threading
        current_thread = threading.current_thread()
        
        for worker in worker_threads:
            try:
                # Skip if trying to stop current thread (causes deadlock / RuntimeError on join)
                worker_thread = None
                if isinstance(worker, threading.Thread):
                    worker_thread = worker
                elif hasattr(worker, 'thread'):
                    worker_thread = worker.thread

                if worker_thread is current_thread:
                    logger_.warning(f"Skipping stop of current thread: {worker.__class__.__name__}")
                    continue
                    
                logger_.info(f"Stopping worker: {worker.__class__.__name__}")
                worker.stop()
            except Exception as e:
                logger_.error(f"Error stopping worker {worker.__class__.__name__}: {e}")

        # Clear the list
        worker_threads = []
        logger_.info("All workers stopped and cleared")
        
    # Log queue status after restart for verification
    with pending_lock:
        pending_count_after = len(pending)
    logger_.info(f"Pending queue has {pending_count_after} items after worker restart - these will be processed by new workers")


def set_batch_parse_flag(value):
    """Set batch_parse_in_progress flag with timestamp tracking"""
    import time
    global batch_parse_in_progress, batch_parse_start_time
    with batch_parse_lock:
        batch_parse_in_progress = value
        batch_parse_start_time = time.time() if value else None


def check_and_clear_stuck_flags():
    """Check if batch_parse flag is stuck and clear it if timeout exceeded."""
    import time
    global batch_parse_in_progress, batch_parse_start_time

    with batch_parse_lock:
        if batch_parse_in_progress and batch_parse_start_time:
            elapsed = time.time() - batch_parse_start_time
            if elapsed > BATCH_OPERATION_TIMEOUT:
                logger_.error(f"⚠️ STUCK FLAG: batch_parse_in_progress has been True for {elapsed:.1f}s — clearing")
                batch_parse_in_progress = False
                batch_parse_start_time = None
                return True

    return False
