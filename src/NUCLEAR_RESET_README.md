# Nuclear Session Reset Patch for OnTheSpot

## Overview

This patch fixes cascading session failures in OnTheSpot by implementing proper "nuclear" session resets that work like a manual restart, without actually restarting the application.

## What It Fixes

**Before this patch:**
- Sessions get corrupted during downloads (Bad file descriptor errors)
- "unpack requires a buffer" errors from librespot
- Rate limiting prevents recovery (30s minimum between resets)
- Cascading failures - once one download fails, others follow
- Only solution: manual restart

**After this patch:**
- Automatic detection of critical session errors
- Complete session cleanup (sockets, file descriptors, memory)
- Forced garbage collection to free resources
- Session validation before use
- Overrides rate limiting for critical errors
- Downloads pause during reset, then resume automatically

## Files Modified

1. **onthespot/api/spotify.py**
   - Added `_cleanup_old_session()` - Deep cleanup of session resources
   - Added `_validate_session()` - Verify session is ready before use
   - Added `_halt_downloads_for_account()` - Pause downloads during reset
   - Added `_resume_downloads_for_account()` - Resume after reset
   - Enhanced `spotify_re_init_session()` - Proper nuclear reset with validation
   
2. **onthespot/downloader.py**
   - Added `_should_trigger_nuclear_reset()` - Detect critical errors
   - Added `_trigger_nuclear_reset()` - Force immediate reset
   - Enhanced error handling to use nuclear resets when needed
   - Added account halt checking before downloads

## How to Apply

### Method 1: Using git apply (recommended)

```bash
cd /path/to/onthespot
git apply /path/to/nuclear_session_reset.patch
```

### Method 2: Using patch command

```bash
cd /path/to/onthespot
patch -p1 < /path/to/nuclear_session_reset.patch
```

### Method 3: Manual application

If the automatic methods fail, manually edit the files:

1. Open `onthespot/api/spotify.py`
2. Find the sections marked with `---` and `+++` in the patch
3. Add the new functions and modify existing ones as shown
4. Repeat for `onthespot/downloader.py`

## What Happens During a Nuclear Reset

1. **Halt Phase**
   ```
   ⏸️  Halting downloads for account
   ```
   - All active downloads for this account pause
   - Account marked as temporarily unavailable

2. **Cleanup Phase**
   ```
   🧹 Deep cleaning old session
   ```
   - Close audio streams
   - Close network sockets
   - Close dealer connections
   - Remove session from memory
   - Force Python garbage collection
   - Wait 2 seconds for OS resource release

3. **Recreation Phase**
   ```
   🔄 Creating fresh session
   ```
   - Create completely new librespot session
   - Up to 4 retry attempts with exponential backoff
   - Retry delays: 2s, 4s, 8s

4. **Validation Phase**
   ```
   🔍 Validating new session
   ```
   - Check session has required attributes
   - Test token retrieval
   - Test lightweight API call
   - 1.5 second warm-up period

5. **Resume Phase**
   ```
   ▶️  Resuming downloads
   ```
   - Account marked as available
   - Downloads continue automatically

## Critical Errors That Trigger Nuclear Reset

The patch automatically detects these critical errors:

### Error Types
- `OSError` (errno 9) - Bad file descriptor
- `OSError` (errno 32) - Broken pipe  
- `OSError` (errno 104) - Connection reset by peer
- `struct.error` - Unpack requires a buffer
- `ConnectionResetError` - Connection forcibly closed
- `BrokenPipeError` - Write to closed socket

### Error Messages
- "bad file descriptor"
- "unpack requires a buffer"
- "connection reset by peer"
- "broken pipe"
- "connection aborted"

## Rate Limiting Behavior

**Normal reconnections:** 30-second minimum interval (unchanged)

**Nuclear resets:** Bypass rate limiting completely when triggered by critical errors

This is safe because:
- Nuclear resets include comprehensive cleanup
- Validation ensures session is truly ready
- Warm-up period prevents immediate re-corruption
- Reserved for genuinely broken sessions

## Logging

New log messages to watch for:

### Success Path
```
💥 TRIGGERING NUCLEAR RESET for account: Bad file descriptor
🔥 FORCE IMMEDIATE RESET (overriding rate limit)
⏸️  Halting downloads for account
🧹 Deep cleaning old session
⏳ Waiting for system resources to be freed...
🔄 Creating fresh session...
🔍 Validating new session...
🌡️  Session warm-up period...
✓ NUCLEAR SESSION RESET SUCCESSFUL (bitrate: 320k)
▶️  Resuming downloads
✓ Nuclear reset successful
```

### Failure Path
```
💥 TRIGGERING NUCLEAR RESET for account: unpack requires a buffer
✗ NUCLEAR SESSION RESET FAILED: All retries exhausted
✗ Nuclear reset failed
```

## Aggressive Retry Worker

When 3+ downloads fail, the retry worker now triggers nuclear resets for ALL accounts:

```
High failure count (3), forcing complete session reset
Nuclear reset for Spotify account 0
Nuclear reset for Spotify account 1
Successfully reconnected 2 Spotify account(s) - now retrying failed downloads
```

This prevents situations where all sessions are bad but rate limiting blocks recovery.

## Testing the Patch

1. **Apply the patch**
2. **Start OnTheSpot and login**
3. **Queue a large album (10+ tracks)**
4. **Monitor logs for:**
   - Nuclear reset triggers
   - Successful validation
   - Downloads resuming automatically

5. **Previously failing scenario:**
   - Multiple albums in queue
   - Rapid downloads causing session stress
   - Should now complete without manual restart

## Rollback

If you need to revert the patch:

```bash
cd /path/to/onthespot
git apply -R /path/to/nuclear_session_reset.patch
```

Or restore from your backup/git history.

## Performance Impact

**Minimal:**
- Nuclear resets only trigger on critical errors
- Normal downloads unaffected
- 2-4 second pause during reset (only for affected account)
- Other accounts continue downloading during reset

**Benefits:**
- Eliminates need for manual restarts
- Recovers from corrupted sessions automatically
- Maintains download queue across recovery
- No downloads lost

## Troubleshooting

### "Patch does not apply cleanly"

Your OnTheSpot version may differ. Try:
1. Check your OnTheSpot version matches the patch
2. Apply manually using the patch as a guide
3. Report the issue with your version info

### "Still seeing Bad file descriptor errors"

Check logs for:
```
💥 TRIGGERING NUCLEAR RESET
```

If you don't see this, the error detection may need tuning for your specific case.

### "Downloads stuck in halted state"

Session reset failed. Check logs for:
```
✗ NUCLEAR SESSION RESET FAILED
```

Manually restart OnTheSpot and report the full error log.

## Compatibility

- **Tested on:** OnTheSpot latest development version
- **Python:** 3.8+
- **librespot:** All versions
- **Platforms:** Linux, macOS, Windows

## Support

If you encounter issues:

1. Check the logs for nuclear reset messages
2. Look for validation failures
3. Note which error originally triggered the reset
4. Report with full log context

## Future Improvements

Potential enhancements:
- Per-account failure tracking
- Adaptive rate limiting based on success rate
- Health monitoring dashboard
- Automatic account rotation on persistent failures

## Credits

Developed to solve persistent session corruption issues in OnTheSpot's librespot integration.

## License

Same as OnTheSpot project.
