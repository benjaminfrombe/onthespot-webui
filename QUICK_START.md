# Quick Start: Nuclear Session Reset Patch

## TL;DR

This patch makes OnTheSpot automatically recover from session corruption without manual restarts.

## Installation (Choose One)

### Option 1: Git Apply (Fastest)
```bash
cd /path/to/onthespot
curl -O https://[your-location]/nuclear_session_reset.patch
git apply nuclear_session_reset.patch
```

### Option 2: Docker Users
If running OnTheSpot in Docker:

```bash
# Copy patch into container
docker cp nuclear_session_reset.patch onthespot:/app/

# Apply patch
docker exec -it onthespot bash -c "cd /app && git apply nuclear_session_reset.patch"

# Restart container
docker restart onthespot
```

### Option 3: Manual Edit
For non-git installations, manually edit:
- `onthespot/api/spotify.py` 
- `onthespot/downloader.py`

Use the patch file as a reference.

## Verify Installation

After applying, check the files contain new functions:

```bash
# Should find these new functions
grep -n "_trigger_nuclear_reset" onthespot/downloader.py
grep -n "_cleanup_old_session" onthespot/api/spotify.py
```

If both return results, patch applied successfully!

## What You'll See

### Before Patch
```
[ERROR] Download stream failed (OSError): [Errno 9] Bad file descriptor
[ERROR] Session reinit failed
[ERROR] Failed to load audio stream after trying 2 account(s)
[Downloads stuck] Manual restart required
```

### After Patch
```
[WARNING] Download stream failed (OSError): [Errno 9] Bad file descriptor
💥 TRIGGERING NUCLEAR RESET for account: Bad file descriptor
🔥 FORCE IMMEDIATE RESET (overriding rate limit)
🧹 Deep cleaning old session
🔄 Creating fresh session...
🔍 Validating new session...
✓ NUCLEAR SESSION RESET SUCCESSFUL (bitrate: 320k)
▶️  Resuming downloads
[Downloads continue automatically]
```

## Test It

1. Start OnTheSpot
2. Queue a large album (15+ tracks)
3. Let it download
4. Watch the logs

If you previously needed restarts, you should now see automatic recovery messages instead.

## Key Differences

| Aspect | Before | After |
|--------|--------|-------|
| Bad FD error | Manual restart needed | Auto-recovers in 5-10s |
| Unpack error | Downloads fail | Auto-recreates session |
| Multiple failures | Cascading failure | Nuclear reset all accounts |
| Rate limiting | Blocks recovery | Bypassed for critical errors |
| Download queue | Lost on restart | Preserved during recovery |

## Monitoring

Watch for these emoji in logs:
- 💥 Nuclear reset triggered
- 🔥 Rate limit override
- ⏸️ Downloads paused
- 🧹 Cleanup in progress
- 🔄 Creating new session
- 🔍 Validating session
- 🌡️ Warming up
- ✓ Success
- ✗ Failed
- ▶️ Downloads resumed

## When to Still Manually Restart

Only if you see repeated failures:
```
✗ NUCLEAR SESSION RESET FAILED: All retries exhausted
✗ Nuclear reset failed
```

This is rare and indicates a deeper issue (network, Spotify API, etc).

## Rollback

If problems occur:
```bash
cd /path/to/onthespot
git apply -R nuclear_session_reset.patch
```

## Questions?

See [NUCLEAR_RESET_README.md](NUCLEAR_RESET_README.md) for complete documentation.

## Docker Compose Example

If you need to persist this patch across container recreations:

```yaml
services:
  onthespot:
    image: onthespot:latest
    volumes:
      - ./patches:/patches
    command: >
      bash -c "
      git apply /patches/nuclear_session_reset.patch || true &&
      python -m onthespot.web
      "
```

Then put `nuclear_session_reset.patch` in the `./patches` directory.
