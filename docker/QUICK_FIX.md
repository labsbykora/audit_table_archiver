# Quick Fix for Docker Issues

## Issue 1: Missing `Any` Import ✅ FIXED

**Error**: `NameError: name 'Any' is not defined` in `s3_rate_limiter.py`

**Fix**: Added `Any` to imports in `src/archiver/s3_rate_limiter.py`

```python
from typing import Any, Optional  # Added Any
```

## Issue 2: Config Using `localhost` in Docker ✅ FIXED

**Error**: Can't connect to database when running in Docker

**Problem**: `config.yaml` uses `host: localhost`, which doesn't work inside Docker containers (localhost refers to the container itself, not the host)

**Fix**: Updated `docker-compose.yml` to use `config.yaml.docker` which uses service names:
- `host: postgres` (instead of `localhost`)
- `endpoint: http://minio:9000` (instead of `http://localhost:9000`)

## Rebuild Required

After fixing the `Any` import, rebuild the Docker image:

```bash
cd docker
docker compose build archiver
```

## Run Again

```bash
docker compose up
```

The archiver service should now work correctly with:
- ✅ Proper imports
- ✅ Correct Docker networking (postgres, minio service names)

