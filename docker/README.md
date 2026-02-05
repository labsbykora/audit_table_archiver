# Docker Compose Setup

This directory contains Docker Compose configuration for running the Audit Table Archiver with PostgreSQL and MinIO (S3-compatible storage).

## Quick Start

### 1. Start Infrastructure Services

Start PostgreSQL and MinIO:

```bash
docker-compose up -d postgres minio
```

Wait for services to be healthy:

```bash
docker-compose ps
```

### 2. Build Archiver Image

Build the archiver service:

```bash
# From project root
docker build -t audit-archiver:latest -f Dockerfile .

# Or from docker directory
cd docker
docker-compose build archiver
```

### 3. Configure Archiver

Copy the Docker-specific config:

```bash
# From project root
cp docker/config.yaml.docker config.yaml

# Edit config.yaml if needed
# Note: Service names (postgres, minio) are used for Docker networking
```

### 4. Run Archiver

**Option A: One-shot run (exits after archival)**

```bash
# From project root
docker-compose -f docker/docker-compose.yml run --rm archiver \
  python -m archiver.main --config /app/config.yaml
```

**Option B: Interactive shell**

```bash
# From project root
docker-compose -f docker/docker-compose.yml run --rm archiver bash

# Inside container:
python -m archiver.main --config /app/config.yaml --dry-run
python -m archiver.main --config /app/config.yaml
```

**Option C: Development mode (with live code mounting)**

```bash
# Use dev compose file
docker-compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml run --rm archiver bash
```

### 5. Restore Archived Data

**List available archives:**

```bash
# From project root
docker-compose -f docker/docker-compose.yml run --rm archiver \
  python -m restore.main --config /app/config.yaml \
    --database test_db --table sample_records
```

**Restore a specific archive file:**

```bash
# From project root
docker-compose -f docker/docker-compose.yml run --rm archiver \
  python -m restore.main --config /app/config.yaml \
    --s3-key "archives/test_db/public/sample_records/year=2026/month=01/day=04/batch_001.jsonl.gz" \
    --conflict-strategy skip
```

**Restore all archives for a table:**

```bash
# From project root
docker-compose -f docker/docker-compose.yml run --rm archiver \
  python -m restore.main --config /app/config.yaml \
    --database test_db --table sample_records \
    --restore-all \
    --conflict-strategy overwrite
```

**Restore with date filtering:**

```bash
# Restore archives from a date range
docker-compose -f docker/docker-compose.yml run --rm archiver \
  python -m restore.main --config /app/config.yaml \
    --database test_db --table sample_records \
    --restore-all \
    --start-date 2026-01-01 \
    --end-date 2026-01-31 \
    --conflict-strategy upsert
```

**Restore conflict strategies:**
- `skip`: Skip records that already exist (default)
- `overwrite`: Replace existing records with archived data
- `upsert`: Update existing records, insert new ones
- `fail`: Fail if any conflicts are detected

**Additional restore options:**
- `--drop-indexes`: Temporarily drop indexes during restore (faster for large restores)
- `--batch-size`: Number of records per batch (default: 1000)
- `--commit-frequency`: Commit every N batches (default: 1)
- `--dry-run`: Preview restore without making changes

### 6. View Logs

```bash
# View all services
docker-compose -f docker/docker-compose.yml logs -f

# View specific service
docker-compose -f docker/docker-compose.yml logs -f archiver
```

## Service Details

### PostgreSQL

- **Container**: `archiver-postgres`
- **Port**: `5432` (exposed to host)
- **User**: `archiver`
- **Password**: `archiver_password`
- **Database**: `test_db`

**Connect from host**:
```bash
psql -h localhost -p 5432 -U archiver -d test_db
# Password: archiver_password
```

**Connect from container**:
```bash
# Use service name: postgres:5432
```

### MinIO

- **Container**: `archiver-minio`
- **S3 API Port**: `9000` (exposed to host)
- **Console Port**: `9001` (exposed to host)
- **Access Key**: `minioadmin`
- **Secret Key**: `minioadmin`

**Access MinIO Console**:
- URL: http://localhost:9001
- Access Key: `minioadmin`
- Secret Key: `minioadmin`

**Connect from host**:
```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://localhost:9000

aws s3 ls --endpoint-url http://localhost:9000
```

**Connect from container**:
```bash
# Use service name: http://minio:9000
```

### Archiver Service

- **Container**: `archiver-service`
- **Config**: Mounted from `../config.yaml`
- **Dependencies**: `postgres`, `minio` (waits for health checks)

**Environment Variables**:
- `DB_PASSWORD`: Database password
- `AWS_ACCESS_KEY_ID`: S3 access key
- `AWS_SECRET_ACCESS_KEY`: S3 secret key
- `AWS_ENDPOINT_URL`: S3 endpoint URL

## Networking

All services use the default Docker Compose network and can communicate using service names:

- **PostgreSQL**: `postgres:5432`
- **MinIO**: `minio:9000` (S3 API) or `minio:9001` (Console)

The archiver service connects to:
- Database: `postgres` (service name)
- S3: `http://minio:9000` (service name)

## Configuration

### Using Environment Variables

The archiver supports environment variables for configuration:

```yaml
databases:
  - name: test_db
    host: postgres
    port: 5432
    user: archiver
    password_env: DB_PASSWORD  # Reads from environment
    tables:
      - name: sample_records
        # ... table config
```

Set in docker-compose.yml:
```yaml
environment:
  DB_PASSWORD: archiver_password
```

### Mounting Config Files

Mount your config file as a volume:

```yaml
volumes:
  - ../config.yaml:/app/config.yaml:ro
```

## Development Workflow

### 1. Start Services

```bash
docker-compose up -d postgres minio
```

### 2. Run Archiver Locally (Not in Docker)

```bash
# Set environment variables
export DB_PASSWORD=archiver_password
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://localhost:9000

# Run archiver
python -m archiver.main --config config.yaml
```

### 3. Or Run Archiver in Docker

```bash
docker-compose run --rm archiver \
  python -m archiver.main --config /app/config.yaml --dry-run
```

## Testing

Run integration tests:

```bash
# Start services
docker-compose up -d

# Run tests (from project root)
pytest tests/integration/ -v
```

## Troubleshooting

### Services Not Starting

Check service health:
```bash
docker-compose ps
docker-compose logs postgres
docker-compose logs minio
```

### Connection Errors

Verify service names in config:
- Database host should be `postgres` (not `localhost`)
- S3 endpoint should be `http://minio:9000` (not `http://localhost:9000`)

### Config File Not Found

Ensure config is mounted:
```bash
docker-compose run --rm archiver ls -la /app/config.yaml
```

### Permission Errors

The Dockerfile creates a non-root user (`archiver`). If you need to write files, ensure volumes are writable:

```yaml
volumes:
  - ./logs:/app/logs  # Ensure directory is writable
```

## Production Deployment

For production, use Kubernetes (see `docs/deployment-kubernetes.md`) or a production Docker setup:

1. Build production image:
   ```bash
   docker build -t audit-archiver:1.0.0 -f Dockerfile .
   ```

2. Use environment variables or secrets for credentials

3. Use a CronJob or scheduled task to run archival

4. Monitor logs and metrics

## Cleanup

Stop and remove all services:

```bash
docker-compose down
```

Remove volumes (data will be lost):

```bash
docker-compose down -v
```

