# Quick Start Guide

Get up and running with the Audit Table Archiver in 5 minutes.

## Prerequisites

- Python 3.9+
- PostgreSQL 11+ (or Docker for local testing)
- S3-compatible storage (AWS S3, MinIO, etc.)
- Docker and Docker Compose (for local development)

## Installation

```bash
# Clone repository
git clone <repository-url>
cd auditlog_manager

# (Recommended) Create and activate a virtual environment:

# On Linux/macOS:
python3 -m venv venv
source venv/bin/activate

# On Windows:
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"
```
/ End of Selection
```

## Local Development Setup

### 1. Start Local Services

```bash
# Start PostgreSQL and MinIO
docker-compose -f docker/docker-compose.yml up -d

# Verify services are running
docker-compose -f docker/docker-compose.yml ps
```

### 2. Create Test Database and Table

```bash
# Connect to PostgreSQL
psql -h localhost -U archiver -d test_db

# Create test table
CREATE TABLE audit_logs (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER,
    action TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at);

# Insert test data (100 days old)
INSERT INTO audit_logs (user_id, action, metadata, created_at)
SELECT 
    (random() * 10)::int,
    'action_' || i,
    jsonb_build_object('key', 'value_' || i),
    NOW() - INTERVAL '100 days' - (i || ' seconds')::interval
FROM generate_series(1, 1000) i;
```

### 3. Create Configuration

Create `config.yaml`:

```yaml
version: "2.0"

s3:
  endpoint: http://localhost:9000  # MinIO endpoint
  bucket: test-archives
  prefix: archives/
  region: us-east-1
  storage_class: STANDARD

defaults:
  retention_days: 90
  batch_size: 100

databases:
  - name: test_db
    host: localhost
    port: 5432
    user: archiver
    password_env: DB_PASSWORD
    tables:
      - name: audit_logs
        schema: public
        timestamp_column: created_at
        primary_key: id
        retention_days: 90
        batch_size: 100
```

Set environment variable:

For Linux/macOS:
```bash
export DB_PASSWORD=archiver_password
```

For Windows PowerShell:
```powershell
$env:DB_PASSWORD="archiver_password"
```

For Windows Command Prompt:
```cmd
set DB_PASSWORD=archiver_password
```

### 4. Set Up MinIO Bucket

```bash
# Install AWS CLI (if not installed)
pip install awscli

# Create bucket
aws --endpoint-url http://localhost:9000 \
    --access-key-id minioadmin \
    --secret-access-key minioadmin \
    s3 mb s3://test-archives
```

### 5. Set S3 Credentials (MinIO)

You have two options for MinIO credentials:

**Option 1: Environment Variables (Recommended)**
```bash
# Linux/macOS
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

# Windows PowerShell
$env:AWS_ACCESS_KEY_ID="minioadmin"
$env:AWS_SECRET_ACCESS_KEY="minioadmin"

# Windows Command Prompt
set AWS_ACCESS_KEY_ID=minioadmin
set AWS_SECRET_ACCESS_KEY=minioadmin
```

**Option 2: Config File (Development Only)**
Add to your `config.yaml`:
```yaml
s3:
  endpoint: http://localhost:9000
  bucket: test-archives
  access_key_id: minioadmin      # ⚠️ Development only
  secret_access_key: minioadmin  # ⚠️ Development only
```

**Note**: For AWS S3 in production, credentials are typically provided via IAM roles or AWS credentials file.

### 6. Run Dry-Run

```bash
# See what would be archived (no changes)
python -m archiver.main --config config.yaml --dry-run --verbose
```

### 7. Run Archival

```bash
# Actually archive the data
python -m archiver.main --config config.yaml --verbose
```

## Verify Results

### Check Database

```bash
# Connect to PostgreSQL
psql -h localhost -U archiver -d test_db

# Verify records deleted
SELECT COUNT(*) FROM audit_logs;  -- Should be 0
```

### Check S3

```bash
# List archived files
aws --endpoint-url http://localhost:9000 \
    --access-key-id minioadmin \
    --secret-access-key minioadmin \
    s3 ls s3://test-archives/archives/ --recursive
```

## Next Steps

- Read the [Architecture Documentation](architecture.md)
- Review [Example Configurations](../docs/examples/)
- Check [Troubleshooting Guide](troubleshooting.md)

## Common Issues

### Connection Errors

**Problem**: Cannot connect to PostgreSQL
```bash
# Check if PostgreSQL is running
docker-compose -f docker/docker-compose.yml ps postgres

# Check connection
psql -h localhost -U archiver -d test_db
```

### S3 Errors

**Problem**: Cannot connect to S3/MinIO
```bash
# Check if MinIO is running
docker-compose -f docker/docker-compose.yml ps minio

# Test S3 connection
aws --endpoint-url http://localhost:9000 \
    --access-key-id minioadmin \
    --secret-access-key minioadmin \
    s3 ls
```

### Permission Errors

**Problem**: Permission denied
- Ensure database user has SELECT, DELETE permissions
- Ensure S3 credentials are correct
- Check file permissions for configuration

## Production Setup

For production, see the [Operations Manual](operations-manual.md).

---

**Need Help?** Open an issue or check the [FAQ](../README.md#faq).

