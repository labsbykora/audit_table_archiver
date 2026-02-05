# Integration Testing Guide

## Overview

Integration tests validate the archiver against real PostgreSQL and MinIO (S3-compatible) services. These tests ensure end-to-end functionality works correctly in a production-like environment.

## Prerequisites

1. **Docker and Docker Compose** installed and running
2. **Python 3.9+** with project dependencies installed
3. **Network access** to localhost ports 5432 (PostgreSQL) and 9000 (MinIO)

## Quick Start

### 1. Start Test Services

Start PostgreSQL and MinIO using Docker Compose:

```bash
# From project root
docker-compose -f docker/docker-compose.yml up -d
```

This will start:
- **PostgreSQL** on `localhost:5432`
  - Database: `test_db`
  - User: `archiver`
  - Password: `archiver_password`
- **MinIO** on `localhost:9000`
  - Access Key: `minioadmin`
  - Secret Key: `minioadmin`
  - Bucket: `test-archives` (created automatically)

### 2. Verify Services Are Running

```bash
# Check containers
docker-compose -f docker/docker-compose.yml ps

# Check PostgreSQL
psql -h localhost -p 5432 -U archiver -d test_db -c "SELECT version();"

# Check MinIO (via curl or browser)
curl http://localhost:9000/minio/health/live
```

### 3. Run Integration Tests

#### Run All Integration Tests

```bash
pytest tests/integration/ -v
```

#### Run Specific Test Files

```bash
# Phase 4 tests
pytest tests/integration/test_phase4_restore.py -v
pytest tests/integration/test_phase4_validate.py -v
pytest tests/integration/test_phase4_wizard.py -v
pytest tests/integration/test_phase4_cost.py -v

# Other integration tests
pytest tests/integration/test_end_to_end.py -v
pytest tests/integration/test_edge_cases.py -v
pytest tests/integration/test_database_operations.py -v
pytest tests/integration/test_s3_operations.py -v
```

#### Run Specific Tests

```bash
# Run a single test
pytest tests/integration/test_phase4_restore.py::test_restore_from_archive -v

# Run tests matching a pattern
pytest tests/integration/ -k "restore" -v
pytest tests/integration/ -k "validate" -v
```

#### Run with Coverage

```bash
pytest tests/integration/ -v --cov=src --cov-report=term
```

#### Run in Parallel (if pytest-xdist installed)

```bash
pytest tests/integration/ -v -n auto
```

## Test Categories

### Phase 4 Integration Tests

- **Restore Tests** (`test_phase4_restore.py`): Test restore utility with real S3 archives
- **Validation Tests** (`test_phase4_validate.py`): Test archive validation with real S3
- **Wizard Tests** (`test_phase4_wizard.py`): Test configuration wizard with real database
- **Cost Tests** (`test_phase4_cost.py`): Test cost estimation calculations

### Core Integration Tests

- **End-to-End Tests** (`test_end_to_end.py`): Full archival workflow
- **Edge Cases** (`test_edge_cases.py`): Error handling and edge cases
- **Database Operations** (`test_database_operations.py`): Database connectivity and operations
- **S3 Operations** (`test_s3_operations.py`): S3 upload/download operations

## Troubleshooting

### Services Not Starting

```bash
# Check Docker is running
docker ps

# Check for port conflicts
netstat -an | grep 5432  # PostgreSQL
netstat -an | grep 9000  # MinIO

# View logs
docker-compose -f docker/docker-compose.yml logs postgres
docker-compose -f docker/docker-compose.yml logs minio
```

### Tests Skipping

If tests are being skipped with messages like:
- `"PostgreSQL container not running"`
- `"MinIO container not running"`

Ensure services are started:
```bash
docker-compose -f docker/docker-compose.yml up -d
# Wait a few seconds for services to be ready
sleep 5
pytest tests/integration/ -v
```

### Connection Errors

If you see connection errors:

1. **PostgreSQL connection refused**:
   ```bash
   # Check if PostgreSQL is listening
   docker-compose -f docker/docker-compose.yml ps postgres
   
   # Restart if needed
   docker-compose -f docker/docker-compose.yml restart postgres
   ```

2. **MinIO connection refused**:
   ```bash
   # Check if MinIO is listening
   docker-compose -f docker/docker-compose.yml ps minio
   
   # Restart if needed
   docker-compose -f docker/docker-compose.yml restart minio
   ```

### Test Data Cleanup

Tests automatically clean up:
- Temporary files (created in system temp directory)
- Test tables (dropped after each test)
- S3 objects (can be manually cleaned if needed)

To manually clean S3:
```bash
# Access MinIO console at http://localhost:9001
# Login with minioadmin/minioadmin
# Delete test-archives bucket contents
```

### Environment Variables

Tests use these environment variables (set automatically by fixtures):
- `TEST_DB_PASSWORD=archiver_password`
- `AWS_ACCESS_KEY_ID=minioadmin`
- `AWS_SECRET_ACCESS_KEY=minioadmin`

You can override these if needed:
```bash
export TEST_DB_PASSWORD=your_password
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
pytest tests/integration/ -v
```

## Continuous Integration

For CI/CD pipelines, ensure:

1. Docker services are started before tests:
   ```yaml
   # Example GitHub Actions
   - name: Start test services
     run: |
       docker-compose -f docker/docker-compose.yml up -d
       sleep 10  # Wait for services to be ready
   
   - name: Run integration tests
     run: pytest tests/integration/ -v
   ```

2. Services are stopped after tests:
   ```yaml
   - name: Stop test services
     run: docker-compose -f docker/docker-compose.yml down
   ```

## Performance Considerations

- Integration tests are slower than unit tests (require real services)
- Typical runtime: 30-60 seconds for all Phase 4 tests
- Use `-n auto` with pytest-xdist for parallel execution
- Consider running integration tests separately from unit tests in CI

## Next Steps

After running integration tests:
1. Review test output for any failures
2. Check test coverage report
3. Investigate any skipped tests
4. Review logs for detailed error messages

