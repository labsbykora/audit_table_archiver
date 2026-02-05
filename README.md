# Audit Table Archiver

Production-grade tool to automatically archive historical PostgreSQL audit table data to S3-compatible object storage, reclaiming disk space while maintaining data integrity and compliance requirements.

## Features

- ✅ **Zero Data Loss**: Multi-level verification ensures data integrity
- ✅ **Production Safe**: <5% database performance impact
- ✅ **Multi-Database**: Support for 100+ databases, 1000+ tables with parallel processing
- ✅ **S3-Compatible**: Works with AWS S3, MinIO, DigitalOcean Spaces, and more
- ✅ **Observable**: Comprehensive logging and Prometheus metrics
- ✅ **Resilient**: Automatic recovery from failures with checkpoint/resume
- ✅ **Transaction Safe**: Verify-then-delete pattern with rollback on failure
- ✅ **Distributed Locking**: Prevent concurrent runs with PostgreSQL advisory locks
- ✅ **Checkpoint/Resume**: Automatic resume from interrupted runs
- ✅ **Schema Tracking**: Detect and track schema changes
- ✅ **Watermark Support**: Incremental archival and restore with watermark tracking
- ✅ **Advanced Verification**: SHA-256 checksums, deletion manifests, sample verification
- ✅ **Legal Hold Support**: Automatic skip of tables under legal hold
- ✅ **Retention Policy Enforcement**: Min/max retention validation with classification support
- ✅ **Encryption Enforcement**: Automatic validation for critical tables
- ✅ **Audit Trail**: Immutable audit log for compliance and governance

## Quick Start

### Installation

```bash
# Clone repository
git clone <repository-url>
cd auditlog_manager

# Install dependencies
pip install -e ".[dev]"

# Or install production dependencies only
pip install -e .
```

### Configuration

Create a configuration file `config.yaml`:

```yaml
version: "2.0"

s3:
  endpoint: null  # null for AWS S3, or https://s3.example.com for S3-compatible
  bucket: audit-archives
  prefix: archives/
  region: us-east-1
  storage_class: STANDARD_IA

defaults:
  retention_days: 90
  batch_size: 10000

databases:
  - name: production_db
    host: db.example.com
    port: 5432
    user: archiver
    password_env: DB_PASSWORD
    tables:
      - name: audit_logs
        schema: public
        timestamp_column: created_at
        primary_key: id
        retention_days: 90
```

### Usage

### Archival

```bash
# Dry run (see what would be archived)
python -m archiver.main --config config.yaml --dry-run

# Run archival
python -m archiver.main --config config.yaml

# Verbose output
python -m archiver.main --config config.yaml --verbose

# Archive specific database only
python -m archiver.main --config config.yaml --database production_db

# Archive specific table only
python -m archiver.main --config config.yaml --database production_db --table audit_logs
```

### Configuration Wizard

```bash
# Interactive configuration wizard
python -m wizard.main --output config.yaml

# Non-interactive mode
python -m wizard.main --non-interactive \
  --database-host localhost \
  --database-name mydb \
  --database-user postgres \
  --s3-bucket my-archive-bucket
```

### Archive Validation

```bash
# Validate all archives
python -m validate.main --config config.yaml

# Validate specific database/table
python -m validate.main --config config.yaml --database production_db --table audit_logs

# Validate date range
python -m validate.main --config config.yaml --start-date 2026-01-01 --end-date 2026-01-31
```

### Restore

```bash
# Restore a single archive file
python -m restore.main \
  --config config.yaml \
  --s3-key archives/db/table/year=2026/month=01/day=15/file.jsonl.gz \
  --database production_db \
  --table audit_logs \
  --conflict-strategy skip

# Restore ALL batches for a table (recommended for bulk restore)
# Automatically skips already-restored archives (watermark tracking)
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --conflict-strategy skip

# Restore batches from a specific date range
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --conflict-strategy skip

# Ignore watermark and restore all archives (for re-restore)
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --ignore-watermark

# List available archives
python -m restore.main \
  --config config.yaml \
  --database production_db \
  --table audit_logs
```

### Cost Estimation

```bash
# Estimate S3 storage costs
python -m cost.main --size-gb 100 --storage-class STANDARD_IA

# Compare all storage classes
python -m cost.main --size-gb 100 --compare

# Estimate from record count
python -m cost.main --records 1000000 --avg-record-size 1024
```

## Development

### Setup Development Environment

```bash
# Install development dependencies
pip install -e ".[dev]"

# Set up pre-commit hooks
pre-commit install

# Start local PostgreSQL and MinIO (Docker Compose)
docker-compose -f docker/docker-compose.yml up -d
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov

# Run only unit tests
pytest -m unit

# Run only integration tests (requires Docker)
# First, start test services:
docker-compose -f docker/docker-compose.yml up -d

# Then run integration tests:
pytest -m integration

# Or run specific integration test files:
pytest tests/integration/test_phase4_restore.py -v
pytest tests/integration/test_phase4_validate.py -v
pytest tests/integration/test_phase4_wizard.py -v
pytest tests/integration/test_phase4_cost.py -v
```

See [Integration Testing Guide](docs/integration-testing.md) for detailed instructions.

### Code Quality

```bash
# Format code
black .

# Lint code
ruff check .

# Type checking
mypy .
```

## Project Structure

```
auditlog_manager/
├── src/
│   ├── archiver/          # Main archiver module
│   ├── restore/           # Restore utility (Phase 2)
│   └── utils/             # Shared utilities
├── tests/                 # Test suite
│   ├── unit/              # Unit tests
│   └── integration/       # Integration tests
├── docs/                  # Documentation
├── docker/                # Docker Compose files
└── k8s/                   # Kubernetes manifests
```

## Requirements

- Python 3.9+
- PostgreSQL 11+
- S3-compatible storage (AWS S3, MinIO, etc.)

## Status

✅ **Phase 1 (MVP) - Complete**
✅ **Phase 2 (Weeks 7-10) - Complete**
✅ **Phase 3 (Weeks 11-14) - Complete**
✅ **Phase 4 (Weeks 15-16) - Complete**

**Current Version: 1.0.0**

All planned features are complete:
- ✅ Core archival with zero data loss guarantee
- ✅ Multi-database support with parallel processing
- ✅ Advanced verification (checksums, manifests, sample verification)
- ✅ Schema detection and drift tracking
- ✅ Watermark-based incremental archival
- ✅ Distributed locking and checkpoint/resume
- ✅ Legal hold support and retention policy enforcement
- ✅ Encryption enforcement and audit trail
- ✅ Prometheus metrics and health checks
- ✅ Multi-channel notifications (Email, Slack, Teams)
- ✅ Advanced S3 features (multipart, rate limiting, local fallback)
- ✅ **Advanced restore utility** with schema migration and conflict resolution
- ✅ **Archive validation utility** for integrity verification
- ✅ **Configuration wizard** for interactive setup
- ✅ **Cost estimation tool** for S3 storage planning

## Documentation

### Getting Started
- [Quick Start Guide](docs/quick-start.md)
- [Architecture](docs/architecture.md)
- [Requirements Document](audit_archiver_requirements.md)
- [Implementation Plan](IMPLEMENTATION_PLAN.md)

### Operations
- [Operations Manual](docs/operations-manual.md)
- [Runbooks](docs/runbooks.md)
- [Troubleshooting Guide](docs/troubleshooting.md)
- [FAQ](docs/faq.md)

### Guides
- [Multi-Database Guide](docs/multi-database-guide.md)
- [Performance Tuning Guide](docs/performance-tuning.md)
- [Compliance & Governance Guide](docs/compliance-guide.md)
- [Manual Restore Guide](docs/manual-restore-guide.md)
- [Security & Credentials](docs/security-credentials.md)

## License

MIT

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

**Note**: This project is in active development. API and configuration may change.
