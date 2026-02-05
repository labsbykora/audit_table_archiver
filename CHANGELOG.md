# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-01-XX

### Added - Phase 4: Advanced Capabilities

#### Advanced Restore Utility
- **Restore CLI**: Full-featured restore utility with conflict resolution
- **Schema Migration**: Automatic schema drift detection and record transformation
- **Conflict Resolution**: Multiple strategies (skip, overwrite, fail, upsert)
- **Performance Optimization**: Bulk loading, index dropping, batch processing
- **Checksum Validation**: Automatic checksum verification during restore
- **Restore Watermark Tracking**: Automatic tracking of restored archives for incremental restores
  - S3 and/or database storage for restore watermarks
  - Automatic skip of already-restored archives
  - `--ignore-watermark` flag for full re-restore
  - Date range restore support with watermark filtering

#### Archive Validation Utility
- **Validation CLI**: Separate command for archive integrity validation
- **Metadata Verification**: Validates all metadata files are present
- **Checksum Validation**: Validates SHA-256 checksums
- **Record Count Verification**: Verifies record counts match metadata
- **Orphaned File Detection**: Finds data files without metadata
- **Integrity Reports**: Human-readable and JSON output formats

#### Configuration Wizard
- **Interactive Wizard**: Step-by-step configuration generator
- **Auto-Detection**: Automatically detects tables, timestamp columns, and primary keys
- **Retention Suggestions**: Suggests retention periods based on data age
- **Non-Interactive Mode**: CLI flags for automation
- **Configuration Validation**: Validates configuration before saving

#### Cost Estimation Tool
- **Cost Calculator**: Estimates S3 storage costs
- **Storage Class Comparison**: Compares costs across all storage classes
- **Compression Awareness**: Accounts for gzip compression
- **Region Support**: Adjusts costs by AWS region
- **Multiple Input Methods**: Config file, data size, or record count

### Technical Details

- **Restore Module**: `src/restore/` with S3 reader, restore engine, schema migrator, conflict resolver
- **Validation Module**: `src/validate/` with archive validator
- **Wizard Module**: `src/wizard/` with configuration wizard
- **Cost Module**: `src/cost/` with cost estimator
- **Documentation**: Complete operations manual, runbooks, FAQ

## [0.1.0] - 2025-01-XX

### Added - Phase 1 MVP

#### Core Functionality
- **Configuration System**: YAML-based configuration with environment variable substitution
- **Database Integration**: Async PostgreSQL connection pooling with asyncpg
- **S3 Client**: Boto3-based S3 client with retry logic and S3-compatible storage support
- **Batch Processing**: Cursor-based pagination with SKIP LOCKED for non-blocking selects
- **Serialization**: PostgreSQL type → JSONL conversion supporting all common types
- **Compression**: Gzip compression with configurable levels
- **Verification**: Multi-level count verification (DB → Memory → S3) and primary key verification
- **Transaction Safety**: Verify-then-delete pattern with automatic rollback on failure
- **CLI Interface**: Full command-line interface with dry-run, filtering, and verbose modes

#### Features
- Multi-database and multi-table support
- Retention period-based archival (configurable per table)
- Batch processing with configurable batch sizes
- S3 upload with retry logic (exponential backoff)
- Upload verification (size and existence checks)
- Transaction-based delete operations
- Comprehensive error handling with context
- Structured JSON logging with correlation IDs
- Dry-run mode for safe testing

#### Testing
- Unit tests for all core modules (>50 test cases)
- Integration tests with real PostgreSQL and MinIO
- Performance benchmarks
- CI/CD pipeline with GitHub Actions

#### Documentation
- Comprehensive README with quick start guide
- Architecture documentation
- Implementation plan
- Phase 1 progress tracking
- Example configurations

### Technical Details

- **Language**: Python 3.9+
- **Database**: PostgreSQL 11+ via asyncpg
- **Storage**: S3-compatible (AWS S3, MinIO, etc.) via boto3
- **Configuration**: YAML with Pydantic validation
- **Logging**: Structured JSON logging with structlog
- **Testing**: pytest with >70% coverage target

### Known Limitations (Phase 3+)

- Prometheus metrics endpoint
- Notifications (email, Slack, etc.)
- Advanced restore utility with schema migration
- Configuration wizard

---

## [Unreleased]

### Added

#### Phase 3 Week 14: Advanced S3 Features & Network Resilience
- **Multipart upload with resume**: Full multipart upload implementation for large files (>10MB) with state tracking and resume capability
- **S3 rate limiting**: Token bucket algorithm for rate limiting API calls, handles 503 SlowDown responses
- **Local disk fallback**: Automatic save to local disk on S3 upload failure with metadata tracking and cleanup
- **Orphaned upload cleanup**: Automatic cleanup of stale multipart uploads (already implemented, verified)
- **Configuration options**: Added `rate_limit_requests_per_second`, `local_fallback_dir`, and `local_fallback_retention_days` to S3 config

#### Phase 3 Week 13: Notifications & Alerting
- **Multi-channel notifications**: Email (SMTP), Slack webhooks, Microsoft Teams webhooks
- **Notification templates**: Pre-formatted messages for success, failure, start, threshold violations, and digest summaries
- **Digest mode**: Daily summary notifications instead of individual notifications
- **Rate limiting**: Alert fatigue prevention (configurable hours between notifications)
- **Quiet hours**: Suppress notifications during specified time windows
- **Event-based alerting**: Notifications on archival start, success, failure, and threshold violations
- **Configuration**: Comprehensive notification settings in YAML config
- **Integration**: Notifications integrated into archival workflow at all key points - Phase 3 Week 12: Advanced Monitoring & Metrics

#### Prometheus Metrics
- **Comprehensive Metrics**: Counters, histograms, and gauges for all archival operations
- **Phase Timing**: Detailed duration tracking per phase (query, serialize, compress, upload, verify, delete, vacuum)
- **Processing Rates**: Batch processing rate metrics (records/second)
- **State Tracking**: Current archiver state (idle, running, failed)
- **HTTP Metrics Endpoint**: Prometheus-compatible `/metrics` endpoint on configurable port (default: 8000)

#### Progress Tracking
- **Real-Time Progress**: Live progress updates with configurable interval (default: 5 seconds)
- **ETA Calculation**: Estimated time remaining based on current processing rate
- **Progress Percentage**: Accurate progress percentage calculation
- **Quiet Mode**: Suppress progress output for cron/scheduled execution
- **Per-Table Tracking**: Progress tracking per database/table/schema

#### Health Check
- **Comprehensive Health Checks**: Database and S3 connectivity checks
- **HTTP Health Endpoint**: `/health` endpoint on configurable port (default: 8001)
- **Status Classification**: Healthy, degraded, or unhealthy status
- **Response Time Tracking**: Health check response times for monitoring
- **JSON Response Format**: Detailed health status in JSON format

#### Configuration
- `monitoring.metrics_enabled`: Enable/disable Prometheus metrics (default: true)
- `monitoring.metrics_port`: Port for metrics endpoint (default: 8000)
- `monitoring.progress_enabled`: Enable/disable progress tracking (default: true)
- `monitoring.progress_update_interval`: Progress update interval in seconds (default: 5.0)
- `monitoring.quiet_mode`: Quiet mode for cron (default: false)
- `monitoring.health_check_enabled`: Enable/disable health check endpoint (default: true)
- `monitoring.health_check_port`: Port for health check endpoint (default: 8001)

#### Testing
- **Unit Tests**: 60 comprehensive unit tests (all passing)
  - 21 tests for `ArchiverMetrics`
  - 16 tests for `ProgressTracker`
  - 11 tests for `HealthChecker`
  - 8 tests for `HealthCheckServer`

### Added - Phase 3 Week 11: Compliance Features

#### Legal Hold Support
- **Legal Hold Checking**: Query database table or API endpoint before archival
- **Automatic Skip**: Tables under legal hold are automatically skipped with audit logging
- **Record-Level Holds**: Support for WHERE clause filters for record-level legal holds
- **Expiration Tracking**: Automatic detection of expired legal holds
- **Multiple Sources**: Support for database table, API endpoint, or configuration file

#### Retention Policy Enforcement
- **Min/Max Validation**: Enforce minimum and maximum retention periods per table
- **Classification-Based Policies**: Support for data classification-specific retention (PII, INTERNAL, etc.)
- **Pre-Archival Validation**: Retention policy checked before archival begins
- **Configuration**: Configurable min/max retention days via `ComplianceConfig`

#### Encryption Support Enhancement
- **Enforcement for Critical Tables**: Critical tables must have encryption enabled
- **Validation**: Automatic validation that critical tables use encryption (not "none")
- **Integration**: Works with existing SSE-S3 and SSE-KMS support

#### Audit Trail
- **Immutable Audit Log**: Append-only audit trail for all operations
- **Multiple Storage Options**: S3 or database storage for audit logs
- **Comprehensive Events**: Logs archive start/success/failure, restore operations, errors
- **Rich Metadata**: Includes timestamp, operator, record count, duration, S3 path, status
- **Event Types**: ARCHIVE_START, ARCHIVE_SUCCESS, ARCHIVE_FAILURE, RESTORE_START, RESTORE_SUCCESS, RESTORE_FAILURE, ERROR

#### Configuration Enhancements
- `legal_holds`: Legal hold configuration (enabled, check_table, api_endpoint, etc.)
- `compliance`: Compliance configuration (min_retention_days, max_retention_days, enforce_encryption, data_classifications)

#### Documentation
- Legal hold setup guide
- Retention policy configuration examples
- Audit trail query examples
- Compliance best practices

### Added - Phase 2 Week 7: Multi-Database Support

#### Multi-Database Processing
- **Parallel Database Processing**: Optional parallel processing of multiple databases with concurrency limits
- **Per-Database Statistics**: Detailed statistics tracking per database (tables, records, batches, timing)
- **Connection Pool Configuration**: Per-database connection pool size configuration with global defaults
- **Enhanced Error Isolation**: Improved error reporting with per-database context and failure isolation

#### Configuration Enhancements
- `parallel_databases`: Enable parallel database processing (default: false)
- `max_parallel_databases`: Maximum concurrent databases (default: 3, max: 10)
- `connection_pool_size`: Global default connection pool size per database (default: 5)
- Per-database `connection_pool_size` override support

#### Documentation
- Multi-database configuration guide
- Example multi-database configuration file
- Best practices for sequential vs parallel processing

### Added - Phase 2 Week 8: Advanced Verification & Checksums

#### Data Integrity Enhancements
- **SHA-256 Checksums**: Calculate and store checksums for JSONL and compressed data
- **Deletion Manifests**: Generate and store manifests detailing deleted primary keys
- **Random Sample Verification**: Verify random samples (1% min 10, max 1000) after deletion
- **Comprehensive Metadata**: Metadata files with schema, counts, checksums, timestamps per batch

#### Verification Features
- Checksum calculation for uncompressed JSONL data
- Checksum calculation for compressed data
- Primary key verification with deletion manifests
- Sample verification to ensure archived records are deleted
- Metadata file generation with full batch information

### Added - Phase 2 Week 9: Schema Management & Watermarks

#### Schema Detection & Tracking
- **Schema Detection**: Query `information_schema` to capture complete table structure
- **Schema Drift Detection**: Compare current schema with previous to detect changes
- **Schema Storage**: Store schema information in metadata files
- **Drift Warnings**: Log warnings when schema changes are detected

#### Watermark Management
- **Watermark Storage**: Store last archived timestamp and primary key (S3 or database)
- **Incremental Archival**: Use watermarks to resume from last position
- **Watermark Updates**: Automatically update watermarks after successful batches
- **Storage Backends**: Support for S3 and database storage backends

### Added - Phase 2 Week 10: Resilience & Recovery

#### Distributed Locking
- **PostgreSQL Advisory Locks**: Prevent concurrent runs using PostgreSQL advisory locks
- **File-Based Locks**: Local file-based locking for development/testing
- **Lock Heartbeat**: Automatic lock extension every 30 seconds
- **Stale Lock Detection**: Detect and handle stale locks
- **Concurrent Run Prevention**: Fail fast if another instance is running

#### Checkpoint & Resume
- **Checkpoint System**: Save checkpoint every N batches (default: 10)
- **Checkpoint Storage**: Store checkpoints in S3 or local files
- **Resume Capability**: Automatically resume from last checkpoint on startup
- **Checkpoint Cleanup**: Delete checkpoints after successful completion

#### Multipart Upload Cleanup
- **Orphaned Upload Detection**: Identify stale multipart uploads
- **Automatic Cleanup**: Clean up orphaned uploads when resuming from checkpoint
- **Prefix Filtering**: Clean up uploads for specific database/table
- **Statistics Tracking**: Track cleanup statistics (found, aborted, failed)

### Added - Performance Optimizations & Error Handling Improvements

#### Enhanced Retry Logic
- **Exponential Backoff with Jitter**: Configurable retry logic with random jitter to prevent thundering herd
- **Async and Sync Support**: Retry utilities for both async and sync functions
- **Configurable Retry Parameters**: Max attempts, delays, exponential base, and retryable exceptions

#### Circuit Breaker Pattern
- **Failure Protection**: Prevents cascading failures with circuit breaker pattern
- **Automatic Recovery**: Circuit breaker automatically attempts recovery after timeout
- **Three States**: CLOSED (normal), OPEN (failing), HALF_OPEN (testing recovery)
- **S3 Integration**: Circuit breaker integrated into S3 client operations

#### Adaptive Batch Sizing
- **Performance-Based Adjustment**: Automatically adjusts batch size based on query performance
- **Target Query Time**: Maintains target query execution time (default: 2 seconds)
- **Configurable Limits**: Min/max batch sizes with configurable adjustment factor

#### S3 Client Enhancements
- **Circuit Breaker Integration**: S3 operations protected by circuit breaker
- **Enhanced Retry Logic**: Improved retry strategy with exponential backoff and jitter
- **Better Error Handling**: More context in error messages and structured error information

#### Documentation
- **Performance Tuning Guide**: Comprehensive guide for optimizing archiver performance
- **Enhanced Troubleshooting**: Added error handling section and performance testing patterns
- **Updated README**: Added links to new documentation and enhanced feature list

### Planned for Phase 2 (Remaining)
- Compliance and governance features
- Advanced monitoring and alerting

---

[0.1.0]: https://github.com/your-org/auditlog_manager/releases/tag/v0.1.0

