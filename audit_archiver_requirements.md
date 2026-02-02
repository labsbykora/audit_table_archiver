# Audit Table Archiver - Requirements Document v2.0

**Version:** 2.0  
**Date:** December 30, 2025  
**Status:** Final Draft  
**Changes from v1.0:** Added 85+ critical requirements for production robustness

---

## 1. Executive Summary

### 1.1 Purpose
Design and implement a production-grade tool to automatically archive historical audit table data from multiple PostgreSQL databases to S3-compatible object storage, reclaim disk space, and maintain data integrity and compliance requirements.

### 1.2 Problem Statement
Audit tables across multiple PostgreSQL databases are growing unbounded, consuming disk space and impacting database performance. Organizations need to:
- Retain audit data for compliance (7+ years typically)
- Reclaim expensive database storage
- Maintain data integrity and recoverability
- Operate without application changes or database partitioning

### 1.3 Success Criteria
- Archive audit data older than configurable retention period (default: 90 days)
- Reduce database storage consumption by 60-80%
- Zero data loss during archival process
- Minimal production database impact (<5% performance degradation)
- Support multiple databases and tables from single tool
- Complete archive runs within maintenance windows (target: <2 hours)
- Handle all failure scenarios gracefully with automatic recovery

---

## 2. Functional Requirements

### 2.1 Multi-Database Support

#### 2.1.1 Database Configuration
**REQ-DB-001:** System SHALL support archiving from multiple PostgreSQL databases on same or different servers  
**REQ-DB-002:** System SHALL support per-database connection configuration (host, port, credentials)  
**REQ-DB-003:** System SHALL process databases sequentially by default to minimize server load  
**REQ-DB-004:** System SHALL optionally support parallel database processing with configurable concurrency limit  
**REQ-DB-005:** System SHALL isolate failures (one database failure SHALL NOT stop processing of other databases)

#### 2.1.2 Credentials Management
**REQ-DB-006:** System SHALL support multiple credential sources:
- Environment variables (preferred)
- AWS Secrets Manager
- HashiCorp Vault
- Configuration file (development only, with security warnings)

**REQ-DB-007:** System SHALL NOT log or expose credentials in logs or error messages  
**REQ-DB-008:** System SHALL support separate read replica connections for query operations  
**REQ-DB-009:** System SHALL validate credentials before beginning archival operations  
**REQ-DB-010:** System SHALL support connection pooling with configurable pool size (default: 5)

#### 2.1.3 PostgreSQL Version Support
**REQ-PG-001:** System SHALL detect PostgreSQL version and adapt behavior accordingly  
**REQ-PG-002:** System SHALL support PostgreSQL versions 11 through 16  
**REQ-PG-003:** For PostgreSQL <9.5, system SHALL use alternative locking strategy to SKIP LOCKED  
**REQ-PG-004:** For PostgreSQL 12+, system SHALL leverage improved VACUUM performance features  
**REQ-PG-005:** System SHALL detect native partitioned tables (PostgreSQL 10+) and handle appropriately  
**REQ-PG-006:** System SHALL use pg_stat_progress_vacuum (PostgreSQL 9.6+) when available for monitoring  
**REQ-PG-007:** System SHALL document minimum supported version (11) and maximum tested version (16)  
**REQ-PG-008:** System SHALL warn if using unsupported PostgreSQL version

### 2.2 Table Selection and Configuration

#### 2.2.1 Explicit Table Configuration
**REQ-TBL-001:** System SHALL accept explicit list of tables per database via configuration file  
**REQ-TBL-002:** Each table configuration SHALL specify:
- Table name (required)
- Timestamp column name (required)
- Primary key column name (required)
- Retention period in days (optional, uses default if not specified)
- Critical flag (optional, enables additional safety checks)
- Schema name (optional, defaults to 'public')

**REQ-TBL-003:** System SHALL validate all configured tables exist before starting archival  
**REQ-TBL-004:** System SHALL validate timestamp and primary key columns exist and are correct data types  
**REQ-TBL-005:** System SHALL verify timestamp column contains valid timestamp values  
**REQ-TBL-006:** System SHALL verify primary key column has unique constraint or primary key constraint  
**REQ-TBL-007:** System SHALL detect and warn about tables without indexes on timestamp column  
**REQ-TBL-008:** System SHALL recommend index creation if missing

#### 2.2.2 Table Discovery (Optional)
**REQ-TBL-009:** System MAY support auto-discovery of tables matching naming patterns  
**REQ-TBL-010:** Discovery mode SHALL support:
- SQL LIKE pattern matching (e.g., `%_audit`)
- Schema filtering
- Explicit exclusion list
- Column name patterns for timestamp detection

**REQ-TBL-011:** Discovery mode SHALL validate discovered tables meet archival requirements  
**REQ-TBL-012:** System SHALL generate configuration file from discovered tables for review

### 2.3 Transaction Management & Atomicity

#### 2.3.1 Transaction Boundaries
**REQ-TXN-001:** Each batch SHALL be processed in independent, isolated transaction  
**REQ-TXN-002:** Transaction scope SHALL be: BEGIN → SELECT → Upload → Verify → DELETE → COMMIT  
**REQ-TXN-003:** If upload fails, transaction SHALL ROLLBACK (no database changes)  
**REQ-TXN-004:** If verification fails, transaction SHALL ROLLBACK (no database changes)  
**REQ-TXN-005:** Transaction SHALL commit only after successful S3 upload verification  
**REQ-TXN-006:** System SHALL use READ COMMITTED isolation level for all transactions

#### 2.3.2 Transaction Timeouts and Monitoring
**REQ-TXN-007:** System SHALL enforce configurable transaction timeout (default: 30 minutes)  
**REQ-TXN-008:** System SHALL monitor transaction age via pg_stat_activity  
**REQ-TXN-009:** System SHALL warn if transaction age exceeds 50% of timeout threshold  
**REQ-TXN-010:** System SHALL abort transaction if timeout exceeded and retry with smaller batch  
**REQ-TXN-011:** System SHALL monitor replication lag and pause if lag exceeds threshold (default: 60 seconds)

#### 2.3.3 Savepoints and Recovery
**REQ-TXN-012:** System SHALL support SAVEPOINT mechanism for sub-batch recovery  
**REQ-TXN-013:** System SHALL create savepoint before each delete operation  
**REQ-TXN-014:** System SHALL rollback to savepoint on delete failure  
**REQ-TXN-015:** System SHALL limit savepoint depth to prevent excessive overhead

### 2.4 Concurrency Control & Locking

#### 2.4.1 Row-Level Locking
**REQ-LOCK-001:** System SHALL use `SELECT ... FOR UPDATE SKIP LOCKED` for row selection  
**REQ-LOCK-002:** System SHALL NOT acquire table-level locks during SELECT operations  
**REQ-LOCK-003:** System SHALL handle lock timeout gracefully (log and retry with backoff)  
**REQ-LOCK-004:** System SHALL support configurable lock wait timeout (default: 10 seconds)  
**REQ-LOCK-005:** System SHALL skip locked rows and continue with available rows

#### 2.4.2 Lock Monitoring and Safety
**REQ-LOCK-006:** System SHALL query pg_stat_activity to detect lock contention  
**REQ-LOCK-007:** System SHALL detect blocking queries and optionally pause archival  
**REQ-LOCK-008:** If locks held exceed 5 minutes, system SHALL alert and optionally abort  
**REQ-LOCK-009:** System SHALL monitor for lock escalation to table level  
**REQ-LOCK-010:** System SHALL coordinate with database activity to avoid blocking critical queries

#### 2.4.3 Distributed Locking
**REQ-LOCK-011:** System SHALL implement distributed locking to prevent concurrent runs  
**REQ-LOCK-012:** Distributed lock SHALL support backends: Redis, PostgreSQL advisory locks, file-based  
**REQ-LOCK-013:** Lock timeout SHALL be configurable (default: 120 minutes)  
**REQ-LOCK-014:** System SHALL release locks on graceful shutdown  
**REQ-LOCK-015:** System SHALL implement lock heartbeat mechanism (update every 30 seconds)  
**REQ-LOCK-016:** System SHALL detect and break stale locks (no heartbeat >2x timeout)

### 2.5 Archival Logic

#### 2.5.1 Age-Based Selection
**REQ-ARC-001:** System SHALL archive records where timestamp column < (current_date - retention_days)  
**REQ-ARC-002:** System SHALL use `<` operator (not `<=`) to avoid timezone boundary issues  
**REQ-ARC-003:** System SHALL support per-table retention periods overriding global defaults  
**REQ-ARC-004:** System SHALL add configurable safety buffer (default: 1 day) to retention calculation  
**REQ-ARC-005:** System SHALL use database server time (`now()`) not client time for cutoff calculation

#### 2.5.2 Batch Processing
**REQ-ARC-006:** System SHALL process records in configurable batch sizes (default: 10,000 records)  
**REQ-ARC-007:** System SHALL use cursor-based pagination with ORDER BY timestamp, primary_key  
**REQ-ARC-008:** System SHALL NOT use OFFSET-based pagination (performance concerns)  
**REQ-ARC-009:** System SHALL track last processed timestamp+primary_key for cursor continuation  
**REQ-ARC-010:** System SHALL support configurable sleep delay between batches (default: 2 seconds)  
**REQ-ARC-011:** System SHALL handle partial batches (last batch may be smaller than batch_size)

#### 2.5.3 Adaptive Batch Sizing
**REQ-ARC-012:** System MAY implement adaptive batch sizing based on performance metrics  
**REQ-ARC-013:** Adaptive sizing SHALL start with initial_batch_size (default: 5,000)  
**REQ-ARC-014:** System SHALL increase batch size by 50% if batch completes in <10 seconds  
**REQ-ARC-015:** System SHALL decrease batch size by 50% if batch exceeds 120 seconds  
**REQ-ARC-016:** System SHALL enforce minimum batch size (1,000) and maximum (50,000)  
**REQ-ARC-017:** System SHALL base adaptation on: query time, row size, or upload time

#### 2.5.4 Watermark Tracking (Incremental Mode)
**REQ-ARC-018:** System SHALL support watermark tracking to avoid reprocessing archived data  
**REQ-ARC-019:** Watermarks SHALL be stored per database+table combination  
**REQ-ARC-020:** Watermark storage options SHALL include: S3, database table, or local file  
**REQ-ARC-021:** System SHALL update watermarks only after successful batch completion  
**REQ-ARC-022:** Watermark SHALL include: last archived timestamp, last primary key, record count  
**REQ-ARC-023:** System SHALL verify watermark integrity before using (checksum validation)

### 2.6 Time and Timezone Handling

#### 2.6.1 Timestamp Normalization
**REQ-TIME-001:** System SHALL normalize all timestamps to UTC before comparison  
**REQ-TIME-002:** System SHALL detect clock skew between client and database (warn if >5 minutes)  
**REQ-TIME-003:** System SHALL handle TIMESTAMP and TIMESTAMPTZ columns appropriately  
**REQ-TIME-004:** System SHALL store cutoff timestamp in UTC in audit log  
**REQ-TIME-005:** System SHALL preserve original timezone information in archived data

#### 2.6.2 DST and Edge Cases
**REQ-TIME-006:** System SHALL detect DST transitions in archive window and warn  
**REQ-TIME-007:** System SHALL handle ambiguous timestamps during DST fall-back  
**REQ-TIME-008:** System SHALL handle non-existent timestamps during DST spring-forward  
**REQ-TIME-009:** System SHALL document timestamp handling behavior for each column type

### 2.7 S3 Storage

#### 2.7.1 S3 Configuration
**REQ-S3-001:** System SHALL support AWS S3 and S3-compatible storage (MinIO, DigitalOcean Spaces, Backblaze B2, etc.)  
**REQ-S3-002:** System SHALL support configurable S3 endpoint URL for non-AWS providers  
**REQ-S3-003:** System SHALL validate S3 bucket accessibility before starting archival  
**REQ-S3-004:** System SHALL support configurable S3 storage classes (STANDARD, STANDARD_IA, GLACIER_IR, INTELLIGENT_TIERING)  
**REQ-S3-005:** System SHALL support S3 versioning for archive protection  
**REQ-S3-006:** System SHALL verify S3 credentials have required permissions (PutObject, GetObject, DeleteObject)

#### 2.7.2 File Organization
**REQ-S3-007:** System SHALL organize archives using hierarchical path structure:
```
s3://{bucket}/{prefix}/{database_name}/{table_name}/year={YYYY}/month={MM}/day={DD}/{filename}
```

**REQ-S3-008:** Filename SHALL include: table name, ISO timestamp, batch number, and extension  
**REQ-S3-009:** Filename format: `{table}_{YYYYMMDDTHHmmssZ}_batch_{NNN}.jsonl.gz`  
**REQ-S3-010:** System SHALL support configurable path prefix for multi-tenant deployments  
**REQ-S3-011:** System SHALL create date partition based on archive execution time (not record timestamp)

#### 2.7.3 File Format
**REQ-S3-012:** System SHALL use JSONL (JSON Lines) format - one JSON object per line  
**REQ-S3-013:** System SHALL support gzip compression (configurable compression level 1-9, default: 6)  
**REQ-S3-014:** Each JSON object SHALL contain all columns from original table row  
**REQ-S3-015:** System SHALL handle PostgreSQL-specific types correctly:
- JSON/JSONB: preserve as nested JSON
- Arrays: convert to JSON arrays
- BYTEA: encode as base64
- UUID: preserve as string
- NUMERIC/DECIMAL: preserve precision as string
- Timestamps: ISO 8601 format with timezone

**REQ-S3-016:** System SHALL include row metadata in each JSON object:
- _archived_at: UTC timestamp of archival
- _batch_id: unique batch identifier
- _source_database: database name
- _source_table: table name

#### 2.7.4 Metadata Files
**REQ-S3-017:** System SHALL generate metadata file per batch containing:
- Database name and host
- Table name and schema
- Record count in batch
- Date range (min/max timestamp from records)
- Batch number
- Archive timestamp (ISO 8601 UTC)
- Schema version and column list
- Checksum (SHA-256 of uncompressed JSONL content)
- Compression algorithm and level
- File size (compressed and uncompressed)
- PostgreSQL version
- Archiver version

**REQ-S3-018:** Metadata files SHALL be named `_metadata.json` in same directory as data file  
**REQ-S3-019:** System SHALL generate batch manifest file listing all batches for a table  
**REQ-S3-020:** Manifest SHALL be updated atomically after each successful batch

#### 2.7.5 Upload Operations and Network Resilience
**REQ-S3-021:** System SHALL use multipart upload for files exceeding threshold (default: 10MB)  
**REQ-S3-022:** Multipart upload part size SHALL be configurable (default: 5MB, max: 5GB)  
**REQ-S3-023:** System SHALL implement retry logic with exponential backoff (3 retries, base: 2s, max: 30s)  
**REQ-S3-024:** System SHALL verify upload success by checking object existence and size match  
**REQ-S3-025:** System SHALL NOT delete database records until S3 upload is verified

#### 2.7.6 Multipart Upload Management
**REQ-NET-001:** System SHALL track uploaded parts and resume from last successful part on failure  
**REQ-NET-002:** System SHALL maintain upload state file for resume capability  
**REQ-NET-003:** Upload state file SHALL include: upload_id, completed_parts[], part_etags[]  
**REQ-NET-004:** System SHALL verify each part upload with ETag validation  
**REQ-NET-005:** On connection reset, system SHALL retry current part only (not entire file)  
**REQ-NET-006:** System SHALL clean up incomplete multipart uploads after configurable timeout (default: 24 hours)  
**REQ-NET-007:** System SHALL implement cleanup job for orphaned multipart uploads  
**REQ-NET-008:** After all retries exhausted, system SHALL save batch to local disk for manual recovery  
**REQ-NET-009:** System SHALL provide utility to resume failed uploads from local disk

#### 2.7.7 S3 Rate Limiting
**REQ-NET-010:** System SHALL respect S3 rate limits (3,500 PUT/s, 5,500 GET/s per prefix)  
**REQ-NET-011:** System SHALL implement token bucket algorithm for S3 API calls  
**REQ-NET-012:** System SHALL track API call rate and throttle proactively  
**REQ-NET-013:** System SHALL handle 503 SlowDown responses with exponential backoff  
**REQ-NET-014:** System SHALL distribute uploads across multiple S3 prefixes if rate limited

### 2.8 Data Integrity and Verification

#### 2.8.1 Verify-Then-Delete Pattern
**REQ-INT-001:** System SHALL follow strict ordering: FETCH → UPLOAD → VERIFY → DELETE → COMMIT  
**REQ-INT-002:** System SHALL use database transactions for delete operations  
**REQ-INT-003:** System SHALL rollback transaction if any step fails  
**REQ-INT-004:** System SHALL verify S3 object exists and matches expected size before deletion  
**REQ-INT-005:** System SHALL perform three-way verification: DB count, fetched count, S3 count

#### 2.8.2 Multi-Level Record Counting
**REQ-VER-001:** System SHALL count records in database BEFORE fetching (SELECT COUNT(*) with same WHERE clause)  
**REQ-VER-002:** System SHALL count records in fetched result set (in-memory)  
**REQ-VER-003:** System SHALL count records after writing to S3 (from JSONL line count)  
**REQ-VER-004:** System SHALL verify all three counts match exactly  
**REQ-VER-005:** System SHALL abort batch if count mismatch detected (never delete on mismatch)  
**REQ-VER-006:** System SHALL log all three counts in audit trail

#### 2.8.3 Primary Key Verification
**REQ-VER-007:** System SHALL extract primary keys from fetched records  
**REQ-VER-008:** System SHALL verify primary keys in S3 match exactly those in delete statement  
**REQ-VER-009:** System SHALL maintain deletion manifest (list of PKs deleted) per batch  
**REQ-VER-010:** System SHALL store deletion manifest in S3 alongside data file  
**REQ-VER-011:** System SHALL support post-archival audit: verify random PKs from S3 not present in database

#### 2.8.4 Checksum Validation
**REQ-VER-012:** System SHALL calculate SHA-256 checksum of uncompressed JSONL before compression  
**REQ-VER-013:** System SHALL store checksum in metadata file  
**REQ-VER-014:** System SHALL optionally verify checksums during restore operations  
**REQ-VER-015:** System SHALL provide separate verification utility to validate all archives  
**REQ-VER-016:** Verification utility SHALL support: checksum validation, count validation, schema validation

#### 2.8.5 Sample Verification
**REQ-VER-017:** System SHALL perform sample verification (random 1% of records, min 10, max 1000)  
**REQ-VER-018:** Sample verification SHALL: download random records from S3, verify not in database  
**REQ-VER-019:** Sample verification SHALL run after batch completion, before next batch  
**REQ-VER-020:** System SHALL log sample verification results

#### 2.8.6 Schema Change Detection
**REQ-INT-006:** System SHALL query information_schema before first batch to capture schema  
**REQ-INT-007:** System SHALL detect table schema changes between archival runs  
**REQ-INT-008:** System SHALL detect: column additions, column removals, type changes, constraint changes  
**REQ-INT-009:** System SHALL log warnings when schema changes are detected  
**REQ-INT-010:** System SHALL optionally fail archival if schema drift is detected (configurable)  
**REQ-INT-011:** System SHALL store schema version in metadata file  
**REQ-INT-012:** System SHALL include full schema DDL in first batch metadata

#### 2.8.7 Foreign Key Protection
**REQ-INT-013:** System SHALL query information_schema to detect foreign key constraints  
**REQ-INT-014:** System SHALL warn if foreign keys reference audit tables  
**REQ-INT-015:** System SHALL list all FK dependencies in validation report  
**REQ-INT-016:** System SHALL NOT block archival due to foreign keys (audit tables should be independent)  
**REQ-INT-017:** System SHALL optionally check for orphaned FK references after deletion

#### 2.8.8 Staged Deletion (Optional)
**REQ-INT-018:** System MAY support staged deletion mode where:
- Records moved to `{table_name}_archived` table
- Retained for configurable period (default: 24 hours)
- Permanently deleted after confirmation period
- Original table and archived table must have identical schema

**REQ-INT-019:** Staged deletion SHALL use RENAME or CREATE TABLE AS SELECT  
**REQ-INT-020:** System SHALL provide utility to promote staged deletions to permanent  
**REQ-INT-021:** System SHALL provide utility to rollback staged deletions

### 2.9 Data Type Handling

#### 2.9.1 Supported Data Types
**REQ-TYPE-001:** System SHALL support all PostgreSQL primitive types (integer, text, numeric, boolean, etc.)  
**REQ-TYPE-002:** System SHALL serialize PostgreSQL arrays as JSON arrays  
**REQ-TYPE-003:** System SHALL preserve JSON/JSONB column values exactly  
**REQ-TYPE-004:** System SHALL handle NULL values explicitly in JSONL (JSON null)  
**REQ-TYPE-005:** System SHALL support BYTEA columns (encode as base64 with prefix)  
**REQ-TYPE-006:** System SHALL support custom ENUM types (serialize as string)  
**REQ-TYPE-007:** System SHALL handle UUID types natively (preserve as string)  
**REQ-TYPE-008:** System SHALL preserve precision for NUMERIC/DECIMAL types (use string representation)  
**REQ-TYPE-009:** System SHALL handle composite types (serialize as nested JSON)  
**REQ-TYPE-010:** System SHALL handle range types (serialize as JSON object with lower/upper bounds)

#### 2.9.2 Large Objects and Wide Rows
**REQ-TYPE-011:** System SHALL warn if row size exceeds configurable threshold (default: 10MB)  
**REQ-TYPE-012:** System SHALL support rows up to PostgreSQL maximum (1GB with TOAST)  
**REQ-TYPE-013:** System SHALL handle TOAST-ed columns transparently  
**REQ-TYPE-014:** System SHALL warn if table has extremely wide rows (>1000 columns)  
**REQ-TYPE-015:** System SHALL chunk large BYTEA columns if exceeding threshold

#### 2.9.3 Unsupported Types
**REQ-TYPE-016:** System SHALL document any unsupported types explicitly  
**REQ-TYPE-017:** System SHALL fail validation if table contains unsupported types  
**REQ-TYPE-018:** System SHALL provide workaround documentation for unsupported types

### 2.10 Resource Management

#### 2.10.1 Memory Limits
**REQ-RES-001:** System SHALL enforce maximum memory usage per batch (default: 500MB)  
**REQ-RES-002:** System SHALL monitor memory usage and warn if exceeding 80% of limit  
**REQ-RES-003:** System SHALL reduce batch size automatically if memory limit reached  
**REQ-RES-004:** System SHALL use streaming for large result sets when possible  
**REQ-RES-005:** System SHALL release memory after each batch completion

#### 2.10.2 Disk Space Management
**REQ-RES-006:** System SHALL check available disk space before starting (need 2x max batch size)  
**REQ-RES-007:** System SHALL monitor disk space during execution  
**REQ-RES-008:** System SHALL abort if available disk space drops below threshold (default: 10GB)  
**REQ-RES-009:** System SHALL clean up temporary files after successful upload  
**REQ-RES-010:** System SHALL implement emergency cleanup on disk space critical

#### 2.10.3 Connection Limits
**REQ-RES-011:** System SHALL limit concurrent database connections (max 5 per database)  
**REQ-RES-012:** System SHALL reuse database connections across batches  
**REQ-RES-013:** System SHALL implement connection health checks  
**REQ-RES-014:** System SHALL close idle connections after configurable timeout (default: 5 minutes)  
**REQ-RES-015:** System SHALL respect database max_connections limit

#### 2.10.4 Processing Limits
**REQ-RES-016:** System SHALL enforce maximum table size for archival (default: 5TB, configurable)  
**REQ-RES-017:** System SHALL estimate completion time before starting  
**REQ-RES-018:** System SHALL abort if estimated duration exceeds maximum (default: 8 hours)  
**REQ-RES-019:** System SHALL support multi-day archival runs with checkpointing  
**REQ-RES-020:** System SHALL limit maximum batches per table per run (default: unlimited, configurable)

### 2.11 Space Reclamation

#### 2.11.1 Vacuum Operations
**REQ-VAC-001:** System SHALL run `VACUUM (ANALYZE)` after each table completion (default)  
**REQ-VAC-002:** System SHALL support optional `VACUUM FULL` via command-line flag `--vacuum-full`  
**REQ-VAC-003:** System SHALL support `VACUUM (ANALYZE, VERBOSE)` for detailed output  
**REQ-VAC-004:** System SHALL measure table size before archival via `pg_total_relation_size()`  
**REQ-VAC-005:** System SHALL measure table size after vacuum  
**REQ-VAC-006:** System SHALL calculate space reclaimed (bytes and percentage)  
**REQ-VAC-007:** System SHALL log space reclaimed per table  
**REQ-VAC-008:** System SHALL document that `VACUUM FULL` locks tables exclusively

#### 2.11.2 Vacuum Monitoring
**REQ-VAC-009:** System SHALL use `pg_stat_progress_vacuum` (PostgreSQL 9.6+) to monitor progress  
**REQ-VAC-010:** System SHALL log vacuum progress every 30 seconds  
**REQ-VAC-011:** System SHALL enforce vacuum timeout (default: 2 hours)  
**REQ-VAC-012:** System SHALL detect and warn if vacuum is ineffective (<10% space reclaimed)

#### 2.11.3 Vacuum Strategy
**REQ-VAC-013:** System SHALL support configurable vacuum strategy per table:
- none: Skip vacuum entirely
- analyze: Run ANALYZE only
- standard: Run VACUUM (ANALYZE)
- full: Run VACUUM FULL (maintenance window required)

**REQ-VAC-014:** System SHALL recommend vacuum strategy based on table characteristics  
**REQ-VAC-015:** System SHALL support deferred vacuum (schedule for later maintenance window)

### 2.12 Idempotency and Duplicate Prevention

#### 2.12.1 Batch Identification
**REQ-IDEM-001:** System SHALL generate deterministic batch IDs using hash of: database, table, timestamp range, batch number  
**REQ-IDEM-002:** Batch ID format: `{db}_{table}_{start_ts}_{end_ts}_{batch_num}` (SHA-256 hash)  
**REQ-IDEM-003:** System SHALL check if batch already archived before processing  
**REQ-IDEM-004:** System SHALL maintain batch manifest in S3 (list of all batch IDs per table)

#### 2.12.2 Duplicate Detection
**REQ-IDEM-005:** System SHALL query batch manifest before starting each batch  
**REQ-IDEM-006:** System SHALL skip batches already in manifest (idempotent operation)  
**REQ-IDEM-007:** System SHALL update manifest atomically using S3 conditional writes (If-None-Match)  
**REQ-IDEM-008:** System SHALL handle manifest update conflicts (concurrent runs) gracefully  
**REQ-IDEM-009:** System SHALL support duplicate detection window (default: 24 hours)  
**REQ-IDEM-010:** System SHALL log when skipping already-archived batches

#### 2.12.3 Force Re-Archive
**REQ-IDEM-011:** System SHALL support `--force-rearchive` flag for manual recovery  
**REQ-IDEM-012:** Force mode SHALL require explicit confirmation (not safe for production)  
**REQ-IDEM-013:** System SHALL log all forced re-archives to audit trail  
**REQ-IDEM-014:** System SHALL append `_v2`, `_v3` suffixes to duplicate archive filenames

### 2.13 Recovery and Restoration

#### 2.13.1 Restore Utility Requirements
**REQ-RST-001:** System SHALL include companion restore utility as separate command  
**REQ-RST-002:** Restore utility SHALL support modes:
- Full table restore from date range
- Specific batch file restore
- Restore to original table
- Restore to different table
- Restore to different database
- Filtered restore (WHERE clause on archive data)

**REQ-RST-003:** Restore utility SHALL validate checksums before insertion  
**REQ-RST-004:** Restore utility SHALL handle schema mismatches gracefully

#### 2.13.2 Schema Evolution Handling
**REQ-RESTORE-001:** Restore SHALL detect schema differences between archive and target table  
**REQ-RESTORE-002:** Restore SHALL support schema migration strategies:
- Strict: Fail on any schema difference
- Lenient: Map columns by name, ignore extra/missing columns
- Transform: Apply user-defined transformation functions

**REQ-RESTORE-003:** Restore SHALL provide schema diff report before proceeding  
**REQ-RESTORE-004:** Restore SHALL validate data types are compatible  
**REQ-RESTORE-005:** Restore SHALL handle column additions (use NULL or default for missing columns)  
**REQ-RESTORE-006:** Restore SHALL handle column removals (ignore columns not in target)  
**REQ-RESTORE-007:** Restore SHALL handle type changes (with explicit casting rules)

#### 2.13.3 Conflict Resolution
**REQ-RESTORE-008:** Restore SHALL support conflict resolution strategies:
- SKIP: Skip records with duplicate primary keys (log skipped)
- OVERWRITE: Update existing records (UPDATE)
- FAIL: Abort on first conflict
- MERGE: Use custom merge logic (user-defined function)
- UPSERT: INSERT ... ON CONFLICT DO UPDATE

**REQ-RESTORE-009:** Restore SHALL detect conflicts before bulk insert  
**REQ-RESTORE-010:** Restore SHALL support dry-run mode showing conflicts without changes  
**REQ-RESTORE-011:** Restore SHALL generate conflict report (conflicting PKs, resolution applied)

#### 2.13.4 Restore Performance
**REQ-RESTORE-012:** Restore SHALL use COPY FROM for bulk loading (not INSERT)  
**REQ-RESTORE-013:** Restore SHALL support configurable batch size for restore (default: 50,000)  
**REQ-RESTORE-014:** Restore SHALL temporarily disable triggers during restore (optional)  
**REQ-RESTORE-015:** Restore SHALL temporarily drop indexes and rebuild after (optional)  
**REQ-RESTORE-016:** Restore SHALL use transactions with configurable commit frequency

#### 2.13.5 Restore Validation
**REQ-RESTORE-017:** Restore SHALL count records restored and compare to archive metadata  
**REQ-RESTORE-018:** Restore SHALL verify sample of restored records (random 1%)  
**REQ-RESTORE-019:** Restore SHALL create restore audit log (what was restored, when, by whom, from which archives)

#### 2.13.6 Incremental and Filtered Restore
**REQ-RESTORE-020:** Restore SHALL support incremental restore (only new records since last restore)  
**REQ-RESTORE-021:** Restore SHALL support filtering by column values during restore (e.g., WHERE user_id = 123)  
**REQ-RESTORE-022:** Restore SHALL estimate restore time and space requirements before proceeding  
**REQ-RESTORE-023:** Restore SHALL support date range filtering (restore only specific time periods)

#### 2.13.7 Rollback Support
**REQ-RST-005:** System SHALL support rollback of recent archival operations  
**REQ-RST-006:** Rollback SHALL be possible within configurable time window (default: 24 hours)  
**REQ-RST-007:** Rollback SHALL restore data from S3 to original table using restore utility  
**REQ-RST-008:** Rollback SHALL verify no conflicting records exist in table  
**REQ-RST-009:** Rollback SHALL update watermarks to pre-archive state

### 2.14 Compliance and Governance

#### 2.14.1 Retention Policies
**REQ-CMP-001:** System SHALL enforce minimum retention periods per table (configurable, default: 7 days)  
**REQ-CMP-002:** System SHALL enforce maximum retention periods per table (configurable, default: 7 years)  
**REQ-CMP-003:** System SHALL prevent archival of records younger than minimum retention  
**REQ-CMP-004:** System SHALL support data classification levels (PUBLIC, INTERNAL, CONFIDENTIAL, PII, FINANCIAL)  
**REQ-CMP-005:** System SHALL enforce classification-specific retention policies  
**REQ-CMP-006:** System SHALL validate retention policy compliance before archival

#### 2.14.2 Encryption
**REQ-CMP-007:** System SHALL support SSL/TLS for all database connections (minimum TLS 1.2)  
**REQ-CMP-008:** System SHALL support S3 server-side encryption (SSE-S3, SSE-KMS, SSE-C)  
**REQ-CMP-009:** System SHALL enforce encryption for tables marked as containing sensitive data  
**REQ-CMP-010:** System SHALL support client-side encryption before S3 upload (optional)  
**REQ-CMP-011:** System SHALL store encryption keys in secure vault (AWS KMS, HashiCorp Vault)  
**REQ-CMP-012:** System SHALL never log decrypted sensitive data

#### 2.14.3 Legal Hold
**REQ-CMP-013:** System SHALL support legal hold mechanism to prevent archival  
**REQ-CMP-014:** Legal holds SHALL be checkable via: database table, external API, or configuration file  
**REQ-CMP-015:** System SHALL query legal holds before archiving each table  
**REQ-CMP-016:** System SHALL skip archival for tables under legal hold and log appropriately  
**REQ-CMP-017:** Legal hold SHALL include: table name, reason, start date, expiration date, requestor  
**REQ-CMP-018:** System SHALL support record-level legal holds (WHERE clause filter)

#### 2.14.4 Audit Trail and Immutability
**REQ-CMP-019:** System SHALL maintain audit log of all archival operations  
**REQ-CMP-020:** Audit log SHALL be stored in separate metadata database or S3  
**REQ-CMP-021:** Audit log SHALL be immutable (append-only)  
**REQ-CMP-022:** Audit log SHALL include: timestamp, operator, database, table, record count, S3 path, status, duration  
**REQ-CMP-023:** Audit log SHALL be tamper-evident (cryptographic chain or S3 Object Lock)  
**REQ-CMP-024:** System SHALL support audit log export for compliance reporting

#### 2.14.5 Access Control and Authorization
**REQ-CMP-025:** System SHALL log all access to archived data  
**REQ-CMP-026:** System SHALL support role-based access control (RBAC) for operations  
**REQ-CMP-027:** System SHALL require authentication for all operations  
**REQ-CMP-028:** System SHALL integrate with enterprise SSO (SAML, OIDC) for authentication  
**REQ-CMP-029:** System SHALL support least-privilege principle (separate roles for archive, restore, admin)

### 2.15 Multi-Tenancy Support (Optional)

#### 2.15.1 Tenant Isolation
**REQ-TNT-001:** System MAY support multi-tenant aware archiving  
**REQ-TNT-002:** System SHALL support tenant_id column filtering  
**REQ-TNT-003:** System SHALL support per-tenant retention policies  
**REQ-TNT-004:** System SHALL isolate tenant data in separate S3 paths  
**REQ-TNT-005:** System SHALL support per-tenant encryption keys

#### 2.15.2 Tenant Discovery and Configuration
**REQ-TNT-006:** System SHALL support auto-discovery of tenants from table data  
**REQ-TNT-007:** System SHALL support explicit tenant list in configuration  
**REQ-TNT-008:** System SHALL enforce resource limits per tenant (max time, max records)  
**REQ-TNT-009:** System SHALL support tenant priority levels (high priority tenants process first)

---

## 3. Non-Functional Requirements

### 3.1 Performance

**REQ-PERF-001:** System SHALL process minimum 10,000 records per minute per table  
**REQ-PERF-002:** System SHALL complete archival of 1 million records within 2 hours  
**REQ-PERF-003:** Database query overhead SHALL NOT exceed 5% of normal load during archival  
**REQ-PERF-004:** System SHALL support read replica usage to minimize primary database impact  
**REQ-PERF-005:** S3 upload operations SHALL support parallel uploads (max 3 concurrent per table)  
**REQ-PERF-006:** System SHALL adaptively adjust batch size based on performance metrics  
**REQ-PERF-007:** System SHALL achieve minimum 10 MB/s upload speed to S3  
**REQ-PERF-008:** System SHALL complete vacuum operations within 50% of archival time

### 3.2 Reliability

**REQ-REL-001:** System SHALL achieve 99.9% success rate for archival operations  
**REQ-REL-002:** System SHALL implement automatic retry for transient failures (network, timeout)  
**REQ-REL-003:** System SHALL support checkpoint/resume for long-running operations  
**REQ-REL-004:** System SHALL persist state to allow resume after crash or interruption  
**REQ-REL-005:** System SHALL implement distributed locking to prevent concurrent runs  
**REQ-REL-006:** System SHALL achieve zero data loss (verified through testing)  
**REQ-REL-007:** System SHALL recover from all transient failures without manual intervention  
**REQ-REL-008:** Mean time to recovery (MTTR) SHALL be less than 1 hour for failures

### 3.3 Observability

#### 3.3.1 Logging
**REQ-OBS-001:** System SHALL use structured logging (JSON format)  
**REQ-OBS-002:** System SHALL support configurable log levels (DEBUG, INFO, WARN, ERROR, CRITICAL)  
**REQ-OBS-003:** System SHALL log to stdout/stderr and optionally to file  
**REQ-OBS-004:** System SHALL include correlation IDs in all log entries per run  
**REQ-OBS-005:** System SHALL implement log rotation (max size: 100MB, keep 10 files)  
**REQ-OBS-006:** System SHALL support remote logging (syslog, CloudWatch Logs, Datadog)

#### 3.3.2 Metrics and Instrumentation
**REQ-OBS-007:** System SHALL expose Prometheus-compatible metrics endpoint  
**REQ-OBS-008:** System SHALL track metrics:
- Records archived per table
- Bytes uploaded to S3 (total and per table)
- Duration per table and per phase (query, upload, delete, vacuum)
- Success/failure counts (per database, per table)
- Space reclaimed (bytes and percentage)
- Upload speed (MB/s current and average)
- Memory usage (current, peak, per batch)
- Database connection count
- Lock wait time
- Transaction duration
- Batch processing rate (records/second)

**REQ-OBS-009:** System SHALL optionally publish metrics to CloudWatch/Datadog/Grafana  
**REQ-OBS-010:** System SHALL support custom metric labels (environment, cluster, etc.)

#### 3.3.3 Progress Tracking
**REQ-OBS-011:** System SHALL display real-time progress during execution  
**REQ-OBS-012:** Progress display SHALL include:
- Current database and table
- Records processed / total
- Percentage complete
- Estimated time remaining (ETA)
- Current processing rate
- Batches completed / total

**REQ-OBS-013:** System SHALL support quiet mode for cron/scheduled execution  
**REQ-OBS-014:** System SHALL support progress bar in interactive mode  
**REQ-OBS-015:** System SHALL update progress every 5 seconds minimum

#### 3.3.4 Debugging and Diagnostics
**REQ-DEBUG-001:** System SHALL implement heartbeat logging (log every 60 seconds during active processing)  
**REQ-DEBUG-002:** System SHALL track memory usage per batch and log if increasing  
**REQ-DEBUG-003:** System SHALL alert if memory growth exceeds 10% per batch (potential leak)  
**REQ-DEBUG-004:** System SHALL implement timeout per batch (default: 30 minutes)  
**REQ-DEBUG-005:** System SHALL log query execution plans for slow queries (>10 seconds)  
**REQ-DEBUG-006:** System SHALL track and log detailed timing breakdown:
- Time in SELECT query
- Time serializing to JSON
- Time compressing
- Time uploading to S3
- Time verifying upload
- Time in DELETE query
- Time in VACUUM

**REQ-DEBUG-007:** System SHALL support verbose debug mode with full SQL logging  
**REQ-DEBUG-008:** System SHALL generate diagnostic report on failure including:
- Full stack trace
- Recent log entries
- System resource state
- Database connection state
- S3 connection state
- Current configuration

**REQ-DEBUG-009:** System SHALL support profiling mode to identify performance bottlenecks

### 3.4 Notifications and Alerting

#### 3.4.1 Notification Channels
**REQ-NOT-001:** System SHALL support multiple notification channels:
- Email (SMTP)
- Slack webhooks
- Microsoft Teams webhooks
- Custom webhooks (JSON POST)
- PagerDuty
- SNS (AWS Simple Notification Service)

**REQ-NOT-002:** System SHALL support multiple recipients per channel  
**REQ-NOT-003:** System SHALL support notification templates with variables  
**REQ-NOT-004:** System SHALL include summary statistics in notifications

#### 3.4.2 Notification Events
**REQ-NOT-005:** System SHALL send notifications on:
- Archival completion (success)
- Archival failure (immediate)
- Threshold violations (record count, size, duration)
- Legal hold detected
- Schema drift detected
- Lock contention detected
- Resource limit exceeded

**REQ-NOT-006:** System SHALL support notification priority levels (info, warning, error, critical)  
**REQ-NOT-007:** System SHALL support different channels for different priority levels

#### 3.4.3 Digest and Rate Limiting
**REQ-NOT-008:** System SHALL support digest mode (single daily summary email)  
**REQ-NOT-009:** System SHALL implement alert fatigue prevention (max 1 alert per issue per 4 hours)  
**REQ-NOT-010:** System SHALL group similar alerts in digest  
**REQ-NOT-011:** System SHALL support quiet hours (no notifications during specified times)

#### 3.4.4 Threshold Alerts
**REQ-MON-001:** System SHALL alert if record count >2 standard deviations from 30-day average  
**REQ-MON-002:** System SHALL alert if batch processing time >3x historical average  
**REQ-MON-003:** System SHALL alert if space reclaimed <50% of expected  
**REQ-MON-004:** System SHALL calculate expected metrics from historical runs (minimum 7 days history)  
**REQ-MON-005:** System SHALL track and alert on trend changes (gradual increase in processing time)  
**REQ-MON-006:** System SHALL support custom threshold definitions per table

### 3.5 Security

**REQ-SEC-001:** System SHALL NOT store plaintext credentials in configuration files  
**REQ-SEC-002:** System SHALL NOT log sensitive data (passwords, encryption keys, PII) unless explicitly configured  
**REQ-SEC-003:** System SHALL support IAM role-based authentication for AWS S3  
**REQ-SEC-004:** System SHALL validate SSL certificates for all HTTPS connections  
**REQ-SEC-005:** System SHALL support certificate pinning for S3 endpoints  
**REQ-SEC-006:** System SHALL run as non-privileged user (not root)  
**REQ-SEC-007:** System SHALL audit all access to archived data  
**REQ-SEC-008:** System SHALL implement rate limiting to prevent abuse  
**REQ-SEC-009:** System SHALL sanitize all user inputs to prevent SQL injection  
**REQ-SEC-010:** System SHALL use parameterized queries exclusively (no string concatenation)  
**REQ-SEC-011:** System SHALL support security scanning integration (SAST, DAST)  
**REQ-SEC-012:** System SHALL implement secrets rotation support

### 3.6 Scalability

**REQ-SCA-001:** System SHALL support archiving from 100+ databases  
**REQ-SCA-002:** System SHALL support 1000+ tables across all databases  
**REQ-SCA-003:** System SHALL handle tables with 100M+ rows  
**REQ-SCA-004:** System SHALL process minimum 100GB of data per run  
**REQ-SCA-005:** System SHALL scale horizontally (parallel database processing)  
**REQ-SCA-006:** System SHALL support sharding of large tables across multiple archival runs  
**REQ-SCA-007:** System SHALL handle 1TB+ total archive size  
**REQ-SCA-008:** System SHALL support distributed deployment (multiple archiver instances)

### 3.7 Maintainability

**REQ-MNT-001:** System SHALL use Python 3.9+ for broad compatibility  
**REQ-MNT-002:** System SHALL minimize dependencies (use standard library where possible)  
**REQ-MNT-003:** System SHALL include comprehensive inline documentation (docstrings)  
**REQ-MNT-004:** System SHALL follow PEP 8 style guidelines  
**REQ-MNT-005:** System SHALL include unit and integration tests (>80% coverage)  
**REQ-MNT-006:** System SHALL use type hints throughout codebase  
**REQ-MNT-007:** System SHALL include contribution guidelines  
**REQ-MNT-008:** System SHALL use semantic versioning (MAJOR.MINOR.PATCH)  
**REQ-MNT-009:** System SHALL maintain CHANGELOG with all releases  
**REQ-MNT-010:** System SHALL support plugin architecture for extensibility

### 3.8 Usability

**REQ-USE-001:** System SHALL provide interactive configuration wizard  
**REQ-USE-002:** System SHALL validate configuration file syntax and semantics  
**REQ-USE-003:** System SHALL provide comprehensive `--help` documentation  
**REQ-USE-004:** System SHALL support dry-run mode with sample output  
**REQ-USE-005:** System SHALL provide meaningful error messages with resolution guidance  
**REQ-USE-006:** System SHALL generate example configuration files  
**REQ-USE-007:** System SHALL support command-line autocompletion (bash, zsh)  
**REQ-USE-008:** System SHALL provide progress indicators for long-running operations  
**REQ-USE-009:** System SHALL support interactive mode for confirming destructive operations  
**REQ-USE-010:** System SHALL provide clear exit codes (0=success, 1=partial, 2=failure)

### 3.9 Disaster Recovery

#### 3.9.1 Backup and Protection
**REQ-DR-001:** System SHALL support S3 versioning for archive protection  
**REQ-DR-002:** System SHALL maintain backup of configuration files in S3  
**REQ-DR-003:** System SHALL maintain backup of archive operation logs in S3  
**REQ-DR-004:** System SHALL support cross-region S3 replication (optional)  
**REQ-DR-005:** System SHALL support S3 Object Lock for immutability (WORM)

#### 3.9.2 Validation and Integrity Checking
**REQ-DR-006:** System SHALL provide separate archive validation job  
**REQ-DR-007:** Validation job SHALL verify:
- All metadata files present
- Checksums match archived data
- Record counts consistent with metadata
- No orphaned files
- Schema consistency across batches

**REQ-DR-008:** Validation job SHALL generate integrity report  
**REQ-DR-009:** System SHALL support scheduled validation runs (weekly recommended)

#### 3.9.3 Recovery Procedures
**REQ-DR-010:** System SHALL document disaster recovery procedures:
- Recovering from accidental deletion
- Recovering from S3 bucket loss (restore from backup/replica)
- Recovering from configuration corruption
- Recovering from database corruption

**REQ-DR-011:** System SHALL provide recovery time objective (RTO) estimate per scenario  
**REQ-DR-012:** System SHALL support emergency stop mechanism (stop file or signal)  
**REQ-DR-013:** System SHALL support emergency rollback to pre-archive state

### 3.10 Cost Management

**REQ-COST-001:** System SHALL support maximum cost limit per run (abort if exceeded)  
**REQ-COST-002:** System SHALL calculate estimated cost before uploading each batch  
**REQ-COST-003:** System SHALL track actual vs estimated costs  
**REQ-COST-004:** System SHALL alert if costs exceed budget by 20%  
**REQ-COST-005:** System SHALL support S3 lifecycle policies in configuration  
**REQ-COST-006:** System SHALL recommend cost optimizations based on access patterns  
**REQ-COST-007:** System SHALL track cost per database/table for chargeback  
**REQ-COST-008:** System SHALL estimate monthly storage costs based on growth rate  
**REQ-COST-009:** System SHALL support cost allocation tags for AWS billing

---

## 4. Operational Requirements

### 4.1 Deployment

**REQ-DEP-001:** System SHALL be deployable as:
- Standalone Python script
- Docker container
- Kubernetes CronJob
- AWS Lambda function (for smaller workloads <15 minutes)
- Systemd service

**REQ-DEP-002:** System SHALL include deployment documentation for each method  
**REQ-DEP-003:** System SHALL provide example systemd service files  
**REQ-DEP-004:** System SHALL provide example Kubernetes manifests  
**REQ-DEP-005:** System SHALL provide Dockerfile with multi-stage build  
**REQ-DEP-006:** System SHALL support configuration via environment variables or config file  
**REQ-DEP-007:** System SHALL support ConfigMap/Secret mounting in Kubernetes  
**REQ-DEP-008:** System SHALL provide Helm chart for Kubernetes deployment

### 4.2 Scheduling

**REQ-SCH-001:** System SHALL be executable via cron or scheduled task  
**REQ-SCH-002:** System SHALL implement single-instance locking (prevent overlapping runs)  
**REQ-SCH-003:** System SHALL support configurable timeout (max execution time)  
**REQ-SCH-004:** System SHALL exit with appropriate status codes (0=success, 1=partial, 2=failure, 3=validation error)  
**REQ-SCH-005:** System SHALL support cron expression parsing for scheduling recommendations  
**REQ-SCH-006:** System SHALL detect and warn if running outside recommended schedule

### 4.3 Monitoring Integration

**REQ-MON-007:** System SHALL integrate with monitoring systems via:
- Prometheus metrics endpoint (HTTP server on configurable port)
- Healthcheck endpoint (HTTP /health)
- Status file output (JSON file updated during run)
- CloudWatch metrics (AWS)
- Datadog metrics

**REQ-MON-008:** System SHALL expose operational metrics:
- Last successful run timestamp
- Records archived in last 24 hours
- Current run status (idle, running, failed)
- Error counts by type
- Tables pending archival
- Estimated completion time

**REQ-MON-009:** Healthcheck endpoint SHALL return:
- HTTP 200 if healthy
- HTTP 503 if unhealthy (with reason)
- JSON body with detailed status

### 4.4 Execution Modes

**REQ-EXE-001:** System SHALL support execution modes:
- `--dry-run`: Show what would be archived without changes
- `--validate-only`: Check configuration and connectivity
- `--database <name>`: Process single database only
- `--table <name>`: Process single table only
- `--verify-archives`: Verify integrity of existing archives
- `--restore`: Restore data from archives (separate utility)
- `--cleanup`: Clean up orphaned multipart uploads

**REQ-EXE-002:** System SHALL support limiting records for testing (`--limit N`)  
**REQ-EXE-003:** System SHALL support date range filtering (`--start-date`, `--end-date`)  
**REQ-EXE-004:** System SHALL support parallel execution (`--parallel --max-workers N`)  
**REQ-EXE-005:** System SHALL support force mode (`--force`) to bypass confirmations  
**REQ-EXE-006:** System SHALL support verbose mode (`--verbose` or `-v`, `-vv`, `-vvv`)

---

## 5. Configuration Management

### 5.1 Configuration File Format

**REQ-CFG-001:** System SHALL use YAML format for configuration  
**REQ-CFG-002:** Configuration SHALL support:
- Global defaults (apply to all databases/tables)
- Per-database overrides
- Per-table overrides
- Hierarchical precedence: table > database > global

**REQ-CFG-003:** Configuration SHALL support environment variable substitution (`${ENV_VAR}` syntax)  
**REQ-CFG-004:** Configuration SHALL support include/import of additional files (`!include other.yaml`)  
**REQ-CFG-005:** Configuration SHALL support comments (YAML standard)  
**REQ-CFG-006:** Configuration SHALL support anchors and aliases for reusability

### 5.2 Configuration Validation

**REQ-CFG-007:** System SHALL validate configuration against JSON schema  
**REQ-CFG-008:** System SHALL provide detailed validation error messages with line numbers  
**REQ-CFG-009:** System SHALL support configuration version for backward compatibility  
**REQ-CFG-010:** System SHALL auto-upgrade old configuration formats with warnings  
**REQ-CFG-011:** System SHALL validate all referenced environment variables exist

### 5.3 Pre-Flight Validation

**REQ-VAL-001:** System SHALL test database connectivity before starting archival  
**REQ-VAL-002:** System SHALL verify SELECT, DELETE, VACUUM permissions on all tables  
**REQ-VAL-003:** System SHALL verify S3 bucket exists and is writable (test write/delete small object)  
**REQ-VAL-004:** System SHALL verify timestamp columns contain valid timestamps (not all NULL)  
**REQ-VAL-005:** System SHALL verify primary key columns have unique constraint  
**REQ-VAL-006:** System SHALL validate retention_days >0 and <36500 (100 years)  
**REQ-VAL-007:** System SHALL verify sufficient disk space for temp files (2x batch size)  
**REQ-VAL-008:** System SHALL check PostgreSQL version compatibility  
**REQ-VAL-009:** System SHALL verify no circular dependencies between tables  
**REQ-VAL-010:** System SHALL validate S3 credentials have required permissions (PutObject, GetObject, DeleteObject, ListBucket)  
**REQ-VAL-011:** System SHALL check for table locks and warn if table is heavily used  
**REQ-VAL-012:** System SHALL estimate total archival time and warn if excessive

### 5.4 Configuration Examples

**REQ-CFG-012:** System SHALL include example configurations for:
- Single database, single table (getting started)
- Multiple databases, multiple tables (production)
- Multi-tenant setup
- Compliance-focused setup (PII, financial data)
- Performance-optimized setup (large tables)
- High-security setup (encryption, legal holds)

**REQ-CFG-013:** System SHALL provide configuration migration guide between versions

---

## 6. Error Handling and Recovery

### 6.1 Error Categories

**REQ-ERR-001:** System SHALL categorize errors as:
- FATAL: Cannot continue execution (config error, S3 unreachable)
- ERROR: Database/table failure (continue with next)
- WARNING: Non-critical issue (schema change detected, slow query)
- INFO: Operational information (batch completed)

**REQ-ERR-002:** System SHALL use distinct exit codes per error category

### 6.2 Error Recovery Strategies

**REQ-ERR-003:** System SHALL retry transient errors automatically:
- Network errors (connection timeout, reset)
- Database deadlocks
- S3 throttling (503 SlowDown)
- Temporary resource exhaustion

**REQ-ERR-004:** System SHALL NOT retry permanent errors:
- Configuration errors
- Permission denied
- Table not found
- Invalid SQL

**REQ-ERR-005:** System SHALL use exponential backoff for retries (2^n seconds, max 30s)  
**REQ-ERR-006:** System SHALL limit retry attempts (default: 3 per operation)  
**REQ-ERR-007:** System SHALL log all errors with full context (stack trace, parameters, state)  
**REQ-ERR-008:** System SHALL continue processing remaining databases/tables after recoverable errors

### 6.3 Checkpoint and Resume

**REQ-ERR-009:** System SHALL create checkpoints every N batches (default: 10)  
**REQ-ERR-010:** Checkpoint SHALL include:
- Last successfully completed batch
- Current watermark (timestamp + PK)
- Processed record count
- Uploaded batches list

**REQ-ERR-011:** System SHALL support resume from last checkpoint on restart  
**REQ-ERR-012:** System SHALL detect interrupted runs and offer resume  
**REQ-ERR-013:** System SHALL clean up stale checkpoints (>7 days old)

### 6.4 Failure Reporting

**REQ-ERR-014:** System SHALL generate failure report listing:
- Failed databases/tables
- Error messages with timestamps
- Recommended resolution steps
- Links to documentation
- Contact information for support

**REQ-ERR-015:** Failure report SHALL be written to S3 and local filesystem  
**REQ-ERR-016:** Failure report SHALL trigger configured alerts (email, Slack, etc.)  
**REQ-ERR-017:** System SHALL support failure report templates (customizable)

---

## 7. Testing Requirements

### 7.1 Unit Testing

**REQ-TST-001:** System SHALL include unit tests for all core functions  
**REQ-TST-002:** Unit tests SHALL achieve minimum 80% code coverage  
**REQ-TST-003:** Unit tests SHALL use mocking for external dependencies (DB, S3)  
**REQ-TST-004:** Unit tests SHALL run in <2 minutes total  
**REQ-TST-005:** System SHALL use pytest framework  
**REQ-TST-006:** System SHALL support test parameterization for multiple scenarios

### 7.2 Integration Testing

**REQ-TST-007:** System SHALL include integration tests with real PostgreSQL instance  
**REQ-TST-008:** Integration tests SHALL use local S3-compatible storage (MinIO in Docker)  
**REQ-TST-009:** Integration tests SHALL verify end-to-end archival and restore  
**REQ-TST-010:** Integration tests SHALL test all major workflows:
- Simple single table archival
- Multi-table archival
- Schema drift handling
- Network failure recovery
- Concurrent run prevention
- Restore with conflicts

**REQ-TST-011:** Integration tests SHALL use Docker Compose for reproducibility  
**REQ-TST-012:** Integration tests SHALL run in <10 minutes total

### 7.3 Chaos Testing

**REQ-TST-013:** System SHALL include chaos tests simulating:
- Network failures during S3 upload (kill connection mid-upload)
- Database connection drops (kill connection mid-query)
- Partial batch uploads (upload 50%, fail)
- Disk space exhaustion
- Memory pressure
- Slow S3 responses (latency injection)

**REQ-TST-014:** Chaos tests SHALL verify data integrity after simulated failures  
**REQ-TST-015:** Chaos tests SHALL verify no data loss occurs  
**REQ-TST-016:** Chaos tests SHALL verify automatic recovery mechanisms

### 7.4 Performance Testing

**REQ-TST-017:** System SHALL include performance benchmarks  
**REQ-TST-018:** Performance tests SHALL verify SLA compliance:
- 10,000 records/minute minimum
- <5% database impact
- 1M records in <2 hours

**REQ-TST-019:** Performance tests SHALL test with realistic data sizes (10K, 100K, 1M, 10M rows)  
**REQ-TST-020:** Performance tests SHALL measure and report:
- Throughput (records/second)
- Latency (time per batch)
- Resource usage (CPU, memory, network)

### 7.5 Regression Testing

**REQ-TST-021:** System SHALL maintain regression test suite  
**REQ-TST-022:** Regression tests SHALL run on every commit (CI/CD)  
**REQ-TST-023:** System SHALL not release if regression tests fail

---

## 8. Documentation Requirements

### 8.1 User Documentation

**REQ-DOC-001:** System SHALL include comprehensive README with:
- Quick start guide (5 minutes to first archive)
- Installation instructions (all platforms)
- Configuration examples
- Troubleshooting guide (common issues and solutions)
- FAQ

**REQ-DOC-002:** System SHALL include operation manual covering:
- Deployment scenarios (standalone, Docker, Kubernetes)
- Scheduling recommendations (cron examples)
- Monitoring setup (Prometheus, CloudWatch)
- Disaster recovery procedures
- Performance tuning guide

**REQ-DOC-003:** System SHALL include configuration reference documenting all options  
**REQ-DOC-004:** Configuration reference SHALL include:
- Option name and type
- Default value
- Description
- Example usage
- Valid values/ranges

### 8.2 Developer Documentation

**REQ-DOC-005:** System SHALL include architecture documentation with diagrams  
**REQ-DOC-006:** System SHALL include API documentation (if applicable)  
**REQ-DOC-007:** System SHALL include contribution guidelines  
**REQ-DOC-008:** System SHALL include development setup guide  
**REQ-DOC-009:** System SHALL include code style guide  
**REQ-DOC-010:** System SHALL include plugin development guide

### 8.3 Runbooks

**REQ-DOC-011:** System SHALL include runbooks for:
- Failed archival recovery (step-by-step)
- Restoring archived data (multiple scenarios)
- Adding new databases/tables
- Performance tuning (identify and fix slow performance)
- Handling full disk scenarios
- Recovering from S3 outage
- Breaking locks (emergency procedures)
- Rotating credentials

**REQ-DOC-012:** Runbooks SHALL include:
- When to use (symptoms)
- Prerequisites and permissions needed
- Step-by-step instructions
- Expected output at each step
- Rollback procedures
- Verification steps

### 8.4 Release Documentation

**REQ-DOC-013:** System SHALL maintain CHANGELOG following Keep a Changelog format  
**REQ-DOC-014:** System SHALL include upgrade guide for each major version  
**REQ-DOC-015:** System SHALL document breaking changes prominently  
**REQ-DOC-016:** System SHALL include migration scripts for configuration changes

---

## 9. Dependencies and Requirements

**REQ-DEP-016:** System SHALL detect missing optional dependencies and suggest installation

### 9.3 External Services

**REQ-DEP-017:** System SHALL support PostgreSQL versions: 11, 12, 13, 14, 15, 16  
**REQ-DEP-018:** System SHALL work with AWS S3 and S3-compatible services:
- AWS S3
- MinIO
- DigitalOcean Spaces
- Backblaze B2
- Wasabi
- Cloudflare R2

**REQ-DEP-019:** System MAY integrate with optional external services:
- Redis (distributed locking)
- AWS Secrets Manager (credential management)
- HashiCorp Vault (credential management)
- SMTP server (email notifications)

---

## 10. Success Metrics and KPIs

### 10.1 Performance Metrics

**Metric 1: Archive Throughput**
- Target: >10,000 records/minute
- Measurement: Total records / total time
- Acceptable range: 8,000 - 50,000 records/minute

**Metric 2: Database Impact**
- Target: <5% CPU increase during archival
- Measurement: pg_stat_activity monitoring
- Acceptable range: 0-10%

**Metric 3: S3 Upload Speed**
- Target: >10 MB/s average
- Measurement: Total bytes / upload time
- Acceptable range: 5-100 MB/s

**Metric 4: Space Reclamation**
- Target: 60-80% reduction in table size
- Measurement: pg_table_size before/after
- Acceptable range: 50-90%

**Metric 5: Completion Time**
- Target: 1M records in <2 hours
- Measurement: Wall clock time
- Acceptable range: <3 hours

### 10.2 Reliability Metrics

**Metric 6: Success Rate**
- Target: >99.9% for scheduled runs
- Measurement: Successful runs / total runs
- Acceptable range: 99-100%

**Metric 7: Data Loss Incidents**
- Target: 0 (zero tolerance)
- Measurement: Records in DB not in S3 or vice versa
- Acceptable range: 0

**Metric 8: Checksum Verification Failures**
- Target: 0 (zero tolerance)
- Measurement: Failed checksum validations
- Acceptable range: 0

**Metric 9: Mean Time to Recovery (MTTR)**
- Target: <1 hour
- Measurement: Time from failure detection to resolution
- Acceptable range: <2 hours

**Metric 10: Automatic Recovery Rate**
- Target: >95% of transient failures recover automatically
- Measurement: Auto-recovered failures / total failures
- Acceptable range: 90-100%

### 10.3 Operational Metrics

**Metric 11: Configuration Time**
- Target: <5 minutes for new table
- Measurement: Time to add table to config and validate
- Acceptable range: <10 minutes

**Metric 12: Time to First Archive**
- Target: <30 minutes after installation
- Measurement: Install to first successful archive
- Acceptable range: <60 minutes

**Metric 13: Manual Intervention Rate**
- Target: <1% of runs require manual intervention
- Measurement: Manual interventions / total runs
- Acceptable range: <5%

**Metric 14: Average Restore Time**
- Target: <15 minutes for typical restore
- Measurement: Time to restore 100K records
- Acceptable range: <30 minutes

---

## 11. Acceptance Criteria

### 11.1 Functional Acceptance

- [ ] Successfully archive data from 5+ databases with 20+ total tables
- [ ] Verify 0% data loss through checksum validation on 10 consecutive runs
- [ ] Complete restore of archived data to temporary table (100% accuracy)
- [ ] Handle simulated network failure during upload without data loss
- [ ] Process 1 million records in <2 hours
- [ ] Reclaim >60% disk space after archival
- [ ] Successfully handle schema drift detection (add/remove column)
- [ ] Successfully skip tables under legal hold
- [ ] Watermark tracking prevents reprocessing of archived data
- [ ] Idempotent operation - running twice produces identical results

### 11.2 Non-Functional Acceptance

- [ ] Achieve <5% database CPU impact during archival (measured via pg_stat_activity)
- [ ] Generate comprehensive structured logs (JSON format)
- [ ] Expose Prometheus metrics endpoint
- [ ] Send notifications on success and failure (test with Slack)
- [ ] Complete dry-run mode execution showing what would be archived
- [ ] Pass all unit tests (>80% coverage)
- [ ] Pass all integration tests
- [ ] Pass chaos testing (network failures, connection drops)
- [ ] Documentation complete and reviewed
- [ ] Support resume from checkpoint after simulated crash

### 11.3 Operational Acceptance

- [ ] Deploy via Docker container successfully
- [ ] Deploy via Kubernetes CronJob successfully
- [ ] Configure and execute with sample data (end-to-end)
- [ ] Review and approve all documentation (README, runbooks, API docs)
- [ ] Conduct runbook walkthrough with operations team
- [ ] Train operations team on tool usage
- [ ] Validate monitoring integration (Prometheus metrics visible)
- [ ] Test alerting (force failure, verify alert received)
- [ ] Validate restore procedure (restore test data successfully)
- [ ] Security review completed (credentials handling, SQL injection prevention)

### 11.4 Compliance Acceptance

- [ ] Encryption enforced for sensitive tables
- [ ] Audit trail captures all operations
- [ ] Legal hold mechanism tested and working
- [ ] Retention policies enforced correctly
- [ ] Access controls implemented (RBAC)
- [ ] Data classification support demonstrated

---

## 12. Risks and Mitigation

### 12.1 Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Data loss during archival | Low | Critical | Multi-level verification, checksums, test thoroughly |
| S3 outage during archival | Medium | High | Local backup, retry logic, resume capability |
| Database connection drops | Medium | Medium | Connection pooling, retry logic, transactions |
| Memory exhaustion on large tables | Medium | Medium | Batch processing, memory limits, streaming |
| Lock contention blocking production | Low | High | SKIP LOCKED, read replicas, monitoring |
| Schema drift causes restore failure | Medium | Medium | Schema versioning, migration support |
| Network partition during delete | Low | Critical | Verify-then-delete pattern, transactions |
| PostgreSQL version incompatibility | Low | Medium | Version detection, adaptation, thorough testing |
| Clock skew causes incorrect archival | Low | High | Use DB server time, detect skew, warn |
| Multipart upload orphaned parts | Medium | Low | Cleanup job, lifecycle policies |

### 12.2 Operational Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Misconfiguration leads to wrong table archived | Low | Critical | Dry-run mode, validation, staged deletion |
| Insufficient S3 budget | Medium | Medium | Cost estimation, alerts, limits |
| Restore takes too long (RTO breach) | Medium | High | Performance testing, parallel restore |
| Missing restore capability when needed | Low | Critical | Regular restore testing, documentation |
| Credentials leaked in logs | Low | High | Sanitize logs, security review |
| Tool runs twice concurrently | Low | Medium | Distributed locking, process checks |
| Operator error during manual restore | Medium | High | Dry-run mode, confirmations, audit trail |
| Monitoring gaps miss failures | Medium | High | Comprehensive metrics, alerting, heartbeat |
| Runbook outdated or unclear | Medium | Medium | Regular reviews, walkthrough testing |
| Team lacks training on tool | Medium | High | Training sessions, documentation, support |

### 12.3 Compliance Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Data retained beyond legal requirement | Medium | High | Lifecycle policies, automated expiration |
| Sensitive data not encrypted | Low | Critical | Enforce encryption, validation checks |
| Audit trail incomplete | Low | High | Immutable logs, regular audits |
| Legal hold violated | Low | Critical | Automated checks, manual verification |
| Unauthorized access to archives | Low | High | Access controls, audit logging |
| Data classification errors | Medium | Medium | Validation, review process |

---

## 13. Implementation Phases

### 13.1 Phase 1: Core Archival (MVP)
**Duration: 4-6 weeks**

**Scope:**
- Single database, single table support
- Basic batch processing with transactions
- S3 upload with retry logic
- Simple verification (count matching)
- Basic logging (structured JSON)
- Dry-run mode
- Command-line interface
- Configuration file support (YAML)

**Deliverables:**
- Working archiver for simple use cases
- Unit tests (>70% coverage)
- Basic documentation (README, configuration guide)

**Success Criteria:**
- Archive 100K records successfully
- Zero data loss verified through count matching
- Basic restore works (manual process)
- Passes unit tests

**Risk:** Feature creep - stick to MVP scope

### 13.2 Phase 2: Production Hardening
**Duration: 3-4 weeks**

**Scope:**
- Multi-database, multi-table support
- Checksum verification (SHA-256)
- Schema drift detection
- Watermark tracking for incremental archival
- Distributed locking (Redis or advisory locks)
- Checkpoint/resume capability
- Comprehensive error handling and recovery
- Integration tests with real PostgreSQL and MinIO
- Network failure simulation and recovery

**Deliverables:**
- Production-ready archiver
- Integration test suite
- Chaos test scenarios
- Deployment documentation (Docker, systemd)

**Success Criteria:**
- Archive from 5+ databases with 20+ tables
- Handle network failures gracefully (resume uploads)
- Pass chaos testing (network drops, DB disconnects)
- Zero data loss in all test scenarios

**Risk:** Underestimating complexity of error handling

### 13.3 Phase 3: Enterprise Features
**Duration: 3-4 weeks**

**Scope:**
- Compliance features (legal hold, encryption, audit trail)
- Advanced monitoring (Prometheus metrics, detailed timing)
- Notifications (email, Slack, webhooks, PagerDuty)
- Cost management and estimation
- Multi-tenancy support (optional)
- Performance optimizations (adaptive batching, parallel uploads)
- Comprehensive documentation (operation manual, runbooks)
- Advanced restore utility with conflict resolution

**Deliverables:**
- Enterprise-grade feature set
- Full documentation suite
- Runbooks for common scenarios
- Training materials

**Success Criteria:**
- Meet all compliance requirements
- Full observability (logs, metrics, alerts)
- Production-ready documentation
- Restore utility handles all edge cases

**Risk:** Scope creep - prioritize must-have features

### 13.4 Phase 4: Advanced Capabilities
**Duration: 2-3 weeks**

**Scope:**
- Advanced restore features (schema migration, filtering)
- Archive validation utility (separate command)
- Configuration wizard (interactive)
- Web UI (optional, view-only dashboard)
- Performance dashboard (Grafana templates)
- Plugin architecture for extensibility
- Multi-region S3 support
- Performance tuning guide

**Deliverables:**
- Complete feature set
- User-friendly tooling
- Extensible architecture
- Performance optimization guide

**Success Criteria:**
- Configuration wizard reduces setup time to <5 minutes
- Archive validation utility catches corruption
- Web UI provides clear visibility
- Plugin system enables custom extensions

**Risk:** Over-engineering - evaluate ROI of each feature

### 13.5 Ongoing: Maintenance and Support
**Post-Release**

**Activities:**
- Bug fixes and patches
- Security updates
- PostgreSQL version compatibility updates
- Performance improvements
- Documentation updates
- User support and training
- Feature requests evaluation

---

## 14. Glossary

**Archival**: Process of moving old data from primary database to object storage

**Audit Table**: Database table storing historical audit/log records, typically append-only

**Batch**: Configurable number of records processed together as a unit

**Checkpoint**: Saved state allowing resume of interrupted operations

**Checksum**: Cryptographic hash (SHA-256) used to verify data integrity

**Cutoff Date**: Calculated date (now - retention_days) determining which records to archive

**Distributed Locking**: Mechanism to prevent multiple archiver instances from running concurrently

**Dry Run**: Execution mode that simulates operations without making changes

**ETA**: Estimated Time of Arrival - projected completion time

**Idempotent**: Operation that produces the same result when run multiple times

**JSONL**: JSON Lines format - one JSON object per line, newline-separated

**Legal Hold**: Mechanism to prevent deletion/archival for legal/compliance reasons

**Metadata**: Additional information about archived data (counts, checksums, schema)

**Multipart Upload**: S3 technique for uploading large files in multiple parts for reliability

**MTTR**: Mean Time To Recovery - average time to recover from failures

**Retention Period**: Duration data must remain in primary database before archival eligibility

**RTO**: Recovery Time Objective - maximum acceptable time to restore data

**S3-Compatible Storage**: Object storage implementing AWS S3 API (MinIO, Spaces, etc.)

**Savepoint**: Database transaction marker allowing partial rollback

**Schema Drift**: Changes to table structure (columns added/removed/changed) between archival runs

**SKIP LOCKED**: PostgreSQL feature to skip rows already locked by other transactions

**Staged Deletion**: Safety mechanism moving records to temporary table before permanent deletion

**TOAST**: PostgreSQL technique for storing large column values out-of-line

**Vacuum**: PostgreSQL operation to reclaim disk space from deleted rows

**Verify-Then-Delete**: Pattern ensuring S3 upload succeeds before deleting from database

**Watermark**: Timestamp or ID marking progress of incremental archival

---

## 15. References

### 15.1 PostgreSQL Documentation
- VACUUM: https://www.postgresql.org/docs/current/sql-vacuum.html
- Transaction Isolation: https://www.postgresql.org/docs/current/transaction-iso.html
- Row Locking: https://www.postgresql.org/docs/current/explicit-locking.html
- pg_stat_activity: https://www.postgresql.org/docs/current/monitoring-stats.html
- SKIP LOCKED: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
- Advisory Locks: https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS

### 15.2 AWS S3 Documentation
- Multipart Upload: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- Storage Classes: https://aws.amazon.com/s3/storage-classes/
- Encryption: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingEncryption.html
- Lifecycle Policies: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- S3 Performance: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html

### 15.3 Standards and Best Practices
- JSON Lines: https://jsonlines.org/
- Semantic Versioning: https://semver.org/
- Keep a Changelog: https://keepachangelog.com/
- Twelve-Factor App: https://12factor.net/
- PEP 8 (Python Style): https://peps.python.org/pep-0008/
- Prometheus Best Practices: https://prometheus.io/docs/practices/naming/

---

## 16. Appendices

### Appendix A: Complete Configuration File Example

```yaml
version: "2.0"

# Global S3 configuration
s3:
  endpoint: null  # null for AWS S3, or https://s3.example.com for S3-compatible
  bucket: audit-archives
  prefix: archives/
  region: us-east-1
  storage_class: STANDARD_IA
  encryption: SSE-S3
  multipart_threshold_mb: 10
  
# Global defaults
defaults:
  retention_days: 90
  batch_size: 10000
  sleep_between_batches: 2
  vacuum_after: true
  vacuum_strategy: standard  # none, analyze, standard, full
  
# Resource limits
resources:
  max_memory_mb: 500
  max_disk_space_gb: 50
  max_connections_per_db: 5

# Observability
observability:
  log_level: INFO
  log_format: json
  log_file: /var/log/archiver/archiver.log
  prometheus_port: 9090
  heartbeat_interval: 60
  enable_profiling: false

# Notifications
notifications:
  email:
    enabled: true
    smtp_host: smtp.example.com
    smtp_port: 587
    smtp_user: archiver@example.com
    smtp_password_env: SMTP_PASSWORD
    from: archiver@example.com
    recipients:
      - dba-team@example.com
    on_success: digest
    on_failure: immediate
  
  slack:
    enabled: true
    webhook_url_env: SLACK_WEBHOOK
    channel: "#database-ops"
    mention_on_failure: "@dba-oncall"
    
  webhooks:
    - url: https://example.com/api/archive-webhook
      on_success: true
      on_failure: true

# Distributed locking
locking:
  enabled: true
  backend: redis  # redis, postgres, file
  redis_url_env: REDIS_URL
  lock_timeout_minutes: 120

# Databases
databases:
  - name: production_db
    host: db1.example.com
    port: 5432
    user: archiver
    password_env: PROD_DB_PASSWORD
    read_replica: db1-replica.example.com
    
    tables:
      - name: audit_logs
        schema: public
        timestamp_column: created_at
        primary_key: id
        retention_days: 90
        classification: PII
        critical: true
        
      - name: user_activity
        timestamp_column: event_time
        primary_key: event_id
        retention_days: 180
        batch_size: 5000  # Override for large rows
        
  - name: analytics_db
    host: db2.example.com
    port: 5432
    user: archiver
    password_env: ANALYTICS_DB_PASSWORD
    
    tables:
      - name: click_events
        timestamp_column: timestamp
        primary_key: id
        retention_days: 30
        batch_size: 20000  # High volume table
        
      - name: session_logs
        timestamp_column: session_start
        primary_key: session_id
        retention_days: 60

# Legal holds
legal_holds:
  enabled: true
  check_table: legal_holds  # Table containing active holds
  database: production_db

# Compliance
compliance:
  enforce_encryption: true
  min_retention_days: 7
  max_retention_days: 2555  # 7 years
```

### Appendix B: Sample Metadata File

```json
{
  "version": "2.0",
  "database": "production_db",
  "host": "db1.example.com",
  "table": "audit_logs",
  "schema": "public",
  "batch_number": 1,
  "batch_id": "sha256:f4a3b5c8...",
  "archived_at": "2025-12-30T02:15:47Z",
  "date_range": {
    "start": "2024-01-01T00:00:00Z",
    "end": "2025-10-01T23:59:59Z"
  },
  "record_count": 10000,
  "checksum": "sha256:a3b5c8f9d2e1...",
  "file_size": {
    "compressed": 5242880,
    "uncompressed": 15728640
  },
  "compression": {
    "algorithm": "gzip",
    "level": 6
  },
  "schema": {
    "columns": [
      {"name": "id", "type": "bigint", "nullable": false},
      {"name": "user_id", "type": "integer", "nullable": true},
      {"name": "action", "type": "text", "nullable": false},
      {"name": "metadata", "type": "jsonb", "nullable": true},
      {"name": "created_at", "type": "timestamp with time zone", "nullable": false}
    ],
    "primary_key": ["id"],
    "indexes": [
      {"name": "idx_audit_logs_created_at", "columns": ["created_at"]},
      {"name": "idx_audit_logs_user_id", "columns": ["user_id"]}
    ]
  },
  "postgres_version": "14.5",
  "archiver_version": "2.0.0",
  "deletion_manifest_path": "s3://audit-archives/archives/production_db/audit_logs/year=2025/month=12/day=30/audit_logs_20251230T021547Z_batch_001_manifest.json"
}
```

### Appendix C: Deletion Manifest Example

```json
{
  "batch_id": "sha256:f4a3b5c8...",
  "database": "production_db",
  "table": "audit_logs",
  "deleted_at": "2025-12-30T02:15:50Z",
  "primary_keys": [
    1001,
    1002,
    1003,
    "... (10000 total)"
  ],
  "primary_key_checksum": "sha256:b9c4d7e2...",
  "delete_statement": "DELETE FROM audit_logs WHERE id = ANY($1)",
  "transaction_id": "12345",
  "rows_deleted": 10000
}
```

### Appendix D: Exit Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 0 | Success | All operations completed successfully |
| 1 | Partial Success | Some databases/tables failed, others succeeded |
| 2 | Failure | Complete failure, no progress made |
| 3 | Validation Error | Configuration or pre-flight validation failed |
| 4 | Lock Error | Could not acquire distributed lock (another instance running) |
| 5 | Permission Error | Insufficient database or S3 permissions |
| 6 | Resource Error | Insufficient memory, disk space, or other resources |
| 7 | Network Error | Unrecoverable network error (S3 unreachable) |

### Appendix E: Prometheus Metrics

```prometheus
# Counter: Total records archived
archiver_records_archived_total{database="prod_db",table="audit_logs"} 1500000

# Counter: Total bytes uploaded to S3
archiver_bytes_uploaded_total{database="prod_db",table="audit_logs"} 524288000

# Histogram: Duration per phase
archiver_duration_seconds{database="prod_db",table="audit_logs",phase="query"} 45.2
archiver_duration_seconds{database="prod_db",table="audit_logs",phase="serialize"} 12.3
archiver_duration_seconds{database="prod_db",table="audit_logs",phase="compress"} 8.7
archiver_duration_seconds{database="prod_db",table="audit_logs",phase="upload"} 120.5
archiver_duration_seconds{database="prod_db",table="audit_logs",phase="verify"} 2.1
archiver_duration_seconds{database="prod_db",table="audit_logs",phase="delete"} 30.1
archiver_duration_seconds{database="prod_db",table="audit_logs",phase="vacuum"} 180.8

# Gauge: Current state
archiver_current_state{} 1  # 0=idle, 1=running, 2=failed

# Counter: Run status
archiver_runs_total{status="success"} 145
archiver_runs_total{status="failure"} 2
archiver_runs_total{status="partial"} 3

# Gauge: Last successful run timestamp
archiver_last_success_timestamp{} 1735527347

# Counter: Errors by type
archiver_errors_total{type="network"} 5
archiver_errors_total{type="database"} 1
archiver_errors_total{type="s3"} 2

# Gauge: Space reclaimed
archiver_space_reclaimed_bytes{database="prod_db",table="audit_logs"} 10737418240

# Gauge: Current batch progress
archiver_current_batch_progress{database="prod_db",table="audit_logs"} 0.65

# Histogram: Batch processing rate (records/second)
archiver_batch_rate_records_per_second{database="prod_db",table="audit_logs"} 185.3

# Gauge: Memory usage
archiver_memory_usage_bytes{} 524288000

# Counter: Checksum verification failures
archiver_checksum_failures_total{} 0
```

### Appendix F: Command-Line Interface Examples

```bash
# Basic archival (all databases, all tables)
python archiver.py --config config.yaml

# Dry run to see what would be archived
python archiver.py --config config.yaml --dry-run

# Archive specific database only
python archiver.py --config config.yaml --database production_db

# Archive specific table only
python archiver.py --config config.yaml --database production_db --table audit_logs

# Validation mode (check config and connectivity)
python archiver.py --config config.yaml --validate-only

# Verbose output
python archiver.py --config config.yaml --verbose

# Very verbose (debug mode)
python archiver.py --config config.yaml -vvv

# Limit records for testing
python archiver.py --config config.yaml --limit 1000

# Force vacuum full (requires maintenance window)
python archiver.py --config config.yaml --vacuum-full

# Verify existing archives
python archiver.py --config config.yaml --verify-archives

# Cleanup orphaned multipart uploads
python archiver.py --config config.yaml --cleanup

# Restore utility (separate command)
python restore.py --config config.yaml \
  --database production_db \
  --table audit_logs \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --conflict-strategy skip \
  --dry-run

# Generate configuration wizard
python archiver.py --init-config

# Estimate costs
python archiver.py --config config.yaml --estimate-cost

# Show version
python archiver.py --version
```

---

## 17. Approval and Sign-off

### 17.1 Review and Approval

| Role | Name | Signature | Date | Status |
|------|------|-----------|------|--------|
| Product Owner | | | | [ ] Approved |
| Technical Lead | | | | [ ] Approved |
| DBA Team Lead | | | | [ ] Approved |
| Security Officer | | | | [ ] Approved |
| Compliance Officer | | | | [ ] Approved |
| DevOps Lead | | | | [ ] Approved |
| QA Lead | | | | [ ] Approved |

### 17.2 Document Control

| Version | Date | Author | Changes Summary | Pages |
|---------|------|--------|-----------------|-------|
| 1.0 | 2025-10-28 | Claude | Initial draft | 35 |
| 2.0 | 2025-12-30 | Claude | Added 85+ critical requirements for production robustness: transaction management, concurrency control, network resilience, data verification, resource management, idempotency, disaster recovery, advanced restore capabilities, comprehensive testing, and detailed appendices | 50+ |

### 17.3 Distribution List

- Product Management Team
- Engineering Team (Backend, Infrastructure)
- Database Administration Team
- Security Team
- Compliance Team
- DevOps/SRE Team
- QA Team
- Documentation Team

### 17.4 Review Schedule

- **Initial Review**: Week 1-2 (stakeholder feedback)
- **Revision Period**: Week 3 (incorporate feedback)
- **Final Review**: Week 4 (approval gates)
- **Sign-off Deadline**: End of Week 4
- **Implementation Start**: Week 5

### 17.5 Next Steps After Approval

1. **Technical Architecture Design** (1 week)
   - System architecture diagram
   - Component design
   - Database schema for metadata
   - API/CLI design
   - Sequence diagrams for critical flows

2. **Detailed Implementation Plan** (1 week)
   - Break down into user stories
   - Sprint planning
   - Resource allocation
   - Risk mitigation plans
   - Definition of done for each phase

3. **Development Environment Setup** (1 week)
   - Repository creation
   - CI/CD pipeline
   - Testing infrastructure (PostgreSQL, MinIO)
   - Development guidelines
   - Code review process

4. **Phase 1 Development** (4-6 weeks)
   - MVP implementation
   - Unit testing
   - Documentation
   - Internal demo

---

## 18. Success Criteria Summary

### Must-Have (Tier 1) - Required for v1.0 Release

✅ **Zero data loss guarantee**
- Multi-level verification
- Checksum validation
- Transaction safety

✅ **Production database safety**
- <5% performance impact
- Non-blocking operations
- Lock monitoring

✅ **Reliability**
- 99.9% success rate
- Automatic recovery from transient failures
- Checkpoint/resume capability

✅ **Multi-database support**
- 5+ databases, 20+ tables
- Independent failure isolation
- Configurable per-database/table settings

✅ **Restore capability**
- Restore from any archive
- Checksum verification
- Conflict resolution

✅ **Observability**
- Structured logging
- Prometheus metrics
- Real-time progress tracking

✅ **Security**
- Encrypted credentials
- SSL/TLS connections
- No sensitive data in logs

### Should-Have (Tier 2) - Required for v2.0

✅ **Advanced verification**
- Sample verification (1% random)
- Post-archival audit
- Schema drift detection

✅ **Compliance features**
- Legal hold support
- Audit trail
- Retention policy enforcement
- Data classification

✅ **Advanced monitoring**
- Detailed timing metrics
- Resource usage tracking
- Threshold-based alerts

✅ **Network resilience**
- Multipart upload with resume
- Retry with exponential backoff
- Orphaned upload cleanup

✅ **Cost optimization**
- Cost estimation
- Storage class selection
- Compression tuning

### Nice-to-Have (Tier 3) - Future Enhancements

🔹 **Web UI**
- Dashboard for monitoring
- Archive browser
- Configuration management

🔹 **Advanced restore**
- Schema migration
- Partial table restore
- Cross-database restore

🔹 **Multi-tenancy**
- Tenant-aware archival
- Per-tenant policies
- Isolated storage paths

🔹 **Machine learning**
- Predictive archival
- Anomaly detection
- Auto-tuning batch sizes

---

## 19. Key Design Principles

### 19.1 Safety First
**Principle**: Never compromise data integrity for performance or convenience

**Application**:
- Verify-then-delete pattern (never delete before S3 confirmation)
- Multi-level count verification (DB → Memory → S3)
- Checksum validation at every step
- Transaction boundaries strictly enforced
- Graceful degradation on errors (fail safe, not fail silent)

**Trade-offs**: May sacrifice some performance for safety guarantees

### 19.2 Observable by Default
**Principle**: System behavior should be transparent and measurable

**Application**:
- Structured logging (JSON) for all operations
- Prometheus metrics for quantitative monitoring
- Progress indicators for long operations
- Detailed timing breakdown per phase
- Comprehensive error context

**Trade-offs**: Slightly higher overhead from logging/metrics

### 19.3 Fail Explicitly
**Principle**: Failures should be loud, clear, and actionable

**Application**:
- Meaningful error messages with resolution guidance
- Distinct error categories (FATAL, ERROR, WARN)
- Immediate alerting on critical failures
- Detailed failure reports with context
- No silent failures

**Trade-offs**: May generate alert fatigue if not tuned properly

### 19.4 Idempotent Operations
**Principle**: Running the same operation multiple times produces identical results

**Application**:
- Deterministic batch IDs based on content
- Duplicate detection via manifest
- Skip already-archived batches
- S3 conditional writes for atomicity
- State tracking for resume

**Trade-offs**: Additional storage for manifests and state

### 19.5 Configuration Over Code
**Principle**: Behavior should be configurable without code changes

**Application**:
- YAML configuration for all parameters
- Per-database and per-table overrides
- Environment variable substitution
- Runtime parameter validation
- Configuration versioning

**Trade-offs**: More complex configuration management

### 19.6 Progressive Disclosure
**Principle**: Simple for basic use, powerful for advanced use

**Application**:
- Sensible defaults for quick start
- Optional advanced features (legal hold, multi-tenancy)
- Dry-run mode for safe exploration
- Interactive wizard for beginners
- Expert mode with all options

**Trade-offs**: Steeper learning curve for advanced features

### 19.7 Defense in Depth
**Principle**: Multiple layers of protection against failures

**Application**:
- Transactions at database level
- Checksums for data integrity
- Retry logic for transient errors
- Checkpoints for long operations
- Backup mechanisms (local disk fallback)

**Trade-offs**: Increased complexity and overhead

### 19.8 Explicit Over Implicit
**Principle**: Make assumptions and behaviors explicit

**Application**:
- Explicit configuration for all tables
- No "magic" auto-discovery by default
- Clear validation messages
- Documented default values
- Verbose mode for detailed operations

**Trade-offs**: More configuration required upfront

---

## 20. Technical Constraints and Assumptions

### 20.1 Hard Constraints

**C-001**: PostgreSQL 11+ only (no MySQL, Oracle, etc. in v1.0)  
**Rationale**: Focus on single database platform for quality

**C-002**: Tables must have single-column primary key of scalar type  
**Rationale**: Simplifies delete operations and verification

**C-003**: Tables must have timestamp column for age-based filtering  
**Rationale**: Required for retention policy enforcement

**C-004**: S3 API compatibility required for object storage  
**Rationale**: Standardized interface for multiple providers

**C-005**: Python 3.9+ (no Python 2.x support)  
**Rationale**: Modern Python features, security updates

**C-006**: Network connectivity required (not suitable for air-gapped)  
**Rationale**: Requires S3 and database access

### 20.2 Soft Constraints

**C-007**: Prefer single-threaded per table (parallelism at database level)  
**Rationale**: Simpler implementation, adequate performance

**C-008**: Target batch completion in 30-120 seconds  
**Rationale**: Balance between throughput and transaction duration

**C-009**: Maximum 5 concurrent database connections per database  
**Rationale**: Avoid exhausting connection pools

**C-010**: Configuration files should be <1MB  
**Rationale**: Manageable for version control and parsing

### 20.3 Key Assumptions

**A-001**: Audit tables are append-only (no updates to old records)  
**Impact**: If violated, may archive records later updated  
**Mitigation**: Document requirement, detect updates in verification

**A-002**: Network bandwidth sufficient for S3 uploads (minimum 10 Mbps)  
**Impact**: If violated, archival may timeout  
**Mitigation**: Monitor upload speed, alert on slow uploads

**A-003**: Database has sufficient temp space for large sorts  
**Impact**: If violated, queries may fail  
**Mitigation**: Pre-flight check for temp space

**A-004**: Retention policies determined by compliance team  
**Impact**: If unclear, may archive too early/late  
**Mitigation**: Validation checks, configurable per table

**A-005**: Archive access patterns are infrequent (monthly or less)  
**Impact**: If violated, may incur high retrieval costs  
**Mitigation**: Use appropriate S3 storage class, document access costs

**A-006**: System clock on archiver host is synchronized (NTP)  
**Impact**: If violated, may archive wrong records  
**Mitigation**: Use database server time, detect clock skew

**A-007**: PostgreSQL has standard configuration (no exotic extensions required)  
**Impact**: If violated, may not work  
**Mitigation**: Document compatible configurations

**A-008**: Tables don't have complex triggers that affect deletes  
**Impact**: If violated, delete performance may degrade  
**Mitigation**: Warn if triggers detected, document impact

**A-009**: S3 bucket has versioning enabled for production  
**Impact**: If disabled, deleted archives unrecoverable  
**Mitigation**: Validate during setup, document requirement

**A-010**: Legal holds are updated in near real-time  
**Impact**: If delayed, may archive held records  
**Mitigation**: Check holds immediately before archival

---

## 21. Performance Benchmarks and Targets

### 21.1 Baseline Performance (Single Table)

**Test Setup**:
- Table: 1M rows, average row size 500 bytes
- PostgreSQL 14 on 4-core, 8GB RAM
- S3 in same region, 100 Mbps network
- Batch size: 10,000 records

**Expected Results**:

| Phase | Time | Percentage |
|-------|------|------------|
| Query (SELECT) | 8-12 min | 15% |
| Serialize to JSON | 4-6 min | 10% |
| Compress (gzip) | 6-8 min | 12% |
| Upload to S3 | 20-25 min | 45% |
| Verify | 1-2 min | 3% |
| Delete | 5-8 min | 12% |
| Vacuum | 2-3 min | 3% |
| **Total** | **46-64 min** | **100%** |

**Throughput**: 15,600 - 21,700 records/minute  
**Meets Target**: ✅ Yes (>10,000 records/minute)

### 21.2 Scaling Characteristics

**Linear Scaling (Expected)**:
- 10M rows: ~8-11 hours
- 100M rows: ~80-110 hours (requires multi-day run with checkpointing)

**Optimization Opportunities**:
1. **Parallel uploads**: 30% faster (upload while fetching next batch)
2. **Read replica**: 10-20% faster (offload queries from primary)
3. **Faster compression**: 15% faster (use pigz for parallel gzip)
4. **Batch size tuning**: 10-30% faster (find sweet spot per table)
5. **SSD storage**: 20% faster (faster vacuum operations)

**Bottleneck Analysis**:
- Primary bottleneck: S3 upload (45% of time)
- Secondary bottleneck: Database query (15% of time)
- Tertiary bottleneck: Compression (12% of time)

### 21.3 Resource Usage Targets

**CPU**:
- Archiver process: 50-100% of 1 core
- Database impact: <5% increase in total CPU
- Compression: Can use 100% of 1 core (gzip single-threaded)

**Memory**:
- Per batch: 50-200 MB (depends on row size)
- Steady state: <500 MB
- Peak: <1 GB

**Network**:
- Upload: 5-15 MB/s average (40-120 Mbps)
- Database: <1 MB/s (negligible)

**Disk I/O**:
- Temporary files: 2x batch size (e.g., 100 MB for 10K rows × 5KB/row)
- Database vacuum: High I/O during vacuum (acceptable)

### 21.4 Database Impact Measurements

**Connection Overhead**:
- Active connections: 1-2 per database
- Connection pool: 5 max per database

**Query Load**:
- Long-running queries: 1 SELECT per batch (30-120 seconds)
- Lock duration: <1 second per row (with SKIP LOCKED)
- Replication lag impact: <5 seconds typical, <30 seconds acceptable

**Table Lock Impact**:
- SELECT: No table locks (row-level only)
- DELETE: No table locks (row-level only)
- VACUUM: Share lock (allows reads, blocks writes briefly)
- VACUUM FULL: Exclusive lock (blocks all operations - maintenance window required)

---

## 22. Disaster Recovery Scenarios

### Scenario 1: Accidental Deletion of Wrong Table

**Symptoms**: Realized archives contain wrong table data

**Impact**: HIGH - Data loss from production table

**Detection**:
- Monitoring alerts (unexpected record count drop)
- User reports (missing recent data)
- Audit log review

**Recovery Procedure**:
1. Immediately stop archiver (kill process or remove lock)
2. Identify affected batches from audit log
3. Check if staged deletion enabled (restore from `_archived` table if <24 hours)
4. Otherwise, restore from S3 archives using restore utility
5. Verify restored data count matches deleted count
6. Update configuration to prevent recurrence
7. Run verification query to confirm data integrity

**Time to Recovery**: 1-4 hours (depending on data size)

**Prevention**:
- Always run dry-run mode first
- Use table whitelist (no wildcards for critical tables)
- Staged deletion for critical tables
- Pre-archival verification step

### Scenario 2: S3 Bucket Accidentally Deleted

**Symptoms**: Cannot access archived data, 404 errors

**Impact**: CRITICAL - All archived data lost

**Detection**:
- S3 monitoring alerts (bucket deletion event)
- Archive verification failures
- Restore attempts fail

**Recovery Procedure**:
1. Check S3 versioning status (if enabled, recover from versions)
2. Check cross-region replication (restore from replica bucket)
3. Check backup storage (if configured)
4. If no backups available: DATA LOSS - archives unrecoverable
5. Assess retention requirements and legal obligations
6. Document incident for compliance

**Time to Recovery**: 2-8 hours (if backups exist), PERMANENT (if no backups)

**Prevention**:
- Enable S3 versioning
- Enable S3 Object Lock (WORM) for compliance
- Cross-region replication
- MFA delete protection
- Least-privilege IAM policies
- Regular backup verification

### Scenario 3: Network Failure During Delete Operation

**Symptoms**: Network drops after S3 upload, before delete completes

**Impact**: MEDIUM - Duplicate data in S3 and database

**Detection**:
- Upload succeeded but delete failed in logs
- Checksum verification passed
- Transaction rollback logged

**Recovery Procedure**:
1. System automatically rolls back transaction (no data deleted)
2. Next run re-archives same data (idempotent)
3. Duplicate detection skips already-archived batches
4. No manual intervention required (automatic recovery)

**Time to Recovery**: Automatic (next run)

**Prevention**:
- Verify-then-delete pattern (implemented)
- Transaction safety (implemented)
- Idempotent operations (implemented)

### Scenario 4: Database Corruption Detected in Archives

**Symptoms**: Checksum failures during verification

**Impact**: HIGH - Data integrity compromised

**Detection**:
- Archive verification job fails
- Checksum mismatch errors
- Random sample verification fails

**Recovery Procedure**:
1. Identify affected batches from verification report
2. Check if original data still in database (if within retention period)
3. If yes: Re-archive affected batches with `--force-rearchive`
4. If no: Check for database backups containing affected data
5. Restore from database backup and re-archive
6. Run full verification on re-archived data
7. Document incident and root cause analysis

**Time to Recovery**: 4-24 hours (depending on data availability)

**Prevention**:
- Multi-level verification during archival
- Regular archive verification jobs (weekly)
- S3 versioning for rollback
- Integrity monitoring

### Scenario 5: Archiver Process Crashes Mid-Run

**Symptoms**: Process terminated unexpectedly, partial batch uploaded

**Impact**: LOW - No data loss, incomplete operation

**Detection**:
- Process monitoring (no heartbeat)
- Checkpoint file exists
- S3 has partial batch or incomplete multipart upload

**Recovery Procedure**:
1. Check checkpoint file for last completed batch
2. Check for orphaned multipart uploads
3. Run archiver with resume option
4. System automatically resumes from last checkpoint
5. Cleans up orphaned uploads
6. Continues with next batch

**Time to Recovery**: Automatic (resume on next run)

**Prevention**:
- Checkpoint mechanism (implemented)
- Multipart upload tracking (implemented)
- Graceful shutdown handlers
- Health checks

### Scenario 6: Legal Hold Violated (Data Archived Under Hold)

**Symptoms**: Archived data needed for legal case, but deleted from database

**Impact**: CRITICAL - Legal compliance violation

**Detection**:
- Legal team requests data
- Data not in database
- Archive shows data was deleted

**Recovery Procedure**:
1. Immediately restore data from S3 archives
2. Verify restored data completeness
3. Place new legal hold to prevent re-archival
4. Document incident for compliance
5. Review legal hold checking mechanism
6. Implement process improvements

**Time to Recovery**: 1-2 hours (restore time)

**Legal Impact**: Potential sanctions, must be reported

**Prevention**:
- Real-time legal hold checks
- Legal hold table maintained by legal team
- Automated legal hold validation before archival
- Dual approval for archival of sensitive tables
- Regular legal hold audit

---

## 23. Frequently Asked Questions (FAQ)

### Q1: Can I archive data while applications are writing to the table?

**A**: Yes, this is safe. The archiver uses `SELECT ... FOR UPDATE SKIP LOCKED` which:
- Only locks rows being archived
- Skips rows locked by other transactions
- Doesn't block application writes to new rows
- Application queries are unaffected

### Q2: What happens if archival run takes longer than the schedule interval?

**A**: The distributed locking mechanism prevents overlapping runs. If a run is still active when the next scheduled run starts:
- The new run detects the active lock
- Exits immediately with exit code 4 (Lock Error)
- Logs a warning message
- Next scheduled run will retry

**Recommendation**: Set lock timeout > expected run duration + buffer (e.g., 2x expected duration)

### Q3: Can I restore archived data back to the original table?

**A**: Yes, using the restore utility:
```bash
python restore.py --database prod_db --table audit_logs \
  --start-date 2025-01-01 --end-date 2025-01-31 \
  --conflict-strategy skip
```

The restore utility supports conflict resolution if data already exists.

### Q4: How much does S3 storage cost for archives?

**A**: Rough estimate:
- STANDARD_IA: $0.0125 per GB/month
- Compression ratio: 3-5x typically
- 1TB uncompressed → ~200-330 GB compressed → ~$2.50-4.12/month

Use `--estimate-cost` command to get precise estimates for your data.

### Q5: What happens if my retention period is too short and I accidentally archive needed data?

**A**: If staged deletion is enabled (24-hour buffer):
- Data is in `{table}_archived` temporary table
- Can be restored immediately
- After 24 hours, data is in S3 archives
- Restore using restore utility (15-60 minutes)

**Recommendation**: Set retention period conservatively (e.g., 90 days instead of 30 days)

### Q6: Can I archive from PostgreSQL on RDS/Aurora?

**A**: Yes, with considerations:
- Ensure archiver user has DELETE and VACUUM permissions
- VACUUM FULL requires maintenance window (locks table)
- Use read replica for queries to reduce primary load
- May need to adjust maintenance windows for vacuum operations

### Q7: How do I verify archives haven't been corrupted?

**A**: Run the verification utility:
```bash
python archiver.py --verify-archives --database prod_db --table audit_logs
```

This checks:
- All metadata files present
- Checksums match archived data
- Record counts consistent
- No orphaned files

**Recommendation**: Run verification weekly as scheduled job

### Q8: What if I need to change the table schema after archiving?

**A**: Archives include full schema definition in metadata. During restore:
- Restore utility detects schema differences
- Offers migration options (add/remove columns, type conversions)
- Can use lenient mode (map columns by name)
- Dry-run mode shows what will happen

**Important**: Keep schema changes backward-compatible when possible

### Q9: Can I use this for GDPR "right to be forgotten" compliance?

**A**: Partially. The archiver can:
- Archive data to S3 for retention
- Delete from production database
- Tag archives with data classification

However, for GDPR deletion:
- Need to implement record-level deletion from archives (not built-in)
- Consider using legal holds to prevent archival of "to be deleted" records
- May need custom solution for permanent deletion from S3

### Q10: How do I handle very large tables (100M+ rows)?

**A**: Strategies:
1. **Multi-day runs**: Enable checkpointing, run over multiple days
2. **Batch size tuning**: Increase batch size to 50,000+ for large tables
3. **Parallel instances**: Run multiple archivers for different date ranges
4. **Incremental archival**: Use watermarks to avoid reprocessing
5. **Pre-partitioning**: Consider PostgreSQL native partitioning

**Performance tip**: First run will be slow (initial backfill), subsequent runs much faster with watermarks

### Q11: What if S3 is temporarily unavailable?

**A**: The archiver:
- Retries upload with exponential backoff (3 attempts)
- If all retries fail, saves batch to local disk
- Transaction rolls back (no data deleted from database)
- Next run retries the batch
- Manual recovery possible from local disk if needed

**No data loss** as verify-then-delete pattern is enforced.

### Q12: Can I run multiple archivers for different tables simultaneously?

**A**: Yes, but carefully:
- Use `--table` flag to target specific tables
- Distributed lock is per-database by default
- Configure separate locks per table if needed
- Monitor database load to avoid overload
- Consider using read replicas

**Recommendation**: Start with sequential, only parallelize if needed for performance

---

## 24. Support and Maintenance

### 24.1 Getting Help

**Documentation**:
- README.md: Quick start guide
- docs/operations-manual.md: Comprehensive operations guide
- docs/api-reference.md: Configuration options
- docs/runbooks/: Step-by-step procedures

**Community Support**:
- GitHub Issues: Bug reports and feature requests
- GitHub Discussions: Q&A and general questions
- Stack Overflow: Tag `postgresql-archiver`

**Enterprise Support**:
- Email: support@example.com
- Response time: 24 hours (P2), 4 hours (P1)
- On-call: For critical production issues

### 24.2 Bug Reports

**Required Information**:
- Archiver version (`--version`)
- PostgreSQL version
- Configuration file (sanitized, no credentials)
- Full error message and stack trace
- Relevant log entries (before/during/after error)
- Steps to reproduce

**Priority Levels**:
- P0 (Critical): Data loss, security vulnerability - Immediate response
- P1 (High): Production down, cannot archive - 4 hour response
- P2 (Medium): Degraded performance, workaround exists - 24 hour response
- P3 (Low): Minor issue, enhancement request - Best effort

### 24.3 Release Cycle

**Versioning**: Semantic Versioning (MAJOR.MINOR.PATCH)
- MAJOR: Breaking changes, major features
- MINOR: New features, backward compatible
- PATCH: Bug fixes only

**Release Schedule**:
- Patch releases: As needed (critical bugs, security)
- Minor releases: Every 2-3 months
- Major releases: Once per year

**Support Policy**:
- Current major version: Full support
- Previous major version: Security updates only (12 months)
- Older versions: End of life, no support

### 24.4 Contributing

**Contribution Types**:
- Bug fixes
- Feature implementations
- Documentation improvements
- Test coverage improvements

**Process**:
1. Open GitHub issue for discussion
2. Fork repository and create feature branch
3. Implement changes with tests
4. Submit pull request
5. Code review and feedback
6. Merge after approval

**Code Standards**:
- Follow PEP 8 style guide
- Add type hints
- Write unit tests (maintain >80% coverage)
- Update documentation
- Add CHANGELOG entry

---

## 25. Conclusion

This requirements document provides a comprehensive, production-ready specification for building a robust PostgreSQL audit table archiver. With 420+ requirements across functional, non-functional, operational, and testing domains, this document ensures:

✅ **Zero data loss** through multi-level verification and transaction safety  
✅ **Production readiness** with comprehensive error handling and recovery  
✅ **Enterprise features** including compliance, security, and governance  
✅ **Operational excellence** through observability, monitoring, and documentation  
✅ **Scalability** to handle multiple databases, thousands of tables, and billions of records

### Key Takeaways

1. **Safety is paramount** - Every decision prioritizes data integrity over performance
2. **Failures are expected** - System designed to handle and recover from all failure modes
3. **Observability is built-in** - Comprehensive logging, metrics, and alerting from day one
4. **Configuration over code** - Flexible, declarative configuration without code changes
5. **Progressive rollout** - Phased implementation allows validation at each stage

### Success Factors

The success of this project depends on:
- **Stakeholder alignment** on requirements and priorities
- **Thorough testing** including chaos testing and real-world scenarios
- **Comprehensive documentation** for operations teams
- **Continuous monitoring** in production
- **Regular reviews** and updates based on feedback

### Final Recommendations

1. **Start with Phase 1 MVP** - Prove core concept before adding complexity
2. **Involve operations early** - Their feedback is critical for usability
3. **Test extensively** - Especially failure scenarios and data integrity
4. **Document everything** - Future you will thank present you
5. **Monitor in production** - Instrument heavily from day one
6. **Iterate based on feedback** - Requirements will evolve with usage

---

**Document Status**: FINAL DRAFT - Ready for Stakeholder Review

**Total Requirements**: 420+  
**Document Pages**: 50+  
**Estimated Implementation**: 12-16 weeks  
**Estimated Team Size**: 2-3 engineers

---

**END OF REQUIREMENTS DOCUMENT v2.0**