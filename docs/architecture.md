# Audit Table Archiver - Technical Architecture

**Version:** 1.0  
**Date:** January 2025

---

## 1. System Overview

The Audit Table Archiver is a Python-based tool that safely archives historical PostgreSQL audit table data to S3-compatible object storage while maintaining zero data loss guarantees.

### 1.1 Core Principles

1. **Safety First**: Never delete data before verification
2. **Idempotency**: Running multiple times produces same result
3. **Observability**: Comprehensive logging and metrics
4. **Resilience**: Automatic recovery from failures
5. **Scalability**: Handle 100+ databases, 1000+ tables

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI Interface                         │
│                    (archiver/main.py)                        │
└───────────────────────┬─────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────┐              ┌───────────────┐
│   Config      │              │  Lock Manager  │
│  Manager      │              │  (Distributed)│
└───────┬───────┘              └───────┬───────┘
        │                               │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │    Database Orchestrator       │
        │  (Process multiple databases)   │
        └───────────────┬───────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────┐              ┌───────────────┐
│   Database    │              │  Schema        │
│   Manager     │              │  Manager       │
│  (asyncpg)    │              │  (Validation) │
└───────┬───────┘              └───────┬───────┘
        │                               │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │      Batch Processor           │
        │  (Core archival workflow)      │
        └───────────────┬───────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────┐              ┌───────────────┐
│  Transaction  │              │   Verifier     │
│   Manager     │              │  (Counts,     │
│  (Safety)     │              │   Checksums)  │
└───────┬───────┘              └───────┬───────┘
        │                               │
        └───────────────┬───────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────┐              ┌───────────────┐
│  Serializer   │              │   S3 Client    │
│  (JSONL)      │              │  (Upload,     │
│               │              │   Multipart)  │
└───────┬───────┘              └───────┬───────┘
        │                               │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │      Metrics & Logging        │
        │  (Prometheus, Structured)     │
        └───────────────────────────────┘
```

---

## 3. Core Components

### 3.1 Configuration Manager

**Responsibility**: Parse, validate, and provide configuration

**Key Features**:
- YAML file parsing
- Environment variable substitution
- Pydantic validation
- Hierarchical overrides (table > database > global)

**Data Flow**:
```
YAML File → Parser → Validator → Config Object → All Modules
```

### 3.2 Database Manager

**Responsibility**: PostgreSQL connection and query execution

**Key Features**:
- Async connection pooling (asyncpg)
- Health checks
- Transaction management
- Read replica support

**Connection Lifecycle**:
```
Start → Create Pool → Acquire Connection → Execute Query → Release → Close Pool
```

### 3.3 Batch Processor

**Responsibility**: Orchestrate archival workflow per batch

**Workflow**:
```
1. SELECT batch (FOR UPDATE SKIP LOCKED)
2. Serialize to JSONL
3. Compress (gzip)
4. Upload to S3
5. Verify (counts, checksum)
6. DELETE (in transaction)
7. COMMIT
```

**Error Handling**:
- Any failure → ROLLBACK
- Retry transient errors
- Log all operations

### 3.4 Transaction Manager

**Responsibility**: Ensure transaction safety

**Features**:
- Transaction boundaries per batch
- Savepoint support
- Timeout enforcement
- Automatic rollback on failure

**Transaction Pattern**:
```python
async with transaction_manager.begin() as tx:
    try:
        # SELECT
        # UPLOAD
        # VERIFY
        # DELETE
        await tx.commit()
    except Exception:
        await tx.rollback()
        raise
```

### 3.5 Verifier

**Responsibility**: Multi-level data integrity verification

**Verification Levels**:
1. **DB Count**: `SELECT COUNT(*)` before fetch
2. **Memory Count**: Count fetched records
3. **S3 Count**: Count lines in JSONL
4. **Checksum**: SHA-256 of uncompressed JSONL
5. **PK Verification**: Verify PKs match

**Failure Handling**:
- Count mismatch → Abort batch, rollback
- Checksum mismatch → Abort batch, rollback
- Never delete on verification failure

### 3.6 S3 Client

**Responsibility**: S3 upload with resilience

**Features**:
- Multipart upload (>10MB)
- Resume capability
- Retry with exponential backoff
- Rate limiting
- Local disk fallback

**Upload Flow**:
```
Small File (<10MB):
  Upload → Verify → Done

Large File (>10MB):
  Init Multipart → Upload Parts → Complete → Verify → Done
  (Resume from last part on failure)
```

### 3.7 Serializer

**Responsibility**: Convert PostgreSQL rows to JSONL

**Type Handling**:
- Primitives: Direct conversion
- Timestamps: ISO 8601 with timezone
- JSON/JSONB: Preserve as nested JSON
- Arrays: JSON arrays
- BYTEA: Base64 encoding
- UUID: String representation
- NUMERIC: String (preserve precision)

**Output Format**:
```jsonl
{"id": 1, "created_at": "2025-01-01T00:00:00Z", ...}
{"id": 2, "created_at": "2025-01-01T00:01:00Z", ...}
```

### 3.8 Lock Manager

**Responsibility**: Prevent concurrent runs

**Backends**:
- **Redis**: Distributed locking with TTL
- **PostgreSQL**: Advisory locks
- **File**: Local file lock (dev only)

**Lock Lifecycle**:
```
Acquire → Heartbeat (30s) → Release
         ↓
    (Stale detection)
```

---

## 4. Data Flow

### 4.1 Archival Flow

```
┌─────────────┐
│   Start     │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│ Acquire Lock   │
└──────┬─────────┘
       │
       ▼
┌─────────────────┐
│ For each DB:    │
│  Connect        │
└──────┬──────────┘
       │
       ▼
┌─────────────────┐
│ For each Table: │
│  Validate Schema│
└──────┬──────────┘
       │
       ▼
┌─────────────────┐
│ While records:  │
│  Process Batch  │
└──────┬──────────┘
       │
       ├─► SELECT (SKIP LOCKED)
       ├─► Serialize → JSONL
       ├─► Compress → gzip
       ├─► Upload → S3
       ├─► Verify (counts, checksum)
       ├─► DELETE (in transaction)
       └─► COMMIT
       │
       ▼
┌─────────────────┐
│ Update          │
│ Watermark       │
└──────┬──────────┘
       │
       ▼
┌─────────────────┐
│ VACUUM          │
│ (optional)      │
└──────┬──────────┘
       │
       ▼
┌─────────────────┐
│ Release Lock   │
└──────┬─────────┘
       │
       ▼
┌─────────────┐
│   End      │
└────────────┘
```

### 4.2 Verify-Then-Delete Pattern

```
┌─────────────┐
│ BEGIN TX    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ SELECT      │
│ (SKIP       │
│  LOCKED)    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Count DB    │
│ Records     │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Serialize  │
│ & Compress │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Upload S3   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Verify S3   │
│ (exists,    │
│  size)      │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Count S3    │
│ Records     │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Verify      │
│ Counts Match│
└──────┬──────┘
       │
       ├─► Match: Continue
       └─► Mismatch: ROLLBACK, Abort
       │
       ▼
┌─────────────┐
│ Checksum    │
│ Verify      │
└──────┬──────┘
       │
       ├─► Match: Continue
       └─► Mismatch: ROLLBACK, Abort
       │
       ▼
┌─────────────┐
│ DELETE      │
│ (by PKs)    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ COMMIT      │
└─────────────┘
```

---

## 5. Error Handling Strategy

### 5.1 Error Categories

**FATAL** (Exit immediately):
- Configuration errors
- S3 unreachable
- Invalid credentials

**ERROR** (Continue with next):
- Database connection failure
- Table not found
- Permission denied

**WARNING** (Log, continue):
- Schema drift detected
- Slow query
- Lock contention

**INFO** (Log):
- Batch completed
- Progress updates

### 5.2 Retry Strategy

**Transient Errors** (Retry with backoff):
- Network timeouts
- S3 503 SlowDown
- Database deadlocks
- Connection resets

**Permanent Errors** (Skip):
- Invalid SQL
- Table not found
- Permission denied
- Configuration errors

**Retry Pattern**:
```
Attempt 1: Immediate
Attempt 2: Wait 2s
Attempt 3: Wait 4s
Attempt 4: Wait 8s (max 30s)
Give up: Log error, skip
```

---

## 6. State Management

### 6.1 Checkpoint System

**Checkpoint Data**:
```python
{
    "database": "prod_db",
    "table": "audit_logs",
    "last_batch": 5,
    "last_timestamp": "2025-01-01T00:00:00Z",
    "last_primary_key": 12345,
    "records_archived": 50000,
    "checkpoint_time": "2025-01-01T01:00:00Z"
}
```

**Checkpoint Lifecycle**:
- Save every N batches (default: 10)
- Store in S3 or local file
- Load on startup if exists
- Resume from checkpoint

### 6.2 Watermark System

**Watermark Data**:
```python
{
    "database": "prod_db",
    "table": "audit_logs",
    "last_archived_timestamp": "2025-01-01T00:00:00Z",
    "last_archived_pk": 12345,
    "total_records": 50000,
    "updated_at": "2025-01-01T01:00:00Z",
    "checksum": "sha256:..."
}
```

**Watermark Usage**:
- Query only records after watermark
- Avoid reprocessing archived data
- Update after successful batch

---

## 7. Scalability Considerations

### 7.1 Horizontal Scaling

**Multiple Instances**:
- Distributed locking prevents conflicts
- Each instance processes different databases/tables
- Use `--database` and `--table` flags to partition work

**Example**:
```bash
# Instance 1
archiver --database db1,db2

# Instance 2
archiver --database db3,db4
```

### 7.2 Vertical Scaling

**Resource Limits**:
- Memory: 500MB per batch (configurable)
- Connections: 5 per database (configurable)
- Batch size: Adaptive (1K-50K records)

**Optimization**:
- Increase batch size for large tables
- Use read replicas for queries
- Parallel database processing (optional)

---

## 8. Security Architecture

### 8.1 Credential Management

**Sources** (priority order):
1. Environment variables (preferred)
2. AWS Secrets Manager
3. HashiCorp Vault
4. Configuration file (dev only, with warnings)

**Never**:
- Log credentials
- Expose in error messages
- Store in version control

### 8.2 Encryption

**In Transit**:
- TLS 1.2+ for database connections
- HTTPS for S3 operations

**At Rest**:
- S3 server-side encryption (SSE-S3, SSE-KMS)
- Optional client-side encryption

### 8.3 Access Control

**Database Permissions**:
- SELECT on tables
- DELETE on tables
- VACUUM permission (optional)

**S3 Permissions**:
- PutObject
- GetObject
- DeleteObject (cleanup)
- ListBucket

---

## 9. Observability Architecture

### 9.1 Logging

**Structured JSON Logs**:
```json
{
    "timestamp": "2025-01-01T00:00:00Z",
    "level": "INFO",
    "correlation_id": "abc123",
    "database": "prod_db",
    "table": "audit_logs",
    "batch": 1,
    "records": 10000,
    "message": "Batch completed"
}
```

**Log Levels**:
- DEBUG: Detailed operations
- INFO: Normal operations
- WARN: Non-critical issues
- ERROR: Failures
- CRITICAL: System failures

### 9.2 Metrics

**Prometheus Metrics**:
- Counters: Records archived, bytes uploaded
- Histograms: Duration per phase
- Gauges: Current state, memory usage

**Metrics Endpoint**:
- HTTP server on configurable port
- `/metrics` endpoint
- Prometheus-compatible format

### 9.3 Tracing

**Correlation IDs**:
- Generated per run
- Included in all logs
- Passed to external services
- Enables request tracing

---

## 10. Deployment Architecture

### 10.1 Standalone Deployment

```
┌─────────────┐
│   System    │
│   Cron/     │
│  Systemd    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Archiver   │
│  Process    │
└──────┬──────┘
       │
       ├─► PostgreSQL
       └─► S3
```

### 10.2 Docker Deployment

```
┌─────────────┐
│   Docker    │
│   Container │
└──────┬──────┘
       │
       ├─► Config (Volume)
       ├─► Credentials (Secrets)
       └─► Logs (Volume)
```

### 10.3 Kubernetes Deployment

```
┌─────────────┐
│  CronJob    │
└──────┬──────┘
       │
       ├─► ConfigMap (Config)
       ├─► Secret (Credentials)
       └─► Service (Metrics)
```

---

## 11. Testing Architecture

### 11.1 Test Environment

```
┌─────────────┐
│   pytest    │
└──────┬──────┘
       │
       ├─► PostgreSQL (Docker)
       ├─► MinIO (Docker)
       └─► Test Data (Fixtures)
```

### 11.2 Test Types

**Unit Tests**:
- Mock external dependencies
- Test individual functions
- Fast execution (<2 minutes)

**Integration Tests**:
- Real PostgreSQL + MinIO
- End-to-end workflows
- Slower execution (<10 minutes)

**Chaos Tests**:
- Simulate failures
- Verify recovery
- Validate data integrity

---

## 12. Future Enhancements

### 12.1 Potential Additions

- **Web UI**: Dashboard for monitoring
- **Plugin System**: Extensible architecture
- **Machine Learning**: Predictive archival, anomaly detection
- **Multi-Region**: Cross-region S3 replication

### 12.2 Performance Optimizations

- **Parallel Uploads**: Upload while fetching next batch
- **Faster Compression**: Use pigz for parallel gzip
- **Query Optimization**: Index hints, query tuning
- **Batch Size Tuning**: ML-based adaptive sizing

---

**END OF ARCHITECTURE DOCUMENT**

