# Operational Runbooks

Step-by-step procedures for common operational tasks and troubleshooting scenarios.

## Table of Contents

1. [Recovering from Failed Archival](#recovering-from-failed-archival)
2. [Handling Partial Uploads](#handling-partial-uploads)
3. [Managing Stuck Locks](#managing-stuck-locks)
4. [Performance Troubleshooting](#performance-troubleshooting)
5. [Database Connection Issues](#database-connection-issues)
6. [S3 Upload Failures](#s3-upload-failures)
7. [Checkpoint Recovery](#checkpoint-recovery)
8. [Emergency Procedures](#emergency-procedures)

---

## Recovering from Failed Archival

### Scenario: Archival Failed Mid-Process

**Symptoms:**
- Process terminated unexpectedly
- Partial data uploaded to S3
- Database records not deleted
- Checkpoint file exists

**Procedure:**

1. **Check Current State**
   ```bash
   # Check if checkpoint exists
   aws s3 ls s3://<bucket>/<prefix>/checkpoints/<database>/<table>/
   
   # Check what was uploaded
   aws s3 ls s3://<bucket>/<prefix>/<database>/<table>/
   ```

2. **Verify Database State**
   ```sql
   -- Check how many records still need archiving
   SELECT COUNT(*) FROM <table>
   WHERE <timestamp_column> < '<cutoff_date>';
   ```

3. **Resume Archival**
   ```bash
   # Archiver will automatically resume from checkpoint
   python -m archiver.main --config config.yaml \
     --database <database> \
     --table <table>
   ```

4. **Verify Completion**
   ```bash
   # Check logs for successful completion
   # Verify all records archived
   # Confirm checkpoint deleted
   ```

**Expected Outcome:**
- Archival resumes from last successful batch
- All remaining records archived
- Checkpoint automatically deleted on completion

---

## Handling Partial Uploads

### Scenario: S3 Upload Interrupted

**Symptoms:**
- Orphaned multipart uploads in S3
- Incomplete files
- Upload errors in logs

**Procedure:**

1. **List Orphaned Uploads**
   ```bash
   aws s3api list-multipart-uploads \
     --bucket <bucket> \
     --prefix <prefix>/<database>/<table>/
   ```

2. **Automatic Cleanup (Recommended)**
   ```bash
   # Resume archival - will auto-cleanup on checkpoint resume
   python -m archiver.main --config config.yaml
   ```

3. **Manual Cleanup (If Needed)**
   ```bash
   # Abort specific multipart upload
   aws s3api abort-multipart-upload \
     --bucket <bucket> \
     --key <key> \
     --upload-id <upload-id>
   ```

4. **Verify Cleanup**
   ```bash
   # Confirm no orphaned uploads remain
   aws s3api list-multipart-uploads --bucket <bucket>
   ```

**Expected Outcome:**
- All orphaned uploads cleaned up
- No S3 storage costs from incomplete uploads
- Archival can proceed normally

---

## Managing Stuck Locks

### Scenario: Lock Held by Dead Process

**Symptoms:**
- Error: `LockError: Lock already held`
- Process crashed but lock remains
- Cannot start new archival

**Procedure:**

1. **Identify Lock Owner**
   ```sql
   -- Check advisory locks
   SELECT 
     locktype,
     objid,
     pid,
     mode,
     granted
   FROM pg_locks
   WHERE locktype = 'advisory';
   
   -- Find process details
   SELECT 
     pid,
     application_name,
     state,
     query_start,
     backend_start
   FROM pg_stat_activity
   WHERE pid IN (
     SELECT pid FROM pg_locks WHERE locktype = 'advisory'
   );
   ```

2. **Check if Process is Alive**
   ```bash
   # Check if process exists
   ps aux | grep archiver
   ```

3. **Wait for Lock Expiration (Recommended)**
   - Locks automatically expire (default: 1 hour TTL)
   - Wait for natural expiration
   - Monitor lock status

4. **Manual Release (If Urgent)**
   ```sql
   -- Release all advisory locks (use with caution)
   SELECT pg_advisory_unlock_all();
   
   -- Or release specific lock
   SELECT pg_advisory_unlock(<objid>);
   ```

5. **Verify Lock Released**
   ```sql
   SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory';
   ```

**Expected Outcome:**
- Lock released
- New archival can start
- No data corruption

---

## Performance Troubleshooting

### Scenario: Slow Archival Performance

**Symptoms:**
- Archival taking longer than expected
- High database CPU usage
- Slow query execution

**Procedure:**

1. **Check Query Performance**
   ```sql
   -- Analyze query plan
   EXPLAIN ANALYZE
   SELECT * FROM <table>
   WHERE <timestamp_column> < '<cutoff_date>'
   ORDER BY <timestamp_column>, <primary_key>
   LIMIT 10000
   FOR UPDATE SKIP LOCKED;
   ```

2. **Verify Indexes**
   ```sql
   -- Check indexes on timestamp column
   SELECT 
     indexname,
     indexdef
   FROM pg_indexes
   WHERE tablename = '<table>'
     AND schemaname = '<schema>';
   
   -- Create index if missing
   CREATE INDEX IF NOT EXISTS idx_<table>_<timestamp_column>
   ON <schema>.<table>(<timestamp_column>);
   ```

3. **Check Database Load**
   ```sql
   -- Check active queries
   SELECT 
     pid,
     state,
     query,
     query_start,
     now() - query_start AS duration
   FROM pg_stat_activity
   WHERE state != 'idle'
   ORDER BY query_start;
   ```

4. **Optimize Batch Size**
   ```yaml
   # Reduce batch size if queries are slow
   defaults:
     batch_size: 5000  # Reduce from 10000
   ```

5. **Use Read Replica**
   ```yaml
   databases:
     - name: production_db
       host: primary.example.com
       read_replica: replica.example.com  # Use replica for queries
   ```

**Expected Outcome:**
- Query performance improved
- Reduced database load
- Faster archival completion

---

## Database Connection Issues

### Scenario: Connection Pool Exhausted

**Symptoms:**
- Error: `Connection pool not initialized`
- Timeout errors
- Connection refused

**Procedure:**

1. **Check Connection Count**
   ```sql
   -- Count active connections
   SELECT 
     application_name,
     state,
     count(*)
   FROM pg_stat_activity
   WHERE application_name = 'audit_archiver'
   GROUP BY application_name, state;
   ```

2. **Check Connection Limits**
   ```sql
   -- Check max connections
   SHOW max_connections;
   
   -- Check current connections
   SELECT count(*) FROM pg_stat_activity;
   ```

3. **Reduce Pool Size**
   ```yaml
   defaults:
     connection_pool_size: 3  # Reduce from 5
   ```

4. **Wait and Retry**
   - Wait for connections to clear (30-60 seconds)
   - Archiver will automatically retry with exponential backoff

5. **Verify Recovery**
   ```bash
   # Check logs for successful reconnection
   python -m archiver.main --config config.yaml --verbose
   ```

**Expected Outcome:**
- Connections restored
- Archival resumes
- No data loss

---

## S3 Upload Failures

### Scenario: Repeated S3 Upload Failures

**Symptoms:**
- Circuit breaker opens
- 503 errors
- Upload timeouts

**Procedure:**

1. **Check S3 Service Status**
   ```bash
   # Test S3 connectivity
   aws s3 ls s3://<bucket>/
   
   # Check S3 metrics (if available)
   # Monitor request rate, error rate
   ```

2. **Verify Credentials**
   ```bash
   # Check AWS credentials
   aws sts get-caller-identity
   
   # Test permissions
   aws s3api head-bucket --bucket <bucket>
   ```

3. **Check Rate Limits**
   ```bash
   # Review S3 request metrics
   # Check for throttling
   ```

4. **Reduce Concurrency**
   ```yaml
   defaults:
     parallel_databases: false  # Disable parallel processing
     batch_size: 20000  # Larger batches = fewer uploads
   ```

5. **Enable Local Fallback**
   ```yaml
   s3:
     local_fallback_dir: /var/archiver/fallback
   ```

6. **Wait for Circuit Breaker Recovery**
   - Circuit breaker auto-recovers after 60 seconds
   - Monitor logs for recovery

**Expected Outcome:**
- Uploads succeed
- Circuit breaker closes
- Archival continues

---

## Checkpoint Recovery

### Scenario: Resuming from Checkpoint

**Symptoms:**
- Previous run interrupted
- Checkpoint file exists in S3
- Need to resume archival

**Procedure:**

1. **Verify Checkpoint Exists**
   ```bash
   # List checkpoint files
   aws s3 ls s3://<bucket>/<prefix>/checkpoints/<database>/<table>/
   ```

2. **Review Checkpoint Contents**
   ```bash
   # Download and inspect checkpoint
   aws s3 cp s3://<bucket>/<prefix>/checkpoints/<database>/<table>/checkpoint.json - | jq .
   ```

3. **Resume Archival**
   ```bash
   # Archiver automatically detects and uses checkpoint
   python -m archiver.main --config config.yaml \
     --database <database> \
     --table <table>
   ```

4. **Monitor Resume**
   ```bash
   # Check logs for "Checkpoint loaded - resuming from interrupted run"
   # Verify batch number continues from checkpoint
   ```

5. **Verify Completion**
   ```bash
   # Check that checkpoint deleted on completion
   # Verify all records archived
   ```

**Expected Outcome:**
- Archival resumes from checkpoint
- No duplicate uploads
- Complete archival

---

## Emergency Procedures

### Scenario: Stop Archival Immediately

**Procedure:**

1. **Send Interrupt Signal**
   ```bash
   # Find process
   ps aux | grep archiver
   
   # Send SIGINT (Ctrl+C)
   kill -SIGINT <pid>
   ```

2. **Verify Safe Stop**
   ```bash
   # Check logs for graceful shutdown
   # Verify current batch completed
   # Check checkpoint created
   ```

3. **Verify Data Integrity**
   ```sql
   -- Check records still in database
   SELECT COUNT(*) FROM <table>
   WHERE <timestamp_column> < '<cutoff_date>';
   ```

**Expected Outcome:**
- Process stops gracefully
- Current batch completed
- Checkpoint saved
- No data loss

---

### Scenario: Rollback Failed Archival

**Procedure:**

1. **Identify Failed Batches**
   ```bash
   # Review logs for failed batches
   # Identify S3 keys uploaded
   ```

2. **Verify Database State**
   ```sql
   -- Check if records were deleted
   SELECT COUNT(*) FROM <table>
   WHERE <timestamp_column> < '<cutoff_date>';
   ```

3. **Restore from S3 (If Needed)**
   ```bash
   # Use restore utility to restore archived data
   python -m restore.main --config config.yaml \
     --database <database> \
     --table <table> \
     --start-date <date> \
     --end-date <date>
   ```

**Expected Outcome:**
- Data restored if needed
- Database state verified
- Ready for re-archival

---

## Best Practices

### Before Operations

1. **Always Use Dry-Run First**
   ```bash
   python -m archiver.main --config config.yaml --dry-run
   ```

2. **Check System Health**
   - Database connectivity
   - S3 accessibility
   - Resource availability

3. **Review Configuration**
   - Verify batch sizes
   - Check connection pool sizes
   - Confirm S3 settings

### During Operations

1. **Monitor Progress**
   - Watch logs for errors
   - Monitor database load
   - Check S3 upload rates

2. **Track Metrics**
   - Records processed per second
   - Upload throughput
   - Error rates

3. **Be Ready to Intervene**
   - Know how to stop gracefully
   - Understand recovery procedures
   - Have rollback plan ready

### After Operations

1. **Verify Completion**
   - Check all records archived
   - Verify S3 uploads complete
   - Confirm checkpoints deleted

2. **Review Logs**
   - Check for warnings
   - Review error messages
   - Document any issues

3. **Update Documentation**
   - Note any issues encountered
   - Update procedures if needed
   - Share learnings with team

---

## Quick Reference

### Common Commands

```bash
# Dry run
python -m archiver.main --config config.yaml --dry-run

# Single database/table
python -m archiver.main --config config.yaml \
  --database <db> --table <table>

# Verbose logging
python -m archiver.main --config config.yaml --verbose

# Check S3 connectivity
aws s3 ls s3://<bucket>/

# List checkpoints
aws s3 ls s3://<bucket>/<prefix>/checkpoints/

# Check database connections
psql -c "SELECT * FROM pg_stat_activity WHERE application_name = 'audit_archiver';"
```

### Common SQL Queries

```sql
-- Check records to archive
SELECT COUNT(*) FROM <table> WHERE <timestamp_column> < '<cutoff_date>';

-- Check indexes
SELECT * FROM pg_indexes WHERE tablename = '<table>';

-- Check locks
SELECT * FROM pg_locks WHERE locktype = 'advisory';

-- Check active queries
SELECT * FROM pg_stat_activity WHERE state != 'idle';
```

---

**Last Updated**: 2026-01-XX  
**Version**: 1.0

