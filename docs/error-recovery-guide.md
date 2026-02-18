# Error Recovery Guide

Complete guide for recovering from common errors and failures during archival operations.

## Overview

This guide provides step-by-step procedures for recovering from various error scenarios. Each procedure includes:
- **Symptoms**: How to identify the error
- **Root Cause**: Common causes
- **Recovery Steps**: Detailed recovery procedure
- **Prevention**: How to avoid the error in the future

---

## Connection Failures

### Database Connection Lost

**Symptoms:**
- Error: `Connection pool not initialized` or `PostgresConnectionError`
- Logs show: `Connection pool unhealthy, attempting to reconnect`
- Archival stops mid-operation

**Root Causes:**
- Database server restarted
- Network interruption
- Connection pool exhausted
- Database maintenance

**Recovery Steps:**

1. **Check Database Status**
   ```bash
   # Verify database is running
   psql -h <host> -U <user> -d <database> -c "SELECT 1"
   ```

2. **Verify Network Connectivity**
   ```bash
   # Test network connection
   telnet <host> <port>
   # or
   nc -zv <host> <port>
   ```

3. **Check Connection Pool**
   ```sql
   -- Check active connections
   SELECT count(*) FROM pg_stat_activity WHERE application_name = 'audit_archiver';
   
   -- Check for blocking queries
   SELECT * FROM pg_stat_activity WHERE wait_event_type IS NOT NULL;
   ```

4. **Restart Archival**
   ```bash
   # The archiver will automatically reconnect
   python -m archiver.main --config config.yaml
   ```

5. **If Automatic Reconnection Fails**
   - Wait 30-60 seconds for connections to clear
   - Check database logs for errors
   - Verify credentials and permissions
   - Restart the archiver process

**Prevention:**
- Use connection pooling (default: 5 connections)
- Monitor database connection count
- Set appropriate connection timeouts
- Use read replicas for queries

---

### S3 Connection Lost

**Symptoms:**
- Error: `S3Error: Connection timeout` or `BotoCoreError`
- Uploads fail with network errors
- Circuit breaker opens

**Root Causes:**
- Network interruption
- S3 service outage
- Rate limiting
- Credential expiration

**Recovery Steps:**

1. **Check S3 Service Status**
   ```bash
   # Test S3 connectivity
   aws s3 ls s3://<bucket>/
   ```

2. **Verify Credentials**
   ```bash
   # Check AWS credentials
   aws sts get-caller-identity
   ```

3. **Check Circuit Breaker Status**
   - Wait for circuit breaker recovery (default: 60 seconds)
   - Check logs for circuit breaker state

4. **Resume Archival**
   ```bash
   # Archiver will retry with exponential backoff
   python -m archiver.main --config config.yaml
   ```

5. **If Uploads Still Fail**
   - Check local fallback directory (if configured)
   - Verify S3 bucket permissions
   - Check network bandwidth
   - Review S3 rate limits

**Prevention:**
- Enable local fallback directory
- Use retry logic (default: 3 attempts)
- Monitor S3 service health
- Use appropriate rate limiting

---

## Data Integrity Errors

### Count Mismatch During Verification

**Symptoms:**
- Error: `VerificationError: Count mismatch: DB count (100) != Memory count (99)`
- Archival stops before deletion
- No data deleted (safe by design)

**Root Causes:**
- Records modified during archival
- Transaction isolation issues
- Concurrent modifications
- Data corruption

**Recovery Steps:**

1. **Verify No Data Loss**
   ```sql
   -- Check records still in database
   SELECT COUNT(*) FROM <table> WHERE <timestamp_column> < '<cutoff_date>';
   ```

2. **Check for Concurrent Modifications**
   ```sql
   -- Check for active transactions
   SELECT * FROM pg_stat_activity WHERE state = 'active';
   ```

3. **Re-run Archival**
   ```bash
   # Archival is idempotent - safe to re-run
   python -m archiver.main --config config.yaml --database <db> --table <table>
   ```

4. **If Mismatch Persists**
   - Review logs for detailed context
   - Check for schema changes
   - Verify timestamp column data types
   - Consider reducing batch size

**Prevention:**
- Use appropriate transaction isolation
- Avoid concurrent modifications during archival
- Use read replicas for queries
- Monitor for schema changes

---

### Checksum Verification Failed

**Symptoms:**
- Error: `VerificationError: Checksum mismatch`
- Upload appears successful but verification fails
- No data deleted

**Root Causes:**
- Network corruption during upload
- S3 storage corruption
- Compression issues
- File system errors

**Recovery Steps:**

1. **Verify S3 Object**
   ```bash
   # Check object exists and size matches
   aws s3 ls s3://<bucket>/<key>
   ```

2. **Re-download and Verify**
   ```bash
   # Download and verify checksum
   aws s3 cp s3://<bucket>/<key> /tmp/test.gz
   sha256sum /tmp/test.gz
   ```

3. **Re-run Archival**
   ```bash
   # Will re-upload with new checksum
   python -m archiver.main --config config.yaml --database <db> --table <table>
   ```

4. **If Checksum Still Fails**
   - Check network stability
   - Verify S3 bucket integrity
   - Review compression settings
   - Check for disk errors

**Prevention:**
- Use multipart uploads for large files
- Enable checksum verification (default: enabled)
- Monitor network stability
- Use reliable storage backends

---

## Transaction Errors

### Transaction Timeout

**Symptoms:**
- Error: `TransactionError: Transaction timeout after 1800 seconds`
- Long-running archival operations fail
- Database connections held open

**Root Causes:**
- Very large batches
- Slow database queries
- Network latency
- Database load

**Recovery Steps:**

1. **Check Transaction Status**
   ```sql
   -- Find long-running transactions
   SELECT pid, now() - xact_start AS duration, query
   FROM pg_stat_activity
   WHERE state = 'active' AND xact_start IS NOT NULL
   ORDER BY duration DESC;
   ```

2. **Reduce Batch Size**
   ```yaml
   defaults:
     batch_size: 5000  # Reduce from default 10000
   ```

3. **Increase Timeout (if appropriate)**
   ```python
   # In code (future: configurable)
   TransactionManager(connection, timeout_seconds=3600)
   ```

4. **Re-run with Smaller Batches**
   ```bash
   python -m archiver.main --config config.yaml
   ```

**Prevention:**
- Use appropriate batch sizes
- Monitor query performance
- Use read replicas
- Schedule during off-peak hours

---

### Deadlock Detected

**Symptoms:**
- Error: `deadlock detected` or `lock timeout`
- Multiple archival processes running
- Database locks held

**Root Causes:**
- Concurrent archival runs
- Lock conflicts
- Long-running transactions
- Missing indexes

**Recovery Steps:**

1. **Check for Concurrent Runs**
   ```bash
   # Check for multiple archiver processes
   ps aux | grep archiver
   ```

2. **Check Database Locks**
   ```sql
   -- Find blocking locks
   SELECT * FROM pg_locks WHERE NOT granted;
   
   -- Find lock waiters
   SELECT * FROM pg_stat_activity WHERE wait_event_type = 'Lock';
   ```

3. **Terminate Conflicting Processes**
   ```sql
   -- Terminate blocking queries (use with caution)
   SELECT pg_terminate_backend(pid) FROM pg_stat_activity
   WHERE application_name = 'audit_archiver' AND pid != pg_backend_pid();
   ```

4. **Wait and Retry**
   - Wait for locks to clear (default: 30 seconds)
   - Re-run archival

**Prevention:**
- Use distributed locking (default: enabled)
- Run single instance per database
- Add appropriate indexes
- Use `FOR UPDATE SKIP LOCKED`

---

## S3 Upload Failures

### Multipart Upload Incomplete

**Symptoms:**
- Error: `S3Error: Multipart upload incomplete`
- Partial files in S3
- Orphaned multipart uploads

**Root Causes:**
- Process interruption
- Network failure during upload
- S3 service issues
- Timeout

**Recovery Steps:**

1. **List Orphaned Uploads**
   ```bash
   # List incomplete multipart uploads
   aws s3api list-multipart-uploads --bucket <bucket>
   ```

2. **Clean Up Orphaned Uploads**
   ```bash
   # Abort incomplete uploads
   aws s3api abort-multipart-upload \
     --bucket <bucket> \
     --key <key> \
     --upload-id <upload-id>
   ```

3. **Re-run Archival**
   ```bash
   # Will automatically clean up on resume
   python -m archiver.main --config config.yaml
   ```

4. **Automatic Cleanup**
   - Archiver automatically cleans up orphaned uploads on checkpoint resume
   - Manual cleanup only needed if process doesn't resume

**Prevention:**
- Enable checkpoint/resume (default: enabled)
- Use appropriate multipart thresholds
- Monitor upload progress
- Use reliable network connections

---

### Rate Limiting

**Symptoms:**
- Error: `S3Error: Request throttled` or `503 Slow Down`
- Uploads fail with 503 errors
- Circuit breaker opens frequently

**Root Causes:**
- Too many concurrent uploads
- Exceeding S3 rate limits
- Small file uploads (high request rate)

**Recovery Steps:**

1. **Reduce Concurrency**
   ```yaml
   defaults:
     parallel_databases: false  # Disable parallel processing
   ```

2. **Increase Batch Size**
   ```yaml
   defaults:
     batch_size: 20000  # Larger batches = fewer uploads
   ```

3. **Wait and Retry**
   - Wait for rate limit window to reset
   - Circuit breaker will automatically retry

4. **Use S3 Rate Limiter**
   ```yaml
   s3:
     rate_limit_requests_per_second: 10  # Limit request rate
   ```

**Prevention:**
- Configure appropriate rate limits
- Use larger batch sizes
- Avoid parallel processing for high-volume operations
- Monitor S3 request metrics

---

## Checkpoint and Resume

### Resuming Interrupted Operations

**Symptoms:**
- Process killed or interrupted
- Checkpoint file exists in S3
- Partial archival completed

**Recovery Steps:**

1. **Verify Checkpoint Exists**
   ```bash
   # List checkpoint files
   aws s3 ls s3://<bucket>/<prefix>/checkpoints/
   ```

2. **Resume from Checkpoint**
   ```bash
   # Archiver automatically resumes from checkpoint
   python -m archiver.main --config config.yaml
   ```

3. **Verify Resume**
   - Check logs for "Checkpoint loaded - resuming from interrupted run"
   - Verify batch number continues from checkpoint

4. **If Checkpoint Corrupted**
   - Delete corrupted checkpoint file
   - Re-run archival (will start from beginning, but is idempotent)

**Prevention:**
- Enable checkpoint storage (default: S3)
- Set appropriate checkpoint intervals
- Monitor checkpoint creation
- Backup checkpoint files

---

## Lock Management

### Stale Lock Cleanup

**Symptoms:**
- Error: `LockError: Lock already held`
- Process crashed but lock remains
- Cannot start new archival

**Root Causes:**
- Process crash without releasing lock
- Database connection lost
- Lock TTL expired but not cleaned up

**Recovery Steps:**

1. **Check Lock Status**
   ```sql
   -- Check for advisory locks
   SELECT * FROM pg_locks WHERE locktype = 'advisory';
   ```

2. **Find Lock Owner**
   ```sql
   -- Find process holding lock
   SELECT pid, application_name, state, query_start
   FROM pg_stat_activity
   WHERE pid IN (
     SELECT pid FROM pg_locks WHERE locktype = 'advisory'
   );
   ```

3. **Wait for Lock Expiration**
   - Locks automatically expire (default: 1 hour)
   - Wait for TTL to expire

4. **Manual Lock Release (if needed)**
   ```sql
   -- Release specific advisory lock (use with caution)
   SELECT pg_advisory_unlock_all();
   ```

**Prevention:**
- Use appropriate lock TTL
- Monitor lock duration
- Implement lock heartbeat (default: enabled)
- Clean up on process exit

---

## Best Practices

### General Recovery Strategy

1. **Always Check Logs First**
   - Review detailed error messages
   - Check correlation IDs for tracing
   - Look for context information

2. **Verify System State**
   - Check database connectivity
   - Verify S3 accessibility
   - Review resource usage

3. **Use Dry-Run Mode**
   ```bash
   # Test recovery without making changes
   python -m archiver.main --config config.yaml --dry-run
   ```

4. **Incremental Recovery**
   - Start with single database/table
   - Verify success before scaling up
   - Monitor closely during recovery

5. **Document Recovery**
   - Record recovery steps taken
   - Note root causes identified
   - Update prevention strategies

### Prevention Checklist

- ✅ Monitor connection pool health
- ✅ Use appropriate batch sizes
- ✅ Enable checkpoint/resume
- ✅ Configure retry logic
- ✅ Set up alerts for failures
- ✅ Regular health checks
- ✅ Test recovery procedures
- ✅ Document operational procedures

---

## Getting Help

If recovery steps don't resolve the issue:

1. **Collect Information**
   - Full error message and stack trace
   - Correlation ID from logs
   - Configuration (sanitized)
   - System state (database, S3, network)

2. **Review Documentation**
   - [Troubleshooting Guide](troubleshooting.md)
   - [Performance Tuning](performance-tuning.md)
   - [Architecture](architecture.md)

3. **Check Logs**
   - Review all log entries with correlation ID
   - Check database logs
   - Review S3 access logs

4. **Open Issue**
   - Provide complete error context
   - Include recovery steps attempted
   - Share relevant log excerpts

---

**Last Updated**: 2026-01-XX  
**Version**: 1.0

