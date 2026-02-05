# Troubleshooting Guide

Complete troubleshooting guide for common issues and their solutions.

Common issues and solutions for the Audit Table Archiver.

## Configuration Issues

### Environment Variable Not Set

**Error**: `Environment variable DB_PASSWORD not set`

**Solution**:

For Linux/macOS (bash/zsh):
```bash
export DB_PASSWORD=your_password
# Or add to your shell profile (.bashrc, .zshrc, etc.)
```

For Windows PowerShell:
```powershell
$env:DB_PASSWORD="your_password"
# For current session only
```

For Windows Command Prompt:
```cmd
set DB_PASSWORD=your_password
# For current session only
```

To make it persistent in PowerShell, add to your profile:
```powershell
# Edit profile: notepad $PROFILE
[System.Environment]::SetEnvironmentVariable("DB_PASSWORD", "your_password", "User")
```

### Invalid Configuration File

**Error**: `Configuration validation failed`

**Solution**:
- Check YAML syntax (use a YAML validator)
- Verify all required fields are present
- Check field types match expected types
- Review example configuration files

## Database Issues

### Connection Failed

**Error**: `Failed to create connection pool`

**Solutions**:
1. **Check PostgreSQL is running**
   ```bash
   # Docker
   docker-compose ps postgres
   
   # System service
   sudo systemctl status postgresql
   ```

2. **Verify connection details**
   - Host, port, database name
   - Username and password
   - Network connectivity

3. **Check PostgreSQL logs**
   ```bash
   # Docker
   docker-compose logs postgres
   ```

### Permission Denied

**Error**: `permission denied for table`

**Solution**:
```sql
-- Grant necessary permissions
GRANT SELECT, DELETE ON TABLE audit_logs TO archiver;
GRANT USAGE ON SCHEMA public TO archiver;
```

### Table Not Found

**Error**: `relation "table_name" does not exist`

**Solution**:
- Verify table name and schema in configuration
- Check table exists: `\dt schema.table_name` in psql
- Ensure schema is correct (default: `public`)

## S3 Issues

### Bucket Not Found

**Error**: `Bucket not found: bucket-name`

**Solution**:
1. **Create bucket**
   ```bash
   aws s3 mb s3://bucket-name
   ```

2. **Check bucket name** in configuration

3. **Verify credentials** have access

### Access Denied

**Error**: `Access denied to bucket`

**Solution**:
- Verify AWS credentials or IAM role
- Check bucket policy allows PutObject, GetObject, DeleteObject
- For MinIO, check root credentials

### Upload Failed

**Error**: `File upload failed after 3 attempts`

**Solutions**:
1. **Check network connectivity**
2. **Verify S3 endpoint** is correct
3. **Check file size** (may need multipart upload)
4. **Review S3 logs** for detailed error

## Performance Issues

### Slow Archival

**Symptoms**: Archival taking longer than expected

**Solutions**:
1. **Increase batch size** (if memory allows)
   ```yaml
   defaults:
     batch_size: 20000  # Increase from default 10000
   ```

2. **Use read replica** for queries
   ```yaml
   databases:
     - name: prod_db
       read_replica: replica.example.com
   ```

3. **Check database indexes**
   ```sql
   -- Ensure index on timestamp column
   CREATE INDEX idx_table_created_at ON table_name(created_at);
   ```

4. **Monitor database load**
   - Check for blocking queries
   - Review pg_stat_activity

### High Memory Usage

**Symptoms**: Process using too much memory

**Solutions**:
1. **Reduce batch size**
   ```yaml
   defaults:
     batch_size: 5000  # Reduce from default
   ```

2. **Process fewer tables** at once
3. **Monitor memory** and adjust accordingly

## Data Integrity Issues

### Count Mismatch

**Error**: `Count mismatch: DB count (100) != Memory count (99)`

**Causes**:
- Records modified during archival
- Transaction isolation issues
- Data corruption

**Solutions**:
1. **Run dry-run first** to identify issues
2. **Check for concurrent modifications**
3. **Review logs** for detailed context
4. **Re-run archival** (idempotent)

### Verification Failed

**Error**: `Verification failed`

**Solutions**:
1. **Check S3 upload** completed successfully
2. **Verify network** didn't drop during upload
3. **Review transaction logs**
4. **Re-run archival** (safe, idempotent)

## Transaction Issues

### Transaction Timeout

**Error**: `Transaction timeout after 1800 seconds`

**Solutions**:
1. **Increase timeout** (if appropriate)
   ```python
   # In code (future: configurable)
   TransactionManager(connection, timeout_seconds=3600)
   ```

2. **Reduce batch size** to complete faster
3. **Check for blocking queries** in database

### Deadlock

**Error**: `deadlock detected`

**Solutions**:
1. **Retry** (automatic in most cases)
2. **Reduce concurrency** if running multiple instances
3. **Check for long-running transactions**

## Logging Issues

### No Logs Appearing

**Solutions**:
1. **Check log level**
   ```bash
   python -m archiver.main --config config.yaml --log-level DEBUG
   ```

2. **Verify log output** (stdout by default)
3. **Check log file** if configured

### Too Many Logs

**Solutions**:
1. **Increase log level**
   ```yaml
   observability:
     log_level: WARN  # Instead of DEBUG
   ```

2. **Use quiet mode** (future feature)

## Error Handling

### Circuit Breaker Open

**Error**: `Circuit breaker is OPEN. Retry after X seconds`

**Cause**: Repeated failures detected (5 consecutive failures by default)

**Solutions**:
1. **Wait for recovery**: Circuit breaker automatically attempts recovery after 60 seconds
2. **Check underlying issue**: Review logs for root cause
3. **Reset manually**: (Future: CLI command to reset circuit breaker)
4. **Review retry configuration**: Adjust failure threshold if needed

### Retry Exhausted

**Error**: `All retry attempts exhausted`

**Solutions**:
1. **Check network connectivity**: Verify S3 endpoint is reachable
2. **Review S3 credentials**: Ensure credentials are valid
3. **Check S3 service status**: Verify S3 service is operational
4. **Increase retry attempts**: (Future: Configurable retry count)

### Lock Acquisition Failed

**Error**: `Lock already held: database:table`

**Cause**: Another instance is already running

**Solutions**:
1. **Check for running instances**: Verify no other archiver processes
2. **Wait for completion**: Let current run finish
3. **Check for stale locks**: (Future: Lock cleanup utility)
4. **Review lock configuration**: Adjust lock TTL if needed

## Common Patterns

### Safe Testing

Always test with `--dry-run` first:
```bash
python -m archiver.main --config config.yaml --dry-run --verbose
```

### Incremental Testing

Test with small batches first:
```yaml
defaults:
  batch_size: 100  # Small for testing
```

### Isolated Testing

Test one table at a time:
```bash
python -m archiver.main --config config.yaml \
  --database test_db \
  --table test_table
```

### Performance Testing

Monitor performance and adjust:
```bash
# Run with verbose logging
python -m archiver.main --config config.yaml --verbose

# Check performance metrics in logs
# Adjust batch size based on query times
```

See [Performance Tuning Guide](performance-tuning.md) for detailed optimization strategies.

## Getting Help

1. **Check Logs**: Review detailed error messages with correlation IDs
2. **Review Documentation**: 
   - [Architecture](architecture.md)
   - [Performance Tuning](performance-tuning.md)
   - [Quick Start](quick-start.md)
3. **Search Issues**: Check GitHub issues for similar problems
4. **Open Issue**: Provide:
   - Error message and stack trace
   - Configuration (sanitized)
   - Steps to reproduce
   - Environment details
   - Correlation ID from logs

---

**Still Stuck?** Open an issue with detailed information.

