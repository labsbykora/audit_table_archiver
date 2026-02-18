# Batch-Level Error Recovery

## Overview

The archiver now supports batch-level error recovery, allowing failed batches to be retried or skipped without failing the entire table archival. This improves reliability and reduces manual intervention.

## Features

### 1. Automatic Retry

Failed batches are automatically retried with exponential backoff:

- **Default retry attempts**: 3
- **Initial retry delay**: 2 seconds
- **Exponential backoff**: Delay doubles with each retry (2s, 4s, 8s)

### 2. Skip on Failure

Optionally skip failed batches and continue processing:

- **Default**: `false` (fail fast)
- **When enabled**: Failed batches are logged but processing continues
- **Use case**: Non-critical data where partial archival is acceptable

### 3. Maximum Failed Batches

Stop table archival if too many batches fail:

- **Default**: 10 failed batches
- **Behavior**: Raises exception after threshold
- **Prevents**: Infinite loops with consistently failing batches

## Configuration

### Global Defaults

```yaml
defaults:
  batch_retry_attempts: 3        # Number of retry attempts (0-10)
  batch_retry_delay: 2.0          # Initial retry delay in seconds
  skip_failed_batches: false      # Skip failed batches and continue
  max_failed_batches: 10          # Stop after N failed batches
```

### Per-Table Override

```yaml
databases:
  - name: production_db
    tables:
      - name: audit_logs
        batch_retry_attempts: 5   # Override global default
        skip_failed_batches: true # Enable skip for this table
```

## Behavior

### Retry Logic

1. **Batch fails** → Log error and track failure
2. **Check retry attempts** → If `batch_retry_attempts > 0`, retry
3. **Exponential backoff** → Wait before each retry (2s, 4s, 8s...)
4. **Re-select batch** → Query same batch again
5. **Process retry** → Attempt to process batch again
6. **Success** → Continue to next batch
7. **All retries exhausted** → Proceed to skip/fail logic

### Skip on Failure

If `skip_failed_batches: true`:

1. **All retries exhausted** → Log error
2. **Advance cursor** → Skip to next batch
3. **Continue processing** → Don't fail table archival
4. **Track failures** → Count towards `max_failed_batches`

### Fail Fast (Default)

If `skip_failed_batches: false`:

1. **All retries exhausted** → Raise exception
2. **Stop table archival** → Fail entire table
3. **Checkpoint saved** → Can resume from last successful batch

## Statistics

Failed batches are tracked in statistics:

```json
{
  "total_failed_batches": 2,
  "failed_batches": {
    "production_db.audit_logs": [
      {
        "batch_number": 5,
        "error": "S3Error: Connection timeout",
        "error_type": "S3Error",
        "timestamp": "2026-01-15T10:30:00Z"
      },
      {
        "batch_number": 12,
        "error": "VerificationError: Checksum mismatch",
        "error_type": "VerificationError",
        "timestamp": "2026-01-15T10:35:00Z"
      }
    ]
  }
}
```

## Use Cases

### 1. Transient Network Failures

**Scenario**: S3 upload fails due to network timeout

**Configuration**:
```yaml
defaults:
  batch_retry_attempts: 3
  skip_failed_batches: false
```

**Behavior**: Retry 3 times with exponential backoff. If all retries fail, stop table archival.

### 2. Non-Critical Data

**Scenario**: Some batches may fail but want to archive as much as possible

**Configuration**:
```yaml
defaults:
  batch_retry_attempts: 2
  skip_failed_batches: true
  max_failed_batches: 20
```

**Behavior**: Retry twice, then skip and continue. Stop if 20+ batches fail.

### 3. Strict Data Integrity

**Scenario**: Cannot tolerate any data loss

**Configuration**:
```yaml
defaults:
  batch_retry_attempts: 5
  skip_failed_batches: false
```

**Behavior**: Retry 5 times. Fail fast if batch cannot be archived.

## Best Practices

### 1. Retry Configuration

- **Transient errors**: 3-5 retries with 2-5s delay
- **Persistent errors**: 1-2 retries (fail fast to identify issues)
- **Network issues**: Higher retry count (5-10)

### 2. Skip on Failure

- **Use sparingly**: Only for non-critical data
- **Monitor failures**: Track failed batches in statistics
- **Set limits**: Use `max_failed_batches` to prevent infinite loops
- **Review logs**: Investigate why batches are failing

### 3. Monitoring

- **Track statistics**: Monitor `total_failed_batches`
- **Alert on failures**: Set alerts for high failure rates
- **Review failed batches**: Check error types and patterns
- **Investigate root causes**: Fix underlying issues

## Error Types

### Retryable Errors

These errors are typically retryable:

- **S3Error**: Network timeouts, connection errors
- **DatabaseError**: Connection pool exhaustion, transient failures
- **TimeoutError**: Query timeouts, operation timeouts

### Non-Retryable Errors

These errors typically indicate configuration or data issues:

- **ConfigurationError**: Invalid configuration
- **VerificationError**: Data integrity issues (may be retryable if transient)
- **LockError**: Lock conflicts (usually not retryable)

## Troubleshooting

### High Failure Rate

**Symptoms**: Many batches failing

**Investigation**:
1. Check error types in statistics
2. Review logs for patterns
3. Verify network connectivity
4. Check database health
5. Review S3 service status

**Solutions**:
- Increase retry attempts
- Fix underlying issues (network, database, S3)
- Reduce batch size
- Enable skip on failure (if appropriate)

### Infinite Retry Loop

**Symptoms**: Same batch keeps failing

**Causes**:
- Persistent configuration error
- Data corruption
- S3 bucket issues

**Solutions**:
- Set `max_failed_batches` to stop after threshold
- Review error messages for root cause
- Fix underlying issue
- Manually skip problematic batch (if skip enabled)

### Skipped Batches Not Archived

**Symptoms**: Batches skipped but data not archived

**Behavior**: Expected when `skip_failed_batches: true`

**Solutions**:
- Review failed batch statistics
- Manually archive skipped batches
- Fix underlying issues and re-run
- Consider disabling skip for critical data

## Examples

### Example 1: Retry with Skip

```yaml
defaults:
  batch_retry_attempts: 3
  batch_retry_delay: 2.0
  skip_failed_batches: true
  max_failed_batches: 10
```

**Flow**:
1. Batch fails → Retry 3 times
2. All retries fail → Skip batch
3. Continue processing
4. After 10 skipped batches → Stop table archival

### Example 2: Strict Retry Only

```yaml
defaults:
  batch_retry_attempts: 5
  batch_retry_delay: 1.0
  skip_failed_batches: false
```

**Flow**:
1. Batch fails → Retry 5 times
2. All retries fail → Stop table archival
3. Checkpoint saved → Can resume later

### Example 3: No Retry, Skip Only

```yaml
defaults:
  batch_retry_attempts: 0
  skip_failed_batches: true
  max_failed_batches: 20
```

**Flow**:
1. Batch fails → Skip immediately
2. Continue processing
3. After 20 skipped batches → Stop table archival

## Migration

### From Previous Version

Previous versions failed entire table on batch failure. New behavior:

- **Default**: Retry 3 times, then fail (same as before)
- **New option**: Enable skip on failure for resilience
- **Statistics**: Failed batches now tracked separately

### Recommended Migration

1. **Start with defaults**: Use default retry behavior
2. **Monitor failures**: Track failed batches in statistics
3. **Adjust as needed**: Tune retry/skip based on error patterns
4. **Enable skip**: Only if appropriate for your use case

---

**Last Updated**: 2026-01-XX  
**Version**: 1.0

