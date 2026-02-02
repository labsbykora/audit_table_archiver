# Performance Tuning Guide

This guide provides recommendations for optimizing the audit table archiver performance.

## Table of Contents

1. [Batch Size Optimization](#batch-size-optimization)
2. [Database Configuration](#database-configuration)
3. [Connection Pool Tuning](#connection-pool-tuning)
4. [S3 Upload Optimization](#s3-upload-optimization)
5. [Query Performance](#query-performance)
6. [Memory Management](#memory-management)
7. [Parallel Processing](#parallel-processing)
8. [Monitoring and Profiling](#monitoring-and-profiling)

## Batch Size Optimization

### Finding the Optimal Batch Size

The optimal batch size depends on:
- **Row size**: Larger rows = smaller batches
- **Network speed**: Faster network = larger batches
- **Database load**: Higher load = smaller batches
- **Memory available**: More memory = larger batches

### Recommended Starting Points

```yaml
defaults:
  batch_size: 10000  # Good starting point for most cases
```

**Adjust based on your data**:
- **Small rows** (< 500 bytes): Start with 20,000-50,000
- **Medium rows** (500-2KB): Start with 10,000-20,000
- **Large rows** (> 2KB): Start with 5,000-10,000

### Adaptive Batch Sizing

The archiver can automatically adjust batch size based on query performance:

```python
from utils.adaptive_batch import AdaptiveBatchSizer

sizer = AdaptiveBatchSizer(
    initial_batch_size=10000,
    min_batch_size=1000,
    max_batch_size=50000,
    target_query_time=2.0,  # Target 2 seconds per query
)
```

## Database Configuration

### Indexes

**Critical**: Ensure indexes exist on:
1. **Timestamp column** (for WHERE clause)
2. **Primary key** (for ORDER BY and DELETE)

```sql
-- Example: Create index on timestamp column
CREATE INDEX idx_audit_logs_created_at 
ON audit_logs(created_at);

-- Composite index for better performance
CREATE INDEX idx_audit_logs_created_at_id 
ON audit_logs(created_at, id);
```

### Connection Pool Size

```yaml
defaults:
  connection_pool_size: 5  # Default per database

databases:
  - name: production_db
    connection_pool_size: 10  # Override for high-traffic database
```

**Guidelines**:
- **Low traffic**: 3-5 connections
- **Medium traffic**: 5-10 connections
- **High traffic**: 10-20 connections
- **Very high traffic**: Consider read replicas

### Read Replicas

For high-traffic databases, use read replicas for queries:

```yaml
databases:
  - name: production_db
    host: primary.example.com
    read_replica: replica.example.com  # Use replica for SELECT queries
```

## S3 Upload Optimization

### Multipart Upload Threshold

For large files, enable multipart uploads:

```yaml
s3:
  multipart_threshold_mb: 10  # Use multipart for files > 10MB
```

### Retry Configuration

The archiver uses exponential backoff with jitter for retries:

- **Initial delay**: 1 second
- **Max delay**: 30 seconds
- **Exponential base**: 2.0
- **Jitter**: Random 0-10% added to prevent thundering herd

### Circuit Breaker

Circuit breaker prevents cascading failures:
- **Failure threshold**: 5 consecutive failures
- **Recovery timeout**: 60 seconds
- Automatically opens circuit on repeated failures

## Query Performance

### Query Optimization Tips

1. **Use EXPLAIN ANALYZE** to understand query plans:
   ```sql
   EXPLAIN ANALYZE
   SELECT * FROM audit_logs
   WHERE created_at < '2024-01-01'
   ORDER BY created_at, id
   LIMIT 10000
   FOR UPDATE SKIP LOCKED;
   ```

2. **Monitor query execution time**:
   - Target: < 2 seconds per batch query
   - If > 5 seconds: Reduce batch size or add indexes

3. **Check for table bloat**:
   ```sql
   SELECT 
     schemaname,
     tablename,
     pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
   FROM pg_tables
   WHERE tablename = 'audit_logs';
   ```

### Vacuum Strategy

Run VACUUM after archival to reclaim space:

```yaml
defaults:
  vacuum_after_archival: true  # Run VACUUM after each table
  vacuum_mode: ANALYZE  # ANALYZE, FULL, or none
```

**Recommendations**:
- **ANALYZE**: Quick, updates statistics (recommended)
- **FULL**: Thorough, reclaims space (use periodically)
- **None**: Skip vacuum (not recommended)

## Memory Management

### Batch Size vs Memory

Estimate memory usage:
```
Memory per batch = batch_size × average_row_size × 2
```

**Example**:
- 10,000 rows × 1KB/row × 2 = ~20MB per batch
- With 10 concurrent batches = ~200MB

### Memory Limits

Set appropriate batch sizes to avoid OOM:

```yaml
defaults:
  batch_size: 5000  # Reduce if memory constrained
```

## Parallel Processing

### Parallel Database Processing

Enable parallel processing for multiple databases:

```yaml
defaults:
  parallel_databases: true
  max_parallel_databases: 3  # Process 3 databases concurrently
```

**Guidelines**:
- **CPU cores**: Set `max_parallel_databases` to number of cores
- **Memory**: Each database uses its own connection pool
- **Network**: Consider S3 rate limits

### When to Use Parallel Processing

**Use parallel processing when**:
- ✅ Multiple databases with independent tables
- ✅ Sufficient CPU and memory resources
- ✅ Network bandwidth available

**Avoid parallel processing when**:
- ❌ Single database with many tables (use sequential)
- ❌ Limited memory or CPU
- ❌ S3 rate limiting issues

## Monitoring and Profiling

### Performance Metrics

Monitor these metrics:
- **Query time**: Time to fetch batch
- **Serialization time**: Time to convert to JSONL
- **Compression time**: Time to compress
- **Upload time**: Time to upload to S3
- **Delete time**: Time to delete from database
- **Total batch time**: End-to-end time per batch

### Profiling

Use Python profiling to identify bottlenecks:

```bash
# Profile the archiver
python -m cProfile -o profile.stats -m archiver.main --config config.yaml

# Analyze results
python -m pstats profile.stats
```

### Logging

Enable debug logging to see timing:

```bash
python -m archiver.main --config config.yaml --log-level DEBUG
```

Look for log entries like:
```
[INFO] Batch processed successfully database=prod_db table=audit_logs batch=1 records=10000
```

## Performance Benchmarks

### Expected Performance

**Baseline** (1M rows, 500 bytes/row, PostgreSQL 14, 4-core, 8GB RAM):

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

### Optimization Opportunities

1. **Parallel uploads**: 30% faster (upload while fetching next batch)
2. **Read replica**: 10-20% faster (offload queries from primary)
3. **Faster compression**: 15% faster (use pigz for parallel gzip)
4. **Batch size tuning**: 10-30% faster (find sweet spot per table)
5. **SSD storage**: 20% faster (faster vacuum operations)

## Troubleshooting Slow Performance

### Slow Queries

**Symptoms**: Query time > 5 seconds

**Solutions**:
1. Add index on timestamp column
2. Reduce batch size
3. Use read replica
4. Check for table bloat (run VACUUM)

### Slow Uploads

**Symptoms**: Upload time > 30 seconds per batch

**Solutions**:
1. Check network bandwidth
2. Verify S3 region (use same region as database)
3. Enable multipart uploads for large files
4. Check S3 rate limits

### High Memory Usage

**Symptoms**: Process using > 1GB memory

**Solutions**:
1. Reduce batch size
2. Process fewer tables concurrently
3. Monitor memory usage and adjust

### Database Impact

**Symptoms**: Database CPU > 10% during archival

**Solutions**:
1. Use read replica for queries
2. Reduce batch size
3. Schedule archival during off-peak hours
4. Increase connection pool size

## Best Practices

1. **Start small**: Begin with default batch size (10,000) and adjust
2. **Monitor first run**: Watch performance metrics on first archival
3. **Tune per table**: Different tables may need different batch sizes
4. **Use read replicas**: For production databases with high traffic
5. **Schedule wisely**: Run during off-peak hours when possible
6. **Monitor continuously**: Track performance over time
7. **Test changes**: Use dry-run mode to test configuration changes

## Configuration Examples

### High-Performance Configuration

```yaml
defaults:
  batch_size: 20000
  connection_pool_size: 10
  parallel_databases: true
  max_parallel_databases: 4

s3:
  multipart_threshold_mb: 5  # Use multipart for smaller files
```

### Memory-Constrained Configuration

```yaml
defaults:
  batch_size: 5000
  connection_pool_size: 3
  parallel_databases: false  # Sequential processing

s3:
  multipart_threshold_mb: 20  # Larger threshold
```

### Network-Constrained Configuration

```yaml
defaults:
  batch_size: 10000
  connection_pool_size: 5
  parallel_databases: false  # Avoid parallel uploads

s3:
  multipart_threshold_mb: 10
```

