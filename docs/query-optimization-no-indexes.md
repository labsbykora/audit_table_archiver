# Query Optimization Without Indexes

**Goal**: Optimize archival queries without adding indexes (to save storage space).

## Quick Start

### 1. Reduce Batch Size (Biggest Impact: 30-50% faster)

```yaml
defaults:
  batch_size: 5000  # Instead of 10000
```

**Why it works**: Smaller batches = less data to scan = faster queries.

**Trade-off**: More batches = more overhead, but each batch is faster.

### 2. Increase PostgreSQL work_mem (20-40% faster)

```sql
-- Set per-connection (or in postgresql.conf)
SET work_mem = '256MB';  -- Default is 4MB
```

**Why it works**: More memory for sorting/aggregation = faster ORDER BY.

**Note**: This is per-query memory. Total usage = work_mem × concurrent queries.

### 3. Enable Parallel Scans (2-4x faster on multi-core)

```ini
# In postgresql.conf
max_parallel_workers_per_gather = 4
min_parallel_table_scan_size = 8MB
```

**Why it works**: PostgreSQL can parallelize sequential scans across CPU cores.

**Requirements**: 
- Multi-core CPU
- Table > 8MB
- Query doesn't use `FOR UPDATE` (but `FOR UPDATE SKIP LOCKED` works)

## Complete Optimization Checklist

### Application-Level (Config File)

✅ **Reduce batch size**
```yaml
defaults:
  batch_size: 5000  # Start here, adjust based on performance
```

✅ **Enable async upload pipeline**
```yaml
defaults:
  async_upload_pipeline: true
  max_concurrent_uploads: 2
```

✅ **Narrow retention window** (if possible)
```yaml
defaults:
  retention_days: 30  # Instead of 90 (if business allows)
```

### Database-Level (PostgreSQL Configuration)

✅ **Increase work_mem**
```sql
-- Per-connection
SET work_mem = '256MB';

-- Or in postgresql.conf (affects all connections)
work_mem = 256MB
```

✅ **Enable parallel scans**
```ini
max_parallel_workers_per_gather = 4
min_parallel_table_scan_size = 8MB
parallel_tuple_cost = 0.1
```

✅ **Increase shared_buffers**
```ini
shared_buffers = '2GB'  # 25% of total RAM for dedicated DB server
```

✅ **Enable autovacuum**
```ini
autovacuum = on
autovacuum_analyze_scale_factor = 0.05
autovacuum_analyze_threshold = 50
```

### Maintenance Tasks

✅ **Run VACUUM ANALYZE regularly**
```sql
-- Updates statistics (helps query planner)
VACUUM ANALYZE public.auditlog_log_line;

-- Or schedule via cron:
0 2 * * * psql -d your_db -c "VACUUM ANALYZE public.auditlog_log_line;"
```

✅ **Reduce table bloat** (during maintenance windows)
```sql
-- Check bloat first
SELECT 
    pg_size_pretty(pg_total_relation_size('public.auditlog_log_line')) AS total_size,
    pg_size_pretty(pg_relation_size('public.auditlog_log_line')) AS table_size;

-- Reclaim space (locks table - use during maintenance)
VACUUM FULL public.auditlog_log_line;
```

✅ **Schedule archival during off-peak hours**
```bash
# Run at 2 AM daily
0 2 * * * /usr/bin/archiver --config /etc/archiver/config.yaml
```

## Performance Impact Summary

| Optimization | Impact | Effort | Storage Cost |
|-------------|--------|--------|--------------|
| Reduce batch size | 30-50% faster | Low | None |
| Increase work_mem | 20-40% faster | Low | None |
| Enable parallel scans | 2-4x faster | Medium | None |
| VACUUM ANALYZE | 10-30% faster | Low | None |
| Reduce table bloat | 20-50% faster | Medium | None |
| Increase shared_buffers | 10-30% faster | Low | None |
| Narrow retention window | 30-60% faster | Low | None |
| Async upload pipeline | 20-30% faster | Low | None |

**Combined impact**: 2-5x faster without adding indexes!

## Example Configuration

### Optimized config.yaml (no indexes)

```yaml
version: "2.0"

s3:
  bucket: your-bucket
  prefix: archives/
  region: us-east-1

defaults:
  # Smaller batches for faster queries
  batch_size: 5000
  
  # Performance optimizations
  query_plan_analysis: true
  slow_query_threshold: 5.0
  warn_on_seq_scan: true
  async_upload_pipeline: true
  max_concurrent_uploads: 2
  
  # Batch error recovery
  batch_retry_attempts: 3
  batch_retry_delay: 2.0
  skip_failed_batches: false
  max_failed_batches: 10

databases:
  - name: production_db
    host: db.example.com
    port: 5432
    user: archiver
    password_env: DB_PASSWORD
    connection_pool_size: 3  # Fewer connections = less overhead
    tables:
      - name: auditlog_log_line
        schema: public
        timestamp_column: create_date
        primary_key: id
        retention_days: 30  # Smaller window = faster queries
        batch_size: 5000    # Override default if needed
```

### PostgreSQL Configuration (postgresql.conf)

```ini
# Memory settings
shared_buffers = 2GB              # 25% of RAM
work_mem = 256MB                  # Per-query memory
effective_cache_size = 6GB        # Estimate of available cache

# Parallel query settings
max_parallel_workers_per_gather = 4
min_parallel_table_scan_size = 8MB
parallel_tuple_cost = 0.1

# Autovacuum settings
autovacuum = on
autovacuum_analyze_scale_factor = 0.05
autovacuum_analyze_threshold = 50

# Connection settings
max_connections = 100
statement_timeout = 30s
idle_in_transaction_session_timeout = 5min
```

## Monitoring Performance

### Check Query Performance

```sql
-- See slow queries
SELECT 
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    max_exec_time
FROM pg_stat_statements
WHERE query LIKE '%auditlog_log_line%'
ORDER BY mean_exec_time DESC
LIMIT 10;
```

### Check Table Bloat

```sql
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                   pg_relation_size(schemaname||'.'||tablename)) AS bloat_size
FROM pg_tables
WHERE tablename = 'auditlog_log_line';
```

### Check Parallel Scan Usage

```sql
-- See if parallel scans are being used
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT * FROM public.auditlog_log_line
WHERE create_date < NOW() - INTERVAL '90 days'
ORDER BY create_date, id
LIMIT 10000
FOR UPDATE SKIP LOCKED;
```

Look for "Parallel Seq Scan" in the output.

## Troubleshooting

### Queries Still Slow After Optimizations

1. **Check if parallel scans are enabled**:
   ```sql
   SHOW max_parallel_workers_per_gather;  -- Should be > 0
   ```

2. **Check table size** (parallel scans need large tables):
   ```sql
   SELECT pg_size_pretty(pg_relation_size('public.auditlog_log_line'));
   -- Needs to be > 8MB for parallel scans
   ```

3. **Check for table bloat**:
   ```sql
   -- If bloat is > 30% of table size, run VACUUM FULL
   ```

4. **Reduce batch size further**:
   ```yaml
   batch_size: 3000  # Or even 2000
   ```

5. **Consider read replica** (if available):
   ```yaml
   databases:
     - name: production_db
       read_replica: replica.example.com  # Use replica for queries
   ```

### Memory Issues

If you see "out of memory" errors:

1. **Reduce work_mem**:
   ```sql
   SET work_mem = '128MB';  -- Instead of 256MB
   ```

2. **Reduce batch size**:
   ```yaml
   batch_size: 3000  # Or lower
   ```

3. **Reduce parallel workers**:
   ```ini
   max_parallel_workers_per_gather = 2  # Instead of 4
   ```

## When to Consider Indexes

**Consider adding indexes if**:
- ✅ All non-index optimizations are applied
- ✅ Queries are still > 5 seconds
- ✅ Storage cost is acceptable
- ✅ Table is frequently queried (not just archived)

**Index storage cost**:
- Single-column index: ~10-20% of table size
- Composite index: ~15-30% of table size

**Example**: 100GB table → 10-30GB index (one-time cost, but permanent)

## Summary

**Best practices for no-index optimization**:

1. ✅ Start with `batch_size: 5000`
2. ✅ Set `work_mem = 256MB` in PostgreSQL
3. ✅ Enable parallel scans (`max_parallel_workers_per_gather = 4`)
4. ✅ Run `VACUUM ANALYZE` regularly
5. ✅ Enable async upload pipeline
6. ✅ Schedule during off-peak hours

**Expected results**:
- Query time: 2-5 seconds per batch (down from 10-20s)
- Processing rate: 500-1000 records/second
- Storage cost: Zero (no indexes)

**If still slow**: Consider adding a single composite index as a last resort.

