# Query Optimization Guide

Best practices for optimizing database queries used by the archiver.

## Overview

The archiver uses cursor-based pagination queries that can be optimized for better performance. This guide covers index strategies, query hints, and performance tuning.

## Index Strategy

### Required Indexes

For optimal performance, ensure these indexes exist:

#### 1. Timestamp Column Index (Critical)

```sql
-- Single column index on timestamp
CREATE INDEX idx_<table>_<timestamp_column>
ON <schema>.<table>(<timestamp_column>);

-- Example
CREATE INDEX idx_audit_logs_created_at
ON public.audit_logs(created_at);
```

**Why**: The WHERE clause filters by timestamp, so this index is essential.

#### 2. Composite Index (Recommended)

```sql
-- Composite index on (timestamp, primary_key)
CREATE INDEX idx_<table>_<timestamp_column>_<primary_key>
ON <schema>.<table>(<timestamp_column>, <primary_key>);

-- Example
CREATE INDEX idx_audit_logs_created_at_id
ON public.audit_logs(created_at, id);
```

**Why**: 
- Supports WHERE clause (timestamp)
- Supports ORDER BY clause (timestamp, primary_key)
- Covers both conditions in single index

#### 3. Primary Key Index (Usually Exists)

```sql
-- Primary key index (usually already exists)
-- Verify it exists:
SELECT * FROM pg_indexes 
WHERE tablename = '<table>' 
  AND indexname LIKE '%<primary_key>%';
```

### Index Types

- **B-tree (default)**: Best for equality and range queries (recommended)
- **BRIN**: For very large tables with natural ordering (timestamp columns)
- **Partial Index**: For filtered queries (e.g., only old records)

### Index Maintenance

```sql
-- Analyze table to update statistics
ANALYZE <schema>.<table>;

-- Reindex if needed (for bloat)
REINDEX INDEX idx_<table>_<timestamp_column>;

-- Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename = '<table>';
```

## Query Patterns

### Batch Selection Query

The archiver uses this query pattern:

```sql
SELECT *
FROM schema.table
WHERE timestamp_column < $1
ORDER BY timestamp_column, primary_key
LIMIT $2
FOR UPDATE SKIP LOCKED;
```

### Optimization Tips

1. **Use Index Scan**
   - Ensure composite index exists
   - PostgreSQL will use it automatically if available

2. **Avoid Sequential Scan**
   - If EXPLAIN shows "Seq Scan", add missing index
   - Sequential scans are slow for large tables

3. **Monitor Query Plans**
   ```sql
   EXPLAIN ANALYZE
   SELECT * FROM <table>
   WHERE <timestamp_column> < '<cutoff_date>'
   ORDER BY <timestamp_column>, <primary_key>
   LIMIT 10000
   FOR UPDATE SKIP LOCKED;
   ```

## Performance Tuning

### Query Performance Targets

- **Query Time**: < 2 seconds per batch (10,000 records)
- **Index Scan**: Preferred over sequential scan
- **Rows Examined**: Should match LIMIT (no over-fetching)

### Slow Query Diagnosis

1. **Check Query Plan**
   ```sql
   EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
   SELECT * FROM <table>
   WHERE <timestamp_column> < '<cutoff_date>'
   ORDER BY <timestamp_column>, <primary_key>
   LIMIT 10000;
   ```

2. **Identify Bottlenecks**
   - Look for "Seq Scan" → Add index
   - Look for "Sort" → Index should cover ORDER BY
   - Look for high "actual time" → Optimize query or reduce batch size

3. **Check Table Bloat**
   ```sql
   SELECT 
     schemaname,
     tablename,
     pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
     n_dead_tup,
     n_live_tup,
     round(n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct
   FROM pg_stat_user_tables
   WHERE tablename = '<table>';
   ```

4. **Vacuum if Needed**
   ```sql
   -- Analyze first
   ANALYZE <schema>.<table>;
   
   -- Vacuum if high bloat
   VACUUM ANALYZE <schema>.<table>;
   ```

## Configuration Tuning

### Batch Size Optimization

Adjust batch size based on query performance:

```yaml
defaults:
  batch_size: 10000  # Default

# If queries are slow (> 5 seconds)
defaults:
  batch_size: 5000  # Reduce

# If queries are fast (< 1 second) and memory allows
defaults:
  batch_size: 20000  # Increase
```

### Connection Pool Tuning

```yaml
defaults:
  connection_pool_size: 5  # Default

# For high-traffic databases
databases:
  - name: production_db
    connection_pool_size: 10  # Increase
```

## Monitoring Query Performance

### Enable Query Logging

```sql
-- Enable slow query logging
ALTER DATABASE <database> SET log_min_duration_statement = 2000;  -- Log queries > 2s
```

### Monitor Active Queries

```sql
-- Check running queries
SELECT 
    pid,
    now() - query_start AS duration,
    state,
    query
FROM pg_stat_activity
WHERE application_name = 'audit_archiver'
  AND state != 'idle'
ORDER BY query_start;
```

### Track Index Usage

```sql
-- Monitor index usage over time
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename = '<table>'
ORDER BY idx_scan DESC;
```

## Common Issues and Solutions

### Issue: Sequential Scan on Large Table

**Symptoms**: Query plan shows "Seq Scan", slow performance

**Solution**:
```sql
-- Create composite index
CREATE INDEX idx_<table>_<timestamp_column>_<primary_key>
ON <schema>.<table>(<timestamp_column>, <primary_key>);

-- Analyze table
ANALYZE <schema>.<table>;
```

### Issue: High Query Time

**Symptoms**: Query takes > 5 seconds

**Solutions**:
1. Reduce batch size
2. Add/optimize indexes
3. Use read replica
4. Vacuum table to reduce bloat

### Issue: Index Not Used

**Symptoms**: Index exists but query plan shows sequential scan

**Solutions**:
1. Update statistics: `ANALYZE <table>;`
2. Check index is on correct columns
3. Verify query matches index columns
4. Consider partial index if filtering

### Issue: Lock Contention

**Symptoms**: Queries waiting on locks

**Solutions**:
1. Use `FOR UPDATE SKIP LOCKED` (already used)
2. Reduce batch size
3. Schedule during off-peak hours
4. Check for blocking queries

## Best Practices

1. **Always Create Indexes Before Archival**
   - Create indexes before first archival run
   - Monitor index creation time for large tables

2. **Regular Maintenance**
   - Run `ANALYZE` regularly (PostgreSQL auto-analyze usually sufficient)
   - Monitor table bloat
   - Reindex if needed

3. **Monitor Performance**
   - Track query execution times
   - Monitor index usage
   - Review query plans periodically

4. **Test Before Production**
   - Use `EXPLAIN ANALYZE` on production-like data
   - Test with actual batch sizes
   - Verify indexes are used

5. **Document Indexes**
   - Document all indexes created
   - Note index creation time
   - Track index maintenance schedule

## Example: Complete Index Setup

```sql
-- 1. Create timestamp index
CREATE INDEX idx_audit_logs_created_at
ON public.audit_logs(created_at);

-- 2. Create composite index (recommended)
CREATE INDEX idx_audit_logs_created_at_id
ON public.audit_logs(created_at, id);

-- 3. Analyze table
ANALYZE public.audit_logs;

-- 4. Verify indexes
SELECT 
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'audit_logs';

-- 5. Test query plan
EXPLAIN ANALYZE
SELECT * FROM public.audit_logs
WHERE created_at < '2024-01-01'
ORDER BY created_at, id
LIMIT 10000
FOR UPDATE SKIP LOCKED;
```

## Performance Benchmarks

### Expected Performance (with proper indexes)

**Table**: 10M rows, indexed timestamp column

| Batch Size | Query Time | Throughput |
|------------|------------|------------|
| 5,000      | 0.5-1.0s   | 5,000-10,000 rows/s |
| 10,000     | 1.0-2.0s   | 5,000-10,000 rows/s |
| 20,000     | 2.0-4.0s   | 5,000-10,000 rows/s |

**Note**: Actual performance depends on:
- Row size
- Index quality
- Database load
- Hardware resources

---

**Last Updated**: 2026-01-XX  
**Version**: 1.0

