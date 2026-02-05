# Multi-Database Support Guide

This guide explains how to configure and use the archiver with multiple databases.

## Overview

The archiver supports processing multiple PostgreSQL databases in a single run. You can configure:

- **Sequential Processing** (default): Databases are processed one at a time
- **Parallel Processing** (optional): Multiple databases processed concurrently with concurrency limits

## Configuration

### Basic Multi-Database Configuration

```yaml
version: "2.0"
defaults:
  retention_days: 90
  batch_size: 10000
  parallel_databases: false  # Sequential mode (default)
  connection_pool_size: 5    # Default pool size per database

s3:
  bucket: "archives"
  prefix: "audit/"
  region: "us-east-1"

databases:
  - name: "db1"
    host: "host1.example.com"
    port: 5432
    user: "archiver"
    password_env: "DB1_PASSWORD"
    tables:
      - name: "audit_logs"
        schema: "public"
        timestamp_column: "created_at"
        primary_key: "id"

  - name: "db2"
    host: "host2.example.com"
    port: 5432
    user: "archiver"
    password_env: "DB2_PASSWORD"
    connection_pool_size: 10  # Override default for this database
    tables:
      - name: "audit_logs"
        schema: "public"
        timestamp_column: "created_at"
        primary_key: "id"
```

### Sequential Processing (Default)

Sequential processing is the default and safest mode:

- Databases are processed one at a time
- Lower resource usage (memory, connections)
- Easier to debug and monitor
- Recommended for production

```yaml
defaults:
  parallel_databases: false  # Sequential mode
```

**Benefits:**
- Predictable resource usage
- Easier troubleshooting
- Lower risk of overwhelming database servers

### Parallel Processing (Optional)

Parallel processing can speed up archival when you have multiple databases:

```yaml
defaults:
  parallel_databases: true
  max_parallel_databases: 3  # Process up to 3 databases concurrently
  connection_pool_size: 5
```

**Configuration Options:**
- `parallel_databases`: Enable parallel processing (default: `false`)
- `max_parallel_databases`: Maximum concurrent databases (default: `3`, max: `10`)

**When to Use:**
- Multiple databases on different servers
- Large number of databases to process
- Maintenance windows are tight
- Sufficient system resources (CPU, memory, network)

**Considerations:**
- Higher resource usage (memory, connections)
- More complex error handling
- May impact database servers if not careful
- Monitor system resources closely

**Best Practices:**
- Start with `max_parallel_databases: 2-3`
- Monitor database server load
- Use separate connection pools per database
- Test in staging first

## Connection Pool Configuration

Each database can have its own connection pool size:

```yaml
databases:
  - name: "high_traffic_db"
    connection_pool_size: 10  # Larger pool for busy database
    # ...
    
  - name: "low_traffic_db"
    connection_pool_size: 3   # Smaller pool for quiet database
    # ...
```

**Guidelines:**
- Default: 5 connections per database
- Range: 1-50 connections
- Larger pools for high-traffic databases
- Smaller pools for low-traffic databases
- Total connections = `connection_pool_size × number_of_databases`

## Error Isolation

The archiver provides **failure isolation** between databases:

- If one database fails, others continue processing
- Each database's errors are logged separately
- Statistics track success/failure per database

**Example Output:**
```json
{
  "databases_processed": 2,
  "databases_failed": 1,
  "database_stats": [
    {
      "database": "db1",
      "success": true,
      "tables_processed": 2,
      "records_archived": 1000
    },
    {
      "database": "db2",
      "success": false,
      "error": "Connection timeout"
    }
  ]
}
```

## Statistics

The archiver provides detailed per-database statistics:

### Global Statistics
- `databases_processed`: Number of successfully processed databases
- `databases_failed`: Number of failed databases
- `tables_processed`: Total tables processed across all databases
- `records_archived`: Total records archived
- `batches_processed`: Total batches processed

### Per-Database Statistics
Each database has its own statistics in `database_stats` array:

- `database`: Database name
- `tables_processed`: Tables successfully archived
- `tables_failed`: Tables that failed
- `records_archived`: Records archived from this database
- `batches_processed`: Batches processed for this database
- `start_time` / `end_time`: Processing time range
- `success`: Overall success status
- `error`: Error message if failed

## Running the Archiver

### Sequential Mode (Default)
```bash
python -m archiver.main --config config.yaml
```

### Parallel Mode
```bash
# Enable in config.yaml:
# defaults:
#   parallel_databases: true
python -m archiver.main --config config.yaml
```

### Filtering Databases
```bash
# Process only specific database
python -m archiver.main --config config.yaml --database db1

# Process specific table in specific database
python -m archiver.main --config config.yaml --database db1 --table audit_logs
```

## Monitoring

### Logs
Each database operation is logged with database context:

```
[info] Processing database database=db1
[info] Processing table database=db1 table=audit_logs
[info] Database archival completed database=db1 records=1000
```

### Statistics Output
After completion, statistics are logged:

```json
{
  "databases_processed": 3,
  "databases_failed": 0,
  "tables_processed": 6,
  "records_archived": 50000,
  "database_stats": [...]
}
```

## Troubleshooting

### Database Connection Failures
- Check credentials (environment variables)
- Verify network connectivity
- Check database server status
- Review connection pool size

### Parallel Processing Issues
- Reduce `max_parallel_databases` if system is overloaded
- Monitor memory usage
- Check database server load
- Consider sequential mode for stability

### Resource Exhaustion
- Reduce `connection_pool_size` per database
- Reduce `max_parallel_databases`
- Process databases in smaller batches
- Use sequential mode

## Best Practices

1. **Start Sequential**: Use sequential mode initially, switch to parallel if needed
2. **Monitor Resources**: Watch memory, CPU, and connection usage
3. **Test First**: Test multi-database configs in staging
4. **Isolate Critical**: Process critical databases separately
5. **Use Appropriate Pool Sizes**: Match pool size to database load
6. **Monitor Logs**: Check per-database statistics for issues

## Example Scenarios

### Scenario 1: Three Production Databases
```yaml
defaults:
  parallel_databases: false  # Sequential for stability
  connection_pool_size: 5

databases:
  - name: "prod_db1"  # Processed first
  - name: "prod_db2"  # Processed second
  - name: "prod_db3"  # Processed third
```

### Scenario 2: Many Development Databases
```yaml
defaults:
  parallel_databases: true
  max_parallel_databases: 5  # Process 5 at a time
  connection_pool_size: 3    # Smaller pools for dev

databases:
  - name: "dev_db1"
  - name: "dev_db2"
  # ... many more
```

### Scenario 3: Mixed Criticality
```yaml
defaults:
  parallel_databases: false  # Sequential for critical DBs

databases:
  - name: "critical_prod_db"
    connection_pool_size: 10
    tables:
      - name: "audit_logs"
        critical: true  # Extra safety checks
        
  - name: "non_critical_db"
    connection_pool_size: 5
```

## See Also

- [Quick Start Guide](quick-start.md)
- [Configuration Examples](examples/)
- [Troubleshooting Guide](troubleshooting.md)

