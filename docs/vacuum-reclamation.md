# Space Reclamation with VACUUM

After archiving data from PostgreSQL tables, the database may not immediately reclaim the disk space. The archiver can automatically run `VACUUM` operations to reclaim space after archival completes.

## Important Note: Space Reclamation

**Regular VACUUM (`analyze`, `standard`) does NOT return space to the operating system.** It only marks dead tuples as reusable within PostgreSQL. The table file on disk does not shrink.

**To return space to the OS**, you must use `VACUUM FULL`, which:
- Requires an exclusive lock (blocks all operations)
- Needs a maintenance window
- Takes significantly longer
- Requires ~2x disk space temporarily

For most use cases, regular VACUUM is sufficient as PostgreSQL will reuse the space for new rows.

## Configuration

Vacuum can be configured at two levels:

### Global Default (applies to all tables)

```yaml
defaults:
  vacuum_after_archive: true      # Enable automatic vacuum after archival
  vacuum_type: analyze             # Type of vacuum to run
```

### Table-Level Override (per-table configuration)

You can override vacuum settings for specific tables:

```yaml
databases:
  - name: production_db
    tables:
      - name: audit_logs
        schema: public
        timestamp_column: created_at
        primary_key: id
        # Table-level vacuum settings (optional - overrides defaults)
        vacuum_after_archive: true  # Enable vacuum for this table
        vacuum_type: full           # Use VACUUM FULL for this large table
      
      - name: small_table
        schema: public
        timestamp_column: created_at
        primary_key: id
        vacuum_after_archive: false  # Disable vacuum for this table
        # vacuum_type not specified - uses default from defaults section
```

**Precedence**: Table-level settings override global defaults. If a table doesn't specify `vacuum_after_archive` or `vacuum_type`, it uses the values from `defaults`.

## Vacuum Types

### `none` (Disabled)
- No vacuum operation is performed
- Use when you want to manage vacuum manually or during maintenance windows

### `analyze` (Recommended - Default)
- Runs `VACUUM ANALYZE`
- **Lightweight**: Doesn't lock the table
- Updates table statistics for query planner
- Reclaims some space (less than `standard`)
- **Safe for production**: Can run during business hours

### `standard`
- Runs `VACUUM`
- **Lightweight**: Doesn't lock the table
- Reclaims more space than `analyze`
- Doesn't update statistics (use `analyze` if you need updated stats)
- **Safe for production**: Can run during business hours

### `full`
- Runs `VACUUM FULL`
- **Aggressive**: Locks the table exclusively
- Reclaims all possible space
- **Requires maintenance window**: Table is locked during operation
- **Not recommended for production** unless you have a maintenance window

## Space Tracking

The archiver tracks space reclaimed by:
1. Measuring table size before vacuum using `pg_total_relation_size()`
2. Running the vacuum operation
3. Measuring table size after vacuum
4. Calculating and logging the space reclaimed (including percentage)

Example log output:
```
[info] Space reclaimed after vacuum
  schema=public
  table=audit_logs
  total_size_reclaimed_mb=1523.45
  table_size_reclaimed_mb=1200.00
  indexes_size_reclaimed_mb=323.45
  reclaim_percentage=25.5
```

### Effectiveness Warning

If vacuum reclaims less than 10% of space on tables larger than 100MB, a warning is logged:

```
[warn] Vacuum may be ineffective - less than 10% space reclaimed
  schema=public
  table=audit_logs
  reclaim_percentage=5.2
  suggestion=Consider running VACUUM FULL during maintenance window
```

## Progress Monitoring

For PostgreSQL 12+, the archiver monitors vacuum progress using `pg_stat_progress_vacuum`:
- Logs progress every 30 seconds
- Shows phase, heap blocks scanned/total, dead tuples
- Non-blocking (doesn't affect vacuum performance)

Example progress log:
```
[info] Vacuum progress
  schema=public
  table=audit_logs
  phase=scanning heap
  heap_blocks_scanned=50000
  heap_blocks_total=100000
  heap_progress_percent=50.0
  elapsed_seconds=120.5
```

## Timeout Protection

Vacuum operations are protected by a configurable timeout (default: 2 hours):
- Prevents runaway vacuum operations
- Uses PostgreSQL's `statement_timeout`
- Logs timeout errors clearly
- Progress monitoring is cancelled on timeout

## Recommendations

### Production Environments
- **Use `analyze`**: Best balance of space reclamation and safety
- **Enable `vacuum_after_archive: true`**: Automatic space reclamation
- **Monitor vacuum duration**: If vacuum takes too long, consider scheduling during off-peak hours

### High-Volume Tables
- **Use `standard`**: Better space reclamation for large tables
- **Consider maintenance windows**: For very large tables, schedule `full` vacuum during maintenance

### Development/Testing
- **Use `none`**: Disable vacuum to speed up testing
- Or use `analyze` to test the feature without production impact

## Error Handling

Vacuum failures are **non-critical**:
- If vacuum fails, archival still succeeds
- Errors are logged as warnings
- Space tracking may be unavailable if size queries fail
- Timeout errors are clearly identified
- Progress monitoring failures are non-critical (logged as debug)

## Security

The vacuum implementation uses PostgreSQL's `quote_ident()` function to prevent SQL injection:
- All table and schema identifiers are properly escaped
- Parameterized queries where possible
- Safe identifier handling throughout

## Example Configuration

### Production (Recommended)
```yaml
defaults:
  vacuum_after_archive: true
  vacuum_type: analyze  # Safe, lightweight, updates stats
```

### High-Volume Production
```yaml
defaults:
  vacuum_after_archive: true
  vacuum_type: standard  # Better space reclamation
```

### Maintenance Window
```yaml
defaults:
  vacuum_after_archive: true
  vacuum_type: full  # Maximum space reclamation (locks table)
```

### Disabled
```yaml
defaults:
  vacuum_after_archive: false  # Manual vacuum management
  # or
  vacuum_type: none
```

## Monitoring

Monitor vacuum operations:
- Check logs for vacuum duration and space reclaimed
- Use Prometheus metrics (if enabled) to track vacuum performance
- Alert on vacuum failures (non-critical but should be investigated)

## Troubleshooting

### Vacuum Takes Too Long
- **Solution**: Use `analyze` instead of `full`
- **Solution**: Schedule vacuum during maintenance windows
- **Solution**: Disable automatic vacuum and run manually

### No Space Reclaimed
- **Normal**: Table may have been recently vacuumed
- **Check**: Verify archival actually deleted records
- **Check**: Table may have minimal bloat

### Vacuum Fails
- **Non-critical**: Archival still succeeds
- **Check**: Database user has VACUUM permissions
- **Check**: Table is not locked by other operations
- **Check**: Database connection is stable

## Permissions Required

The database user must have:
- `VACUUM` permission on the table
- `ANALYZE` permission (for `analyze` type)
- Access to `pg_stat_user_tables` (for size tracking)

## Performance Impact

- **`analyze`**: Minimal impact, safe for production
- **`standard`**: Low impact, safe for production
- **`full`**: High impact, locks table, requires maintenance window

## See Also

- [PostgreSQL VACUUM Documentation](https://www.postgresql.org/docs/current/sql-vacuum.html)
- [Performance Tuning Guide](performance-tuning.md)
- [Configuration Examples](examples/)

