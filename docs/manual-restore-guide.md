# Restore Guide

This guide explains how to restore archived data from S3 back to PostgreSQL using the automated restore utility.

## Prerequisites

1. **Access to S3/MinIO** (credentials configured via environment variables or config file)
2. **PostgreSQL access** (credentials and permissions)
3. **Python 3.9+** with required packages
4. **Archiver configuration file** (`config.yaml`)

## Why Batches?

The archiver splits data into batches (configurable via `batch_size`, default: 10,000 records) for:
- **Memory Efficiency**: Avoids loading millions of records at once
- **Transaction Safety**: Smaller transactions are easier to rollback
- **Progress Tracking**: Checkpoints allow resuming from the last successful batch
- **Error Isolation**: One failed batch doesn't affect others
- **Performance**: Reduces database lock duration

Each batch creates a separate `.jsonl.gz` file in S3 with its own metadata and checksums.

## Restore Methods

### Method 1: Restore All Batches (Recommended)

The easiest way to restore is to restore all batches for a table at once:

```bash
# Restore ALL batches for a table
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --conflict-strategy skip

# Restore batches from a specific date range
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --conflict-strategy skip

# Dry-run to preview what would be restored
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --dry-run
```

**Windows PowerShell:**
```powershell
python -m restore.main `
  --config config.yaml `
  --restore-all `
  --database production_db `
  --table audit_logs `
  --conflict-strategy skip
```

The restore utility will:
1. List all archive files for the table (optionally filtered by date range)
2. **Automatically skip already-restored archives** (if restore watermark is enabled)
3. Restore each file sequentially
4. Provide a summary of total records restored, skipped, and failed
5. Continue with remaining files if one fails (error isolation)
6. **Update restore watermark** to track what was restored

### Method 2: Restore Single Archive File

To restore a specific batch file:

```bash
# First, list available archives
python -m restore.main \
  --config config.yaml \
  --database production_db \
  --table audit_logs

# Then restore a specific file
python -m restore.main \
  --config config.yaml \
  --s3-key archives/db/table/year=2026/month=01/day=15/file.jsonl.gz \
  --database production_db \
  --table audit_logs \
  --conflict-strategy skip
```

### Method 3: Manual Restore (Advanced)

For advanced use cases, you can manually download and restore files:

If you prefer using PostgreSQL's COPY command directly:

## Advanced: Manual Restore Process

If you need to manually restore files (e.g., for debugging or special cases):

### Step 1: List Archived Files

```bash
# For MinIO (local)
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

# List all archived files
aws --endpoint-url http://localhost:9000 \
    s3 ls s3://test-archives/archives/ --recursive

# List files for a specific table
aws --endpoint-url http://localhost:9000 \
    s3 ls s3://test-archives/archives/test_db/audit_logs/ --recursive
```

**Windows PowerShell:**
```powershell
$env:AWS_ACCESS_KEY_ID="minioadmin"
$env:AWS_SECRET_ACCESS_KEY="minioadmin"

aws --endpoint-url http://localhost:9000 s3 ls s3://test-archives/archives/ --recursive
```

### Step 2: Download Archived File

```bash
# Download a specific file
aws --endpoint-url http://localhost:9000 \
    s3 cp s3://test-archives/archives/test_db/audit_logs/year=2026/month=01/day=04/audit_logs_20260104T052021Z_batch_001.jsonl.gz \
    ./restore_data.jsonl.gz
```

**Windows PowerShell:**
```powershell
aws --endpoint-url http://localhost:9000 `
    s3 cp s3://test-archives/archives/test_db/audit_logs/year=2026/month=01/day=04/audit_logs_20260104T052021Z_batch_001.jsonl.gz `
    .\restore_data.jsonl.gz
```

### Step 3: Decompress and Inspect

```bash
# Decompress
gunzip restore_data.jsonl.gz
# Or: gzip -d restore_data.jsonl.gz

# View first few records
head -n 5 restore_data.jsonl | python -m json.tool

# Count records
wc -l restore_data.jsonl
```

**Windows PowerShell:**
```powershell
# PowerShell doesn't have native gunzip, use Python
python -c "import gzip; open('restore_data.jsonl', 'wb').write(gzip.open('restore_data.jsonl.gz', 'rb').read())"

# View first record
Get-Content restore_data.jsonl -TotalCount 1 | python -m json.tool

# Count records
(Get-Content restore_data.jsonl).Count
```

### Step 4: Restore Using PostgreSQL COPY

#### 4.1: Convert JSONL to CSV (if needed)

Create a Python script to convert JSONL to CSV:

```python
import json
import csv
import gzip

# Read JSONL and convert to CSV
with gzip.open('restore_data.jsonl.gz', 'rt') as f_in:
    records = [json.loads(line) for line in f_in]
    
# Remove metadata columns
metadata_cols = {'_archived_at', '_batch_id', '_source_database', '_source_table'}
if records:
    data_cols = [col for col in records[0].keys() if col not in metadata_cols]
    
    with open('restore_data.csv', 'w', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=data_cols)
        writer.writeheader()
        for record in records:
            writer.writerow({col: record.get(col) for col in data_cols})
```

#### 4.2: Use PostgreSQL COPY

```sql
-- Connect to database
\c test_db

-- Create temporary table (if needed)
CREATE TEMP TABLE temp_restore AS SELECT * FROM audit_logs LIMIT 0;

-- Copy data
\copy temp_restore FROM 'restore_data.csv' WITH CSV HEADER

-- Verify
SELECT COUNT(*) FROM temp_restore;

-- Insert into actual table (handling conflicts)
INSERT INTO audit_logs
SELECT * FROM temp_restore
ON CONFLICT (id) DO NOTHING;

-- Or use UPDATE on conflict
INSERT INTO audit_logs
SELECT * FROM temp_restore
ON CONFLICT (id) DO UPDATE SET
    user_id = EXCLUDED.user_id,
    action = EXCLUDED.action,
    metadata = EXCLUDED.metadata,
    created_at = EXCLUDED.created_at;
```

## Restore Watermark Tracking

The restore utility automatically tracks which archives have been restored using a **restore watermark** system. This enables efficient incremental restores by skipping archives that have already been restored.

### How It Works

1. **First Restore**: All archives are processed
2. **Subsequent Restores**: Only new archives (after the last restored date) are processed
3. **Watermark Storage**: Stored in S3 (default) or database, tracking:
   - Last restored archive date
   - Last restored S3 key
   - Total archives restored count

### Configuration

Restore watermark is **enabled by default**. Configure in `config.yaml`:

```yaml
restore_watermark:
  enabled: true                    # Enable/disable watermark tracking
  storage_type: "s3"               # "s3", "database", or "both"
  update_after_each_archive: true  # Update after each archive or only at end
```

### Ignoring Watermark

To restore all archives regardless of watermark (e.g., for re-restore or testing):

```bash
# Restore all archives, ignoring watermark
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --ignore-watermark
```

**Note**: Even when ignoring the watermark for filtering, the watermark is still updated at the end to track what was restored.

### Date Range Restores

When using date range filters (`--start-date`, `--end-date`), the watermark is **not automatically ignored**:

- Date filtering happens first (only archives in the date range are listed)
- Then watermark filtering is applied (skips already-restored archives within that range)

To restore a specific date range regardless of watermark:

```bash
# Restore date range, ignoring watermark
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --ignore-watermark
```

### Watermark Location

Watermarks are stored at:
- **S3**: `{prefix}/{database}/{table}/.restore_watermark.json`
- **Database**: `restore_watermarks` table (created automatically)

## Restore Options

### Conflict Resolution Strategies

The restore utility supports multiple conflict resolution strategies:

- **`skip`** (default): Skip conflicting records (ON CONFLICT DO NOTHING)
- **`overwrite`**: Overwrite existing records (ON CONFLICT DO UPDATE)
- **`fail`**: Fail on conflicts (no ON CONFLICT clause)
- **`upsert`**: Update existing, insert new (ON CONFLICT DO UPDATE with all columns)

```bash
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --conflict-strategy overwrite
```

### Schema Migration

The restore utility can handle schema changes:

- **`strict`**: Fail if schemas don't match exactly
- **`lenient`**: Add missing columns with defaults, remove extra columns
- **`transform`**: Attempt type conversions for changed columns
- **`none`**: No schema migration (assumes schemas match)

```bash
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --schema-migration-strategy lenient
```

### Performance Options

```bash
# Drop indexes before restore (faster, but requires reindexing)
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --drop-indexes

# Commit every N batches (default: 1)
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --commit-frequency 10
```

## Verify Restoration

Verify that data was restored correctly:

```sql
-- Check record count
SELECT COUNT(*) FROM audit_logs;

-- Check date range
SELECT MIN(created_at), MAX(created_at) FROM audit_logs;

-- Sample records
SELECT * FROM audit_logs ORDER BY created_at DESC LIMIT 10;
```

## File Format

Archived files are in **JSONL** (JSON Lines) format, compressed with **gzip**:

- **Format**: `.jsonl.gz`
- **Structure**: One JSON object per line
- **Metadata columns**: `_archived_at`, `_batch_id`, `_source_database`, `_source_table` (can be ignored during restore)
- **Data columns**: All original table columns

Example record:
```json
{
  "id": 1,
  "user_id": 123,
  "action": "login",
  "metadata": {"ip": "192.168.1.1"},
  "created_at": "2025-01-01T00:00:00Z",
  "_archived_at": "2026-01-04T05:20:21Z",
  "_batch_id": "test_db_audit_logs_abc123",
  "_source_database": "test_db",
  "_source_table": "audit_logs"
}
```

## Restore Summary

After restoring, the utility provides a summary:

```
Restore Summary:
  Files processed: 1035
  Files failed: 0
  Records restored: 1,035,000
  Records skipped: 0
  Records failed: 0
```

## Best Practices

1. **Always use `--dry-run` first** to preview what will be restored
2. **Use `--restore-all` for bulk restores** instead of restoring files individually
3. **Filter by date range** when restoring large datasets to avoid restoring unnecessary data
4. **Test in development first** before restoring to production
5. **Backup your database** before restoring (especially if using `overwrite` strategy)
6. **Monitor restore progress** using verbose logging (`--verbose`)
7. **Validate restored data** using the validation utility after restore

## Troubleshooting

### Error: "relation does not exist"
- Ensure the table exists in the target database
- Check schema name (default is `public`)

### Error: "column does not exist"
- Verify table schema matches archived data
- Check for schema changes since archival

### Error: "duplicate key value violates unique constraint"
- Use `ON CONFLICT DO NOTHING` to skip duplicates
- Or use `ON CONFLICT DO UPDATE` to update existing records

### Error: "permission denied"
- Ensure database user has INSERT permission
- Check table permissions: `GRANT INSERT ON TABLE audit_logs TO archiver;`

## Next Steps

For production use, consider:
1. Creating a backup before restore (if restoring to production)
2. Testing restore process in a development environment first
3. Documenting restore procedures for your team
4. Waiting for Phase 4 automated restore utility

