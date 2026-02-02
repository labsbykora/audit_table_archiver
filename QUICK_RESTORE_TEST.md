# Quick Manual Restore Test Guide

This guide helps you quickly test the manual restore functionality.

## Prerequisites

1. **Archived data exists in S3** - You should have already run the archiver
2. **PostgreSQL is running** - Database should be accessible
3. **Python dependencies installed** - `asyncpg`, `boto3` (should be installed with the project)

## Step 1: List Archived Files

List what files are available for restore:

```powershell
python scripts/list_archives.py --s3-endpoint http://localhost:9000 --s3-bucket test-archives
```

This will show you all archived files with their paths and sizes.

## Step 2: Test Restore (Dry Run)

Test the restore process without actually restoring:

```powershell
python scripts/manual_restore.py `
    --s3-endpoint http://localhost:9000 `
    --s3-bucket test-archives `
    --s3-key "archives/test_db/audit_logs/year=2026/month=01/day=04/audit_logs_20260104T052021Z_batch_001.jsonl.gz" `
    --db-host localhost `
    --db-port 5432 `
    --db-name test_db `
    --db-user archiver `
    --db-password archiver_password `
    --table audit_logs `
    --dry-run
```

**Important**: The S3 key path format uses `year=2026/month=01/day=04` (not `2026/01/04`). 
Copy the exact path from Step 1 output, including the `year=`, `month=`, `day=` format.

This will:
- Download the file from S3
- Decompress it
- Parse the JSONL
- Show what would be restored (without actually restoring)

## Step 3: Check Current Table State

Before restoring, check how many records are currently in the table:

```sql
-- Connect to PostgreSQL
psql -h localhost -U archiver -d test_db

-- Check count
SELECT COUNT(*) FROM audit_logs;
```

Or use Python:
```powershell
python -c "import asyncpg, asyncio; async def check(): conn = await asyncpg.connect(host='localhost', port=5432, database='test_db', user='archiver', password='archiver_password'); print(f'Records: {await conn.fetchval(\"SELECT COUNT(*) FROM audit_logs\")}'); await conn.close(); asyncio.run(check())"
```

## Step 4: Perform Actual Restore

Remove the `--dry-run` flag to actually restore:

```powershell
python scripts/manual_restore.py `
    --s3-endpoint http://localhost:9000 `
    --s3-bucket test-archives `
    --s3-key "archives/test_db/audit_logs/year=2026/month=01/day=04/audit_logs_20260104T052021Z_batch_001.jsonl.gz" `
    --db-host localhost `
    --db-port 5432 `
    --db-name test_db `
    --db-user archiver `
    --db-password archiver_password `
    --table audit_logs
```

**Note**: The script uses `ON CONFLICT DO NOTHING`, so if records already exist (same primary key), they will be skipped.

## Step 5: Verify Restoration

After restoring, verify the data:

```sql
-- Check count increased
SELECT COUNT(*) FROM audit_logs;

-- View some restored records
SELECT * FROM audit_logs ORDER BY created_at DESC LIMIT 10;
```

## Example Workflow

1. **Archive some data** (if not already done):
   ```powershell
   python -m archiver.main --config config.yaml --verbose
   ```

2. **List archived files**:
   ```powershell
   python scripts/list_archives.py --s3-endpoint http://localhost:9000 --s3-bucket test-archives
   ```

3. **Test restore (dry-run)**:
   ```powershell
   python scripts/manual_restore.py --s3-endpoint http://localhost:9000 --s3-bucket test-archives --s3-key "ARCHIVE_FILE_PATH" --db-host localhost --db-port 5432 --db-name test_db --db-user archiver --db-password archiver_password --table audit_logs --dry-run
   ```

4. **Perform restore** (remove `--dry-run`):
   ```powershell
   python scripts/manual_restore.py --s3-endpoint http://localhost:9000 --s3-bucket test-archives --s3-key "ARCHIVE_FILE_PATH" --db-host localhost --db-port 5432 --db-name test_db --db-user archiver --db-password archiver_password --table audit_logs
   ```

5. **Verify**:
   - Check record count increased
   - Query restored records
   - Verify data integrity

## Troubleshooting

### Error: "The specified key does not exist"
- Check the S3 key path - use the exact path from `list_archives.py`
- Path format: `archives/test_db/audit_logs/year=2026/month=01/day=04/filename.jsonl.gz`

### Error: "permission denied for table"
- Grant INSERT permission: `GRANT INSERT ON TABLE audit_logs TO archiver;`

### Error: "duplicate key value violates unique constraint"
- The script uses `ON CONFLICT DO NOTHING` by default
- If you need to update existing records, modify the script to use `ON CONFLICT DO UPDATE`

### Records not restored
- Check if records already exist (same primary keys)
- Verify the table schema matches the archived data
- Check for schema changes since archival

## Next Steps

For production use, you'll want:
- Automated restore utility (Phase 4)
- Schema migration support
- Conflict resolution strategies
- Restore validation and reporting

For now, this manual restore process works for Phase 1 MVP testing and emergency restores.

