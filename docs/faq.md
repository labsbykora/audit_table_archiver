# Frequently Asked Questions (FAQ)

Common questions and answers about the Audit Table Archiver.

## General Questions

### What is the Audit Table Archiver?

The Audit Table Archiver is a production-grade tool that automatically archives historical PostgreSQL audit table data to S3-compatible object storage, reclaiming disk space while maintaining data integrity and compliance requirements.

### What databases are supported?

PostgreSQL 11 and higher. The tool uses `asyncpg` for async database operations.

### What S3-compatible storage is supported?

- AWS S3
- MinIO
- DigitalOcean Spaces
- Any S3-compatible storage (via endpoint configuration)

### Is data loss possible?

No. The tool uses a multi-level verification system:
1. Count verification (DB → Memory → S3)
2. SHA-256 checksums
3. Primary key verification
4. Deletion manifests
5. Transaction-based deletes with rollback on failure

### What is the performance impact on the database?

The tool is designed to have <5% performance impact:
- Uses read replicas (optional)
- Batch processing with configurable sizes
- SKIP LOCKED for non-blocking selects
- Configurable sleep between batches
- Connection pooling

## Configuration

### How do I create a configuration file?

Use the interactive wizard:

```bash
python -m wizard.main --output config.yaml
```

Or manually create one based on `docs/examples/config-simple.yaml`.

### Can I use passwords directly in the config file?

Yes, but it's **not recommended for production**. Use `password_env` instead:

```yaml
databases:
  - name: mydb
    password_env: DB_PASSWORD  # Preferred
    # password: "secret"  # Development only
```

### How do I configure multiple databases?

See `docs/examples/config-multi-database.yaml` for an example.

### Can I archive multiple tables from the same database?

Yes, list multiple tables in the `tables` section:

```yaml
databases:
  - name: mydb
    tables:
      - name: audit_logs
        timestamp_column: created_at
        primary_key: id
      - name: user_events
        timestamp_column: event_time
        primary_key: event_id
```

## Operations

### How do I run the archiver?

```bash
# Basic run
python -m archiver.main --config config.yaml

# Dry run (no data deleted)
python -m archiver.main --config config.yaml --dry-run
```

### How do I schedule archival?

See `docs/operations-manual.md` for cron, systemd, and Kubernetes examples.

### What happens if archival is interrupted?

The tool automatically saves checkpoints and resumes from the last successful batch on the next run.

### Can multiple instances run simultaneously?

No. The tool uses distributed locking (PostgreSQL advisory locks, Redis, or file-based) to prevent concurrent runs.

### How do I monitor archival progress?

- **Health Check**: `curl http://localhost:8001/health`
- **Prometheus Metrics**: `curl http://localhost:8000/metrics`
- **Logs**: Structured JSON logs to stdout
- **Notifications**: Email, Slack, Teams (if configured)

## Data Integrity

### How is data integrity verified?

1. **Count Verification**: Records counted in DB, memory, and S3
2. **Checksums**: SHA-256 checksums for compressed and uncompressed data
3. **Primary Key Verification**: Deletion manifests track deleted primary keys
4. **Sample Verification**: Random sampling of archived records
5. **Transaction Safety**: Verify-then-delete with rollback on failure

### How do I validate archived data?

```bash
# Validate all archives
python -m validate.main --config config.yaml

# Validate specific database/table
python -m validate.main --config config.yaml --database mydb --table audit_logs
```

### What if validation fails?

1. Check logs for specific errors
2. Verify S3 connectivity
3. Check for corrupted archives
4. Restore from database backup if needed
5. Re-archive if data still exists in database

## Restore

### Why does the archiver split data into multiple batch files?

The archiver processes data in batches (configurable via `batch_size`, default: 10,000 records) for several important reasons:

1. **Memory Efficiency**: Processing all records at once would require loading potentially millions of records into memory, which could cause out-of-memory errors
2. **Transaction Safety**: Smaller transactions are safer and easier to rollback if something goes wrong
3. **Progress Tracking**: Batch processing allows for checkpoints and resumability - if the process is interrupted, it can resume from the last successful batch
4. **Error Isolation**: If one batch fails, others can still succeed, minimizing data loss risk
5. **Performance**: Smaller batches reduce database lock duration and improve overall throughput

Each batch creates a separate `.jsonl.gz` file in S3 with its own metadata and checksums for verification.

### How do I restore all batches for a table?

You can restore all batches at once using the `--restore-all` flag:

```bash
# Restore all batches for a table
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database mydb \
  --table audit_logs

# Restore batches from a specific date range
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database mydb \
  --table audit_logs \
  --start-date 2026-01-01 \
  --end-date 2026-01-31

# Dry-run to see what would be restored
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database mydb \
  --table audit_logs \
  --dry-run
```

The restore utility will:
1. List all archive files for the table (optionally filtered by date range)
2. **Automatically skip already-restored archives** (if restore watermark is enabled)
3. Restore each file sequentially
4. Provide a summary of total records restored, skipped, and failed
5. **Update restore watermark** to track what was restored

### How do I restore a single batch file?

To restore a specific batch file:

```bash
python -m restore.main \
  --config config.yaml \
  --s3-key <archive_key> \
  --database mydb \
  --table audit_logs
```

First, list available archives to find the exact S3 key:

```bash
python -m restore.main \
  --config config.yaml \
  --database mydb \
  --table audit_logs
```

See `docs/manual-restore-guide.md` for detailed procedures.

### What conflict resolution strategies are available?

- `skip`: Skip conflicting records
- `overwrite`: Overwrite existing records
- `fail`: Fail on conflicts
- `upsert`: Update existing, insert new

### Can I restore to a different database/table?

Yes, specify different database/table in restore command:

```bash
python -m restore.main \
  --config config.yaml \
  --s3-key <archive_key> \
  --database different_db \
  --table different_table
```

### How does restore watermark tracking work?

The restore utility automatically tracks which archives have been restored using a **restore watermark**:

- **First restore**: All archives are processed
- **Subsequent restores**: Only new archives (after the last restored date) are automatically skipped
- **Watermark storage**: Stored in S3 (default) or database, tracking the last restored archive date and S3 key

This enables efficient incremental restores without manually tracking what's been restored.

### Can I ignore the restore watermark?

Yes, use the `--ignore-watermark` flag to restore all archives regardless of watermark:

```bash
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --ignore-watermark
```

**Note**: Even when ignoring the watermark for filtering, the watermark is still updated at the end to track what was restored.

### How do date range restores work with watermarks?

When using date range filters (`--start-date`, `--end-date`), the watermark is **not automatically ignored**:

1. Date filtering happens first (only archives in the date range are listed)
2. Then watermark filtering is applied (skips already-restored archives within that range)

To restore a specific date range regardless of watermark:

```bash
python -m restore.main \
  --config config.yaml \
  --restore-all \
  --database production_db \
  --table audit_logs \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --ignore-watermark
```

### Where is the restore watermark stored?

By default, watermarks are stored in S3 at:
- `{prefix}/{database}/{table}/.restore_watermark.json`

You can also store them in the database (or both):

```yaml
restore_watermark:
  enabled: true
  storage_type: "both"  # "s3", "database", or "both"
```

When using database storage, a `restore_watermarks` table is created automatically.

## Performance

### How do I improve archival performance?

1. **Increase batch size**:
   ```yaml
   defaults:
     batch_size: 20000  # Increase from default 10000
   ```

2. **Decrease sleep between batches**:
   ```yaml
   defaults:
     sleep_between_batches: 1  # Decrease from default 2
   ```

3. **Use read replicas**:
   ```yaml
   databases:
     - name: mydb
       read_replica: replica.example.com
   ```

4. **Enable parallel database processing**:
   ```yaml
   defaults:
     parallel_databases: true
     max_parallel_databases: 3
   ```

See `docs/performance-tuning.md` for detailed guidance.

### What is the typical archival rate?

- **Target**: >10,000 records/minute
- **Actual**: Depends on:
  - Record size
  - Network bandwidth
  - Database performance
  - S3 upload speed

## Costs

### How do I estimate S3 storage costs?

```bash
# Estimate from data size
python -m cost.main --size-gb 100 --storage-class STANDARD_IA

# Compare all storage classes
python -m cost.main --size-gb 100 --compare
```

### Which storage class should I use?

- **STANDARD_IA**: Best for frequently accessed archives (default)
- **GLACIER**: Best for rarely accessed archives (cheaper)
- **DEEP_ARCHIVE**: Best for long-term archival (cheapest, slowest retrieval)

### What is the compression ratio?

Default is ~70% compression (0.3 ratio), meaning 100 GB uncompressed becomes ~30 GB compressed.

## Compliance

### Does the tool support legal hold?

Yes. Configure legal hold checking:

```yaml
legal_holds:
  enabled: true
  source: database  # or "api" or "config"
  table: legal_holds
```

### How are retention policies enforced?

Configure minimum/maximum retention:

```yaml
compliance:
  retention_policy:
    enabled: true
    min_retention_days: 30
    max_retention_days: 2555  # 7 years
```

### Is encryption supported?

Yes. Configure encryption:

```yaml
s3:
  encryption: SSE-S3  # or SSE-KMS
  kms_key_id: arn:aws:kms:...  # if using SSE-KMS
```

## Troubleshooting

### Archival is failing. What should I check?

1. **Check logs**: `tail -n 100 /var/log/archiver.log`
2. **Check health**: `curl http://localhost:8001/health`
3. **Test database**: `psql -h db.example.com -U archiver -d mydb`
4. **Test S3**: `aws s3 ls s3://bucket-name/`
5. **Validate config**: `python -m archiver.main --config config.yaml --dry-run`

### I'm seeing connection pool exhaustion errors.

Increase connection pool size:

```yaml
defaults:
  connection_pool_size: 20  # Increase from default 10
```

### S3 uploads are failing.

1. Check S3 credentials
2. Check network connectivity
3. Enable local fallback:
   ```yaml
   s3:
     local_fallback_dir: /var/archiver/fallback
   ```

### How do I check for orphaned files?

```bash
python -m validate.main --config config.yaml | grep -i orphaned
```

## Support

### Where can I get help?

1. Check `docs/troubleshooting.md`
2. Review `docs/operations-manual.md`
3. Check logs for error messages
4. Open an issue on GitHub

### How do I report a bug?

1. Check existing issues on GitHub
2. Create a new issue with:
   - Error logs
   - Configuration (sanitized)
   - Steps to reproduce
   - Expected vs actual behavior

### How do I request a feature?

Open a feature request on GitHub with:
- Use case description
- Expected behavior
- Benefits

