# Runbooks

Operational runbooks for common tasks and incident response.

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Weekly Maintenance](#weekly-maintenance)
3. [Monthly Tasks](#monthly-tasks)
4. [Incident Response](#incident-response)
5. [Recovery Procedures](#recovery-procedures)

## Daily Operations

### Check Archival Status

```bash
# Check last run status
tail -n 100 /var/log/archiver.log | grep -E "(completed|error|failed)"

# Check health endpoint
curl http://localhost:8001/health

# Check Prometheus metrics
curl http://localhost:8000/metrics | grep archiver
```

### Monitor Notifications

Check for notifications:
- Email alerts
- Slack notifications
- PagerDuty alerts (if configured)

### Review Logs

```bash
# Check for errors
grep -i error /var/log/archiver.log | tail -20

# Check for warnings
grep -i warning /var/log/archiver.log | tail -20
```

## Weekly Maintenance

### Validate Archives

```bash
# Validate all archives
python -m validate.main --config config.yaml --output-format json > validation-report-$(date +%Y%m%d).json

# Check for orphaned files
python -m validate.main --config config.yaml | grep -i orphaned
```

### Review Costs

```bash
# Estimate current storage costs
python -m cost.main --config config.yaml --size-gb <estimated_size>

# Compare storage classes
python -m cost.main --size-gb <estimated_size> --compare
```

### Check S3 Storage

```bash
# List archives
aws s3 ls s3://audit-archives/ --recursive | wc -l

# Check storage usage
aws s3 ls s3://audit-archives/ --recursive --summarize
```

## Monthly Tasks

### Performance Review

1. Review archival performance metrics
2. Check batch processing times
3. Review error rates
4. Optimize batch sizes if needed

### Configuration Review

1. Review retention periods
2. Check for schema changes
3. Update configuration if needed
4. Test configuration changes

### Cost Optimization

1. Review S3 storage costs
2. Consider moving to cheaper storage classes
3. Review compression ratios
4. Optimize retention periods

## Incident Response

### Archival Failure

**Symptoms:**
- Error logs showing archival failures
- Health check endpoint showing unhealthy status
- Notifications indicating failures

**Response:**

1. **Check Logs**
   ```bash
   tail -n 200 /var/log/archiver.log
   ```

2. **Check Health**
   ```bash
   curl http://localhost:8001/health
   ```

3. **Identify Issue**
   - Database connectivity?
   - S3 connectivity?
   - Configuration error?
   - Data corruption?

4. **Resolve Issue**
   - Fix database connection
   - Fix S3 credentials
   - Correct configuration
   - Restore from checkpoint

5. **Resume Archival**
   ```bash
   python -m archiver.main --config config.yaml
   ```

### Data Loss Suspected

**Symptoms:**
- Validation failures
- Checksum mismatches
- Record count discrepancies

**Response:**

1. **Stop Archival**
   ```bash
   # Kill running process
   pkill -f "archiver.main"
   ```

2. **Validate Archives**
   ```bash
   python -m validate.main --config config.yaml --verbose
   ```

3. **Check Database**
   ```sql
   -- Verify record counts
   SELECT COUNT(*) FROM audit_logs;
   ```

4. **Check S3**
   ```bash
   # List all archives
   aws s3 ls s3://audit-archives/ --recursive
   ```

5. **Restore if Needed**
   ```bash
   # Restore all batches from last known good date
   python -m restore.main \
     --config config.yaml \
     --restore-all \
     --database production_db \
     --table audit_logs \
     --start-date <last_good_date> \
     --conflict-strategy skip

   # Or restore a specific archive file
   python -m restore.main \
     --config config.yaml \
     --s3-key <last_good_archive> \
     --database production_db \
     --table audit_logs
   ```

### S3 Outage

**Symptoms:**
- S3 upload failures
- Health check showing S3 unhealthy
- Local fallback directory filling up

**Response:**

1. **Check S3 Status**
   ```bash
   aws s3 ls s3://audit-archives/
   ```

2. **Enable Local Fallback** (if not already enabled)
   ```yaml
   s3:
     local_fallback_dir: /var/archiver/fallback
   ```

3. **Monitor Fallback Directory**
   ```bash
   du -sh /var/archiver/fallback
   ```

4. **Resume Uploads After S3 Recovery**
   ```bash
   # Archives in fallback will be automatically retried
   # Or manually upload using S3 sync
   aws s3 sync /var/archiver/fallback s3://audit-archives/
   ```

### Database Connection Issues

**Symptoms:**
- Connection pool exhaustion
- Connection timeout errors
- Database unavailable

**Response:**

1. **Check Database Status**
   ```bash
   psql -h db.example.com -U archiver -d production_db -c "SELECT 1;"
   ```

2. **Check Connection Pool**
   ```yaml
   # Increase pool size in config
   defaults:
     connection_pool_size: 20
   ```

3. **Check Database Load**
   ```sql
   SELECT count(*) FROM pg_stat_activity;
   ```

4. **Restart Archiver**
   ```bash
   systemctl restart archiver
   ```

## Recovery Procedures

### Restore from Archive

**Scenario:** Need to restore archived data

**Procedure:**

1. **List Available Archives**
   ```bash
   # List all archives for a table
   python -m restore.main \
     --config config.yaml \
     --database production_db \
     --table audit_logs
   ```

2. **Restore All Batches (Recommended)**
   ```bash
   # Restore ALL batches for a table
   python -m restore.main \
     --config config.yaml \
     --restore-all \
     --database production_db \
     --table audit_logs \
     --conflict-strategy skip

   # Or restore from a specific date range
   python -m restore.main \
     --config config.yaml \
     --restore-all \
     --database production_db \
     --table audit_logs \
     --start-date 2026-01-01 \
     --end-date 2026-01-31 \
     --conflict-strategy skip
   ```

3. **Restore Single Archive (Alternative)**
   ```bash
   # Restore a specific archive file
   python -m restore.main \
     --config config.yaml \
     --s3-key <archive_key> \
     --database production_db \
     --table audit_logs \
     --conflict-strategy skip
   ```

4. **Verify Restore**
   ```sql
   -- Check record count
   SELECT COUNT(*) FROM audit_logs;

   -- Check date range
   SELECT MIN(created_at), MAX(created_at) FROM audit_logs;

   -- Validate using validation utility
   python -m validate.main \
     --config config.yaml \
     --database production_db \
     --table audit_logs
   ```

### Recover from Checkpoint

**Scenario:** Archival interrupted, need to resume

**Procedure:**

1. **Check for Checkpoint**
   ```bash
   # Checkpoints are automatically detected
   # No manual intervention needed
   ```

2. **Resume Archival**
   ```bash
   python -m archiver.main --config config.yaml
   ```

3. **Verify Resume**
   ```bash
   # Check logs for "Resuming from checkpoint"
   tail -n 50 /var/log/archiver.log
   ```

### Fix Corrupted Archive

**Scenario:** Validation detects corrupted archive

**Procedure:**

1. **Identify Corrupted Archive**
   ```bash
   python -m validate.main --config config.yaml | grep -i "checksum\|invalid"
   ```

2. **Check if Data Still in Database**
   ```sql
   -- If data still exists, re-archive
   SELECT COUNT(*) FROM audit_logs WHERE created_at < '2026-01-01';
   ```

3. **Re-archive if Needed**
   ```bash
   # Adjust retention to re-archive
   # Or manually restore from database backup
   ```

### Recover from Configuration Error

**Scenario:** Configuration error prevents archival

**Procedure:**

1. **Validate Configuration**
   ```bash
   python -m archiver.main --config config.yaml --dry-run
   ```

2. **Fix Configuration**
   ```bash
   # Use wizard to regenerate
   python -m wizard.main --output config.yaml
   ```

3. **Test Configuration**
   ```bash
   python -m archiver.main --config config.yaml --dry-run
   ```

## Emergency Contacts

- **On-Call Engineer**: [Contact Info]
- **Database Team**: [Contact Info]
- **S3/Storage Team**: [Contact Info]
- **Security Team**: [Contact Info]

## Escalation Path

1. **Level 1**: Check logs, health endpoint, basic troubleshooting
2. **Level 2**: Review runbooks, check configuration, validate archives
3. **Level 3**: Escalate to on-call engineer
4. **Level 4**: Escalate to database/storage teams
5. **Level 5**: Escalate to security team (if data breach suspected)

