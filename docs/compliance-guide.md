# Compliance & Governance Guide

This guide covers the compliance features available in the Audit Table Archiver, including legal hold support, retention policy enforcement, encryption requirements, and audit trail.

## Table of Contents

1. [Legal Hold Support](#legal-hold-support)
2. [Retention Policy Enforcement](#retention-policy-enforcement)
3. [Encryption Requirements](#encryption-requirements)
4. [Audit Trail](#audit-trail)

---

## Legal Hold Support

Legal holds prevent archival of data that may be required for legal proceedings or investigations.

### Configuration

```yaml
legal_holds:
  enabled: true
  check_table: legal_holds  # Database table name (format: schema.table or table)
  check_database: production_db  # Optional: database to check (if different from target)
  api_endpoint: https://api.example.com/legal-holds  # Optional: API endpoint
  api_timeout: 5  # API request timeout in seconds
```

### Database Table Format

If using a database table, create a table with the following structure:

```sql
CREATE TABLE legal_holds (
    table_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    start_date TIMESTAMPTZ NOT NULL,
    expiration_date TIMESTAMPTZ,  -- NULL for indefinite holds
    requestor TEXT NOT NULL,
    where_clause TEXT,  -- Optional: WHERE clause for record-level holds
    PRIMARY KEY (table_name, schema_name)
);

-- Example: Place a hold on audit_logs table
INSERT INTO legal_holds (
    table_name, schema_name, reason, start_date, expiration_date, requestor
) VALUES (
    'audit_logs', 'public', 'Legal case XYZ-2024', 
    NOW(), '2024-12-31 23:59:59+00', 'legal@example.com'
);
```

### API Endpoint Format

If using an API endpoint, the API should return JSON in the following format:

```json
{
  "has_hold": true,
  "table_name": "audit_logs",
  "schema_name": "public",
  "reason": "Legal case XYZ-2024",
  "start_date": "2024-01-01T00:00:00Z",
  "expiration_date": "2024-12-31T23:59:59Z",
  "requestor": "legal@example.com",
  "where_clause": "user_id = 123"  // Optional
}
```

The API endpoint URL format is:
```
{api_endpoint}/legal-holds/{database_name}/{schema_name}/{table_name}
```

### Behavior

- **Automatic Skip**: Tables with active legal holds are automatically skipped
- **Audit Logging**: All skipped tables are logged to the audit trail
- **Expiration**: Expired holds are automatically ignored
- **Record-Level**: WHERE clause filters allow record-level holds

---

## Retention Policy Enforcement

Retention policies ensure data is not archived too early or kept too long, meeting compliance requirements.

### Configuration

```yaml
compliance:
  min_retention_days: 7  # Minimum retention period (prevents archiving too early)
  max_retention_days: 2555  # Maximum retention period (7 years default)
  enforce_encryption: true  # Require encryption for critical tables
  data_classifications:
    PII: 2555  # 7 years for PII data
    INTERNAL: 365  # 1 year for internal data
    CONFIDENTIAL: 1825  # 5 years for confidential data
```

### Validation Rules

1. **Minimum Retention**: Table retention must be >= `min_retention_days`
2. **Maximum Retention**: Table retention must be <= `max_retention_days`
3. **Classification Matching**: If classification is specified, retention should match (warning if not)
4. **Pre-Archival Check**: Validation occurs before archival begins

### Example

```yaml
databases:
  - name: production_db
    tables:
      - name: audit_logs
        retention_days: 90  # Must be between 7 and 2555 days
        critical: true  # Requires encryption if enforce_encryption is true
```

### Error Messages

- `Retention period (5 days) is below minimum (7 days) required by compliance policy`
- `Retention period (10000 days) exceeds maximum (2555 days) allowed by compliance policy`
- `Retention days must be specified for compliance validation`

---

## Encryption Requirements

Critical tables can be required to use encryption to meet security and compliance requirements.

### Configuration

```yaml
s3:
  encryption: SSE-S3  # or SSE-KMS, SSE-C, or "none" for S3-compatible storage

compliance:
  enforce_encryption: true  # Require encryption for critical tables

databases:
  - name: production_db
    tables:
      - name: audit_logs
        critical: true  # This table requires encryption
```

### Validation

When `enforce_encryption: true` and a table is marked as `critical: true`:
- The archiver validates that `s3.encryption` is not `"none"`
- If encryption is disabled, archival fails with an error
- Error: `Encryption is required for critical tables but is set to 'none'`

### Supported Encryption Methods

- **SSE-S3**: Server-side encryption with S3-managed keys (AWS S3)
- **SSE-KMS**: Server-side encryption with KMS-managed keys (AWS S3)
- **SSE-C**: Server-side encryption with customer-provided keys
- **none**: No encryption (for S3-compatible storage like MinIO)

---

## Audit Trail

The audit trail provides an immutable record of all archival operations for compliance and governance.

### Configuration

The audit trail is automatically enabled and uses S3 storage by default. Events are stored in:
```
{prefix}/audit/year={YYYY}/month={MM}/day={DD}/{timestamp}_{event_type}.json
```

### Event Types

- **ARCHIVE_START**: Archival operation started
- **ARCHIVE_SUCCESS**: Archival operation completed successfully
- **ARCHIVE_FAILURE**: Archival operation failed
- **RESTORE_START**: Restore operation started (future)
- **RESTORE_SUCCESS**: Restore operation completed (future)
- **RESTORE_FAILURE**: Restore operation failed (future)
- **ERROR**: General error event

### Event Structure

```json
{
  "timestamp": "2024-01-15T10:30:00.123456+00:00",
  "event_type": "archive_success",
  "database": "production_db",
  "table": "audit_logs",
  "schema": "public",
  "record_count": 10000,
  "s3_path": "s3://bucket/archives/production_db/audit_logs/...",
  "status": "success",
  "duration_seconds": 45.5,
  "operator": "system",
  "error_message": null,
  "metadata": {
    "legal_hold": {
      "reason": "...",
      "requestor": "..."
    }
  }
}
```

### Querying Audit Trail

#### From S3

```bash
# List all audit events for a specific date
aws s3 ls s3://bucket/archives/audit/year=2024/month=01/day=15/

# Download and view an audit event
aws s3 cp s3://bucket/archives/audit/year=2024/month=01/day=15/20240115T103000.123456_archive_success.json -
```

#### From Database

If using database storage, query the `archiver_audit_log` table:

```sql
-- All archive operations for a table
SELECT * FROM archiver_audit_log
WHERE database_name = 'production_db'
  AND table_name = 'audit_logs'
ORDER BY timestamp DESC;

-- Failed operations in the last 24 hours
SELECT * FROM archiver_audit_log
WHERE status = 'failed'
  AND timestamp > NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC;

-- Operations by operator
SELECT operator, COUNT(*), SUM(record_count) as total_records
FROM archiver_audit_log
WHERE event_type = 'archive_success'
GROUP BY operator;
```

### Compliance Reporting

The audit trail can be used for:
- **Compliance Audits**: Complete record of all data archival operations
- **Incident Investigation**: Track what data was archived and when
- **Performance Monitoring**: Duration and record counts for optimization
- **Access Control**: Track which operators performed which operations

---

## Best Practices

### Legal Holds

1. **Centralized Management**: Use a single legal holds table or API for all databases
2. **Regular Review**: Periodically review and remove expired holds
3. **Documentation**: Always include clear reasons and requestor information
4. **Monitoring**: Set up alerts for tables skipped due to legal holds

### Retention Policies

1. **Classification-Based**: Use data classifications to enforce consistent retention
2. **Regular Review**: Review retention policies annually or when regulations change
3. **Documentation**: Document the rationale for each retention period
4. **Validation**: Test retention policy changes in staging before production

### Encryption

1. **Critical Tables**: Mark all tables containing sensitive data as `critical: true`
2. **KMS Keys**: Use SSE-KMS with customer-managed keys for additional control
3. **Key Rotation**: Implement key rotation policies for KMS keys
4. **Monitoring**: Monitor encryption status in audit logs

### Audit Trail

1. **Retention**: Configure S3 lifecycle policies for audit log retention
2. **Access Control**: Restrict access to audit logs (read-only for most users)
3. **Regular Review**: Periodically review audit logs for anomalies
4. **Backup**: Ensure audit logs are backed up and protected from deletion

---

## Troubleshooting

### Legal Hold Not Detected

- Verify the legal holds table exists and has the correct schema
- Check that the table name matches exactly (case-sensitive)
- Verify the hold is active (start_date <= now() and expiration_date > now() or NULL)
- Check database connection and permissions

### Retention Policy Validation Fails

- Verify `retention_days` is set for the table
- Check that retention is within min/max bounds
- Review classification-specific requirements if applicable
- Check configuration file for typos

### Encryption Validation Fails

- Verify `enforce_encryption: true` in compliance config
- Check that critical tables have `critical: true` set
- Ensure `s3.encryption` is not `"none"` for critical tables
- For S3-compatible storage, consider using client-side encryption

### Audit Trail Not Logging

- Verify S3 bucket permissions (write access)
- Check S3 client configuration
- Review logs for upload errors
- Ensure audit trail is not disabled in configuration

---

## Examples

### Complete Compliance Configuration

```yaml
version: "2.0"

s3:
  bucket: audit-archives
  prefix: archives/
  encryption: SSE-KMS  # Required for critical tables

legal_holds:
  enabled: true
  check_table: legal_holds
  check_database: production_db

compliance:
  min_retention_days: 7
  max_retention_days: 2555
  enforce_encryption: true
  data_classifications:
    PII: 2555
    INTERNAL: 365
    CONFIDENTIAL: 1825

databases:
  - name: production_db
    tables:
      - name: audit_logs
        retention_days: 2555  # 7 years for PII
        critical: true  # Requires encryption
      - name: user_activity
        retention_days: 365  # 1 year for internal data
        critical: false
```

---

For more information, see:
- [Quick Start Guide](quick-start.md)
- [Architecture Documentation](architecture.md)
- [Troubleshooting Guide](troubleshooting.md)

