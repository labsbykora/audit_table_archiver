# Operations Manual

Complete guide for operating the Audit Table Archiver in production environments.

## Table of Contents

1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Running the Archiver](#running-the-archiver)
4. [Monitoring](#monitoring)
5. [Troubleshooting](#troubleshooting)
6. [Maintenance](#maintenance)
7. [Backup and Recovery](#backup-and-recovery)

## Installation

### Prerequisites

- Python 3.9 or higher
- PostgreSQL 11 or higher
- S3-compatible storage (AWS S3, MinIO, etc.)
- Network access to database and S3

### Installation Steps

```bash
# Clone repository
git clone <repository-url>
cd auditlog_manager

# Install production dependencies
pip install -e .

# Or install with development tools
pip install -e ".[dev]"
```

### Verify Installation

```bash
# Check version
python -m archiver.main --version

# Test configuration
python -m archiver.main --config config.yaml --dry-run
```

## Configuration

### Quick Setup with Wizard

The easiest way to create a configuration is using the interactive wizard:

```bash
python -m wizard.main --output config.yaml
```

The wizard will:
- Connect to your database
- Auto-detect tables
- Suggest retention periods
- Generate a validated configuration

### Manual Configuration

See `docs/examples/config-simple.yaml` for a basic configuration example.

### Configuration Validation

```bash
# Validate configuration
python -m archiver.main --config config.yaml --dry-run
```

## Running the Archiver

### Basic Usage

```bash
# Run archival
python -m archiver.main --config config.yaml
```

### Scheduled Execution

#### Cron (Linux/macOS)

```bash
# Edit crontab
crontab -e

# Run daily at 2 AM
0 2 * * * /usr/bin/python3 -m archiver.main --config /etc/archiver/config.yaml >> /var/log/archiver.log 2>&1
```

#### Systemd (Linux)

Create `/etc/systemd/system/archiver.service`:

```ini
[Unit]
Description=Audit Table Archiver
After=network.target postgresql.service

[Service]
Type=oneshot
User=archiver
Environment="DB_PASSWORD=${DB_PASSWORD}"
# Use JSON format for log aggregation (journald can forward to syslog/aggregators)
ExecStart=/usr/bin/python3 -m archiver.main --config /etc/archiver/config.yaml --log-format json
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/archiver.timer`:

```ini
[Unit]
Description=Run Audit Archiver Daily
Requires=archiver.service

[Timer]
OnCalendar=daily
OnCalendar=02:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable and start:

```bash
sudo systemctl enable archiver.timer
sudo systemctl start archiver.timer
```

#### Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: audit-archiver
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: archiver
            image: audit-archiver:latest
            # Use JSON format for log aggregation (Kubernetes automatically collects stdout/stderr)
            command: ["python", "-m", "archiver.main", "--config", "/etc/archiver/config.yaml", "--log-format", "json"]
            env:
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: db-secrets
                  key: password
            volumeMounts:
            - name: config
              mountPath: /etc/archiver
          volumes:
          - name: config
            configMap:
              name: archiver-config
          restartPolicy: OnFailure
```

### Command-Line Options

```bash
# Dry run (no data deleted)
--dry-run

# Verbose output
--verbose

# Filter by database
--database DATABASE_NAME

# Filter by table
--table TABLE_NAME

# Quiet mode (for cron)
--quiet
```

## Monitoring

### Health Check Endpoint

The archiver exposes a health check endpoint (default port 8001):

```bash
# Check health
curl http://localhost:8001/health

# Response:
{
  "status": "healthy",
  "timestamp": "2026-01-15T10:30:00Z",
  "components": {
    "database": "healthy",
    "s3": "healthy"
  }
}
```

### Prometheus Metrics

Metrics are exposed on port 8000 (default):

```bash
# Scrape metrics
curl http://localhost:8000/metrics
```

Key metrics:
- `archiver_records_archived_total`: Total records archived
- `archiver_batches_processed_total`: Total batches processed
- `archiver_errors_total`: Total errors encountered
- `archiver_batch_duration_seconds`: Batch processing duration
- `archiver_current_progress_percent`: Current archival progress

### Logging

#### Log Format Options

The archiver supports two log formats:

- **Console format** (default): Human-readable, colored output for terminal use
- **JSON format**: Structured JSON logs (one JSON object per line) for log aggregation

```bash
# Default: Console format
python -m archiver.main --config config.yaml

# JSON format for log aggregation
python -m archiver.main --config config.yaml --log-format json
```

**Note**: `--log-format json` only changes the OUTPUT FORMAT of logs. It does NOT automatically send logs to any log aggregation system. You need to configure your environment to collect and forward logs.

#### Sending Logs to Log Aggregators

The archiver writes logs to **stdout/stderr** (standard output/error streams). To send logs to log aggregation systems, configure your environment:

**Quick Reference:**

| Environment | Log Collection Method | Configuration Complexity |
|------------|----------------------|-------------------------|
| **Docker** | Docker logging drivers | Easy - Built-in |
| **Kubernetes** | Automatic stdout/stderr collection | Easy - Built-in |
| **AWS ECS/EC2** | CloudWatch Logs Agent | Medium - Agent setup |
| **GCP GKE** | Cloud Logging (automatic) | Easy - Built-in |
| **Azure AKS** | Azure Monitor (automatic) | Easy - Built-in |
| **Systemd** | journald → rsyslog | Medium - Service config |
| **Standalone** | Filebeat/Fluentd/Logstash | Medium - Agent setup |
| **Splunk** | Universal Forwarder | Medium - Agent setup |
| **Datadog** | Datadog Agent | Medium - Agent setup |

**1. Docker/Kubernetes:**
- Logs are automatically collected from stdout/stderr
- Configure your container orchestration to forward to your log aggregator
- Example: Kubernetes Fluentd/Fluent Bit, Docker logging drivers

**2. Systemd Service:**
```ini
[Service]
# Logs go to journald, which can forward to syslog/aggregators
StandardOutput=journal
StandardError=journal
```

**3. Log Shippers (Fluentd, Logstash, Filebeat):**
- Configure log shipper to read from stdout/stderr or log files
- Forward to your aggregation system (ELK, Splunk, CloudWatch, etc.)

**4. Direct File Output:**
```bash
# Redirect JSON logs to file, then ship to aggregator
python -m archiver.main --config config.yaml --log-format json > /var/log/archiver/archiver.log 2>&1
```

**5. Cloud-Native (AWS, GCP, Azure):**
- CloudWatch Logs Agent (AWS): Automatically collects stdout/stderr
- Cloud Logging (GCP): Automatically collects container logs
- Azure Monitor: Automatically collects container logs

#### Detailed Examples

**Example 1: Docker with Fluentd Logging Driver**

```bash
# Run container with Fluentd logging driver
docker run \
  -e DB_PASSWORD="${DB_PASSWORD}" \
  -v $(pwd)/config.yaml:/etc/archiver/config.yaml \
  --log-driver=fluentd \
  --log-opt fluentd-address=localhost:24224 \
  --log-opt tag=archiver \
  audit-archiver:latest \
  python -m archiver.main --config /etc/archiver/config.yaml --log-format json
```

**Example 2: Kubernetes with Fluent Bit DaemonSet**

The Kubernetes cluster automatically collects stdout/stderr. Configure Fluent Bit to forward:

```yaml
# Fluent Bit ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: fluent-bit-config
data:
  fluent-bit.conf: |
    [INPUT]
        Name              tail
        Path              /var/log/containers/*archiver*.log
        Parser            json
        Tag               archiver.*
        Refresh_Interval  5

    [OUTPUT]
        Name  es
        Match archiver.*
        Host  elasticsearch.logging.svc.cluster.local
        Port  9200
        Index archiver-logs
        Type  _doc
```

**Example 3: Systemd with rsyslog forwarding**

```ini
# /etc/systemd/system/archiver.service
[Service]
ExecStart=/usr/bin/python3 -m archiver.main --config /etc/archiver/config.yaml --log-format json
StandardOutput=journal
StandardError=journal
```

```conf
# /etc/rsyslog.d/archiver.conf
# Forward archiver logs to remote syslog server
if $programname == 'archiver' then @remote-syslog-server:514
& stop
```

**Example 4: Filebeat to ELK Stack**

```yaml
# filebeat.yml
filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /var/log/archiver/*.log
  json.keys_under_root: true
  json.add_error_key: true

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "archiver-logs-%{+yyyy.MM.dd}"

processors:
  - add_fields:
      fields:
        service: archiver
        environment: production
```

```bash
# Run archiver, redirecting JSON logs to file
python -m archiver.main --config config.yaml --log-format json >> /var/log/archiver/archiver.log 2>&1
```

**Example 5: AWS CloudWatch Logs**

```bash
# Install CloudWatch Logs Agent
wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
sudo rpm -U ./amazon-cloudwatch-agent.rpm

# Configure agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
  -s
```

```json
// /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/archiver/archiver.log",
            "log_group_name": "/aws/archiver/logs",
            "log_stream_name": "{instance_id}",
            "timestamp_format": "%Y-%m-%dT%H:%M:%S.%fZ"
          }
        ]
      }
    }
  }
}
```

**Example 6: GCP Cloud Logging**

```yaml
# Kubernetes deployment with Cloud Logging
apiVersion: apps/v1
kind: Deployment
metadata:
  name: archiver
spec:
  template:
    spec:
      containers:
      - name: archiver
        image: audit-archiver:latest
        command: ["python", "-m", "archiver.main", "--config", "/etc/archiver/config.yaml", "--log-format", "json"]
        # GCP automatically collects stdout/stderr from containers
        # No additional configuration needed
```

**Example 7: Splunk Universal Forwarder**

```bash
# Install Splunk Universal Forwarder
wget -O splunkforwarder.tgz https://download.splunk.com/products/universalforwarder/releases/9.x.x/splunkforwarder-9.x.x-linux-x86_64.tgz
tar -xzf splunkforwarder.tgz -C /opt

# Configure inputs
# /opt/splunkforwarder/etc/system/local/inputs.conf
[monitor:///var/log/archiver]
disabled = false
index = archiver
sourcetype = json
```

**Example 8: Datadog Agent**

```yaml
# /etc/datadog-agent/conf.d/python.d/conf.yaml
init_config:

instances:
  - python_version: 3
    tags:
      - service:archiver
      - env:production

# Log collection
# /etc/datadog-agent/conf.d/logs.d/archiver.yaml
logs:
  - type: file
    path: /var/log/archiver/archiver.log
    service: archiver
    source: python
    sourcecategory: application
    log_processing_rules:
      - type: multi_line
        name: json_logs
        pattern: \{
```

**Example 9: Local File with Log Rotation**

```bash
# Create log directory
sudo mkdir -p /var/log/archiver
sudo chown archiver:archiver /var/log/archiver

# Run with log rotation
python -m archiver.main --config config.yaml --log-format json | \
  rotatelogs -l /var/log/archiver/archiver-%Y%m%d.log 86400
```

Or use Python's logging rotation:

```python
# Custom logging setup (if needed)
import logging.handlers

handler = logging.handlers.RotatingFileHandler(
    '/var/log/archiver/archiver.log',
    maxBytes=100*1024*1024,  # 100MB
    backupCount=10
)
```

**Example 10: Docker Compose with Log Aggregation**

```yaml
# docker-compose.yml
version: '3.8'
services:
  archiver:
    image: audit-archiver:latest
    command: ["python", "-m", "archiver.main", "--config", "/etc/archiver/config.yaml", "--log-format", "json"]
    volumes:
      - ./config.yaml:/etc/archiver/config.yaml
    logging:
      driver: "fluentd"
      options:
        fluentd-address: "localhost:24224"
        tag: "archiver"
  
  fluentd:
    image: fluent/fluentd:latest
    volumes:
      - ./fluentd.conf:/fluentd/etc/fluent.conf
    ports:
      - "24224:24224"
  
  elasticsearch:
    image: elasticsearch:8.0.0
    ports:
      - "9200:9200"
  
  kibana:
    image: kibana:8.0.0
    ports:
      - "5601:5601"
    depends_on:
      - elasticsearch
```

```ruby
# fluentd.conf
<source>
  @type forward
  port 24224
</source>

<filter archiver.**>
  @type parser
  key_name message
  reserve_data true
  <parse>
    @type json
  </parse>
</filter>

<match archiver.**>
  @type elasticsearch
  host elasticsearch
  port 9200
  index_name archiver-logs
  type_name _doc
</match>
```

#### Logging

Logs are written to stdout in JSON format:

```json
{
  "timestamp": "2026-01-15T10:30:00Z",
  "level": "info",
  "logger": "archiver",
  "message": "Batch processed",
  "database": "production_db",
  "table": "audit_logs",
  "batch_id": "batch_123",
  "records": 1000
}
```

### Notifications

Configure notifications in `config.yaml`:

```yaml
notifications:
  enabled: true
  email:
    enabled: true
    smtp_host: smtp.example.com
    from_email: archiver@example.com
    to_emails:
      - ops@example.com
  slack:
    enabled: true
    webhook_url_env: SLACK_WEBHOOK_URL
  digest_mode: true  # Daily summary instead of individual alerts
```

## Troubleshooting

### Common Issues

#### Database Connection Errors

```bash
# Test database connectivity
psql -h db.example.com -U archiver -d production_db

# Check connection pool settings
# Increase pool_size in config if seeing connection errors
```

#### S3 Upload Failures

```bash
# Test S3 connectivity
aws s3 ls s3://audit-archives/

# Check credentials
aws configure list

# Enable local fallback in config
s3:
  local_fallback_dir: /var/archiver/fallback
```

#### Lock Conflicts

If multiple instances are running:

```bash
# Check for existing locks
psql -c "SELECT * FROM pg_locks WHERE locktype = 'advisory';"

# Manually release lock (if needed)
psql -c "SELECT pg_advisory_unlock_all();"
```

### Validation

Validate archives after archival:

```bash
# Validate all archives
python -m validate.main --config config.yaml

# Validate specific database/table
python -m validate.main --config config.yaml --database production_db --table audit_logs

# Validate date range
python -m validate.main --config config.yaml --start-date 2026-01-01 --end-date 2026-01-31
```

### Checkpoint Recovery

If archival is interrupted, it will automatically resume from the last checkpoint:

```bash
# Checkpoint files are stored in S3 or local directory
# Automatic resume on next run
python -m archiver.main --config config.yaml
```

## Maintenance

### Cost Estimation

Estimate S3 storage costs:

```bash
# Estimate from data size
python -m cost.main --size-gb 100 --storage-class STANDARD_IA

# Compare all storage classes
python -m cost.main --size-gb 100 --compare

# Estimate from record count
python -m cost.main --records 1000000 --avg-record-size 1024
```

### Archive Validation

Regularly validate archived data:

```bash
# Weekly validation
python -m validate.main --config config.yaml --output-format json > validation-report.json
```

### Cleanup

Clean up old checkpoints and fallback files:

```bash
# Checkpoints are automatically cleaned up after successful archival
# Fallback files older than retention_days are automatically removed
```

## Backup and Recovery

### Restore from Archive

See `docs/manual-restore-guide.md` for detailed restore procedures.

#### Restore Single Archive File

```bash
# Restore a specific archive file
python -m restore.main \
  --config config.yaml \
  --s3-key archives/db/table/year=2026/month=01/day=15/file.jsonl.gz \
  --database production_db \
  --table audit_logs \
  --conflict-strategy skip
```

#### Restore All Batches (Bulk Restore)

The archiver splits data into batches (default: 10,000 records per batch) for memory efficiency, transaction safety, and progress tracking. To restore all batches for a table:

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

The restore utility will:
- List all archive files for the table (optionally filtered by date range)
- Restore each file sequentially
- Provide a summary of total records restored, skipped, and failed
- Continue with remaining files if one fails (error isolation)

#### List Available Archives

```bash
# List all archives for a table
python -m restore.main \
  --config config.yaml \
  --database production_db \
  --table audit_logs
```

### Backup Configuration

Backup your configuration:

```bash
# Backup config
cp config.yaml config.yaml.backup

# Store in version control (sanitized)
git add config.yaml.example
```

### Disaster Recovery

1. **Configuration Loss**: Restore from backup or regenerate using wizard
2. **Archive Corruption**: Use validation tool to identify corrupted archives
3. **Database Loss**: Restore from archives using restore utility
4. **S3 Loss**: Restore from local fallback (if enabled)

## Performance Tuning

See `docs/performance-tuning.md` for detailed performance optimization guide.

Key settings:

```yaml
defaults:
  batch_size: 10000  # Increase for faster archival
  sleep_between_batches: 2  # Decrease for faster archival

s3:
  multipart_threshold_mb: 100  # Use multipart for large files
  rate_limit_requests_per_second: 10  # Limit S3 API calls
```

## Security

### Credentials Management

- **Never** commit passwords to version control
- Use environment variables for passwords
- Use IAM roles for S3 access (preferred)
- Rotate credentials regularly

See `docs/security-credentials.md` for detailed security guide.

### Compliance

See `docs/compliance-guide.md` for compliance features:
- Legal hold support
- Retention policy enforcement
- Encryption enforcement
- Audit trail

## Support

For issues or questions:
1. Check `docs/troubleshooting.md`
2. Review logs for error messages
3. Validate configuration
4. Check health endpoint
5. Open an issue on GitHub

