# Load Testing Guide

Guide for conducting load tests to verify production readiness.

## Overview

Load testing verifies that the Audit Table Archiver can handle production workloads over extended periods. This guide covers test scenarios, setup, and success criteria.

## Prerequisites

- Test environment with production-like resources
- Sufficient test data (millions of records)
- Monitoring tools (Prometheus, Grafana)
- Database monitoring (pg_stat_statements, pg_stat_activity)

## Test Scenarios

### 1. Sustained Load Test (24 Hours)

**Purpose**: Verify no memory leaks, connection pool stability, and long-term reliability.

**Setup**:
- 1M+ records in test table
- Continuous archival for 24 hours
- Monitor memory, CPU, connection count

**Success Criteria**:
- ✅ No memory leaks (<50% growth over 24h)
- ✅ Connection pool stable (no connection errors)
- ✅ All records archived successfully
- ✅ No crashes or hangs

**Run Test**:
```bash
# Mark test as slow and run
pytest tests/performance/test_load_scenarios.py::test_sustained_load_1h -v -s --slow

# For 24-hour test, modify test duration and run in background
pytest tests/performance/test_load_scenarios.py -v -s --slow -k "sustained"
```

**Monitoring**:
```bash
# Monitor memory usage
watch -n 60 'ps aux | grep archiver | awk "{print \$6/1024 \" MB\"}"'

# Monitor database connections
watch -n 30 'psql -c "SELECT count(*) FROM pg_stat_activity WHERE application_name = '\''audit_archiver'\''"'

# Monitor Prometheus metrics
curl http://localhost:9090/api/v1/query?query=archiver_memory_usage_bytes
```

### 2. High Concurrency Test

**Purpose**: Verify handling of multiple tables/databases simultaneously.

**Setup**:
- 5-10 tables with 100K records each
- Process all tables in parallel
- Monitor resource usage

**Success Criteria**:
- ✅ All tables processed successfully
- ✅ Throughput >5000 records/minute
- ✅ No resource exhaustion
- ✅ No connection pool exhaustion

**Run Test**:
```bash
pytest tests/performance/test_load_scenarios.py::test_high_concurrency_multiple_tables -v -s
```

### 3. Large Batch Size Test

**Purpose**: Verify memory management with very large batches.

**Setup**:
- 200K+ records
- Batch size: 50,000 records
- Monitor memory usage

**Success Criteria**:
- ✅ Memory usage <2GB for 50K batch
- ✅ Throughput >10K records/minute
- ✅ No memory errors

**Run Test**:
```bash
pytest tests/performance/test_load_scenarios.py::test_large_batch_size -v -s
```

### 4. Connection Pool Stability Test

**Purpose**: Verify connection pool handles many operations without leaks.

**Setup**:
- Multiple archival runs
- Large connection pool (10+ connections)
- Monitor connection count

**Success Criteria**:
- ✅ No connection errors
- ✅ Connection count stable
- ✅ Pool properly releases connections

**Run Test**:
```bash
pytest tests/performance/test_load_scenarios.py::test_connection_pool_stability -v -s
```

### 5. Database Load Impact Test

**Purpose**: Verify archival doesn't significantly impact database performance.

**Setup**:
- Monitor database metrics during archival
- Compare before/during/after metrics

**Success Criteria**:
- ✅ CPU increase <5%
- ✅ Connection count within limits
- ✅ No significant lock contention
- ✅ Query duration stable

**Run Test**:
```bash
pytest tests/performance/test_load_scenarios.py::test_database_load_impact -v -s
```

**Monitor Database**:
```sql
-- Monitor active queries
SELECT 
    pid,
    application_name,
    state,
    query_start,
    now() - query_start AS duration,
    query
FROM pg_stat_activity
WHERE application_name = 'audit_archiver'
ORDER BY query_start;

-- Monitor connection count
SELECT 
    state,
    count(*) as connections
FROM pg_stat_activity
WHERE application_name = 'audit_archiver'
GROUP BY state;

-- Monitor lock waits
SELECT 
    blocked_locks.pid AS blocked_pid,
    blocking_locks.pid AS blocking_pid,
    blocked_activity.query AS blocked_query,
    blocking_activity.query AS blocking_query
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted
AND blocked_locks.pid != blocking_locks.pid;
```

## Load Test Scripts

### Manual Load Test Script

```bash
#!/bin/bash
# load-test.sh - Manual load test script

set -euo pipefail

CONFIG_FILE="${1:-config.yaml}"
DURATION_HOURS="${2:-1}"
TABLE_NAME="${3:-load_test_table}"

echo "Starting load test:"
echo "  Config: $CONFIG_FILE"
echo "  Duration: $DURATION_HOURS hours"
echo "  Table: $TABLE_NAME"

# Insert test data
echo "Inserting test data..."
psql -c "
  CREATE TABLE IF NOT EXISTS $TABLE_NAME (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    action TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
  
  -- Insert 100K records
  INSERT INTO $TABLE_NAME (user_id, action, created_at)
  SELECT 
    (random() * 10)::int,
    'action_' || generate_series,
    NOW() - INTERVAL '100 days' + (generate_series * INTERVAL '1 second')
  FROM generate_series(1, 100000);
"

# Run archiver in loop
START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION_HOURS * 3600))
ITERATION=0

while [ $(date +%s) -lt $END_TIME ]; do
    ITERATION=$((ITERATION + 1))
    echo "Iteration $ITERATION: $(date)"
    
    # Run archiver
    python -m archiver.main --config "$CONFIG_FILE" --table "$TABLE_NAME" || {
        echo "ERROR: Archiver failed at iteration $ITERATION"
        exit 1
    }
    
    # Re-insert some data for next iteration
    psql -c "
      INSERT INTO $TABLE_NAME (user_id, action, created_at)
      SELECT 
        (random() * 10)::int,
        'action_' || generate_series,
        NOW() - INTERVAL '100 days' + (generate_series * INTERVAL '1 second')
      FROM generate_series(1, 10000);
    "
    
    echo "  Completed iteration $ITERATION"
    sleep 60  # Wait 1 minute between iterations
done

echo "Load test completed: $ITERATION iterations"
```

### Kubernetes Load Test Job

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: archiver-load-test
  namespace: audit-archiver
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 86400  # 24 hours
  template:
    spec:
      serviceAccountName: archiver-sa
      restartPolicy: Never
      containers:
      - name: archiver
        image: audit-archiver:1.0.0
        command:
        - /bin/bash
        - -c
        - |
          # Load test script
          for i in {1..100}; do
            echo "Iteration $i: $(date)"
            python -m archiver.main --config /etc/archiver/config.yaml
            sleep 300  # 5 minutes between iterations
          done
        env:
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: archiver-secrets
              key: DB_PASSWORD
        resources:
          requests:
            memory: "1Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "4000m"
        volumeMounts:
        - name: config
          mountPath: /etc/archiver
      volumes:
      - name: config
        configMap:
          name: archiver-config
```

## Success Criteria Summary

| Test Scenario | Duration | Success Criteria |
|--------------|----------|------------------|
| Sustained Load | 24 hours | No memory leaks, no crashes, all records archived |
| High Concurrency | 1 hour | >5000 rec/min, no resource exhaustion |
| Large Batch | 30 min | <2GB memory, >10K rec/min |
| Connection Pool | 1 hour | No connection errors, stable pool |
| DB Load Impact | 1 hour | <5% CPU increase, no lock contention |

## Monitoring During Load Tests

### Memory Monitoring

```bash
# Continuous memory monitoring
watch -n 5 'ps aux | grep "[p]ython.*archiver" | awk "{sum+=\$6} END {print sum/1024 \" MB\"}"'

# Memory graph over time
while true; do
    echo "$(date +%s),$(ps aux | grep '[p]ython.*archiver' | awk '{sum+=$6} END {print sum/1024}')"
    sleep 60
done > memory_usage.csv
```

### CPU Monitoring

```bash
# CPU usage
top -p $(pgrep -f "python.*archiver" | head -1)

# CPU graph
while true; do
    echo "$(date +%s),$(ps aux | grep '[p]ython.*archiver' | awk '{sum+=$3} END {print sum}')"
    sleep 60
done > cpu_usage.csv
```

### Database Monitoring

```sql
-- Create monitoring view
CREATE OR REPLACE VIEW archiver_monitor AS
SELECT 
    NOW() as timestamp,
    count(*) FILTER (WHERE state = 'active') as active_connections,
    count(*) FILTER (WHERE state = 'idle') as idle_connections,
    count(*) as total_connections,
    avg(EXTRACT(EPOCH FROM (NOW() - query_start))) as avg_query_duration
FROM pg_stat_activity
WHERE application_name = 'audit_archiver';

-- Monitor over time
SELECT * FROM archiver_monitor;
```

### Prometheus Metrics

```promql
# Memory usage over time
rate(archiver_memory_usage_bytes[5m])

# Records per second
rate(archiver_records_archived_total[5m])

# Error rate
rate(archiver_errors_total[5m])

# Database connection count
archiver_db_connections_active
```

## Troubleshooting Load Tests

### Memory Leaks

**Symptoms**: Memory usage continuously increasing

**Solutions**:
- Check for unclosed connections
- Verify checkpoint cleanup
- Review large object retention
- Enable Python garbage collection debugging

### Connection Pool Exhaustion

**Symptoms**: Connection errors, timeouts

**Solutions**:
- Increase pool size
- Reduce batch size
- Check for connection leaks
- Verify proper connection release

### Database Performance Degradation

**Symptoms**: Slow queries, high CPU, lock contention

**Solutions**:
- Use read replica for queries
- Reduce batch size
- Add delays between batches
- Optimize database indexes

## Load Test Report Template

```markdown
# Load Test Report

**Date**: YYYY-MM-DD
**Duration**: X hours
**Environment**: [staging/production-like]

## Test Configuration
- Records: X million
- Batch size: X,XXX
- Databases: X
- Tables: X

## Results

### Performance
- Throughput: X,XXX records/minute
- Memory usage: X GB (peak)
- CPU usage: X% (average)
- Database impact: X% CPU increase

### Stability
- Memory leaks: None detected
- Connection errors: 0
- Crashes: 0
- Failed batches: X / X total

### Database Impact
- CPU increase: X%
- Connection count: X / X max
- Lock contention: None
- Query performance: Stable

## Conclusion
[PASS/FAIL] - Ready for production
```

---

**See Also**:
- [Performance Tuning Guide](performance-tuning.md)
- [Operations Manual](operations-manual.md)
- [Troubleshooting Guide](troubleshooting.md)


