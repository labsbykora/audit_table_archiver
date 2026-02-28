# Read Replica Load Balancing

## Overview

Read replica load balancing distributes read-only queries (SELECT) across PostgreSQL read replicas while ensuring write operations (DELETE, VACUUM, etc.) always use the primary database. This significantly reduces load on the primary database and improves archival performance.

## Benefits

### Performance
- **Reduced Primary Load**: Offloads 80-90% of queries (SELECT operations) to replicas
- **Parallel Query Execution**: Multiple replicas can process queries concurrently
- **Better Resource Utilization**: Distributes CPU, memory, and I/O across replicas
- **Lower Latency**: Replicas often have less contention than primary

### Reliability
- **Automatic Failover**: Falls back to primary if replica is unavailable
- **Health Monitoring**: Continuously checks replica lag and availability
- **Graceful Degradation**: Continues operating even if replicas fail

### Scalability
- **Horizontal Scaling**: Add more replicas to handle increased load
- **Read-Heavy Workloads**: Optimized for archival which is primarily read operations
- **Cost Efficiency**: Replicas can use cheaper instance types

## Architecture

### Current State
```
┌─────────────┐
│   Primary   │ ← All queries (SELECT + DELETE)
└─────────────┘
```

### With Load Balancing
```
┌─────────────┐
│   Primary   │ ← Writes only (DELETE, VACUUM, transactions)
└─────────────┘
       ↑
       │ replication
       │
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Replica 1  │  │  Replica 2  │  │  Replica 3  │ ← Reads (SELECT)
└─────────────┘  └─────────────┘  └─────────────┘
       ↑                ↑                ↑
       └────────────────┴────────────────┘
              Load Balancer
```

## Query Classification

### Read Operations (Use Replicas)
- `SELECT` queries (batch selection, counting, schema detection)
- `EXPLAIN` queries (query plan analysis)
- `SELECT ... FOR SHARE` (read locks)
- Metadata queries (`pg_catalog`, `information_schema`)

### Write Operations (Use Primary)
- `DELETE` (archival deletion)
- `VACUUM` / `VACUUM ANALYZE` / `VACUUM FULL`
- `TRUNCATE`
- Transactions (any operation in a transaction)
- Schema changes (`CREATE`, `ALTER`, `DROP`)
- Lock acquisition (`SELECT ... FOR UPDATE`)

## Implementation Strategy

### 1. Connection Pool Management

**Primary Pool**: Always available, used for writes
**Replica Pools**: One pool per replica, used for reads

```python
class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        self.primary_pool: Optional[asyncpg.Pool] = None
        self.replica_pools: list[ReplicaPool] = []
        self.replica_selector: ReplicaSelector = RoundRobinSelector()
```

### 2. Replica Selection Strategies

#### Round-Robin (Default)
- Distributes queries evenly across replicas
- Simple, predictable, good for uniform load

#### Least Connections
- Routes to replica with fewest active connections
- Better for uneven query complexity

#### Latency-Based
- Routes to replica with lowest latency
- Requires periodic latency measurement

#### Replication Lag Aware
- Routes to replica with lowest lag
- Ensures data freshness for time-sensitive queries

### 3. Health Checking

Monitor replica health:
- **Connection Health**: Can we connect?
- **Replication Lag**: How far behind is the replica?
- **Query Latency**: How fast are queries?
- **Error Rate**: Are queries failing?

```python
class ReplicaHealth:
    is_healthy: bool
    lag_seconds: float
    avg_latency_ms: float
    error_rate: float
    last_check: datetime
```

### 4. Automatic Failover

**Fallback Logic**:
1. Try selected replica
2. If unhealthy, try next replica
3. If all replicas fail, use primary
4. Log fallback for monitoring

**Circuit Breaker Pattern**:
- Temporarily exclude unhealthy replicas
- Retry after cooldown period
- Prevents cascading failures

### 5. Replication Lag Handling

**Configurable Lag Threshold**:
- Default: 10 seconds
- Queries requiring fresh data can use primary
- Stale data acceptable for archival (historical data)

**Lag Monitoring**:
```sql
SELECT 
    EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())) AS lag_seconds
FROM pg_stat_replication
WHERE application_name = 'replica_name';
```

## Configuration

### Single Replica
```yaml
databases:
  - name: production
    host: primary.example.com
    read_replica: replica.example.com
```

### Multiple Replicas
```yaml
databases:
  - name: production
    host: primary.example.com
    read_replicas:
      - host: replica1.example.com
        port: 5432
        weight: 1.0
      - host: replica2.example.com
        port: 5432
        weight: 1.0
      - host: replica3.example.com
        port: 5432
        weight: 0.5  # Lower weight for less capable replica
```

### Advanced Options
```yaml
defaults:
  read_replica:
    enabled: true
    selection_strategy: round_robin  # round_robin, least_connections, latency, lag_aware
    max_lag_seconds: 10
    health_check_interval: 30
    circuit_breaker:
      failure_threshold: 5
      recovery_timeout: 60
    fallback_to_primary: true
```

## Performance Impact

### Expected Improvements

**Query Throughput**:
- 2-3x improvement with 2 replicas
- 4-5x improvement with 4 replicas
- Diminishing returns after 4-6 replicas

**Primary Database Load**:
- 80-90% reduction in SELECT queries
- 100% of writes still on primary (required)
- Overall CPU reduction: 60-70%

**Latency**:
- Replica queries: 20-40% faster (less contention)
- Write operations: Unchanged (must use primary)

### Benchmarks (Estimated)

| Scenario | Without Replicas | With 2 Replicas | With 4 Replicas |
|----------|-----------------|-----------------|------------------|
| SELECT queries/sec | 100 | 200-250 | 350-400 |
| Primary CPU usage | 80% | 30-40% | 20-30% |
| Average query latency | 50ms | 30ms | 25ms |
| Batch processing time | 100% | 50-60% | 40-50% |

## Operational Considerations

### Monitoring

**Key Metrics**:
- Replica health status
- Replication lag per replica
- Query distribution (primary vs replicas)
- Fallback frequency
- Connection pool utilization

**Alerts**:
- Replica lag > threshold
- Replica health check failures
- High fallback rate (indicates replica issues)

### Troubleshooting

**Replica Not Used**:
- Check health status
- Verify replication lag
- Check connection pool availability

**High Replication Lag**:
- Increase replica resources
- Check network latency
- Verify replication configuration

**All Queries Using Primary**:
- Replicas may be unhealthy
- Check circuit breaker status
- Verify configuration

## Implementation Phases

### Phase 1: Basic Support
- Single replica support
- Round-robin selection
- Basic health checking
- Fallback to primary

### Phase 2: Multiple Replicas
- Multiple replica configuration
- Advanced selection strategies
- Replication lag monitoring
- Circuit breaker pattern

### Phase 3: Optimization
- Latency-based routing
- Connection pool optimization
- Query classification improvements
- Performance metrics

## Migration Path

1. **Enable with Monitoring**: Start with read-only queries, monitor closely
2. **Gradual Rollout**: Enable per-database, verify stability
3. **Optimize**: Tune selection strategy and thresholds
4. **Scale**: Add more replicas as needed

## Limitations

- **Replication Lag**: Replicas may be slightly behind (acceptable for archival)
- **Write Operations**: Must always use primary (by design)
- **Transaction Context**: All operations in a transaction use primary
- **Schema Queries**: Some metadata queries may need primary for consistency

## Best Practices

1. **Monitor Replication Lag**: Set appropriate thresholds
2. **Health Checks**: Regular, but not too frequent (avoid overhead)
3. **Connection Pooling**: Size appropriately for read workload
4. **Failover Testing**: Regularly test replica failover scenarios
5. **Load Distribution**: Use weights to balance across replicas of different sizes

