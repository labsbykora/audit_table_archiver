# API Reference

## Overview

This document provides a comprehensive reference for the audit table archiver API, including classes, methods, configuration options, and usage examples.

## Core Classes

### Archiver

Main class for orchestrating the archival process.

#### Constructor

```python
Archiver(
    config: ArchiverConfig,
    dry_run: bool = False,
    logger: Optional[structlog.BoundLogger] = None
)
```

**Parameters:**
- `config` (ArchiverConfig): Configuration object containing database, S3, and archival settings
- `dry_run` (bool): If True, perform validation without actual archival (default: False)
- `logger` (BoundLogger, optional): Custom logger instance

**Example:**
```python
from archiver import Archiver
from archiver.config import load_config

config = load_config("config.yaml")
archiver = Archiver(config, dry_run=False)
result = await archiver.archive()
```

#### Methods

##### `archive() -> dict[str, Any]`

Main entry point for archival process.

**Returns:**
- Dictionary containing archival statistics:
  - `databases_processed`: Number of databases processed
  - `tables_processed`: Number of tables processed
  - `records_archived`: Total records archived
  - `batches_processed`: Total batches processed
  - `errors`: List of errors encountered
  - `duration_seconds`: Total processing time

**Example:**
```python
result = await archiver.archive()
print(f"Archived {result['records_archived']} records")
```

**Raises:**
- `ArchiverError`: If archival fails

---

### DatabaseManager

Manages PostgreSQL database connections with read replica load balancing.

#### Constructor

```python
DatabaseManager(
    config: DatabaseConfig,
    pool_size: int = 5,
    logger: Optional[BoundLogger] = None,
    max_reconnect_attempts: int = 3,
    read_replica_enabled: bool = True,
    read_replica_selection_strategy: str = "round_robin",
    read_replica_max_lag_seconds: float = 10.0,
    read_replica_health_check_interval: int = 30,
    read_replica_circuit_breaker_failure_threshold: int = 5,
    read_replica_circuit_breaker_recovery_timeout: int = 60,
    read_replica_fallback_to_primary: bool = True
)
```

**Parameters:**
- `config` (DatabaseConfig): Database configuration
- `pool_size` (int): Connection pool size (default: 5)
- `logger` (BoundLogger, optional): Logger instance
- `max_reconnect_attempts` (int): Maximum reconnection attempts (default: 3)
- `read_replica_enabled` (bool): Enable read replica load balancing (default: True)
- `read_replica_selection_strategy` (str): Selection strategy: "round_robin", "least_connections", "lag_aware" (default: "round_robin")
- `read_replica_max_lag_seconds` (float): Maximum acceptable replication lag (default: 10.0)
- `read_replica_health_check_interval` (int): Health check interval in seconds (default: 30)
- `read_replica_circuit_breaker_failure_threshold` (int): Failures before excluding replica (default: 5)
- `read_replica_circuit_breaker_recovery_timeout` (int): Seconds before retrying excluded replica (default: 60)
- `read_replica_fallback_to_primary` (bool): Fall back to primary if replicas unavailable (default: True)

#### Methods

##### `async connect() -> None`

Create connection pools (primary and replicas).

**Raises:**
- `DatabaseError`: If connection fails

##### `async disconnect() -> None`

Close all connection pools.

##### `async health_check() -> bool`

Check database connection health.

**Returns:**
- `True` if healthy, `False` otherwise

##### `async fetch(query: str, *args: Any) -> list[asyncpg.Record]`

Execute SELECT query and return all rows. Automatically routes to replica if available.

**Parameters:**
- `query` (str): SQL query string
- `*args`: Query parameters

**Returns:**
- List of records

**Example:**
```python
records = await db_manager.fetch(
    "SELECT * FROM users WHERE age > $1",
    18
)
```

##### `async fetchval(query: str, *args: Any) -> Any`

Execute query and return single value. Routes to replica for SELECT queries.

**Parameters:**
- `query` (str): SQL query string
- `*args`: Query parameters

**Returns:**
- Single value or None

**Example:**
```python
count = await db_manager.fetchval(
    "SELECT COUNT(*) FROM users"
)
```

##### `async fetchrow(query: str, *args: Any) -> Optional[asyncpg.Record]`

Execute query and return one row. Routes to replica for SELECT queries.

**Parameters:**
- `query` (str): SQL query string
- `*args`: Query parameters

**Returns:**
- Single record or None

##### `async execute(query: str, *args: Any) -> str`

Execute write query (DELETE, VACUUM, etc.). Always uses primary database.

**Parameters:**
- `query` (str): SQL query string
- `*args`: Query parameters

**Returns:**
- Command status string

**Example:**
```python
await db_manager.execute(
    "DELETE FROM users WHERE id = $1",
    user_id
)
```

##### `@asynccontextmanager async acquire_connection() -> AsyncGenerator[asyncpg.Connection, None]`

Acquire connection from primary pool (context manager).

**Example:**
```python
async with db_manager.acquire_connection() as conn:
    await conn.execute("SELECT 1")
```

##### `@asynccontextmanager async transaction() -> AsyncGenerator[asyncpg.Connection, None]`

Start database transaction (always uses primary).

**Example:**
```python
async with db_manager.transaction() as conn:
    await conn.execute("DELETE FROM users WHERE id = $1", user_id)
    await conn.execute("INSERT INTO archive_users ...")
```

---

### BatchProcessor

Handles batch selection and processing of records.

#### Constructor

```python
BatchProcessor(
    db_manager: DatabaseManager,
    db_config: DatabaseConfig,
    table_config: TableConfig,
    logger: Optional[BoundLogger] = None,
    query_analyzer: Optional[QueryAnalyzer] = None
)
```

**Parameters:**
- `db_manager` (DatabaseManager): Database manager instance
- `db_config` (DatabaseConfig): Database configuration
- `table_config` (TableConfig): Table configuration
- `logger` (BoundLogger, optional): Logger instance
- `query_analyzer` (QueryAnalyzer, optional): Query analyzer for performance monitoring

#### Methods

##### `async count_eligible_records() -> int`

Count records eligible for archival.

**Returns:**
- Number of eligible records

##### `async select_batch(batch_size: int, last_cursor: Optional[Any] = None) -> Optional[list[asyncpg.Record]]`

Select a batch of records for archival.

**Parameters:**
- `batch_size` (int): Number of records to select
- `last_cursor` (Any, optional): Cursor from previous batch for pagination

**Returns:**
- List of records or None if no more records

**Example:**
```python
batch = await batch_processor.select_batch(batch_size=5000)
if batch:
    # Process batch
    pass
```

##### `extract_primary_keys(records: list[asyncpg.Record]) -> list[Any]`

Extract primary key values from records.

**Parameters:**
- `records` (list): List of records

**Returns:**
- List of primary key values

---

### S3Client

Manages S3 operations for archive storage.

#### Constructor

```python
S3Client(
    config: S3Config,
    logger: Optional[BoundLogger] = None
)
```

**Parameters:**
- `config` (S3Config): S3 configuration
- `logger` (BoundLogger, optional): Logger instance

#### Methods

##### `async upload_file(local_path: Path, s3_key: str, verify: bool = True) -> dict[str, Any]`

Upload file to S3.

**Parameters:**
- `local_path` (Path): Local file path
- `s3_key` (str): S3 object key
- `verify` (bool): Verify upload after completion (default: True)

**Returns:**
- Dictionary with upload metadata:
  - `s3_key`: S3 object key
  - `size_bytes`: File size
  - `checksum`: SHA256 checksum

**Example:**
```python
result = await s3_client.upload_file(
    Path("/tmp/batch.jsonl.gz"),
    "archives/db/table/batch_001.jsonl.gz"
)
```

##### `async download_file(s3_key: str, local_path: Path) -> None`

Download file from S3.

**Parameters:**
- `s3_key` (str): S3 object key
- `local_path` (Path): Local destination path

##### `async object_exists(s3_key: str) -> bool`

Check if object exists in S3.

**Parameters:**
- `s3_key` (str): S3 object key

**Returns:**
- `True` if exists, `False` otherwise

---

### VacuumManager

Manages PostgreSQL VACUUM operations for space reclamation.

#### Constructor

```python
VacuumManager(
    db_manager: DatabaseManager,
    logger: Optional[BoundLogger] = None,
    vacuum_timeout_seconds: int = 3600,
    min_reclaim_percentage: float = 10.0,
    min_table_size_for_warning_mb: float = 100.0
)
```

**Parameters:**
- `db_manager` (DatabaseManager): Database manager instance
- `logger` (BoundLogger, optional): Logger instance
- `vacuum_timeout_seconds` (int): Maximum vacuum duration (default: 3600)
- `min_reclaim_percentage` (float): Minimum reclaim percentage to avoid warning (default: 10.0)
- `min_table_size_for_warning_mb` (float): Minimum table size for effectiveness warning (default: 100.0)

#### Methods

##### `async get_table_size(schema_name: str, table_name: str) -> dict[str, int]`

Get table size information.

**Parameters:**
- `schema_name` (str): Schema name
- `table_name` (str): Table name

**Returns:**
- Dictionary with:
  - `total_size_bytes`: Total size including indexes
  - `table_size_bytes`: Table size only
  - `indexes_size_bytes`: Indexes size

##### `async vacuum_table_after_archive(
    schema_name: str,
    table_name: str,
    vacuum_type: str = "analyze",
    dry_run: bool = False
) -> dict[str, Any]`

Run VACUUM after archival.

**Parameters:**
- `schema_name` (str): Schema name
- `table_name` (str): Table name
- `vacuum_type` (str): Vacuum type: "none", "analyze", "standard", "full" (default: "analyze")
- `dry_run` (bool): If True, don't execute vacuum (default: False)

**Returns:**
- Dictionary with results:
  - `success` (bool): Whether vacuum succeeded
  - `vacuum_type` (str): Type of vacuum performed
  - `space_reclaimed_bytes` (int): Space reclaimed
  - `space_reclaimed_percentage` (float): Percentage reclaimed
  - `duration_seconds` (float): Vacuum duration

**Example:**
```python
result = await vacuum_manager.vacuum_table_after_archive(
    "public",
    "audit_logs",
    vacuum_type="standard"
)
print(f"Reclaimed {result['space_reclaimed_bytes']} bytes")
```

---

### QueryAnalyzer

Analyzes query execution plans for performance monitoring.

#### Constructor

```python
QueryAnalyzer(
    logger: Optional[BoundLogger] = None,
    slow_query_threshold: float = 2.0,
    warn_on_seq_scan: bool = True
)
```

**Parameters:**
- `logger` (BoundLogger, optional): Logger instance
- `slow_query_threshold` (float): Threshold in seconds for slow query warnings (default: 2.0)
- `warn_on_seq_scan` (bool): Warn on sequential scans (default: True)

#### Methods

##### `async analyze_query_plan(
    db_manager: DatabaseManager,
    query: str,
    params: tuple[Any, ...],
    query_time: float,
    database: str,
    table: str
) -> dict[str, Any]`

Analyze query execution plan.

**Parameters:**
- `db_manager` (DatabaseManager): Database manager
- `query` (str): SQL query
- `params` (tuple): Query parameters
- `query_time` (float): Query execution time in seconds
- `database` (str): Database name
- `table` (str): Table name

**Returns:**
- Dictionary with analysis results:
  - `is_slow` (bool): Whether query is slow
  - `has_seq_scan` (bool): Whether query uses sequential scan
  - `execution_time` (float): Execution time
  - `plan` (dict): Query plan details
  - `suggestions` (list): Optimization suggestions

---

## Configuration Classes

### ArchiverConfig

Main configuration class.

**Fields:**
- `version` (str): Configuration version
- `s3` (S3Config): S3 configuration
- `defaults` (DefaultsConfig): Default settings
- `databases` (list[DatabaseConfig]): Database configurations
- `notifications` (NotificationConfig, optional): Notification settings
- `monitoring` (MonitoringConfig, optional): Monitoring settings
- `legal_holds` (LegalHoldConfig, optional): Legal hold settings
- `compliance` (ComplianceConfig, optional): Compliance settings

### DatabaseConfig

Database configuration.

**Fields:**
- `name` (str): Database name
- `host` (str): Database host
- `port` (int): Database port (default: 5432)
- `user` (str): Database user
- `password_env` (str, optional): Environment variable for password
- `password` (str, optional): Database password (development only)
- `read_replica` (str, optional): Single read replica host (deprecated)
- `read_replicas` (list[dict], optional): List of read replica configurations
- `connection_pool_size` (int, optional): Connection pool size override
- `tables` (list[TableConfig]): List of tables to archive

**Read Replica Configuration:**
```python
read_replicas = [
    {
        "host": "replica1.example.com",
        "port": 5432,
        "weight": 1.0,
        "name": "replica-1"
    }
]
```

### TableConfig

Table configuration.

**Fields:**
- `name` (str): Table name
- `schema_name` (str): Schema name (default: "public")
- `timestamp_column` (str): Timestamp column for age-based filtering
- `primary_key` (str): Primary key column name
- `retention_days` (int, optional): Retention period override
- `batch_size` (int, optional): Batch size override
- `critical` (bool): Critical flag (default: False)
- `vacuum_after_archive` (bool, optional): Vacuum override
- `vacuum_type` (str, optional): Vacuum type override

### DefaultsConfig

Default settings applied to all databases/tables.

**Key Fields:**
- `batch_size` (int): Default batch size (default: 10000)
- `retention_days` (int): Default retention period (default: 90)
- `query_plan_analysis` (bool): Enable query analysis (default: True)
- `slow_query_threshold` (float): Slow query threshold in seconds (default: 2.0)
- `async_upload_pipeline` (bool): Enable async uploads (default: False)
- `vacuum_after_archive` (bool): Run VACUUM after archival (default: False)
- `vacuum_type` (str): Vacuum type: "none", "analyze", "standard", "full" (default: "analyze")
- `read_replica_enabled` (bool): Enable read replica load balancing (default: True)
- `read_replica_selection_strategy` (str): Selection strategy (default: "round_robin")

---

## Utility Functions

### `load_config(config_path: Path) -> ArchiverConfig`

Load configuration from YAML file.

**Parameters:**
- `config_path` (Path): Path to configuration file

**Returns:**
- `ArchiverConfig` instance

**Example:**
```python
from pathlib import Path
from archiver.config import load_config

config = load_config(Path("config.yaml"))
```

---

## Error Handling

### Exceptions

#### `ArchiverError`

Base exception for all archiver errors.

**Attributes:**
- `message` (str): Error message
- `context` (dict): Additional context

#### `DatabaseError`

Database-related errors.

#### `S3Error`

S3-related errors.

#### `ConfigurationError`

Configuration validation errors.

**Example:**
```python
from archiver.exceptions import ArchiverError, DatabaseError

try:
    await archiver.archive()
except DatabaseError as e:
    print(f"Database error: {e.message}")
    print(f"Context: {e.context}")
except ArchiverError as e:
    print(f"Archiver error: {e.message}")
```

---

## Usage Examples

### Basic Archival

```python
from archiver import Archiver
from archiver.config import load_config
from pathlib import Path

# Load configuration
config = load_config(Path("config.yaml"))

# Create archiver
archiver = Archiver(config)

# Run archival
result = await archiver.archive()

print(f"Archived {result['records_archived']} records")
print(f"Processed {result['tables_processed']} tables")
```

### Custom Database Manager

```python
from archiver.database import DatabaseManager
from archiver.config import DatabaseConfig

# Create database config
db_config = DatabaseConfig(
    name="mydb",
    host="localhost",
    port=5432,
    user="archiver",
    password_env="DB_PASSWORD",
    read_replicas=[
        {"host": "replica1.example.com", "port": 5432, "weight": 1.0}
    ],
    tables=[...]
)

# Create database manager with custom settings
db_manager = DatabaseManager(
    db_config,
    pool_size=10,
    read_replica_enabled=True,
    read_replica_selection_strategy="lag_aware"
)

await db_manager.connect()

# Use for queries
count = await db_manager.fetchval("SELECT COUNT(*) FROM users")
```

### Programmatic Configuration

```python
from archiver.config import (
    ArchiverConfig,
    S3Config,
    DatabaseConfig,
    TableConfig,
    DefaultsConfig
)

# Create configuration programmatically
config = ArchiverConfig(
    version="2.0",
    s3=S3Config(
        bucket="my-bucket",
        prefix="archives/",
        region="us-east-1"
    ),
    defaults=DefaultsConfig(
        batch_size=5000,
        retention_days=90,
        read_replica_enabled=True
    ),
    databases=[
        DatabaseConfig(
            name="mydb",
            host="localhost",
            user="archiver",
            password_env="DB_PASSWORD",
            tables=[
                TableConfig(
                    name="audit_logs",
                    timestamp_column="created_at",
                    primary_key="id"
                )
            ]
        )
    ]
)

archiver = Archiver(config)
await archiver.archive()
```

---

## Best Practices

1. **Always use environment variables for passwords**: Use `password_env` instead of `password`
2. **Enable query plan analysis**: Set `query_plan_analysis: true` for performance monitoring
3. **Use read replicas**: Configure `read_replicas` for better performance
4. **Monitor slow queries**: Set appropriate `slow_query_threshold`
5. **Enable async uploads**: Set `async_upload_pipeline: true` for better throughput
6. **Configure VACUUM**: Enable `vacuum_after_archive` to reclaim space
7. **Use dry-run for testing**: Test with `dry_run=True` before production runs

---

## See Also

- [Configuration Guide](configuration.md)
- [Performance Tuning](performance-tuning.md)
- [Read Replica Load Balancing](read-replica-load-balancing.md)
- [Troubleshooting](troubleshooting.md)

