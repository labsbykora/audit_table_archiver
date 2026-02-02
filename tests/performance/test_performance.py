"""Performance benchmarks and tests."""

import time
from datetime import datetime, timedelta, timezone

import pytest

from archiver.batch_processor import BatchProcessor
from archiver.compressor import Compressor
from archiver.serializer import PostgreSQLSerializer


@pytest.mark.performance
@pytest.mark.asyncio
async def test_serialization_performance() -> None:
    """Test serialization performance."""
    serializer = PostgreSQLSerializer()

    # Create test data (1000 records)
    test_records = []
    for i in range(1000):
        test_records.append(
            {
                "id": i,
                "user_id": i % 10,
                "action": f"action_{i}",
                "metadata": {"key": f"value_{i}", "nested": {"inner": i}},
                "created_at": datetime.now(timezone.utc),
            }
        )

    # Measure serialization time
    start = time.time()
    serialized = [
        serializer.serialize_row(
            row=row,
            batch_id="test-batch",
            database_name="test_db",
            table_name="test_table",
            archived_at=datetime.now(timezone.utc),
        )
        for row in test_records
    ]
    jsonl_data = serializer.to_jsonl(serialized)
    elapsed = time.time() - start

    # Should serialize 1000 records in <1 second
    assert elapsed < 1.0, f"Serialization too slow: {elapsed:.2f}s for 1000 records"

    records_per_second = 1000 / elapsed
    assert records_per_second > 1000, f"Too slow: {records_per_second:.0f} records/second"


@pytest.mark.performance
def test_compression_performance() -> None:
    """Test compression performance."""
    compressor = Compressor(compression_level=6)

    # Create test data (1MB)
    test_data = b"test data " * 100000  # ~1MB

    # Measure compression time
    start = time.time()
    compressed, uncompressed_size, compressed_size = compressor.compress(test_data)
    elapsed = time.time() - start

    # Should compress 1MB in <1 second
    assert elapsed < 1.0, f"Compression too slow: {elapsed:.2f}s for 1MB"

    mb_per_second = (uncompressed_size / 1024 / 1024) / elapsed
    assert mb_per_second > 1, f"Too slow: {mb_per_second:.2f} MB/s"


@pytest.mark.performance
@pytest.mark.asyncio
async def test_batch_selection_performance(
    db_connection, test_table: str
) -> None:
    """Test batch selection performance."""
    import os
    from archiver.config import DatabaseConfig, TableConfig
    from archiver.database import DatabaseManager

    # Insert 10,000 test records
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    for i in range(10000):
        await db_connection.execute(
            f"""
            INSERT INTO {test_table} (user_id, action, created_at)
            VALUES ($1, $2, $3)
            """,
            i % 10,
            f"action_{i}",
            old_date + timedelta(seconds=i),
        )

    os.environ["TEST_DB_PASSWORD"] = "archiver_password"

    table_config = TableConfig(
        name=test_table,
        schema="public",
        timestamp_column="created_at",
        primary_key="id",
        retention_days=90,
        batch_size=1000,
    )

    db_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="TEST_DB_PASSWORD",
        tables=[table_config],
    )

    db_manager = DatabaseManager(db_config)
    await db_manager.connect()

    try:
        batch_processor = BatchProcessor(db_manager, db_config, table_config)

        # Measure batch selection time
        start = time.time()
        records = await batch_processor.select_batch(batch_size=1000)
        elapsed = time.time() - start

        # Should select 1000 records in <5 seconds
        assert elapsed < 5.0, f"Batch selection too slow: {elapsed:.2f}s for 1000 records"

        records_per_second = len(records) / elapsed
        assert records_per_second > 200, f"Too slow: {records_per_second:.0f} records/second"

    finally:
        await db_manager.disconnect()
        # Cleanup
        await db_connection.execute(f"DELETE FROM {test_table}")


@pytest.mark.performance
@pytest.mark.asyncio
async def test_end_to_end_throughput(
    archiver_config, test_table: str, db_connection
) -> None:
    """Test end-to-end throughput (target: >10K records/minute)."""
    from archiver.archiver import Archiver

    # Insert 10,000 test records
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    for i in range(10000):
        await db_connection.execute(
            f"""
            INSERT INTO {test_table} (user_id, action, created_at)
            VALUES ($1, $2, $3)
            """,
            i % 10,
            f"action_{i}",
            old_date + timedelta(seconds=i),
        )

    # Update config
    archiver_config.databases[0].tables[0].name = test_table
    archiver_config.databases[0].tables[0].batch_size = 1000

    archiver = Archiver(archiver_config, dry_run=False)

    # Measure archival time
    start = time.time()
    stats = await archiver.archive()
    elapsed = time.time() - start

    records_archived = stats["records_archived"]
    records_per_minute = (records_archived / elapsed) * 60

    # Target: >10,000 records/minute
    assert (
        records_per_minute > 10000
    ), f"Throughput too low: {records_per_minute:.0f} records/minute (target: >10,000)"

    print(f"\nPerformance: {records_per_minute:.0f} records/minute")
    print(f"Total time: {elapsed:.2f} seconds")
    print(f"Records archived: {records_archived}")

