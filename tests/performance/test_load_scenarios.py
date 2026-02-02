"""Load testing scenarios for production readiness."""

import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

from archiver.archiver import Archiver
from archiver.config import ArchiverConfig


@pytest.mark.performance
@pytest.mark.slow
@pytest.mark.asyncio
async def test_sustained_load_1h(
    archiver_config: ArchiverConfig,
    test_table: str,
    db_connection,
):
    """Test sustained load for 1 hour to check for memory leaks."""
    import psutil
    import os

    # Insert large dataset
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    # Insert 100K records
    total_records = 100000
    batch_size = 10000

    print(f"Setting up test: Inserting {total_records:,} records...")

    for i in range(0, total_records, batch_size):
        values = []
        for j in range(batch_size):
            record_id = i + j
            values.append(
                f"({record_id}, {record_id % 10}, 'action_{record_id}', "
                f"'{old_date + timedelta(seconds=record_id)}'::timestamptz)"
            )

        await db_connection.execute(
            f"""
            INSERT INTO {test_table} (id, user_id, action, created_at)
            VALUES {', '.join(values)}
            """
        )

    # Update config
    archiver_config.databases[0].tables[0].name = test_table
    archiver_config.databases[0].tables[0].batch_size = 5000

    # Monitor memory over time
    process = psutil.Process(os.getpid())
    memory_samples = []

    start_time = time.time()
    end_time = start_time + 3600  # 1 hour

    iteration = 0
    while time.time() < end_time:
        iteration += 1
        print(f"Iteration {iteration}: {time.time() - start_time:.0f}s elapsed")

        # Re-insert data (simulate ongoing archival)
        for i in range(0, 50000, 10000):
            values = []
            for j in range(10000):
                record_id = i + j + (iteration * 100000)
                values.append(
                    f"({record_id}, {record_id % 10}, 'action_{record_id}', "
                    f"'{old_date + timedelta(seconds=record_id)}'::timestamptz)"
                )

            await db_connection.execute(
                f"""
                INSERT INTO {test_table} (id, user_id, action, created_at)
                VALUES {', '.join(values)}
                ON CONFLICT (id) DO NOTHING
                """
            )

        # Run archiver
        archiver = Archiver(archiver_config, dry_run=False)
        stats = await archiver.archive()

        # Sample memory
        memory_mb = process.memory_info().rss / 1024 / 1024
        memory_samples.append(memory_mb)

        print(f"  Memory: {memory_mb:.0f} MB, Archived: {stats.get('records_archived', 0)}")

        # Check for memory leak (memory should not grow unbounded)
        if len(memory_samples) > 10:
            recent_avg = sum(memory_samples[-10:]) / 10
            initial_avg = sum(memory_samples[:10]) / 10
            growth = recent_avg - initial_avg

            # Allow up to 50% growth over 1 hour (reasonable for Python GC)
            max_growth_mb = initial_avg * 0.5
            assert (
                growth < max_growth_mb
            ), f"Potential memory leak: {growth:.0f} MB growth (allowed: {max_growth_mb:.0f} MB)"

        # Wait before next iteration
        await asyncio.sleep(60)  # 1 minute between iterations

    print(f"\nLoad test completed: {len(memory_samples)} iterations")
    print(f"Memory range: {min(memory_samples):.0f} - {max(memory_samples):.0f} MB")
    print(f"Memory trend: Stable (no significant leak detected)")


@pytest.mark.performance
@pytest.mark.asyncio
async def test_high_concurrency_multiple_tables(
    archiver_config: ArchiverConfig,
    db_connection,
):
    """Test concurrent processing of multiple tables."""
    # Create multiple test tables
    tables = []
    for i in range(5):
        table_name = f"test_table_{i}"
        await db_connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                user_id INTEGER,
                action TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        # Insert 10K records per table
        now = datetime.now(timezone.utc)
        old_date = now - timedelta(days=100)

        for j in range(10000):
            await db_connection.execute(
                f"""
                INSERT INTO {table_name} (user_id, action, created_at)
                VALUES ($1, $2, $3)
                """,
                j % 10,
                f"action_{j}",
                old_date + timedelta(seconds=j),
            )

        tables.append(table_name)

    # Update config with multiple tables
    table_configs = []
    for table_name in tables:
        table_config = archiver_config.databases[0].tables[0].model_copy()
        table_config.name = table_name
        table_config.batch_size = 2000
        table_configs.append(table_config)

    archiver_config.databases[0].tables = table_configs

    # Run archiver (processes tables sequentially, but tests resource management)
    start_time = time.time()

    archiver = Archiver(archiver_config, dry_run=False)
    stats = await archiver.archive()

    elapsed = time.time() - start_time

    # Should process all tables successfully
    assert stats["tables_processed"] == len(tables)
    assert stats["records_archived"] == 10000 * len(tables)

    # Performance should be reasonable (allowing for overhead)
    records_per_minute = (stats["records_archived"] / elapsed) * 60
    assert (
        records_per_minute > 5000
    ), f"Concurrent processing too slow: {records_per_minute:.0f} records/min"

    print(f"\nConcurrent processing: {stats['records_archived']:,} records in {elapsed:.1f}s")
    print(f"Throughput: {records_per_minute:.0f} records/minute")

    # Cleanup
    for table_name in tables:
        await db_connection.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.performance
@pytest.mark.asyncio
async def test_large_batch_size(
    archiver_config: ArchiverConfig,
    test_table: str,
    db_connection,
):
    """Test with very large batch size to verify memory management."""
    import psutil
    import os

    # Insert 200K records
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    total_records = 200000
    batch_size = 50000  # Very large batch

    print(f"Setting up test: Inserting {total_records:,} records...")

    for i in range(0, total_records, 10000):
        values = []
        for j in range(10000):
            record_id = i + j
            values.append(
                f"({record_id}, {record_id % 10}, 'action_{record_id}', "
                f"'{old_date + timedelta(seconds=record_id)}'::timestamptz)"
            )

        await db_connection.execute(
            f"""
            INSERT INTO {test_table} (id, user_id, action, created_at)
            VALUES {', '.join(values)}
            """
        )

    # Update config with large batch
    archiver_config.databases[0].tables[0].name = test_table
    archiver_config.databases[0].tables[0].batch_size = batch_size

    # Monitor memory
    process = psutil.Process(os.getpid())
    memory_before = process.memory_info().rss / 1024 / 1024  # MB

    start_time = time.time()

    # Run archiver
    archiver = Archiver(archiver_config, dry_run=False)
    stats = await archiver.archive()

    elapsed = time.time() - start_time
    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    memory_peak = memory_after - memory_before

    # Memory should be reasonable even with large batches
    # Allow up to 2GB for 50K batch (assuming ~40KB per record average)
    assert (
        memory_peak < 2048
    ), f"Memory usage too high: {memory_peak:.0f} MB for {batch_size:,} batch"

    # Performance should still be good
    records_per_minute = (stats["records_archived"] / elapsed) * 60
    assert (
        records_per_minute > 10000
    ), f"Large batch performance too slow: {records_per_minute:.0f} records/min"

    print(f"\nLarge batch test: {stats['records_archived']:,} records in {elapsed:.1f}s")
    print(f"Memory peak: {memory_peak:.0f} MB")
    print(f"Throughput: {records_per_minute:.0f} records/minute")


@pytest.mark.performance
@pytest.mark.asyncio
async def test_connection_pool_stability(
    archiver_config: ArchiverConfig,
    test_table: str,
    db_connection,
):
    """Test connection pool stability over long run."""
    # Insert 50K records
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    total_records = 50000
    batch_size = 5000

    print(f"Setting up test: Inserting {total_records:,} records...")

    for i in range(0, total_records, 10000):
        values = []
        for j in range(10000):
            record_id = i + j
            values.append(
                f"({record_id}, {record_id % 10}, 'action_{record_id}', "
                f"'{old_date + timedelta(seconds=record_id)}'::timestamptz)"
            )

        await db_connection.execute(
            f"""
            INSERT INTO {test_table} (id, user_id, action, created_at)
            VALUES {', '.join(values)}
            """
        )

    # Update config with smaller batches to test many connections
    archiver_config.databases[0].tables[0].name = test_table
    archiver_config.databases[0].tables[0].batch_size = batch_size
    archiver_config.databases[0].connection_pool_size = 10  # Larger pool

    # Run archiver multiple times (simulates long-running process)
    for iteration in range(5):
        print(f"Iteration {iteration + 1}/5")

        # Re-insert some data
        for i in range(10000):
            record_id = i + (iteration * 10000)
            await db_connection.execute(
                f"""
                INSERT INTO {test_table} (id, user_id, action, created_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (id) DO NOTHING
                """,
                record_id,
                record_id % 10,
                f"action_{record_id}",
                old_date + timedelta(seconds=record_id),
            )

        # Run archiver (tests connection pool reuse)
        archiver = Archiver(archiver_config, dry_run=False)
        stats = await archiver.archive()

        print(f"  Archived: {stats.get('records_archived', 0):,} records")

        # Should not have connection errors
        assert stats.get("errors", 0) == 0, "Connection pool errors detected"

    print("\nConnection pool stability test: PASSED (no connection errors)")


@pytest.mark.performance
@pytest.mark.asyncio
async def test_database_load_impact(
    archiver_config: ArchiverConfig,
    test_table: str,
    db_connection,
):
    """Test that archival doesn't significantly impact database performance."""
    # This is a placeholder - actual database load monitoring would require
    # database metrics collection (pg_stat_statements, etc.)
    # In real scenario, monitor:
    # - CPU usage
    # - Connection count
    # - Query duration
    # - Lock waits

    print("Database load impact test - manual verification required")
    print("Monitor database metrics during archival:")
    print("  - CPU usage should be <5% increase")
    print("  - Connection count should be within pool limits")
    print("  - No significant increase in lock waits")
    print("  - Query duration should remain stable")

    # Insert test data
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    total_records = 100000

    print(f"\nInserting {total_records:,} test records...")

    for i in range(0, total_records, 10000):
        values = []
        for j in range(10000):
            record_id = i + j
            values.append(
                f"({record_id}, {record_id % 10}, 'action_{record_id}', "
                f"'{old_date + timedelta(seconds=record_id)}'::timestamptz)"
            )

        await db_connection.execute(
            f"""
            INSERT INTO {test_table} (id, user_id, action, created_at)
            VALUES {', '.join(values)}
            """
        )

    # Update config
    archiver_config.databases[0].tables[0].name = test_table
    archiver_config.databases[0].tables[0].batch_size = 10000

    # Run archiver (would monitor DB metrics in production)
    archiver = Archiver(archiver_config, dry_run=False)
    stats = await archiver.archive()

    print(f"\nArchival completed: {stats['records_archived']:,} records")
    print("Verify database metrics show <5% CPU impact")


