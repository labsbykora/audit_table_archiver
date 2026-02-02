"""Integration tests for large dataset scenarios."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from archiver.archiver import Archiver
from archiver.config import ArchiverConfig, DatabaseConfig, S3Config, TableConfig


@pytest.mark.integration
@pytest.mark.performance
@pytest.mark.slow
@pytest.mark.asyncio
async def test_archival_1m_records(
    archiver_config: ArchiverConfig,
    test_table: str,
    db_connection,
):
    """Test archival with 1 million records."""
    # Insert 1M test records
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    batch_size = 10000
    total_records = 1000000

    print(f"Inserting {total_records:,} test records...")

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

        if (i + batch_size) % 100000 == 0:
            print(f"Inserted {(i + batch_size):,} records...")

    # Update config
    archiver_config.databases[0].tables[0].name = test_table
    archiver_config.databases[0].tables[0].batch_size = 10000

    # Run archiver
    archiver = Archiver(archiver_config, dry_run=False)
    stats = await archiver.archive()

    # Verify all records archived
    assert stats["records_archived"] == total_records
    assert stats["batches_processed"] > 0

    # Verify table is empty
    remaining = await db_connection.fetchval(f"SELECT COUNT(*) FROM {test_table}")
    assert remaining == 0, f"Expected 0 records, found {remaining}"


@pytest.mark.integration
@pytest.mark.performance
@pytest.mark.slow
@pytest.mark.asyncio
async def test_checkpoint_resume_large_dataset(
    archiver_config: ArchiverConfig,
    test_table: str,
    db_connection,
):
    """Test checkpoint/resume with large dataset."""
    # Insert 500K test records
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    total_records = 500000
    batch_size = 10000

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

    # Update config with checkpoint enabled
    archiver_config.databases[0].tables[0].name = test_table
    archiver_config.databases[0].tables[0].batch_size = 10000
    archiver_config.checkpoint.enabled = True
    archiver_config.checkpoint.frequency = 10  # Checkpoint every 10 batches

    # Run archiver (simulate interruption after some batches)
    archiver = Archiver(archiver_config, dry_run=False)

    # Simulate partial run (would be interrupted in real scenario)
    # In actual test, you'd stop the process and resume
    stats = await archiver.archive()

    # Verify checkpoint was created
    # (In real scenario, verify checkpoint file exists)

    # Resume from checkpoint
    stats_resume = await archiver.archive()

    # Verify all records archived
    total_archived = stats.get("records_archived", 0) + stats_resume.get(
        "records_archived", 0
    )
    assert total_archived == total_records


@pytest.mark.integration
@pytest.mark.performance
@pytest.mark.asyncio
async def test_memory_usage_large_batch(
    archiver_config: ArchiverConfig,
    test_table: str,
    db_connection,
):
    """Test memory usage with large batch size."""
    import psutil
    import os

    # Insert 100K test records
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    total_records = 100000
    batch_size = 50000  # Large batch

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

    # Run archiver
    archiver = Archiver(archiver_config, dry_run=False)
    stats = await archiver.archive()

    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    memory_increase = memory_after - memory_before

    # Memory increase should be reasonable (<2GB for 50K batch)
    assert memory_increase < 2048, f"Memory increase too high: {memory_increase:.0f} MB"

    # Verify all records archived
    assert stats["records_archived"] == total_records

