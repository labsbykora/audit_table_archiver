"""Integration tests for Phase 4: Restore utility."""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from archiver.config import DatabaseConfig, S3Config, TableConfig
from archiver.database import DatabaseManager
from restore.restore_engine import RestoreEngine
from restore.s3_reader import S3ArchiveReader
from utils.checksum import ChecksumCalculator


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restore_from_archive(
    db_connection,
    s3_client,
    test_table,
    test_data,
):
    """Test end-to-end restore from S3 archive."""
    # First, create an archive by archiving some data
    # This simulates a real-world scenario where data was archived and needs to be restored

    # test_data is already inserted by fixture, but we need it in the format we expect
    # Get the actual data from the database
    rows = await db_connection.fetch(
        f"SELECT id, user_id, action, metadata, created_at FROM {test_table} ORDER BY id"
    )

    # Create a simple archive file in S3
    s3_config = S3Config(
        bucket="test-archives",
        region=os.getenv("S3_REGION", "us-east-1"),
        prefix="test/",
        endpoint=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
    )

    # Serialize test data to JSONL
    jsonl_content = "\n".join(
        json.dumps(
            {
                "id": row["id"],
                "user_id": row["user_id"],
                "action": row["action"],
                "metadata": row["metadata"],
                "created_at": (
                    row["created_at"].isoformat()
                    if isinstance(row["created_at"], datetime)
                    else str(row["created_at"])
                ),
            }
        )
        for row in rows
    )

    # Compress and upload
    import gzip

    jsonl_bytes = jsonl_content.encode("utf-8")
    compressed = gzip.compress(jsonl_bytes)

    # Calculate checksum on uncompressed data (as expected by metadata)
    checksum_calc = ChecksumCalculator()
    checksum = checksum_calc.calculate_sha256(jsonl_bytes)

    # Upload data file - write to temp file first
    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
        tmp_file.write(compressed)
        tmp_file_path = Path(tmp_file.name)

    try:
        s3_key = f"test_db/public/{test_table}/year=2026/month=01/day=06/test_batch.jsonl.gz"
        s3_client.upload_file(
            file_path=tmp_file_path,
            s3_key=s3_key,
        )

        # Create metadata file
        metadata = {
            "batch_info": {
                "batch_id": "test_batch",
                "database": "test_db",
                "schema": "public",
                "table": test_table,
                "archived_at": datetime.now(timezone.utc).isoformat(),
            },
            "data_info": {
                "record_count": len(rows),
                "total_size_bytes": len(compressed),
                "compression": "gzip",
            },
            "checksums": {
                "jsonl_sha256": checksum,
            },
            "schema": {
                "columns": [
                    {"name": "id", "data_type": "bigint", "is_nullable": False},
                    {"name": "user_id", "data_type": "integer", "is_nullable": True},
                    {"name": "action", "data_type": "text", "is_nullable": False},
                    {"name": "metadata", "data_type": "jsonb", "is_nullable": True},
                    {
                        "name": "created_at",
                        "data_type": "timestamp with time zone",
                        "is_nullable": False,
                    },
                ],
                "primary_key": {"columns": ["id"]},
            },
        }

        metadata_key = s3_key.replace(".jsonl.gz", ".metadata.json")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as meta_file:
            json.dump(metadata, meta_file, indent=2)
            meta_file_path = Path(meta_file.name)

        try:
            s3_client.upload_file(
                file_path=meta_file_path,
                s3_key=metadata_key,
            )
        finally:
            meta_file_path.unlink(missing_ok=True)
    finally:
        tmp_file_path.unlink(missing_ok=True)

    # Now test restore
    # First, delete the original data
    await db_connection.execute(f"DELETE FROM {test_table}")

    # Set environment variable for database password
    os.environ["TEST_DB_PASSWORD"] = "archiver_password"

    # Create restore engine
    db_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="TEST_DB_PASSWORD",
        tables=[
            TableConfig(
                name=test_table,
                schema_name="public",
                timestamp_column="created_at",
                primary_key="id",
                retention_days=90,
            )
        ],
    )
    db_manager = DatabaseManager(db_config)
    await db_manager.connect()

    try:
        restore_engine = RestoreEngine(
            db_manager=db_manager,
        )

        # Read archive first, then restore
        from restore.s3_reader import S3ArchiveReader

        reader = S3ArchiveReader(s3_config)
        full_s3_key = f"test/{s3_key}"  # Add prefix that was added during upload
        archive_file = await reader.read_archive(full_s3_key, validate_checksum=True)

        # Restore the archive
        stats = await restore_engine.restore_archive(
            archive=archive_file,
            conflict_strategy="skip",
            drop_indexes=False,
        )

        # Verify restore was successful
        assert stats["records_restored"] == len(rows)
        assert stats["records_processed"] == len(rows)
        assert stats["records_failed"] == 0

        # Verify data was restored
        restored_data = await db_connection.fetch(
            f"SELECT id, user_id, action, metadata, created_at FROM {test_table} ORDER BY id"
        )
        assert len(restored_data) == len(rows)

        # Verify data integrity - compare with original rows
        for i, restored_row in enumerate(restored_data):
            assert restored_row["id"] == rows[i]["id"]
            assert restored_row["action"] == rows[i]["action"]
            assert restored_row["user_id"] == rows[i]["user_id"]

    finally:
        await db_manager.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restore_with_conflict_detection(
    db_connection,
    s3_client,
    test_table,
    test_data,
):
    """Test restore with conflict detection."""
    # Get all existing data (100 records from fixture)
    all_rows = await db_connection.fetch(
        f"SELECT id, user_id, action, metadata, created_at FROM {test_table} ORDER BY id"
    )

    # Delete all but the first 2 records to simulate partial data
    # This way, when we restore all 100, only 2 will conflict
    await db_connection.execute(
        f"DELETE FROM {test_table} WHERE id NOT IN (SELECT id FROM {test_table} ORDER BY id LIMIT 2)"
    )

    # Create archive with all original test data (100 records)
    s3_config = S3Config(
        bucket="test-archives",
        region=os.getenv("S3_REGION", "us-east-1"),
        prefix="test/",
        endpoint=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
    )

    jsonl_content = "\n".join(
        json.dumps(
            {
                "id": row["id"],
                "user_id": row["user_id"],
                "action": row["action"],
                "metadata": row["metadata"],
                "created_at": (
                    row["created_at"].isoformat()
                    if isinstance(row["created_at"], datetime)
                    else str(row["created_at"])
                ),
            }
        )
        for row in all_rows
    )

    import gzip

    jsonl_bytes = jsonl_content.encode("utf-8")
    compressed = gzip.compress(jsonl_bytes)

    checksum_calc = ChecksumCalculator()
    checksum = checksum_calc.calculate_sha256(jsonl_bytes)  # Calculate on uncompressed data

    # Upload data file - write to temp file first
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
        tmp_file.write(compressed)
        tmp_file_path = Path(tmp_file.name)

    try:
        s3_key = (
            f"test_db/public/{test_table}/year=2026/month=01/day=06/test_batch_conflict.jsonl.gz"
        )
        s3_client.upload_file(
            file_path=tmp_file_path,
            s3_key=s3_key,
        )

        metadata = {
            "batch_info": {
                "batch_id": "test_batch_conflict",
                "database": "test_db",
                "schema": "public",
                "table": test_table,
                "archived_at": datetime.now(timezone.utc).isoformat(),
            },
            "data_info": {
                "record_count": len(all_rows),
                "total_size_bytes": len(compressed),
                "compression": "gzip",
            },
            "checksums": {
                "jsonl_sha256": checksum,
            },
            "schema": {
                "columns": [
                    {"name": "id", "data_type": "bigint", "is_nullable": False},
                    {"name": "user_id", "data_type": "integer", "is_nullable": True},
                    {"name": "action", "data_type": "text", "is_nullable": False},
                    {"name": "metadata", "data_type": "jsonb", "is_nullable": True},
                    {
                        "name": "created_at",
                        "data_type": "timestamp with time zone",
                        "is_nullable": False,
                    },
                ],
                "primary_key": {"columns": ["id"]},
            },
        }

        metadata_key = s3_key.replace(".jsonl.gz", ".metadata.json")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as meta_file:
            json.dump(metadata, meta_file, indent=2)
            meta_file_path = Path(meta_file.name)

        try:
            s3_client.upload_file(
                file_path=meta_file_path,
                s3_key=metadata_key,
            )
        finally:
            meta_file_path.unlink(missing_ok=True)
    finally:
        tmp_file_path.unlink(missing_ok=True)

    # Set environment variable for database password
    os.environ["TEST_DB_PASSWORD"] = "archiver_password"

    # Test restore with skip strategy (should skip conflicts)
    db_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="TEST_DB_PASSWORD",
        tables=[
            TableConfig(
                name=test_table,
                schema_name="public",
                timestamp_column="created_at",
                primary_key="id",
                retention_days=90,
            )
        ],
    )
    db_manager = DatabaseManager(db_config)
    await db_manager.connect()

    try:
        restore_engine = RestoreEngine(
            db_manager=db_manager,
        )

        # Read archive first, then restore
        from restore.s3_reader import S3ArchiveReader

        reader = S3ArchiveReader(s3_config)
        full_s3_key = f"{s3_config.prefix}{s3_key}" if s3_config.prefix else s3_key
        archive_file = await reader.read_archive(full_s3_key, validate_checksum=True)

        stats = await restore_engine.restore_archive(
            archive=archive_file,
            conflict_strategy="skip",
            detect_conflicts=True,
        )

        # Should have detected conflicts (first 2 records already exist)
        # With "skip" strategy, conflicting records are filtered out, but non-conflicting ones are restored
        assert stats["conflicts_detected"] == 2
        assert (
            stats["records_restored"] == len(all_rows) - 2
        )  # 98 records restored (100 total - 2 conflicts)

        # Verify final state - with skip strategy, 2 existing + 98 restored = 100 total
        final_count = await db_connection.fetchval(f"SELECT COUNT(*) FROM {test_table}")
        assert final_count == len(
            all_rows
        )  # All 100 records should exist (2 existing + 98 restored)

    finally:
        await db_manager.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restore_s3_reader_integration(
    db_connection,
    s3_client,
    test_table,
    test_data,
):
    """Test S3ArchiveReader integration with real S3."""
    # Use the same S3 config as the fixture for consistency
    s3_config = s3_client.config

    # Get data from database
    rows = await db_connection.fetch(
        f"SELECT id, user_id, action, metadata, created_at FROM {test_table} ORDER BY id"
    )

    jsonl_content = "\n".join(
        json.dumps(
            {
                "id": row["id"],
                "user_id": row["user_id"],
                "action": row["action"],
                "metadata": row["metadata"],
                "created_at": (
                    row["created_at"].isoformat()
                    if isinstance(row["created_at"], datetime)
                    else str(row["created_at"])
                ),
            }
        )
        for row in rows
    )

    import gzip

    jsonl_bytes = jsonl_content.encode("utf-8")
    compressed = gzip.compress(jsonl_bytes)

    checksum_calc = ChecksumCalculator()
    checksum = checksum_calc.calculate_sha256(jsonl_bytes)  # Calculate on uncompressed data

    # Upload data file - write to temp file first
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
        tmp_file.write(compressed)
        tmp_file_path = Path(tmp_file.name)

    try:
        s3_key = f"test_db/public/{test_table}/year=2026/month=01/day=06/test_reader.jsonl.gz"
        s3_client.upload_file(
            file_path=tmp_file_path,
            s3_key=s3_key,
        )

        metadata = {
            "batch_info": {
                "batch_id": "test_reader",
                "database": "test_db",
                "schema": "public",
                "table": test_table,
                "archived_at": datetime.now(timezone.utc).isoformat(),
            },
            "data_info": {
                "record_count": len(rows),
                "total_size_bytes": len(compressed),
                "compression": "gzip",
            },
            "checksums": {
                "jsonl_sha256": checksum,
            },
        }

        metadata_key = s3_key.replace(".jsonl.gz", ".metadata.json")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as meta_file:
            json.dump(metadata, meta_file, indent=2)
            meta_file_path = Path(meta_file.name)

        try:
            s3_client.upload_file(
                file_path=meta_file_path,
                s3_key=metadata_key,
            )
        finally:
            meta_file_path.unlink(missing_ok=True)
    finally:
        tmp_file_path.unlink(missing_ok=True)

    # Test S3ArchiveReader
    reader = S3ArchiveReader(s3_config)

    # Read archive (with prefix)
    full_s3_key = f"{s3_config.prefix}{s3_key}" if s3_config.prefix else s3_key
    archive_file = await reader.read_archive(full_s3_key, validate_checksum=True)

    assert archive_file.record_count == len(rows)
    assert archive_file.metadata is not None
    assert archive_file.metadata["batch_info"]["batch_id"] == "test_reader"

    # Parse records
    records = archive_file.parse_records()
    assert len(records) == len(rows)

    # Verify first record
    assert records[0]["id"] == rows[0]["id"]
    assert records[0]["action"] == rows[0]["action"]

    # Test list archives
    archives = await reader.list_archives(
        database_name="test_db",
        table_name=test_table,
    )
    assert full_s3_key in archives or any(full_s3_key in a for a in archives)
