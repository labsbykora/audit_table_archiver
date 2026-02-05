"""Edge case tests for restore operations."""

import gzip
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from archiver.config import DatabaseConfig, S3Config, TableConfig
from archiver.exceptions import DatabaseError, S3Error
from restore.restore_engine import RestoreEngine
from restore.s3_reader import S3ArchiveReader
from utils.checksum import ChecksumCalculator


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restore_schema_change_during_operation(
    db_connection,
    s3_client,
    test_table: str,
):
    """Test restore when schema changes mid-operation."""
    # Create archive with schema v1 (3 columns)
    test_data = [
        {"id": 1, "user_id": 100, "action": "test_action"},
        {"id": 2, "user_id": 200, "action": "test_action2"},
    ]

    jsonl_content = "\n".join(json.dumps(record) for record in test_data)
    jsonl_bytes = jsonl_content.encode("utf-8")
    compressed = gzip.compress(jsonl_bytes)

    checksum_calc = ChecksumCalculator()
    checksum = checksum_calc.calculate_sha256(jsonl_bytes)

    # Upload archive
    s3_key = f"test_db/public/{test_table}/test_batch.jsonl.gz"
    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
        tmp_file.write(compressed)
        tmp_file_path = Path(tmp_file.name)

    s3_client.upload_file(file_path=tmp_file_path, s3_key=s3_key)

    # Create metadata
    metadata = {
        "batch_info": {
            "batch_id": "test_batch",
            "database": "test_db",
            "schema": "public",
            "table": test_table,
            "archived_at": datetime.now(timezone.utc).isoformat(),
        },
        "data_info": {
            "record_count": len(test_data),
            "total_size_bytes": len(compressed),
            "compression": "gzip",
        },
        "checksums": {"jsonl_sha256": checksum},
        "schema": {
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "user_id", "type": "integer"},
                {"name": "action", "type": "text"},
            ]
        },
    }

    metadata_key = f"{s3_key}.metadata.json"
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp_meta:
        json.dump(metadata, tmp_meta)
        tmp_meta_path = Path(tmp_meta.name)

    s3_client.upload_file(file_path=tmp_meta_path, s3_key=metadata_key)

    # Add new column to table (schema change)
    await db_connection.execute(
        f"ALTER TABLE {test_table} ADD COLUMN new_column TEXT DEFAULT 'default_value'"
    )

    # Restore archive (should handle schema migration)
    s3_config = S3Config(
        bucket="test-archives",
        region="us-east-1",
        prefix="test/",
        endpoint="http://localhost:9000",
    )

    db_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="TEST_DB_PASSWORD",
        tables=[
            TableConfig(
                name=test_table,
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )

    restore_engine = RestoreEngine(
        db_config=db_config,
        s3_config=s3_config,
        table_config=db_config.tables[0],
    )

    # Restore should succeed with schema migration
    result = await restore_engine.restore_archive(
        s3_key=s3_key,
        conflict_strategy="skip",
        schema_migration_strategy="lenient",
    )

    assert result["records_restored"] == len(test_data)
    assert result["records_failed"] == 0

    # Verify data restored correctly
    rows = await db_connection.fetch(f"SELECT * FROM {test_table} ORDER BY id")
    assert len(rows) == len(test_data)

    # Cleanup
    tmp_file_path.unlink()
    tmp_meta_path.unlink()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restore_partial_failure_rollback(
    db_connection,
    s3_client,
    test_table: str,
):
    """Test that partial restore failures rollback correctly."""
    # Create archive with invalid data (will cause failure)
    test_data = [
        {"id": 1, "user_id": 100, "action": "valid"},
        {"id": 2, "user_id": "invalid_type", "action": "invalid"},  # Invalid type
    ]

    jsonl_content = "\n".join(json.dumps(record) for record in test_data)
    jsonl_bytes = jsonl_content.encode("utf-8")
    compressed = gzip.compress(jsonl_bytes)

    checksum_calc = ChecksumCalculator()
    checksum = checksum_calc.calculate_sha256(jsonl_bytes)

    # Upload archive
    s3_key = f"test_db/public/{test_table}/test_batch.jsonl.gz"
    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
        tmp_file.write(compressed)
        tmp_file_path = Path(tmp_file.name)

    s3_client.upload_file(file_path=tmp_file_path, s3_key=s3_key)

    # Create metadata
    metadata = {
        "batch_info": {
            "batch_id": "test_batch",
            "database": "test_db",
            "schema": "public",
            "table": test_table,
            "archived_at": datetime.now(timezone.utc).isoformat(),
        },
        "data_info": {
            "record_count": len(test_data),
            "total_size_bytes": len(compressed),
            "compression": "gzip",
        },
        "checksums": {"jsonl_sha256": checksum},
        "schema": {
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "user_id", "type": "integer"},
                {"name": "action", "type": "text"},
            ]
        },
    }

    metadata_key = f"{s3_key}.metadata.json"
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp_meta:
        json.dump(metadata, tmp_meta)
        tmp_meta_path = Path(tmp_meta.name)

    s3_client.upload_file(file_path=tmp_meta_path, s3_key=metadata_key)

    # Get initial count
    initial_count = await db_connection.fetchval(f"SELECT COUNT(*) FROM {test_table}")

    # Attempt restore (should fail and rollback)
    s3_config = S3Config(
        bucket="test-archives",
        region="us-east-1",
        prefix="test/",
        endpoint="http://localhost:9000",
    )

    db_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="TEST_DB_PASSWORD",
        tables=[
            TableConfig(
                name=test_table,
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )

    restore_engine = RestoreEngine(
        db_config=db_config,
        s3_config=s3_config,
        table_config=db_config.tables[0],
    )

    # Restore should fail and rollback
    with pytest.raises(DatabaseError):
        await restore_engine.restore_archive(
            s3_key=s3_key,
            conflict_strategy="skip",
        )

    # Verify no partial data inserted (transaction rollback)
    final_count = await db_connection.fetchval(f"SELECT COUNT(*) FROM {test_table}")
    assert final_count == initial_count, "Transaction should have rolled back"

    # Cleanup
    tmp_file_path.unlink()
    tmp_meta_path.unlink()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restore_corrupted_archive(
    db_connection,
    s3_client,
    test_table: str,
):
    """Test restore with corrupted archive file."""
    # Create corrupted archive (invalid gzip)
    corrupted_data = b"This is not valid gzip data"

    s3_key = f"test_db/public/{test_table}/corrupted_batch.jsonl.gz"
    with tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl.gz") as tmp_file:
        tmp_file.write(corrupted_data)
        tmp_file_path = Path(tmp_file.name)

    s3_client.upload_file(file_path=tmp_file_path, s3_key=s3_key)

    # Attempt restore (should fail validation)
    s3_config = S3Config(
        bucket="test-archives",
        region="us-east-1",
        prefix="test/",
        endpoint="http://localhost:9000",
    )

    s3_reader = S3ArchiveReader(s3_config=s3_config)

    # Should raise error when trying to read corrupted file
    with pytest.raises(S3Error):
        await s3_reader.read_archive(s3_key=s3_key)

    # Cleanup
    tmp_file_path.unlink()
