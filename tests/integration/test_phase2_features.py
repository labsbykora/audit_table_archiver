"""Integration tests for Phase 2 features (checksums, metadata, watermarks, schema)."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from archiver.checkpoint import CheckpointManager
from archiver.database import DatabaseManager
from archiver.schema_detector import SchemaDetector
from archiver.watermark_manager import WatermarkManager


@pytest.mark.asyncio
async def test_watermark_save_and_load(s3_client, db_config, test_table):
    """Test watermark save and load from S3."""
    db_manager = DatabaseManager(db_config, pool_size=2)
    await db_manager.connect()

    try:
        watermark_manager = WatermarkManager(storage_type="s3")

        # Save watermark
        last_timestamp = datetime.now(timezone.utc) - timedelta(days=1)
        last_primary_key = 12345

        await watermark_manager.save_watermark(
            database_name=db_config.name,
            table_name=test_table,
            last_timestamp=last_timestamp,
            last_primary_key=last_primary_key,
            s3_client=s3_client,
            db_manager=db_manager,
        )

        # Load watermark
        watermark = await watermark_manager.load_watermark(
            database_name=db_config.name,
            table_name=test_table,
            s3_client=s3_client,
            db_manager=db_manager,
        )

        assert watermark is not None
        assert watermark["last_primary_key"] == last_primary_key  # Can be int or str
        assert isinstance(watermark["last_timestamp"], datetime)

    finally:
        await db_manager.disconnect()


@pytest.mark.asyncio
async def test_schema_detection(postgres_ready, db_config, test_table):
    """Test schema detection from PostgreSQL."""
    db_manager = DatabaseManager(db_config, pool_size=2)
    await db_manager.connect()

    try:
        schema_detector = SchemaDetector()

        schema = await schema_detector.detect_table_schema(
            db_manager=db_manager,
            schema_name="public",
            table_name=test_table,
        )

        assert schema is not None
        assert schema["table_name"] == test_table
        assert schema["schema_name"] == "public"
        assert "columns" in schema
        assert len(schema["columns"]) > 0
        assert "primary_key" in schema

        # Verify column structure
        for column in schema["columns"]:
            assert "name" in column
            assert "data_type" in column
            assert "is_nullable" in column  # Schema detector uses is_nullable, not nullable

    finally:
        await db_manager.disconnect()


@pytest.mark.asyncio
async def test_checkpoint_with_watermark(s3_client, db_config, test_table):
    """Test that checkpoint and watermark work together."""
    checkpoint_manager = CheckpointManager(storage_type="s3", checkpoint_interval=5)
    watermark_manager = WatermarkManager(storage_type="s3")

    from archiver.checkpoint import Checkpoint

    # Create and save checkpoint
    checkpoint = Checkpoint(
        database_name=db_config.name,
        table_name=test_table,
        schema_name="public",
        batch_number=5,
        last_timestamp=datetime.now(timezone.utc) - timedelta(days=1),
        last_primary_key=12345,
        records_archived=5000,
        batches_processed=5,
        checkpoint_time=datetime.now(timezone.utc),
    )

    await checkpoint_manager.save_checkpoint(
        checkpoint=checkpoint,
        s3_client=s3_client,
    )

    # Load checkpoint
    loaded_checkpoint = await checkpoint_manager.load_checkpoint(
        database_name=db_config.name,
        table_name=test_table,
        s3_client=s3_client,
    )

    assert loaded_checkpoint is not None
    assert loaded_checkpoint.batch_number == 5
    assert loaded_checkpoint.records_archived == 5000

    # Save watermark
    await watermark_manager.save_watermark(
        database_name=db_config.name,
        table_name=test_table,
        last_timestamp=checkpoint.last_timestamp,
        last_primary_key=checkpoint.last_primary_key,
        s3_client=s3_client,
    )

    # Load watermark
    watermark = await watermark_manager.load_watermark(
        database_name=db_config.name,
        table_name=test_table,
        s3_client=s3_client,
    )

    assert watermark is not None
    assert watermark["last_primary_key"] == checkpoint.last_primary_key  # Can be int or str

    # Clean up
    await checkpoint_manager.delete_checkpoint(
        database_name=db_config.name,
        table_name=test_table,
        s3_client=s3_client,
    )


@pytest.mark.asyncio
async def test_metadata_file_upload(s3_client, s3_config, test_table):
    """Test that metadata files are uploaded correctly."""
    import tempfile

    from archiver.metadata import MetadataGenerator

    metadata_generator = MetadataGenerator()

    metadata = metadata_generator.generate_batch_metadata(
        database_name="test_db",
        table_name=test_table,
        schema_name="public",
        batch_number=1,
        batch_id="batch_123",
        record_count=1000,
        jsonl_checksum="abc123def456",
        compressed_checksum="def456ghi789",
        uncompressed_size=100000,
        compressed_size=50000,
        primary_keys=[1, 2, 3, 4, 5],
    )

    metadata_json = metadata_generator.metadata_to_json(metadata)

    # Write to temp file and upload
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json", encoding="utf-8") as tmp_file:
        tmp_path = Path(tmp_file.name)
        tmp_file.write(metadata_json)

    try:
        metadata_key = f"{s3_config.prefix}test_db/{test_table}/year=2025/month=01/day=01/batch_123.metadata.json"
        s3_client.upload_file(tmp_path, metadata_key)

        # Verify metadata file exists
        assert s3_client.object_exists(metadata_key) is True

        # Download and verify content
        metadata_data = s3_client.get_object_bytes(metadata_key)
        loaded_metadata = json.loads(metadata_data.decode("utf-8"))

        assert loaded_metadata["batch_info"]["database"] == "test_db"
        assert loaded_metadata["batch_info"]["table"] == test_table
        assert loaded_metadata["data_info"]["record_count"] == 1000
        assert loaded_metadata["checksums"]["jsonl_sha256"] == "abc123def456"

    finally:
        tmp_path.unlink()


@pytest.mark.asyncio
async def test_deletion_manifest_upload(s3_client, s3_config, test_table):
    """Test that deletion manifests are uploaded correctly."""
    import tempfile

    from archiver.deletion_manifest import DeletionManifestGenerator

    manifest_generator = DeletionManifestGenerator()

    manifest = manifest_generator.generate_manifest(
        database_name="test_db",
        table_name=test_table,
        schema_name="public",
        batch_number=1,
        batch_id="batch_123",
        primary_key_column="id",
        primary_keys=[1, 2, 3, 4, 5],
        deleted_count=5,
    )

    manifest_json = manifest_generator.manifest_to_json(manifest)

    # Write to temp file and upload
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json", encoding="utf-8") as tmp_file:
        tmp_path = Path(tmp_file.name)
        tmp_file.write(manifest_json)

    try:
        manifest_key = f"{s3_config.prefix}test_db/{test_table}/year=2025/month=01/day=01/batch_123.manifest.json"
        s3_client.upload_file(tmp_path, manifest_key)

        # Verify manifest file exists
        assert s3_client.object_exists(manifest_key) is True

        # Download and verify content
        manifest_data = s3_client.get_object_bytes(manifest_key)
        loaded_manifest = json.loads(manifest_data.decode("utf-8"))

        assert loaded_manifest["manifest_info"]["database"] == "test_db"
        assert loaded_manifest["manifest_info"]["table"] == test_table
        assert loaded_manifest["deletion_info"]["primary_keys_count"] == 5
        assert loaded_manifest["deletion_info"]["deleted_count"] == 5
        assert len(loaded_manifest["primary_keys"]) == 5

    finally:
        tmp_path.unlink()

