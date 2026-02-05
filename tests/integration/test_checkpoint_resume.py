"""Integration tests for checkpoint and resume functionality."""

from datetime import datetime, timedelta, timezone

import pytest

from archiver.checkpoint import Checkpoint, CheckpointManager


@pytest.mark.asyncio
async def test_checkpoint_save_and_load(s3_client, s3_config):
    """Test saving and loading checkpoints from S3."""
    checkpoint_manager = CheckpointManager(storage_type="s3", logger=None)

    # Create a checkpoint
    checkpoint = Checkpoint(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=5,
        last_timestamp=datetime.now(timezone.utc) - timedelta(days=1),
        last_primary_key=12345,
        records_archived=5000,
        batches_processed=5,
        checkpoint_time=datetime.now(timezone.utc),
        batch_id="batch_123",
    )

    # Save checkpoint
    await checkpoint_manager.save_checkpoint(
        checkpoint=checkpoint,
        s3_client=s3_client,
    )

    # Load checkpoint
    loaded_checkpoint = await checkpoint_manager.load_checkpoint(
        database_name="test_db",
        table_name="test_table",
        s3_client=s3_client,
    )

    assert loaded_checkpoint is not None
    assert loaded_checkpoint.database_name == checkpoint.database_name
    assert loaded_checkpoint.table_name == checkpoint.table_name
    assert loaded_checkpoint.batch_number == checkpoint.batch_number
    assert loaded_checkpoint.records_archived == checkpoint.records_archived
    assert loaded_checkpoint.last_primary_key == checkpoint.last_primary_key

    # Clean up
    await checkpoint_manager.delete_checkpoint(
        database_name="test_db",
        table_name="test_table",
        s3_client=s3_client,
    )


@pytest.mark.asyncio
async def test_checkpoint_not_found(s3_client):
    """Test loading non-existent checkpoint."""
    checkpoint_manager = CheckpointManager(storage_type="s3", logger=None)

    loaded_checkpoint = await checkpoint_manager.load_checkpoint(
        database_name="nonexistent_db",
        table_name="nonexistent_table",
        s3_client=s3_client,
    )

    assert loaded_checkpoint is None


@pytest.mark.asyncio
async def test_checkpoint_should_save():
    """Test checkpoint save interval logic."""
    checkpoint_manager = CheckpointManager(
        storage_type="s3",
        checkpoint_interval=10,
        logger=None,
    )

    # Should save at batch 10, 20, 30, etc.
    assert checkpoint_manager.should_save_checkpoint(10) is True
    assert checkpoint_manager.should_save_checkpoint(20) is True
    assert checkpoint_manager.should_save_checkpoint(30) is True

    # Should not save at other batches
    assert checkpoint_manager.should_save_checkpoint(9) is False
    assert checkpoint_manager.should_save_checkpoint(11) is False
    assert checkpoint_manager.should_save_checkpoint(19) is False


@pytest.mark.asyncio
async def test_checkpoint_local_storage(tmp_path):
    """Test checkpoint save/load with local file storage."""
    checkpoint_manager = CheckpointManager(
        storage_type="local",
        checkpoint_interval=10,
        logger=None,
    )

    checkpoint = Checkpoint(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=5,
        last_timestamp=datetime.now(timezone.utc) - timedelta(days=1),
        last_primary_key=12345,
        records_archived=5000,
        batches_processed=5,
        checkpoint_time=datetime.now(timezone.utc),
    )

    # Save checkpoint
    await checkpoint_manager.save_checkpoint(
        checkpoint=checkpoint,
        local_path=tmp_path,
    )

    # Verify file exists
    checkpoint_file = tmp_path / "test_db_test_table.checkpoint.json"
    assert checkpoint_file.exists()

    # Load checkpoint
    loaded_checkpoint = await checkpoint_manager.load_checkpoint(
        database_name="test_db",
        table_name="test_table",
        local_path=tmp_path,
    )

    assert loaded_checkpoint is not None
    assert loaded_checkpoint.database_name == checkpoint.database_name
    assert loaded_checkpoint.batch_number == checkpoint.batch_number

    # Delete checkpoint
    await checkpoint_manager.delete_checkpoint(
        database_name="test_db",
        table_name="test_table",
        local_path=tmp_path,
    )

    assert not checkpoint_file.exists()


@pytest.mark.asyncio
async def test_checkpoint_to_dict_and_from_dict():
    """Test checkpoint serialization."""
    checkpoint = Checkpoint(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=5,
        last_timestamp=datetime.now(timezone.utc) - timedelta(days=1),
        last_primary_key=12345,
        records_archived=5000,
        batches_processed=5,
        checkpoint_time=datetime.now(timezone.utc),
        batch_id="batch_123",
    )

    # Convert to dict
    checkpoint_dict = checkpoint.to_dict()

    assert checkpoint_dict["database"] == "test_db"
    assert checkpoint_dict["table"] == "test_table"
    assert checkpoint_dict["batch_number"] == 5
    assert checkpoint_dict["records_archived"] == 5000

    # Convert back from dict
    loaded_checkpoint = Checkpoint.from_dict(checkpoint_dict)

    assert loaded_checkpoint.database_name == checkpoint.database_name
    assert loaded_checkpoint.batch_number == checkpoint.batch_number
    assert loaded_checkpoint.records_archived == checkpoint.records_archived
    assert loaded_checkpoint.last_primary_key == checkpoint.last_primary_key
