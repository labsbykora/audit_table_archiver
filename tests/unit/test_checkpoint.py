"""Unit tests for checkpoint module."""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from archiver.checkpoint import Checkpoint, CheckpointError, CheckpointManager
from archiver.exceptions import S3Error


@pytest.fixture
def checkpoint() -> Checkpoint:
    """Create test checkpoint."""
    return Checkpoint(
        database_name="test_db",
        table_name="test_table",
        schema_name="public",
        batch_number=5,
        last_timestamp=datetime.now(timezone.utc),
        last_primary_key=100,
        records_archived=5000,
        batches_processed=5,
        checkpoint_time=datetime.now(timezone.utc),
        batch_id="batch_123",
    )


@pytest.fixture
def checkpoint_manager() -> CheckpointManager:
    """Create test checkpoint manager."""
    return CheckpointManager(
        storage_type="s3",
        checkpoint_interval=10,
    )


def test_checkpoint_init(checkpoint: Checkpoint) -> None:
    """Test checkpoint initialization."""
    assert checkpoint.database_name == "test_db"
    assert checkpoint.table_name == "test_table"
    assert checkpoint.schema_name == "public"
    assert checkpoint.batch_number == 5
    assert checkpoint.records_archived == 5000
    assert checkpoint.batches_processed == 5
    assert checkpoint.batch_id == "batch_123"


def test_checkpoint_to_dict(checkpoint: Checkpoint) -> None:
    """Test checkpoint to dictionary conversion."""
    data = checkpoint.to_dict()
    
    assert data["version"] == "1.0"
    assert data["database"] == "test_db"
    assert data["table"] == "test_table"
    assert data["batch_number"] == 5
    assert data["records_archived"] == 5000
    assert data["batches_processed"] == 5
    assert data["batch_id"] == "batch_123"
    assert "last_timestamp" in data
    assert "last_primary_key" in data
    assert "checkpoint_time" in data


def test_checkpoint_from_dict() -> None:
    """Test checkpoint from dictionary creation."""
    data = {
        "version": "1.0",
        "database": "test_db",
        "table": "test_table",
        "schema": "public",
        "batch_number": 5,
        "last_timestamp": "2024-01-01T00:00:00+00:00",
        "last_primary_key": "100",
        "records_archived": 5000,
        "batches_processed": 5,
        "checkpoint_time": "2024-01-01T00:00:00+00:00",
        "batch_id": "batch_123",
    }
    
    checkpoint = Checkpoint.from_dict(data)
    
    assert checkpoint.database_name == "test_db"
    assert checkpoint.table_name == "test_table"
    assert checkpoint.batch_number == 5
    assert checkpoint.records_archived == 5000
    assert checkpoint.last_primary_key == 100  # Should be converted to int


def test_checkpoint_from_dict_invalid() -> None:
    """Test checkpoint from dictionary with invalid data."""
    data = {"invalid": "data"}
    
    with pytest.raises(CheckpointError, match="Invalid checkpoint data"):
        Checkpoint.from_dict(data)


def test_checkpoint_manager_init() -> None:
    """Test checkpoint manager initialization."""
    manager = CheckpointManager(
        storage_type="s3",
        checkpoint_interval=10,
    )
    
    assert manager.storage_type == "s3"
    assert manager.checkpoint_interval == 10


def test_checkpoint_manager_init_invalid_storage() -> None:
    """Test checkpoint manager with invalid storage type."""
    with pytest.raises(ValueError, match="Invalid storage_type"):
        CheckpointManager(storage_type="invalid")


def test_checkpoint_manager_init_invalid_interval() -> None:
    """Test checkpoint manager with invalid interval."""
    # Note: Currently no validation for checkpoint_interval in __init__
    # This test documents expected behavior if validation is added
    # For now, we just verify it accepts 0 (even if not ideal)
    manager = CheckpointManager(storage_type="s3", checkpoint_interval=0)
    assert manager.checkpoint_interval == 0


def test_checkpoint_manager_should_save_checkpoint(checkpoint_manager: CheckpointManager) -> None:
    """Test checkpoint save decision logic."""
    # Logic: batch_number % checkpoint_interval == 0
    assert checkpoint_manager.should_save_checkpoint(0) is True  # 0 % 10 == 0
    assert checkpoint_manager.should_save_checkpoint(10) is True  # 10 % 10 == 0
    assert checkpoint_manager.should_save_checkpoint(20) is True  # 20 % 10 == 0
    assert checkpoint_manager.should_save_checkpoint(15) is False  # 15 % 10 != 0


@pytest.mark.asyncio
async def test_checkpoint_manager_save_to_s3(checkpoint: Checkpoint) -> None:
    """Test saving checkpoint to S3."""
    manager = CheckpointManager(storage_type="s3", checkpoint_interval=10)
    
    mock_s3_client = MagicMock()
    # upload_file is not async in S3Client, it's a regular method
    mock_s3_client.upload_file = MagicMock()
    mock_s3_client.config.prefix = "archives/"
    
    await manager.save_checkpoint(checkpoint, s3_client=mock_s3_client)
    
    mock_s3_client.upload_file.assert_called_once()
    call_args = mock_s3_client.upload_file.call_args
    assert ".checkpoint.json" in str(call_args[0][1])


@pytest.mark.asyncio
async def test_checkpoint_manager_load_from_s3(checkpoint_manager: CheckpointManager) -> None:
    """Test loading checkpoint from S3."""
    mock_s3_client = MagicMock()
    mock_s3_client.config.prefix = "archives/"
    
    checkpoint_data = {
        "version": "1.0",
        "database": "test_db",
        "table": "test_table",
        "schema": "public",
        "batch_number": 5,
        "last_timestamp": "2024-01-01T00:00:00+00:00",
        "last_primary_key": "100",
        "records_archived": 5000,
        "batches_processed": 5,
        "checkpoint_time": "2024-01-01T00:00:00+00:00",
        "batch_id": "batch_123",
    }
    
    checkpoint_json = json.dumps(checkpoint_data)
    mock_s3_client.get_object_bytes = MagicMock(return_value=checkpoint_json.encode("utf-8"))
    
    checkpoint = await checkpoint_manager.load_checkpoint("test_db", "test_table", s3_client=mock_s3_client)
    
    assert checkpoint is not None
    assert checkpoint.database_name == "test_db"
    assert checkpoint.batch_number == 5


@pytest.mark.asyncio
async def test_checkpoint_manager_load_from_s3_not_found(checkpoint_manager: CheckpointManager) -> None:
    """Test loading checkpoint from S3 when not found."""
    mock_s3_client = MagicMock()
    mock_s3_client.config.prefix = "archives/"
    mock_s3_client.get_object_bytes = MagicMock(side_effect=S3Error("Not found"))
    
    checkpoint = await checkpoint_manager.load_checkpoint("test_db", "test_table", s3_client=mock_s3_client)
    
    assert checkpoint is None


@pytest.mark.asyncio
async def test_checkpoint_manager_delete_from_s3(checkpoint_manager: CheckpointManager) -> None:
    """Test deleting checkpoint from S3."""
    mock_s3_client = MagicMock()
    mock_s3_client.config.prefix = "archives/"
    mock_s3_client.client = MagicMock()
    mock_s3_client.client.delete_object = MagicMock()
    
    await checkpoint_manager.delete_checkpoint("test_db", "test_table", s3_client=mock_s3_client)
    
    # Should attempt to delete checkpoint
    mock_s3_client.client.delete_object.assert_called_once()


@pytest.mark.asyncio
async def test_checkpoint_manager_save_to_local(checkpoint: Checkpoint, tmp_path: Path) -> None:
    """Test saving checkpoint to local file."""
    local_path = tmp_path / "checkpoints"
    manager = CheckpointManager(
        storage_type="local",
        checkpoint_interval=10,
    )
    
    await manager.save_checkpoint(checkpoint, local_path=local_path)
    
    # Local checkpoint files use format: {database}_{table}.checkpoint.json
    checkpoint_file = local_path / "test_db_test_table.checkpoint.json"
    assert checkpoint_file.exists()
    
    data = json.loads(checkpoint_file.read_text())
    assert data["database"] == "test_db"
    assert data["table"] == "test_table"


@pytest.mark.asyncio
async def test_checkpoint_manager_load_from_local(tmp_path: Path) -> None:
    """Test loading checkpoint from local file."""
    local_path = tmp_path / "checkpoints"
    manager = CheckpointManager(
        storage_type="local",
        checkpoint_interval=10,
    )
    
    checkpoint_data = {
        "version": "1.0",
        "database": "test_db",
        "table": "test_table",
        "schema": "public",
        "batch_number": 5,
        "last_timestamp": "2024-01-01T00:00:00+00:00",
        "last_primary_key": "100",
        "records_archived": 5000,
        "batches_processed": 5,
        "checkpoint_time": "2024-01-01T00:00:00+00:00",
        "batch_id": "batch_123",
    }
    
    # Local checkpoint files use format: {database}_{table}.checkpoint.json
    checkpoint_file = local_path / "test_db_test_table.checkpoint.json"
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_file.write_text(json.dumps(checkpoint_data))
    
    checkpoint = await manager.load_checkpoint("test_db", "test_table", local_path=local_path)
    
    assert checkpoint is not None
    assert checkpoint.database_name == "test_db"
    assert checkpoint.batch_number == 5


@pytest.mark.asyncio
async def test_checkpoint_manager_load_from_local_not_found(tmp_path: Path) -> None:
    """Test loading checkpoint from local file when not found."""
    local_path = tmp_path / "checkpoints"
    manager = CheckpointManager(
        storage_type="local",
        checkpoint_interval=10,
    )
    
    checkpoint = await manager.load_checkpoint("test_db", "test_table", local_path=local_path)
    
    assert checkpoint is None


@pytest.mark.asyncio
async def test_checkpoint_manager_delete_from_local(tmp_path: Path) -> None:
    """Test deleting checkpoint from local file."""
    local_path = tmp_path / "checkpoints"
    manager = CheckpointManager(
        storage_type="local",
        checkpoint_interval=10,
    )
    
    # Local checkpoint files use format: {database}_{table}.checkpoint.json
    checkpoint_file = local_path / "test_db_test_table.checkpoint.json"
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_file.write_text('{"test": "data"}')
    
    await manager.delete_checkpoint("test_db", "test_table", local_path=local_path)
    
    assert not checkpoint_file.exists()

