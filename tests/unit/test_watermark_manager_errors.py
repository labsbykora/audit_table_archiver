"""Unit tests for watermark manager error paths."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from archiver.database import DatabaseManager
from archiver.exceptions import ArchiverError, DatabaseError, S3Error
from archiver.s3_client import S3Client
from archiver.watermark_manager import WatermarkManager


@pytest.fixture
def mock_s3_client() -> MagicMock:
    """Create a mock S3 client."""
    s3_config = MagicMock()
    s3_config.prefix = "archives/"

    s3_client = MagicMock(spec=S3Client)
    s3_client.config = s3_config
    return s3_client


@pytest.fixture
def mock_db_manager() -> MagicMock:
    """Create a mock database manager."""
    return MagicMock(spec=DatabaseManager)


@pytest.mark.asyncio
async def test_load_watermark_s3_missing_client() -> None:
    """Test load_watermark with S3 storage but no client."""
    manager = WatermarkManager(storage_type="s3")

    with pytest.raises(ValueError, match="s3_client is required"):
        await manager.load_watermark("db1", "table1")


@pytest.mark.asyncio
async def test_load_watermark_database_missing_manager() -> None:
    """Test load_watermark with database storage but no manager."""
    manager = WatermarkManager(storage_type="database")

    with pytest.raises(ValueError, match="db_manager is required"):
        await manager.load_watermark("db1", "table1")


@pytest.mark.asyncio
async def test_save_watermark_s3_missing_client() -> None:
    """Test save_watermark with S3 storage but no client."""
    manager = WatermarkManager(storage_type="s3")

    with pytest.raises(ValueError, match="s3_client is required"):
        await manager.save_watermark(
            "db1", "table1", datetime.now(timezone.utc), 123
        )


@pytest.mark.asyncio
async def test_save_watermark_database_missing_manager() -> None:
    """Test save_watermark with database storage but no manager."""
    manager = WatermarkManager(storage_type="database")

    with pytest.raises(ValueError, match="db_manager is required"):
        await manager.save_watermark(
            "db1", "table1", datetime.now(timezone.utc), 123
        )


@pytest.mark.asyncio
async def test_load_watermark_s3_error(mock_s3_client: MagicMock) -> None:
    """Test load_watermark from S3 with error."""
    manager = WatermarkManager(storage_type="s3")

    mock_s3_client.get_object_bytes.side_effect = ArchiverError("S3 error")

    result = await manager.load_watermark("db1", "table1", s3_client=mock_s3_client)
    assert result is None  # Should return None on error, not raise


@pytest.mark.asyncio
async def test_load_watermark_s3_invalid_json(mock_s3_client: MagicMock) -> None:
    """Test load_watermark from S3 with invalid JSON."""
    manager = WatermarkManager(storage_type="s3")

    mock_s3_client.get_object_bytes.return_value = b"invalid json"

    # Should return None (implementation catches and returns None)
    result = await manager.load_watermark("db1", "table1", s3_client=mock_s3_client)
    assert result is None


@pytest.mark.asyncio
async def test_save_watermark_s3_upload_error(mock_s3_client: MagicMock, tmp_path) -> None:
    """Test save_watermark to S3 with upload error."""
    manager = WatermarkManager(storage_type="s3")

    mock_s3_client.upload_file.side_effect = S3Error("Upload failed")

    # Should raise S3Error (not caught in _save_watermark_to_s3)
    with pytest.raises(S3Error, match="Upload failed"):
        await manager.save_watermark(
            "db1", "table1", datetime.now(timezone.utc), 123, s3_client=mock_s3_client
        )


@pytest.mark.asyncio
async def test_load_watermark_database_not_found(mock_db_manager: MagicMock) -> None:
    """Test load_watermark from database when not found."""
    manager = WatermarkManager(storage_type="database")

    mock_db_manager.fetchone = AsyncMock(return_value=None)

    result = await manager.load_watermark(
        "db1", "table1", db_manager=mock_db_manager
    )
    assert result is None


@pytest.mark.asyncio
async def test_save_watermark_database_error(mock_db_manager: MagicMock) -> None:
    """Test save_watermark to database with error."""
    manager = WatermarkManager(storage_type="database")

    mock_db_manager.execute = AsyncMock(side_effect=Exception("Database error"))

    with pytest.raises(DatabaseError, match="Failed to save watermark"):
        await manager.save_watermark(
            "db1", "table1", datetime.now(timezone.utc), 123, db_manager=mock_db_manager
        )


@pytest.mark.asyncio
async def test_create_watermark_table_error(mock_db_manager: MagicMock) -> None:
    """Test watermark table creation error."""
    manager = WatermarkManager(storage_type="database")

    mock_db_manager.execute = AsyncMock(side_effect=Exception("Create table error"))

    # _create_watermark_table is called in _save_watermark_to_database
    # which catches and raises DatabaseError
    with pytest.raises(DatabaseError, match="Failed to save watermark"):
        await manager.save_watermark(
            "db1", "table1", datetime.now(timezone.utc), 123, db_manager=mock_db_manager
        )


def test_invalid_storage_type() -> None:
    """Test initialization with invalid storage type."""
    with pytest.raises(ValueError, match="Invalid storage_type"):
        WatermarkManager(storage_type="invalid")

