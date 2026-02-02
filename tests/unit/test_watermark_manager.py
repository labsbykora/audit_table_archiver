"""Unit tests for watermark management."""

import json
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from archiver.watermark_manager import WatermarkManager
from archiver.s3_client import S3Client
from archiver.config import S3Config
from archiver.database import DatabaseManager
from archiver.config import DatabaseConfig


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client."""
    config = S3Config(
        bucket="test-bucket",
        prefix="test/",
        region="us-east-1",
    )
    s3_client = MagicMock(spec=S3Client)
    s3_client.config = config
    return s3_client


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager."""
    db_manager = MagicMock(spec=DatabaseManager)
    return db_manager


@pytest.mark.asyncio
async def test_load_watermark_from_s3(mock_s3_client):
    """Test loading watermark from S3."""
    watermark_data = {
        "database": "test_db",
        "table": "test_table",
        "last_timestamp": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
        "last_primary_key": "12345",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    mock_s3_client.get_object_bytes.return_value = json.dumps(watermark_data).encode("utf-8")

    manager = WatermarkManager(storage_type="s3")

    watermark = await manager.load_watermark(
        database_name="test_db",
        table_name="test_table",
        s3_client=mock_s3_client,
    )

    assert watermark is not None
    assert watermark["last_primary_key"] == "12345"
    assert isinstance(watermark["last_timestamp"], datetime)


@pytest.mark.asyncio
async def test_load_watermark_from_s3_not_found(mock_s3_client):
    """Test loading non-existent watermark from S3."""
    from botocore.exceptions import ClientError

    error_response = {"Error": {"Code": "NoSuchKey"}}
    mock_s3_client.get_object_bytes.side_effect = ClientError(
        error_response, "GetObject"
    )

    manager = WatermarkManager(storage_type="s3")

    watermark = await manager.load_watermark(
        database_name="test_db",
        table_name="test_table",
        s3_client=mock_s3_client,
    )

    assert watermark is None


@pytest.mark.asyncio
async def test_save_watermark_to_s3(mock_s3_client):
    """Test saving watermark to S3."""
    manager = WatermarkManager(storage_type="s3")

    last_timestamp = datetime.now(timezone.utc) - timedelta(days=1)
    last_primary_key = 12345

    await manager.save_watermark(
        database_name="test_db",
        table_name="test_table",
        last_timestamp=last_timestamp,
        last_primary_key=last_primary_key,
        s3_client=mock_s3_client,
    )

    # Verify upload_file was called
    assert mock_s3_client.upload_file.called


@pytest.mark.asyncio
async def test_load_watermark_from_database(mock_db_manager):
    """Test loading watermark from database."""
    watermark_row = {
        "last_timestamp": datetime.now(timezone.utc) - timedelta(days=1),
        "last_primary_key": "12345",
        "updated_at": datetime.now(timezone.utc),
    }

    mock_db_manager.fetchone = AsyncMock(return_value=watermark_row)

    manager = WatermarkManager(storage_type="database")

    watermark = await manager.load_watermark(
        database_name="test_db",
        table_name="test_table",
        db_manager=mock_db_manager,
    )

    assert watermark is not None
    assert watermark["last_primary_key"] == "12345"
    assert isinstance(watermark["last_timestamp"], datetime)


@pytest.mark.asyncio
async def test_load_watermark_from_database_not_found(mock_db_manager):
    """Test loading non-existent watermark from database."""
    mock_db_manager.fetchone = AsyncMock(return_value=None)

    manager = WatermarkManager(storage_type="database")

    watermark = await manager.load_watermark(
        database_name="test_db",
        table_name="test_table",
        db_manager=mock_db_manager,
    )

    assert watermark is None


@pytest.mark.asyncio
async def test_save_watermark_to_database(mock_db_manager):
    """Test saving watermark to database."""
    mock_db_manager.execute = AsyncMock()

    manager = WatermarkManager(storage_type="database")

    last_timestamp = datetime.now(timezone.utc) - timedelta(days=1)
    last_primary_key = 12345

    await manager.save_watermark(
        database_name="test_db",
        table_name="test_table",
        last_timestamp=last_timestamp,
        last_primary_key=last_primary_key,
        db_manager=mock_db_manager,
    )

    # Verify execute was called (for table creation and upsert)
    assert mock_db_manager.execute.call_count >= 2


@pytest.mark.asyncio
async def test_file_lock_watermark_storage(tmp_path):
    """Test file-based watermark storage (if we add it)."""
    # Note: File-based storage is not currently implemented for watermarks
    # This test is a placeholder for future implementation
    pass

