"""Unit tests for restore watermark management."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from archiver.database import DatabaseManager
from archiver.exceptions import S3Error
from archiver.s3_client import S3Client
from restore.restore_watermark import RestoreWatermark, RestoreWatermarkManager


class TestRestoreWatermark:
    """Tests for RestoreWatermark class."""

    def test_init(self):
        """Test RestoreWatermark initialization."""
        watermark = RestoreWatermark(
            database_name="test_db",
            table_name="test_table",
            last_restored_date=datetime(2026, 1, 8, tzinfo=timezone.utc),
            last_restored_s3_key="archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
            total_archives_restored=5,
        )

        assert watermark.database_name == "test_db"
        assert watermark.table_name == "test_table"
        assert watermark.last_restored_date == datetime(2026, 1, 8, tzinfo=timezone.utc)
        assert watermark.total_archives_restored == 5

    def test_to_dict(self):
        """Test converting watermark to dictionary."""
        watermark = RestoreWatermark(
            database_name="test_db",
            table_name="test_table",
            last_restored_date=datetime(2026, 1, 8, tzinfo=timezone.utc),
            last_restored_s3_key="archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
            total_archives_restored=5,
        )

        result = watermark.to_dict()

        assert result["version"] == "1.0"
        assert result["database"] == "test_db"
        assert result["table"] == "test_table"
        assert result["last_restored_date"] == "2026-01-08T00:00:00+00:00"
        assert result["total_archives_restored"] == 5

    def test_from_dict(self):
        """Test creating watermark from dictionary."""
        data = {
            "version": "1.0",
            "database": "test_db",
            "table": "test_table",
            "last_restored_date": "2026-01-08T00:00:00Z",
            "last_restored_s3_key": "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
            "total_archives_restored": 5,
            "updated_at": "2026-01-08T10:00:00Z",
        }

        watermark = RestoreWatermark.from_dict(data)

        assert watermark.database_name == "test_db"
        assert watermark.table_name == "test_table"
        assert watermark.last_restored_date == datetime(2026, 1, 8, tzinfo=timezone.utc)
        assert watermark.total_archives_restored == 5


class TestRestoreWatermarkManager:
    """Tests for RestoreWatermarkManager class."""

    def test_init_s3(self):
        """Test initialization with S3 storage."""
        manager = RestoreWatermarkManager(storage_type="s3")
        assert manager.storage_type == "s3"

    def test_init_database(self):
        """Test initialization with database storage."""
        manager = RestoreWatermarkManager(storage_type="database")
        assert manager.storage_type == "database"

    def test_init_both(self):
        """Test initialization with both storage types."""
        manager = RestoreWatermarkManager(storage_type="both")
        assert manager.storage_type == "both"

    def test_init_invalid_storage_type(self):
        """Test initialization with invalid storage type."""
        with pytest.raises(ValueError, match="Invalid storage_type"):
            RestoreWatermarkManager(storage_type="invalid")

    def test_extract_date_from_s3_key_hive_style(self):
        """Test extracting date from Hive-style S3 key."""
        manager = RestoreWatermarkManager()
        s3_key = "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz"

        result = manager.extract_date_from_s3_key(s3_key)

        assert result == datetime(2026, 1, 8, tzinfo=timezone.utc)

    def test_extract_date_from_s3_key_simple_format(self):
        """Test extracting date from simple format S3 key."""
        manager = RestoreWatermarkManager()
        s3_key = "archives/test_db/test_table/2026/01/08/batch_001.jsonl.gz"

        result = manager.extract_date_from_s3_key(s3_key)

        assert result == datetime(2026, 1, 8, tzinfo=timezone.utc)

    def test_extract_date_from_s3_key_iso_timestamp(self):
        """Test extracting date from ISO timestamp in filename."""
        manager = RestoreWatermarkManager()
        s3_key = "archives/test_db/test_table/20260108T114123Z_batch_001.jsonl.gz"

        result = manager.extract_date_from_s3_key(s3_key)

        assert result == datetime(2026, 1, 8, tzinfo=timezone.utc)

    def test_extract_date_from_s3_key_invalid(self):
        """Test extracting date from invalid S3 key."""
        manager = RestoreWatermarkManager()
        s3_key = "archives/test_db/test_table/invalid_path.jsonl.gz"

        result = manager.extract_date_from_s3_key(s3_key)

        assert result is None

    def test_should_restore_archive_no_watermark(self):
        """Test should_restore_archive when no watermark exists."""
        manager = RestoreWatermarkManager()
        s3_key = "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz"

        result = manager.should_restore_archive(s3_key, None)

        assert result is True

    def test_should_restore_archive_newer_date(self):
        """Test should_restore_archive when archive date is newer."""
        manager = RestoreWatermarkManager()
        watermark = RestoreWatermark(
            database_name="test_db",
            table_name="test_table",
            last_restored_date=datetime(2026, 1, 7, tzinfo=timezone.utc),
            last_restored_s3_key="archives/test_db/test_table/year=2026/month=01/day=07/batch_001.jsonl.gz",
        )
        s3_key = "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz"

        result = manager.should_restore_archive(s3_key, watermark)

        assert result is True

    def test_should_restore_archive_older_date(self):
        """Test should_restore_archive when archive date is older."""
        manager = RestoreWatermarkManager()
        watermark = RestoreWatermark(
            database_name="test_db",
            table_name="test_table",
            last_restored_date=datetime(2026, 1, 8, tzinfo=timezone.utc),
            last_restored_s3_key="archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
        )
        s3_key = "archives/test_db/test_table/year=2026/month=01/day=07/batch_001.jsonl.gz"

        result = manager.should_restore_archive(s3_key, watermark)

        assert result is False

    def test_should_restore_archive_same_date_same_key(self):
        """Test should_restore_archive when same date and same key."""
        manager = RestoreWatermarkManager()
        s3_key = "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz"
        watermark = RestoreWatermark(
            database_name="test_db",
            table_name="test_table",
            last_restored_date=datetime(2026, 1, 8, tzinfo=timezone.utc),
            last_restored_s3_key=s3_key,
        )

        result = manager.should_restore_archive(s3_key, watermark)

        assert result is False

    def test_should_restore_archive_same_date_different_key(self):
        """Test should_restore_archive when same date but different key."""
        manager = RestoreWatermarkManager()
        watermark = RestoreWatermark(
            database_name="test_db",
            table_name="test_table",
            last_restored_date=datetime(2026, 1, 8, tzinfo=timezone.utc),
            last_restored_s3_key="archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
        )
        s3_key = "archives/test_db/test_table/year=2026/month=01/day=08/batch_002.jsonl.gz"

        result = manager.should_restore_archive(s3_key, watermark)

        assert result is True  # Different key, should restore

    def test_should_restore_archive_no_date_extracted(self):
        """Test should_restore_archive when date cannot be extracted."""
        manager = RestoreWatermarkManager()
        watermark = RestoreWatermark(
            database_name="test_db",
            table_name="test_table",
            last_restored_date=datetime(2026, 1, 8, tzinfo=timezone.utc),
            last_restored_s3_key="archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
        )
        s3_key = "archives/test_db/test_table/invalid_path.jsonl.gz"

        result = manager.should_restore_archive(s3_key, watermark)

        assert result is True  # Can't extract date, restore to be safe

    @pytest.mark.asyncio
    async def test_load_watermark_from_s3_success(self):
        """Test loading watermark from S3 successfully."""
        manager = RestoreWatermarkManager(storage_type="s3")

        s3_client = Mock(spec=S3Client)
        s3_client.config = Mock()
        s3_client.config.prefix = "archives"

        watermark_data = {
            "version": "1.0",
            "database": "test_db",
            "table": "test_table",
            "last_restored_date": "2026-01-08T00:00:00Z",
            "last_restored_s3_key": "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
            "total_archives_restored": 5,
            "updated_at": "2026-01-08T10:00:00Z",
        }
        s3_client.get_object_bytes = Mock(
            return_value=json.dumps(watermark_data).encode("utf-8")
        )

        result = await manager.load_watermark(
            "test_db", "test_table", s3_client=s3_client
        )

        assert result is not None
        assert result.database_name == "test_db"
        assert result.table_name == "test_table"
        assert result.last_restored_date == datetime(2026, 1, 8, tzinfo=timezone.utc)
        assert result.total_archives_restored == 5

    @pytest.mark.asyncio
    async def test_load_watermark_from_s3_not_found(self):
        """Test loading watermark from S3 when not found."""
        manager = RestoreWatermarkManager(storage_type="s3")

        s3_client = Mock(spec=S3Client)
        s3_client.config = Mock()
        s3_client.config.prefix = "archives"
        s3_client.get_object_bytes = Mock(side_effect=S3Error("NoSuchKey", context={}))

        result = await manager.load_watermark(
            "test_db", "test_table", s3_client=s3_client
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_save_watermark_to_s3(self):
        """Test saving watermark to S3."""
        manager = RestoreWatermarkManager(storage_type="s3")

        s3_client = Mock(spec=S3Client)
        s3_client.config = Mock()
        s3_client.config.prefix = "archives"
        s3_client.upload_file = Mock()

        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_file = Mock()
            mock_file.name = "/tmp/test_watermark.json"
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=None)
            mock_temp.return_value = mock_file

            with patch("pathlib.Path") as mock_path:
                mock_path_instance = Mock()
                mock_path_instance.unlink = Mock()
                mock_path.return_value = mock_path_instance

                await manager.save_watermark(
                    "test_db",
                    "test_table",
                    datetime(2026, 1, 8, tzinfo=timezone.utc),
                    "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
                    5,
                    s3_client=s3_client,
                )

                s3_client.upload_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_load_watermark_from_database_success(self):
        """Test loading watermark from database successfully."""
        manager = RestoreWatermarkManager(storage_type="database")

        db_manager = Mock(spec=DatabaseManager)
        db_manager.fetchone = AsyncMock(
            return_value={
                "last_restored_date": datetime(2026, 1, 8, tzinfo=timezone.utc),
                "last_restored_s3_key": "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
                "total_archives_restored": 5,
                "updated_at": datetime(2026, 1, 8, 10, 0, 0, tzinfo=timezone.utc),
            }
        )

        result = await manager.load_watermark(
            "test_db", "test_table", db_manager=db_manager
        )

        assert result is not None
        assert result.database_name == "test_db"
        assert result.table_name == "test_table"
        assert result.last_restored_date == datetime(2026, 1, 8, tzinfo=timezone.utc)
        assert result.total_archives_restored == 5

    @pytest.mark.asyncio
    async def test_load_watermark_from_database_not_found(self):
        """Test loading watermark from database when not found."""
        manager = RestoreWatermarkManager(storage_type="database")

        db_manager = Mock(spec=DatabaseManager)
        db_manager.fetchone = AsyncMock(return_value=None)

        result = await manager.load_watermark(
            "test_db", "test_table", db_manager=db_manager
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_save_watermark_to_database(self):
        """Test saving watermark to database."""
        manager = RestoreWatermarkManager(storage_type="database")

        db_manager = Mock(spec=DatabaseManager)
        db_manager.execute = AsyncMock()

        await manager.save_watermark(
            "test_db",
            "test_table",
            datetime(2026, 1, 8, tzinfo=timezone.utc),
            "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
            5,
            db_manager=db_manager,
        )

        # Should call execute twice: once for CREATE TABLE, once for INSERT
        assert db_manager.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_load_watermark_both_s3_first(self):
        """Test loading watermark with both storage types, S3 returns first."""
        manager = RestoreWatermarkManager(storage_type="both")

        s3_client = Mock(spec=S3Client)
        s3_client.config = Mock()
        s3_client.config.prefix = "archives"

        watermark_data = {
            "version": "1.0",
            "database": "test_db",
            "table": "test_table",
            "last_restored_date": "2026-01-08T00:00:00Z",
            "last_restored_s3_key": "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
            "total_archives_restored": 5,
            "updated_at": "2026-01-08T10:00:00Z",
        }
        s3_client.get_object_bytes = Mock(
            return_value=json.dumps(watermark_data).encode("utf-8")
        )

        result = await manager.load_watermark(
            "test_db", "test_table", s3_client=s3_client, db_manager=None
        )

        assert result is not None
        assert result.database_name == "test_db"

    @pytest.mark.asyncio
    async def test_load_watermark_both_database_fallback(self):
        """Test loading watermark with both storage types, database as fallback."""
        manager = RestoreWatermarkManager(storage_type="both")

        s3_client = Mock(spec=S3Client)
        s3_client.config = Mock()
        s3_client.config.prefix = "archives"
        s3_client.get_object_bytes = Mock(side_effect=S3Error("NoSuchKey", context={}))

        db_manager = Mock(spec=DatabaseManager)
        db_manager.fetchone = AsyncMock(
            return_value={
                "last_restored_date": datetime(2026, 1, 8, tzinfo=timezone.utc),
                "last_restored_s3_key": "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
                "total_archives_restored": 5,
                "updated_at": datetime(2026, 1, 8, 10, 0, 0, tzinfo=timezone.utc),
            }
        )

        result = await manager.load_watermark(
            "test_db", "test_table", s3_client=s3_client, db_manager=db_manager
        )

        assert result is not None
        assert result.database_name == "test_db"

    @pytest.mark.asyncio
    async def test_load_watermark_missing_s3_client(self):
        """Test loading watermark without S3 client when required."""
        manager = RestoreWatermarkManager(storage_type="s3")

        with pytest.raises(ValueError, match="s3_client is required"):
            await manager.load_watermark("test_db", "test_table", s3_client=None)

    @pytest.mark.asyncio
    async def test_load_watermark_missing_db_manager(self):
        """Test loading watermark without database manager when required."""
        manager = RestoreWatermarkManager(storage_type="database")

        with pytest.raises(ValueError, match="db_manager is required"):
            await manager.load_watermark("test_db", "test_table", db_manager=None)

    @pytest.mark.asyncio
    async def test_save_watermark_missing_s3_client(self):
        """Test saving watermark without S3 client when required."""
        manager = RestoreWatermarkManager(storage_type="s3")

        with pytest.raises(ValueError, match="s3_client is required"):
            await manager.save_watermark(
                "test_db",
                "test_table",
                datetime(2026, 1, 8, tzinfo=timezone.utc),
                "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
                5,
                s3_client=None,
            )

    @pytest.mark.asyncio
    async def test_save_watermark_missing_db_manager(self):
        """Test saving watermark without database manager when required."""
        manager = RestoreWatermarkManager(storage_type="database")

        with pytest.raises(ValueError, match="db_manager is required"):
            await manager.save_watermark(
                "test_db",
                "test_table",
                datetime(2026, 1, 8, tzinfo=timezone.utc),
                "archives/test_db/test_table/year=2026/month=01/day=08/batch_001.jsonl.gz",
                5,
                db_manager=None,
            )

