"""Unit tests for audit trail."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from archiver.audit_trail import AuditEventType, AuditTrail
from archiver.database import DatabaseManager
from archiver.s3_client import S3Client


class TestAuditTrail:
    """Tests for AuditTrail class."""

    def test_init_s3_storage(self):
        """Test initialization with S3 storage."""
        trail = AuditTrail(storage_type="s3")
        assert trail.storage_type == "s3"

    def test_init_database_storage(self):
        """Test initialization with database storage."""
        trail = AuditTrail(storage_type="database")
        assert trail.storage_type == "database"

    def test_init_invalid_storage(self):
        """Test initialization with invalid storage type raises error."""
        with pytest.raises(ValueError) as exc_info:
            AuditTrail(storage_type="invalid")

        assert "Invalid storage_type" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_log_event_s3_storage(self):
        """Test logging event to S3."""
        trail = AuditTrail(storage_type="s3")

        mock_s3_client = MagicMock(spec=S3Client)
        mock_s3_client.config = MagicMock()
        mock_s3_client.config.prefix = ""
        mock_s3_client.upload_file = MagicMock()

        await trail.log_event(
            event_type=AuditEventType.ARCHIVE_START,
            database_name="test_db",
            table_name="test_table",
            schema_name="public",
            status="started",
            s3_client=mock_s3_client,
        )

        # Verify upload_file was called
        assert mock_s3_client.upload_file.called
        call_args = mock_s3_client.upload_file.call_args
        assert call_args is not None
        # Check that the key contains "audit"
        assert "audit" in call_args[0][1] or "audit" in str(call_args)

    @pytest.mark.asyncio
    async def test_log_event_database_storage(self):
        """Test logging event to database."""
        trail = AuditTrail(storage_type="database")

        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_db_manager.execute = AsyncMock()

        await trail.log_event(
            event_type=AuditEventType.ARCHIVE_SUCCESS,
            database_name="test_db",
            table_name="test_table",
            schema_name="public",
            record_count=100,
            status="success",
            duration_seconds=5.5,
            db_manager=mock_db_manager,
        )

        # Verify execute was called (for table creation and insert)
        assert mock_db_manager.execute.called
        # Should be called at least twice (create table, create index, insert)
        assert mock_db_manager.execute.call_count >= 2

    @pytest.mark.asyncio
    async def test_log_event_s3_missing_client(self):
        """Test that logging to S3 without client raises error."""
        trail = AuditTrail(storage_type="s3")

        with pytest.raises(ValueError) as exc_info:
            await trail.log_event(
                event_type=AuditEventType.ARCHIVE_START,
                database_name="test_db",
                status="started",
            )

        assert "s3_client is required" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_log_event_database_missing_manager(self):
        """Test that logging to database without manager raises error."""
        trail = AuditTrail(storage_type="database")

        with pytest.raises(ValueError) as exc_info:
            await trail.log_event(
                event_type=AuditEventType.ARCHIVE_START,
                database_name="test_db",
                status="started",
            )

        assert "db_manager is required" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_log_event_with_metadata(self):
        """Test logging event with additional metadata."""
        trail = AuditTrail(storage_type="s3")

        mock_s3_client = MagicMock(spec=S3Client)
        mock_s3_client.config = MagicMock()
        mock_s3_client.config.prefix = ""
        mock_s3_client.upload_file = MagicMock()

        metadata = {"custom_field": "custom_value", "batch_id": "abc123"}

        await trail.log_event(
            event_type=AuditEventType.ARCHIVE_SUCCESS,
            database_name="test_db",
            table_name="test_table",
            record_count=100,
            status="success",
            metadata=metadata,
            s3_client=mock_s3_client,
        )

        assert mock_s3_client.upload_file.called

    @pytest.mark.asyncio
    async def test_log_event_with_error_message(self):
        """Test logging event with error message."""
        trail = AuditTrail(storage_type="database")

        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_db_manager.execute = AsyncMock()

        await trail.log_event(
            event_type=AuditEventType.ARCHIVE_FAILURE,
            database_name="test_db",
            table_name="test_table",
            status="failed",
            error_message="Connection timeout",
            db_manager=mock_db_manager,
        )

        assert mock_db_manager.execute.called

    @pytest.mark.asyncio
    async def test_log_event_all_event_types(self):
        """Test logging all event types."""
        trail = AuditTrail(storage_type="s3")

        mock_s3_client = MagicMock(spec=S3Client)
        mock_s3_client.config = MagicMock()
        mock_s3_client.config.prefix = ""
        mock_s3_client.upload_file = MagicMock()

        event_types = [
            AuditEventType.ARCHIVE_START,
            AuditEventType.ARCHIVE_SUCCESS,
            AuditEventType.ARCHIVE_FAILURE,
            AuditEventType.RESTORE_START,
            AuditEventType.RESTORE_SUCCESS,
            AuditEventType.RESTORE_FAILURE,
            AuditEventType.ERROR,
        ]

        for event_type in event_types:
            await trail.log_event(
                event_type=event_type,
                database_name="test_db",
                status="test",
                s3_client=mock_s3_client,
            )

        # Should have called upload_file for each event type
        assert mock_s3_client.upload_file.call_count == len(event_types)

    @pytest.mark.asyncio
    async def test_log_event_with_operator(self):
        """Test logging event with operator information."""
        trail = AuditTrail(storage_type="database")

        mock_db_manager = MagicMock(spec=DatabaseManager)
        mock_db_manager.execute = AsyncMock()

        await trail.log_event(
            event_type=AuditEventType.ARCHIVE_START,
            database_name="test_db",
            operator="admin@example.com",
            status="started",
            db_manager=mock_db_manager,
        )

        assert mock_db_manager.execute.called

    @pytest.mark.asyncio
    async def test_log_event_with_s3_path(self):
        """Test logging event with S3 path."""
        trail = AuditTrail(storage_type="s3")

        mock_s3_client = MagicMock(spec=S3Client)
        mock_s3_client.config = MagicMock()
        mock_s3_client.config.prefix = "archives/"
        mock_s3_client.upload_file = MagicMock()

        await trail.log_event(
            event_type=AuditEventType.ARCHIVE_SUCCESS,
            database_name="test_db",
            table_name="test_table",
            s3_path="s3://bucket/archives/test_db/test_table/data.jsonl.gz",
            status="success",
            s3_client=mock_s3_client,
        )

        assert mock_s3_client.upload_file.called

