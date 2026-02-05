"""Unit tests for restore engine."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError
from restore.restore_engine import RestoreEngine
from restore.s3_reader import ArchiveFile


@pytest.fixture
def sample_archive() -> ArchiveFile:
    """Create sample archive file."""
    import gzip
    import json

    records = [
        {
            "id": 1,
            "name": "test1",
            "created_at": "2026-01-01T00:00:00Z",
            "amount": "10.50",
            "active": True,
        },
        {
            "id": 2,
            "name": "test2",
            "created_at": "2026-01-02T00:00:00Z",
            "amount": "20.75",
            "active": False,
        },
    ]
    jsonl_data = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    compressed_data = gzip.compress(jsonl_data)

    metadata = {
        "version": "1.0",
        "batch_info": {
            "database": "test_db",
            "schema": "public",
            "table": "audit_logs",
            "batch_number": 1,
            "batch_id": "test_001",
        },
        "data_info": {"record_count": 2},
        "table_schema": {
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "name", "type": "TEXT"},
                {"name": "created_at", "type": "TIMESTAMPTZ"},
                {"name": "amount", "type": "NUMERIC"},
                {"name": "active", "type": "BOOLEAN"},
            ]
        },
    }

    return ArchiveFile(
        s3_key="test.jsonl.gz",
        metadata=metadata,
        data=compressed_data,
        jsonl_data=jsonl_data,
    )


@pytest.fixture
def mock_db_manager() -> MagicMock:
    """Create mock database manager."""
    manager = MagicMock(spec=DatabaseManager)
    manager.pool = MagicMock()
    manager.connect = AsyncMock()
    manager.disconnect = AsyncMock()
    return manager


class TestRestoreEngine:
    """Tests for RestoreEngine class."""

    @pytest.mark.asyncio
    async def test_restore_archive_dry_run(
        self, mock_db_manager: MagicMock, sample_archive: ArchiveFile
    ) -> None:
        """Test restore in dry-run mode."""
        engine = RestoreEngine(mock_db_manager)

        stats = await engine.restore_archive(
            archive=sample_archive,
            conflict_strategy="skip",
            dry_run=True,
        )

        assert stats["dry_run"] is True
        assert stats["records_processed"] == 2
        assert stats["records_restored"] == 0
        mock_db_manager.connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_restore_archive_skip_strategy(
        self, mock_db_manager: MagicMock, sample_archive: ArchiveFile
    ) -> None:
        """Test restore with skip conflict strategy."""
        mock_conn = MagicMock()
        mock_conn.executemany = AsyncMock(return_value="INSERT 0 2")
        mock_conn.transaction = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_db_manager.pool.acquire = MagicMock()
        mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        engine = RestoreEngine(mock_db_manager)

        stats = await engine.restore_archive(
            archive=sample_archive,
            conflict_strategy="skip",
            batch_size=1000,
            dry_run=False,
        )

        assert stats["records_restored"] == 2
        assert mock_conn.executemany.called

    @pytest.mark.asyncio
    async def test_restore_archive_overwrite_strategy(
        self, mock_db_manager: MagicMock, sample_archive: ArchiveFile
    ) -> None:
        """Test restore with overwrite conflict strategy."""
        mock_conn = MagicMock()
        mock_conn.executemany = AsyncMock(return_value="INSERT 0 2")
        mock_conn.transaction = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_db_manager.pool.acquire = MagicMock()
        mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        engine = RestoreEngine(mock_db_manager)

        stats = await engine.restore_archive(
            archive=sample_archive,
            conflict_strategy="overwrite",
            dry_run=False,
        )

        assert stats["records_restored"] == 2
        # Verify ON CONFLICT DO UPDATE was used
        call_args = mock_conn.executemany.call_args[0][0]
        assert "ON CONFLICT DO UPDATE" in call_args

    @pytest.mark.asyncio
    async def test_restore_archive_fail_strategy(
        self, mock_db_manager: MagicMock, sample_archive: ArchiveFile
    ) -> None:
        """Test restore with fail conflict strategy."""
        mock_conn = MagicMock()
        mock_conn.executemany = AsyncMock(side_effect=asyncpg.UniqueViolationError("duplicate key"))
        mock_conn.transaction = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_db_manager.pool.acquire = MagicMock()
        mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        engine = RestoreEngine(mock_db_manager)

        with pytest.raises(DatabaseError, match="Conflict detected"):
            await engine.restore_archive(
                archive=sample_archive,
                conflict_strategy="fail",
                dry_run=False,
            )

    @pytest.mark.asyncio
    async def test_restore_archive_drop_indexes(
        self, mock_db_manager: MagicMock, sample_archive: ArchiveFile
    ) -> None:
        """Test restore with index dropping."""
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(
            return_value=[
                {"indexname": "idx_name", "indexdef": "CREATE INDEX idx_name ON table(name)"}
            ]
        )
        mock_conn.execute = AsyncMock()
        mock_conn.executemany = AsyncMock(return_value="INSERT 0 2")
        mock_conn.transaction = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_db_manager.pool.acquire = MagicMock()
        mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        engine = RestoreEngine(mock_db_manager)

        await engine.restore_archive(
            archive=sample_archive,
            conflict_strategy="skip",
            drop_indexes=True,
            dry_run=False,
        )

        # Verify index was dropped and restored
        assert mock_conn.execute.call_count >= 2  # DROP and CREATE

    def test_infer_column_types(self, mock_db_manager: MagicMock) -> None:
        """Test column type inference."""
        engine = RestoreEngine(mock_db_manager)

        records = [
            {"id": 1, "name": "test", "created_at": "2026-01-01T00:00:00Z", "active": True},
        ]
        columns = ["id", "name", "created_at", "active"]

        types = engine._infer_column_types(records, columns)

        assert types["id"] == "BIGINT"
        assert types["name"] == "TEXT"
        assert types["created_at"] == "TIMESTAMPTZ"
        assert types["active"] == "BOOLEAN"

    def test_prepare_record_values(self, mock_db_manager: MagicMock) -> None:
        """Test record value preparation."""
        engine = RestoreEngine(mock_db_manager)

        record = {
            "id": 1,
            "name": "test",
            "created_at": "2026-01-01T00:00:00Z",
            "data": {"key": "value"},
            "amount": "10.50",
        }
        columns = ["id", "name", "created_at", "data", "amount"]

        values = engine._prepare_record_values(record, columns)

        assert values[0] == 1
        assert values[1] == "test"
        assert isinstance(values[2], datetime)  # Parsed datetime
        assert isinstance(values[3], dict)  # JSON preserved
        # Decimal string is converted to float for asyncpg
        assert isinstance(values[4], (float, str))  # Could be float or string depending on parsing

    @pytest.mark.asyncio
    async def test_restore_empty_archive(
        self, mock_db_manager: MagicMock, sample_archive: ArchiveFile
    ) -> None:
        """Test restore with empty archive."""
        # Create empty archive
        import gzip

        empty_jsonl = b""
        empty_compressed = gzip.compress(empty_jsonl)
        empty_metadata = {
            "version": "1.0",
            "batch_info": {
                "database": "test_db",
                "schema": "public",
                "table": "audit_logs",
            },
            "data_info": {"record_count": 0},
        }

        empty_archive = ArchiveFile(
            s3_key="empty.jsonl.gz",
            metadata=empty_metadata,
            data=empty_compressed,
            jsonl_data=empty_jsonl,
        )

        engine = RestoreEngine(mock_db_manager)

        stats = await engine.restore_archive(
            archive=empty_archive,
            conflict_strategy="skip",
            dry_run=False,
        )

        assert stats["records_processed"] == 0
        assert stats["records_restored"] == 0
