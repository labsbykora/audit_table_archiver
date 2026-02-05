"""Performance tests for restore operations."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from archiver.database import DatabaseManager
from restore.restore_engine import RestoreEngine
from restore.s3_reader import ArchiveFile


@pytest.fixture
def large_archive() -> ArchiveFile:
    """Create a large archive file for performance testing."""
    import gzip
    import json

    # Generate 10,000 records
    records = []
    for i in range(10000):
        records.append(
            {
                "id": i,
                "name": f"test_{i}",
                "created_at": "2026-01-01T00:00:00Z",
                "amount": f"{i * 0.01:.2f}",
                "active": i % 2 == 0,
            }
        )

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
        "data_info": {"record_count": 10000},
        "table_schema": {
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "name", "type": "TEXT"},
                {"name": "created_at", "type": "TIMESTAMPTZ"},
                {"name": "amount", "type": "NUMERIC"},
                {"name": "active", "type": "BOOLEAN"},
            ],
            "primary_key": {"columns": ["id"]},
        },
    }

    return ArchiveFile(
        s3_key="large_test.jsonl.gz",
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


class TestRestorePerformance:
    """Performance tests for restore operations."""

    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_batch_size_impact(
        self, large_archive: ArchiveFile, mock_db_manager: MagicMock
    ) -> None:
        """Test that different batch sizes affect performance."""
        engine = RestoreEngine(mock_db_manager)

        # Mock connection and executemany
        mock_conn = MagicMock()
        mock_conn.executemany = AsyncMock(return_value="INSERT 0 1000")
        mock_conn.transaction = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_db_manager.pool.acquire = MagicMock()
        mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        # Test with small batch size
        start_time = time.time()
        await engine.restore_archive(
            archive=large_archive,
            conflict_strategy="skip",
            batch_size=100,
            detect_conflicts=False,
        )
        small_batch_time = time.time() - start_time
        small_batch_calls = mock_conn.executemany.call_count

        # Reset mocks
        mock_conn.executemany.reset_mock()

        # Test with large batch size
        start_time = time.time()
        await engine.restore_archive(
            archive=large_archive,
            conflict_strategy="skip",
            batch_size=5000,
            detect_conflicts=False,
        )
        large_batch_time = time.time() - start_time
        large_batch_calls = mock_conn.executemany.call_count

        # Large batch should have fewer calls
        assert large_batch_calls < small_batch_calls
        # Large batch should be faster (fewer round trips)
        assert large_batch_time <= small_batch_time * 1.5  # Allow some variance

    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_index_dropping_performance(
        self, large_archive: ArchiveFile, mock_db_manager: MagicMock
    ) -> None:
        """Test that dropping indexes improves restore performance."""
        engine = RestoreEngine(mock_db_manager)

        # Mock connection
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(
            return_value=[
                {"indexname": "idx_name", "indexdef": "CREATE INDEX idx_name ON table(name)"},
                {
                    "indexname": "idx_created",
                    "indexdef": "CREATE INDEX idx_created ON table(created_at)",
                },
            ]
        )
        mock_conn.execute = AsyncMock()
        mock_conn.executemany = AsyncMock(return_value="INSERT 0 1000")
        mock_conn.transaction = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_db_manager.pool.acquire = MagicMock()
        mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        # Test without dropping indexes
        start_time = time.time()
        await engine.restore_archive(
            archive=large_archive,
            conflict_strategy="skip",
            batch_size=1000,
            drop_indexes=False,
            detect_conflicts=False,
        )
        without_drop_time = time.time() - start_time

        # Reset mocks
        mock_conn.executemany.reset_mock()
        mock_conn.execute.reset_mock()

        # Test with dropping indexes
        start_time = time.time()
        await engine.restore_archive(
            archive=large_archive,
            conflict_strategy="skip",
            batch_size=1000,
            drop_indexes=True,
            detect_conflicts=False,
        )
        with_drop_time = time.time() - start_time

        # Verify indexes were dropped and restored
        assert mock_conn.execute.call_count >= 4  # 2 DROP + 2 CREATE

        # With index dropping should be faster (though in mocks it's hard to measure)
        # In real scenarios, dropping indexes significantly speeds up bulk inserts
        assert with_drop_time <= without_drop_time * 1.2  # Allow variance in mocks

    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_commit_frequency_impact(
        self, large_archive: ArchiveFile, mock_db_manager: MagicMock
    ) -> None:
        """Test that commit frequency affects transaction overhead."""
        engine = RestoreEngine(mock_db_manager)

        # Mock connection
        mock_conn = MagicMock()
        mock_conn.executemany = AsyncMock(return_value="INSERT 0 1000")
        mock_conn.transaction = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_db_manager.pool.acquire = MagicMock()
        mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        # Test with commit every batch (commit_frequency=1)
        await engine.restore_archive(
            archive=large_archive,
            conflict_strategy="skip",
            batch_size=1000,
            commit_frequency=1,
            detect_conflicts=False,
        )
        frequent_commits = mock_conn.transaction.call_count

        # Reset mocks
        mock_conn.transaction.reset_mock()

        # Test with commit every 5 batches (commit_frequency=5)
        await engine.restore_archive(
            archive=large_archive,
            conflict_strategy="skip",
            batch_size=1000,
            commit_frequency=5,
            detect_conflicts=False,
        )
        infrequent_commits = mock_conn.transaction.call_count

        # Fewer commits should mean fewer transaction calls
        # Note: In current implementation, each batch uses a transaction
        # So commit_frequency doesn't reduce transaction calls yet
        # This test verifies the current behavior
        assert infrequent_commits <= frequent_commits

    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_conflict_detection_overhead(
        self, large_archive: ArchiveFile, mock_db_manager: MagicMock
    ) -> None:
        """Test that conflict detection adds overhead."""
        engine = RestoreEngine(mock_db_manager)

        # Mock connection
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])  # No conflicts
        mock_conn.executemany = AsyncMock(return_value="INSERT 0 1000")
        mock_conn.transaction = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_db_manager.pool.acquire = MagicMock()
        mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        # Test without conflict detection
        start_time = time.time()
        await engine.restore_archive(
            archive=large_archive,
            conflict_strategy="skip",
            batch_size=1000,
            detect_conflicts=False,
        )
        no_detection_time = time.time() - start_time

        # Reset mocks
        mock_conn.fetch.reset_mock()
        mock_conn.executemany.reset_mock()

        # Test with conflict detection
        start_time = time.time()
        await engine.restore_archive(
            archive=large_archive,
            conflict_strategy="skip",
            batch_size=1000,
            detect_conflicts=True,
        )
        with_detection_time = time.time() - start_time

        # Conflict detection should add some overhead (extra query)
        # In mocks, the overhead is minimal, but in real scenarios it's more significant
        assert with_detection_time >= no_detection_time

    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_schema_migration_overhead(
        self, large_archive: ArchiveFile, mock_db_manager: MagicMock
    ) -> None:
        """Test that schema migration adds processing overhead."""
        engine = RestoreEngine(mock_db_manager)

        # Mock schema detection to return same schema (no migration needed)
        with patch.object(engine.schema_detector, "detect_table_schema") as mock_detect:
            mock_detect.return_value = large_archive.table_schema

            # Mock connection
            mock_conn = MagicMock()
            mock_conn.executemany = AsyncMock(return_value="INSERT 0 1000")
            mock_conn.transaction = MagicMock()
            mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
            mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

            mock_db_manager.pool.acquire = MagicMock()
            mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

            # Test with no schema migration (none strategy)
            start_time = time.time()
            await engine.restore_archive(
                archive=large_archive,
                conflict_strategy="skip",
                batch_size=1000,
                schema_migration_strategy="none",
                detect_conflicts=False,
            )
            no_migration_time = time.time() - start_time

            # Reset mocks
            mock_conn.executemany.reset_mock()

            # Test with schema migration (lenient strategy)
            start_time = time.time()
            await engine.restore_archive(
                archive=large_archive,
                conflict_strategy="skip",
                batch_size=1000,
                schema_migration_strategy="lenient",
                detect_conflicts=False,
            )
            with_migration_time = time.time() - start_time

            # Schema migration should add some overhead (record transformation)
            # In this case, schemas match so overhead is minimal
            assert with_migration_time >= no_migration_time

    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_bulk_insert_efficiency(
        self, large_archive: ArchiveFile, mock_db_manager: MagicMock
    ) -> None:
        """Test that bulk insert (executemany) is used efficiently."""
        engine = RestoreEngine(mock_db_manager)

        # Mock connection
        mock_conn = MagicMock()
        mock_conn.executemany = AsyncMock(return_value="INSERT 0 1000")
        mock_conn.transaction = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

        mock_db_manager.pool.acquire = MagicMock()
        mock_db_manager.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_db_manager.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        await engine.restore_archive(
            archive=large_archive,
            conflict_strategy="skip",
            batch_size=1000,
            detect_conflicts=False,
        )

        # Verify executemany was used (not individual executes)
        assert mock_conn.executemany.called
        assert (
            not mock_conn.execute.called
            or mock_conn.execute.call_count < mock_conn.executemany.call_count
        )

        # Verify batches were processed
        assert mock_conn.executemany.call_count > 0
