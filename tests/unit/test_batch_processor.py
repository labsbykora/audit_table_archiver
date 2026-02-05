"""Unit tests for batch processor module."""

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from archiver.batch_processor import BatchProcessor
from archiver.config import DatabaseConfig, TableConfig
from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError


@pytest.fixture
def db_config() -> DatabaseConfig:
    """Create test database configuration."""
    os.environ["TEST_DB_PASSWORD"] = "test_password"
    return DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="test_user",
        password_env="TEST_DB_PASSWORD",
        tables=[
            TableConfig(
                schema_name="public",
                name="test_table",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )


@pytest.fixture
def table_config() -> TableConfig:
    """Create test table configuration."""
    return TableConfig(
        name="test_table",
        schema_name="public",
        timestamp_column="created_at",
        primary_key="id",
        retention_days=90,
        batch_size=10000,
    )


@pytest.fixture
def db_manager(db_config: DatabaseConfig) -> DatabaseManager:
    """Create test database manager."""
    return DatabaseManager(db_config)


@pytest.fixture
def batch_processor(
    db_manager: DatabaseManager, db_config: DatabaseConfig, table_config: TableConfig
) -> BatchProcessor:
    """Create test batch processor."""
    return BatchProcessor(db_manager, db_config, table_config)


@pytest.mark.asyncio
async def test_calculate_cutoff_date(batch_processor: BatchProcessor) -> None:
    """Test cutoff date calculation."""
    cutoff = batch_processor.calculate_cutoff_date()

    assert isinstance(cutoff, datetime)
    assert cutoff.tzinfo is not None  # Should be timezone-aware

    # Should be approximately 91 days ago (90 retention + 1 buffer)
    now = datetime.now(timezone.utc)
    days_diff = (now - cutoff).days
    assert 90 <= days_diff <= 92  # Allow some variance


@pytest.mark.asyncio
async def test_calculate_cutoff_date_with_buffer(batch_processor: BatchProcessor) -> None:
    """Test cutoff date calculation with custom buffer."""
    cutoff = batch_processor.calculate_cutoff_date(safety_buffer_days=2)

    now = datetime.now(timezone.utc)
    days_diff = (now - cutoff).days
    assert 91 <= days_diff <= 93  # 90 retention + 2 buffer


@pytest.mark.asyncio
async def test_count_eligible_records(batch_processor: BatchProcessor) -> None:
    """Test counting eligible records."""
    mock_db_manager = MagicMock()
    # First call: _is_timestamp_column_timezone_aware (data_type lookup)
    # Second call: actual COUNT(*) query
    mock_db_manager.fetchval = AsyncMock(side_effect=["timestamp with time zone", 100])
    batch_processor.db_manager = mock_db_manager

    count = await batch_processor.count_eligible_records()

    assert count == 100
    assert mock_db_manager.fetchval.call_count == 2


@pytest.mark.asyncio
async def test_count_eligible_records_error(batch_processor: BatchProcessor) -> None:
    """Test count_eligible_records raises error on failure."""
    mock_db_manager = MagicMock()
    mock_db_manager.fetchval = AsyncMock(side_effect=Exception("Query failed"))
    batch_processor.db_manager = mock_db_manager

    with pytest.raises(DatabaseError, match="Failed to count eligible records"):
        await batch_processor.count_eligible_records()


@pytest.mark.asyncio
async def test_select_batch_first_batch(batch_processor: BatchProcessor) -> None:
    """Test selecting first batch."""
    mock_db_manager = MagicMock()
    mock_records = [MagicMock(), MagicMock()]
    mock_db_manager.fetch = AsyncMock(return_value=mock_records)
    batch_processor.db_manager = mock_db_manager

    records = await batch_processor.select_batch(batch_size=10)

    assert len(records) == 2
    mock_db_manager.fetch.assert_called_once()
    # Verify query contains SKIP LOCKED
    call_args = mock_db_manager.fetch.call_args[0]
    query = call_args[0]
    assert "SKIP LOCKED" in query
    assert "ORDER BY" in query


@pytest.mark.asyncio
async def test_select_batch_with_cursor(batch_processor: BatchProcessor) -> None:
    """Test selecting batch with cursor (pagination)."""
    mock_db_manager = MagicMock()
    mock_records = [MagicMock()]
    mock_db_manager.fetch = AsyncMock(return_value=mock_records)
    batch_processor.db_manager = mock_db_manager

    last_timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
    last_pk = 100

    records = await batch_processor.select_batch(
        batch_size=10, last_timestamp=last_timestamp, last_primary_key=last_pk
    )

    assert len(records) == 1
    mock_db_manager.fetch.assert_called_once()
    # Verify query uses cursor
    call_args = mock_db_manager.fetch.call_args[0]
    query = call_args[0]
    assert "$2" in query  # Should have cursor parameters
    assert "$3" in query


@pytest.mark.asyncio
async def test_select_batch_error(batch_processor: BatchProcessor) -> None:
    """Test select_batch raises error on failure."""
    mock_db_manager = MagicMock()
    mock_db_manager.fetch = AsyncMock(side_effect=Exception("Query failed"))
    batch_processor.db_manager = mock_db_manager

    with pytest.raises(DatabaseError, match="Failed to select batch"):
        await batch_processor.select_batch(batch_size=10)


def test_records_to_dicts(batch_processor: BatchProcessor) -> None:
    """Test converting records to dictionaries."""
    # Mock asyncpg records (they're like dicts)
    mock_record1 = MagicMock()
    mock_record1.__getitem__ = lambda self, key: {"id": 1, "name": "test1"}[key]
    mock_record1.keys = lambda: {"id": 1, "name": "test1"}.keys()
    mock_record1.items = lambda: {"id": 1, "name": "test1"}.items()

    mock_record2 = MagicMock()
    mock_record2.__getitem__ = lambda self, key: {"id": 2, "name": "test2"}[key]
    mock_record2.keys = lambda: {"id": 2, "name": "test2"}.keys()
    mock_record2.items = lambda: {"id": 2, "name": "test2"}.items()

    # Use actual dicts for simplicity
    records = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

    result = batch_processor.records_to_dicts(records)

    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_extract_primary_keys(batch_processor: BatchProcessor) -> None:
    """Test extracting primary keys from records."""
    records = [
        {"id": 1, "name": "test1"},
        {"id": 2, "name": "test2"},
        {"id": 3, "name": "test3"},
    ]

    pks = batch_processor.extract_primary_keys(records)

    assert pks == [1, 2, 3]


def test_get_last_cursor(batch_processor: BatchProcessor) -> None:
    """Test getting cursor from last record."""
    records = [
        {"id": 1, "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc)},
        {"id": 2, "created_at": datetime(2025, 1, 2, tzinfo=timezone.utc)},
        {"id": 3, "created_at": datetime(2025, 1, 3, tzinfo=timezone.utc)},
    ]

    last_timestamp, last_pk = batch_processor.get_last_cursor(records)

    assert last_timestamp == datetime(2025, 1, 3, tzinfo=timezone.utc)
    assert last_pk == 3


def test_get_last_cursor_empty(batch_processor: BatchProcessor) -> None:
    """Test getting cursor from empty batch."""
    records = []
    last_timestamp, last_pk = batch_processor.get_last_cursor(records)

    assert last_timestamp is None
    assert last_pk is None
