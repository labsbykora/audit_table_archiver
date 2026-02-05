"""Unit tests for batch processor error paths and edge cases."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from archiver.batch_processor import BatchProcessor
from archiver.config import DatabaseConfig, TableConfig
from archiver.database import DatabaseManager


@pytest.fixture
def table_config() -> TableConfig:
    """Create table config."""
    return TableConfig(
        schema_name="public",
        name="test_table",
        timestamp_column="created_at",
        primary_key="id",
        retention_days=90,
        batch_size=100,
    )


@pytest.fixture
def db_config() -> DatabaseConfig:
    """Create database config."""
    return DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="test_user",
        password_env="TEST_PASSWORD",
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
def mock_db_manager() -> MagicMock:
    """Create a mock database manager."""
    return MagicMock(spec=DatabaseManager)


@pytest.fixture
def batch_processor(
    mock_db_manager: MagicMock, db_config: DatabaseConfig, table_config: TableConfig
) -> BatchProcessor:
    """Create a BatchProcessor instance."""
    return BatchProcessor(
        db_manager=mock_db_manager,
        db_config=db_config,
        table_config=table_config,
    )


@pytest.mark.asyncio
async def test_count_eligible_records_error(batch_processor: BatchProcessor) -> None:
    """Test count_eligible_records with database error."""
    from archiver.exceptions import DatabaseError

    batch_processor.db_manager.fetchval = AsyncMock(
        side_effect=asyncpg.PostgresError("Database error")
    )

    with pytest.raises(DatabaseError, match="Failed to count eligible records"):
        await batch_processor.count_eligible_records()


@pytest.mark.asyncio
async def test_select_batch_no_records(batch_processor: BatchProcessor) -> None:
    """Test select_batch when no records are available."""
    batch_processor.db_manager.fetch = AsyncMock(return_value=[])

    result = await batch_processor.select_batch(batch_size=100)

    assert result == []  # Returns empty list, not None


@pytest.mark.asyncio
async def test_select_batch_with_watermark(batch_processor: BatchProcessor) -> None:
    """Test select_batch with watermark (incremental archival)."""
    last_timestamp = datetime.now(timezone.utc) - timedelta(days=10)
    last_pk = 100

    # Mock records
    mock_records = [
        {"id": 101, "created_at": last_timestamp + timedelta(hours=1)},
        {"id": 102, "created_at": last_timestamp + timedelta(hours=2)},
    ]
    batch_processor.db_manager.fetch = AsyncMock(return_value=mock_records)

    result = await batch_processor.select_batch(
        batch_size=100, last_timestamp=last_timestamp, last_primary_key=last_pk
    )

    assert result is not None
    # Verify watermark was used in query
    fetch_calls = [str(call) for call in batch_processor.db_manager.fetch.call_args_list]
    assert any("created_at >" in str(call) or "id >" in str(call) for call in fetch_calls)


@pytest.mark.asyncio
async def test_select_batch_skip_locked(batch_processor: BatchProcessor) -> None:
    """Test select_batch uses SKIP LOCKED."""
    mock_records = [{"id": 1, "created_at": datetime.now(timezone.utc)}]
    batch_processor.db_manager.fetch = AsyncMock(return_value=mock_records)

    await batch_processor.select_batch(batch_size=100)

    # Verify SKIP LOCKED was used
    fetch_calls = [str(call) for call in batch_processor.db_manager.fetch.call_args_list]
    assert any("SKIP LOCKED" in str(call) for call in fetch_calls)


def test_calculate_cutoff_date(batch_processor: BatchProcessor) -> None:
    """Test calculate_cutoff_date."""
    cutoff = batch_processor.calculate_cutoff_date(safety_buffer_days=1)

    # Should be retention_days + safety_buffer_days ago
    expected = datetime.now(timezone.utc) - timedelta(days=91)

    # Allow small time difference
    assert abs((cutoff - expected).total_seconds()) < 60


def test_calculate_cutoff_date_custom_buffer(batch_processor: BatchProcessor) -> None:
    """Test calculate_cutoff_date with custom buffer."""
    cutoff = batch_processor.calculate_cutoff_date(safety_buffer_days=5)

    expected = datetime.now(timezone.utc) - timedelta(days=95)
    assert abs((cutoff - expected).total_seconds()) < 60


def test_records_to_dicts(batch_processor: BatchProcessor) -> None:
    """Test records_to_dicts conversion."""
    # Use actual dicts (records_to_dicts expects dict-like objects with items() method)
    records = [
        {"id": 1, "name": "test"},
        {"id": 2, "name": "test2"},
    ]
    result = batch_processor.records_to_dicts(records)

    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


def test_extract_primary_keys(batch_processor: BatchProcessor) -> None:
    """Test extract_primary_keys."""
    records = [
        {"id": 1, "name": "test1"},
        {"id": 2, "name": "test2"},
        {"id": 3, "name": "test3"},
    ]

    result = batch_processor.extract_primary_keys(records)

    assert result == [1, 2, 3]


def test_extract_primary_keys_missing_column(batch_processor: BatchProcessor) -> None:
    """Test extract_primary_keys with missing primary key column."""
    records = [
        {"name": "test1"},  # Missing "id"
        {"id": 2, "name": "test2"},
    ]

    with pytest.raises(KeyError):
        batch_processor.extract_primary_keys(records)


def test_get_last_cursor(batch_processor: BatchProcessor) -> None:
    """Test get_last_cursor."""
    now = datetime.now(timezone.utc)
    records = [
        {"id": 1, "created_at": now},
        {"id": 2, "created_at": now},
        {"id": 3, "created_at": now},
    ]

    # get_last_cursor returns (last_timestamp, last_primary_key)
    last_timestamp, last_pk = batch_processor.get_last_cursor(records)

    assert last_pk == 3
    assert last_timestamp is not None
    assert last_timestamp == now


def test_get_last_cursor_empty(batch_processor: BatchProcessor) -> None:
    """Test get_last_cursor with empty records."""
    # Based on existing test, it returns None, None for empty records
    last_timestamp, last_pk = batch_processor.get_last_cursor([])
    assert last_timestamp is None
    assert last_pk is None


def test_get_timestamp_range(batch_processor: BatchProcessor) -> None:
    """Test get_timestamp_range."""
    now = datetime.now(timezone.utc)
    records = [
        {"id": 1, "created_at": now - timedelta(hours=2)},
        {"id": 2, "created_at": now - timedelta(hours=1)},
        {"id": 3, "created_at": now},
    ]

    result = batch_processor.get_timestamp_range(records)

    assert result["min"] == now - timedelta(hours=2)
    assert result["max"] == now


def test_get_timestamp_range_empty(batch_processor: BatchProcessor) -> None:
    """Test get_timestamp_range with empty records."""
    result = batch_processor.get_timestamp_range([])

    assert result["min"] is None
    assert result["max"] is None


def test_get_timestamp_range_missing_column(batch_processor: BatchProcessor) -> None:
    """Test get_timestamp_range with missing timestamp column."""
    records = [
        {"id": 1},  # Missing "created_at"
        {"id": 2, "created_at": datetime.now(timezone.utc)},
    ]

    # Should handle missing column gracefully
    result = batch_processor.get_timestamp_range(records)
    # At least one record has timestamp, so should have a range
    assert result["max"] is not None
