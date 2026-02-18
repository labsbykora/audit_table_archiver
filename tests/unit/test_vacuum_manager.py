"""Unit tests for vacuum manager."""

import asyncio

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from archiver.config import DatabaseConfig, TableConfig
from archiver.database import DatabaseManager
from archiver.exceptions import DatabaseError
from archiver.vacuum_manager import VacuumManager


def create_mock_acquire_connection(mock_conn: AsyncMock) -> MagicMock:
    """Create a mock acquire_connection async context manager.

    Args:
        mock_conn: Mock connection object

    Returns:
        Mock async context manager for acquire_connection
    """
    mock_context = MagicMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_conn, unsafe=True)
    mock_context.__aexit__ = AsyncMock(return_value=None, unsafe=True)
    return mock_context


@pytest.fixture
def db_config() -> DatabaseConfig:
    """Create database configuration for testing."""
    from archiver.config import TableConfig
    return DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="test_user",
        password_env="TEST_DB_PASSWORD",
        tables=[
            TableConfig(
                name="test_table",
                schema_name="public",
                timestamp_column="created_at",
                primary_key="id",
                retention_days=90,
            )
        ],
    )


@pytest.fixture
def vacuum_manager() -> VacuumManager:
    """Create vacuum manager for testing."""
    return VacuumManager()


@pytest.mark.asyncio
async def test_get_table_size_success(vacuum_manager: VacuumManager, db_config: DatabaseConfig) -> None:
    """Test getting table size successfully."""
    # Create mock database manager with context manager support
    mock_db_manager = MagicMock(spec=DatabaseManager)
    # Create a proper mock record that supports attribute access
    from types import SimpleNamespace
    mock_record = SimpleNamespace(
        total_size_bytes=1000000,
        table_size_bytes=800000,
        indexes_size_bytes=200000,
    )
    # Create mock connection
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=mock_record)
    # Mock acquire_connection context manager
    mock_db_manager.acquire_connection = MagicMock(return_value=create_mock_acquire_connection(mock_conn))

    result = await vacuum_manager.get_table_size(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
    )

    assert result["total_size_bytes"] == 1000000
    assert result["table_size_bytes"] == 800000
    assert result["indexes_size_bytes"] == 200000
    mock_conn.fetchrow.assert_called_once()


@pytest.mark.asyncio
async def test_get_table_size_failure(vacuum_manager: VacuumManager, db_config: DatabaseConfig) -> None:
    """Test getting table size when query fails."""
    mock_db_manager = MagicMock(spec=DatabaseManager)
    # Create mock connection
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(side_effect=Exception("Database error"))
    # Mock acquire_connection context manager
    mock_db_manager.acquire_connection = MagicMock(return_value=create_mock_acquire_connection(mock_conn))

    with pytest.raises(DatabaseError):
        await vacuum_manager.get_table_size(
            db_manager=mock_db_manager,
            schema_name="public",
            table_name="test_table",
        )


@pytest.mark.asyncio
async def test_run_vacuum_none(vacuum_manager: VacuumManager, db_config: DatabaseConfig) -> None:
    """Test running vacuum with type 'none' (disabled)."""
    mock_db_manager = MagicMock(spec=DatabaseManager)

    result = await vacuum_manager.run_vacuum(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
        vacuum_type="none",
        dry_run=False,
    )

    assert result["vacuum_type"] == "none"
    assert result["success"] is True
    assert result["skipped"] is True
    mock_db_manager.execute.assert_not_called()


@pytest.mark.asyncio
async def test_run_vacuum_dry_run(vacuum_manager: VacuumManager, db_config: DatabaseConfig) -> None:
    """Test running vacuum in dry run mode."""
    mock_db_manager = MagicMock(spec=DatabaseManager)

    result = await vacuum_manager.run_vacuum(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
        vacuum_type="analyze",
        dry_run=True,
    )

    assert result["vacuum_type"] == "VACUUM ANALYZE"
    assert result["success"] is True
    assert result["dry_run"] is True
    mock_db_manager.execute.assert_not_called()


@pytest.mark.asyncio
async def test_run_vacuum_analyze(vacuum_manager: VacuumManager, db_config: DatabaseConfig) -> None:
    """Test running VACUUM ANALYZE."""
    mock_db_manager = MagicMock(spec=DatabaseManager)
    # Create mock connection
    mock_conn = AsyncMock()
    # Mock quote_ident query result
    from types import SimpleNamespace
    quote_result = SimpleNamespace(
        schema_quoted='"public"',
        table_quoted='"test_table"',
    )
    mock_conn.fetchrow = AsyncMock(return_value=quote_result)
    mock_conn.execute = AsyncMock(return_value="VACUUM")
    # Mock acquire_connection context manager
    mock_db_manager.acquire_connection = MagicMock(return_value=create_mock_acquire_connection(mock_conn))

    result = await vacuum_manager.run_vacuum(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
        vacuum_type="analyze",
        dry_run=False,
    )

    assert result["vacuum_type"] == "VACUUM ANALYZE"
    assert result["success"] is True
    assert "duration_seconds" in result
    # Check that quote_ident was called
    assert mock_conn.fetchrow.call_count >= 1
    # Check that VACUUM ANALYZE command was executed
    execute_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
    assert any("VACUUM ANALYZE" in call for call in execute_calls)


@pytest.mark.asyncio
async def test_run_vacuum_full(vacuum_manager: VacuumManager, db_config: DatabaseConfig) -> None:
    """Test running VACUUM FULL."""
    mock_db_manager = MagicMock(spec=DatabaseManager)
    # Create mock connection
    mock_conn = AsyncMock()
    # Mock quote_ident query result
    from types import SimpleNamespace
    quote_result = SimpleNamespace(
        schema_quoted='"public"',
        table_quoted='"test_table"',
    )
    mock_conn.fetchrow = AsyncMock(return_value=quote_result)
    mock_conn.execute = AsyncMock(return_value="VACUUM")
    # Mock acquire_connection context manager
    mock_db_manager.acquire_connection = MagicMock(return_value=create_mock_acquire_connection(mock_conn))

    result = await vacuum_manager.run_vacuum(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
        vacuum_type="full",
        dry_run=False,
    )

    assert result["vacuum_type"] == "VACUUM FULL"
    assert result["success"] is True
    # Check that VACUUM FULL command was executed
    execute_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
    assert any("VACUUM FULL" in call for call in execute_calls)


@pytest.mark.asyncio
async def test_run_vacuum_failure(vacuum_manager: VacuumManager, db_config: DatabaseConfig) -> None:
    """Test vacuum failure handling."""
    mock_db_manager = MagicMock(spec=DatabaseManager)
    # Create mock connection
    mock_conn = AsyncMock()
    # Mock quote_ident query result
    from types import SimpleNamespace
    quote_result = SimpleNamespace(
        schema_quoted='"public"',
        table_quoted='"test_table"',
    )
    mock_conn.fetchrow = AsyncMock(return_value=quote_result)
    mock_conn.execute = AsyncMock(side_effect=Exception("Vacuum failed"))
    # Mock acquire_connection context manager
    mock_db_manager.acquire_connection = MagicMock(return_value=create_mock_acquire_connection(mock_conn))

    result = await vacuum_manager.run_vacuum(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
        vacuum_type="analyze",
        dry_run=False,
    )

    assert result["success"] is False
    assert "error" in result
    assert result["error"] == "Vacuum failed"


@pytest.mark.asyncio
async def test_vacuum_table_after_archive_with_space_tracking(
    vacuum_manager: VacuumManager, db_config: DatabaseConfig
) -> None:
    """Test vacuum with space tracking."""
    mock_db_manager = MagicMock(spec=DatabaseManager)
    # Create mock connection
    mock_conn = AsyncMock()

    # Mock size before vacuum
    from types import SimpleNamespace
    size_before_record = SimpleNamespace(
        total_size_bytes=1000000,
        table_size_bytes=800000,
        indexes_size_bytes=200000,
    )

    # Mock size after vacuum (smaller)
    size_after_record = SimpleNamespace(
        total_size_bytes=600000,
        table_size_bytes=500000,
        indexes_size_bytes=100000,
    )

    # Mock quote_ident query result
    quote_result = SimpleNamespace(
        schema_quoted='"public"',
        table_quoted='"test_table"',
    )

    # Setup fetchrow to return different values for different calls
    # Order: quote_ident (for get_table_size), size_before, quote_ident (for run_vacuum), size_after
    fetchrow_calls = [
        size_before_record,  # For get_table_size before (uses quote_ident internally)
        quote_result,  # For quote_ident in run_vacuum
        size_after_record,  # For get_table_size after
    ]
    mock_conn.fetchrow = AsyncMock(side_effect=fetchrow_calls)
    mock_conn.execute = AsyncMock(return_value="VACUUM")

    # Mock acquire_connection context manager - need separate contexts for each call
    def create_context():
        return create_mock_acquire_connection(mock_conn)
    mock_db_manager.acquire_connection = MagicMock(side_effect=create_context)

    result = await vacuum_manager.vacuum_table_after_archive(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
        vacuum_type="analyze",
        dry_run=False,
    )

    assert result["success"] is True
    assert result["space_reclaimed"] is not None
    assert result["space_reclaimed"]["total_size_bytes"] == 400000
    assert result["space_reclaimed"]["table_size_bytes"] == 300000
    assert result["space_reclaimed"]["indexes_size_bytes"] == 100000
    assert result["space_reclaimed"]["reclaim_percentage"] == 40.0


@pytest.mark.asyncio
async def test_vacuum_table_after_archive_size_tracking_failure(
    vacuum_manager: VacuumManager, db_config: DatabaseConfig
) -> None:
    """Test vacuum when size tracking fails (non-critical)."""
    mock_db_manager = MagicMock(spec=DatabaseManager)
    # Create mock connection
    mock_conn = AsyncMock()
    # Mock quote_ident query result
    from types import SimpleNamespace
    quote_result = SimpleNamespace(
        schema_quoted='"public"',
        table_quoted='"test_table"',
    )

    # Setup fetchrow to fail on size queries but succeed on quote_ident
    def fetchrow_side_effect(*args, **kwargs):
        query = args[0] if args else ""
        if "quote_ident" in query:
            return quote_result
        else:
            raise Exception("Size query failed")

    mock_conn.fetchrow = AsyncMock(side_effect=fetchrow_side_effect)
    mock_conn.execute = AsyncMock(return_value="VACUUM")

    # Mock acquire_connection context manager
    def create_context():
        return create_mock_acquire_connection(mock_conn)
    mock_db_manager.acquire_connection = MagicMock(side_effect=create_context)

    # Should still succeed even if size tracking fails
    result = await vacuum_manager.vacuum_table_after_archive(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
        vacuum_type="analyze",
        dry_run=False,
    )

    assert result["success"] is True
    assert result["space_reclaimed"] is None  # Size tracking failed


@pytest.mark.asyncio
async def test_safe_identifier(vacuum_manager: VacuumManager) -> None:
    """Test safe identifier quoting."""
    # Simple identifier (no quoting needed)
    result = VacuumManager._safe_identifier("test_table")
    assert result == "test_table"

    # Identifier with special characters (needs quoting)
    result = VacuumManager._safe_identifier("test-table")
    assert result == '"test-table"'

    # Already quoted identifier (strips quotes, then re-quotes if needed)
    result = VacuumManager._safe_identifier('"test_table"')
    assert result == "test_table"  # Strips quotes, then doesn't re-quote (no special chars)


@pytest.mark.asyncio
async def test_vacuum_effectiveness_warning(
    vacuum_manager: VacuumManager, db_config: DatabaseConfig
) -> None:
    """Test that warning is logged when vacuum reclaims <10% space."""
    mock_db_manager = MagicMock(spec=DatabaseManager)
    # Create mock connection
    mock_conn = AsyncMock()

    # Mock size before vacuum (large table > 100MB)
    from types import SimpleNamespace
    size_before_record = SimpleNamespace(
        total_size_bytes=200 * 1024 * 1024,  # 200MB
        table_size_bytes=150 * 1024 * 1024,
        indexes_size_bytes=50 * 1024 * 1024,
    )

    # Mock size after vacuum (only 5% reclaimed)
    size_after_record = SimpleNamespace(
        total_size_bytes=190 * 1024 * 1024,  # 190MB (only 10MB reclaimed = 5%)
        table_size_bytes=142 * 1024 * 1024,
        indexes_size_bytes=48 * 1024 * 1024,
    )

    # Mock quote_ident query result
    quote_result = SimpleNamespace(
        schema_quoted='"public"',
        table_quoted='"test_table"',
    )

    fetchrow_calls = [
        size_before_record,  # For get_table_size before
        quote_result,  # For quote_ident in run_vacuum
        size_after_record,  # For get_table_size after
    ]
    mock_conn.fetchrow = AsyncMock(side_effect=fetchrow_calls)
    mock_conn.execute = AsyncMock(return_value="VACUUM")

    # Mock acquire_connection context manager - need separate contexts for each call
    def create_context():
        return create_mock_acquire_connection(mock_conn)
    mock_db_manager.acquire_connection = MagicMock(side_effect=create_context)

    # Mock logger to capture warning
    with patch.object(vacuum_manager.logger, "warning") as mock_warning:
        result = await vacuum_manager.vacuum_table_after_archive(
            db_manager=mock_db_manager,
            schema_name="public",
            table_name="test_table",
            vacuum_type="analyze",
            dry_run=False,
        )

        assert result["success"] is True
        assert result["space_reclaimed"]["reclaim_percentage"] == 5.0
        # Check that warning was logged
        mock_warning.assert_called()
        warning_call = mock_warning.call_args
        assert "ineffective" in str(warning_call).lower() or "less than 10%" in str(warning_call).lower()


@pytest.mark.asyncio
async def test_vacuum_timeout(vacuum_manager: VacuumManager, db_config: DatabaseConfig) -> None:
    """Test vacuum timeout handling."""
    mock_db_manager = MagicMock(spec=DatabaseManager)
    # Create mock connection
    mock_conn = AsyncMock()
    # Mock quote_ident query result
    from types import SimpleNamespace
    quote_result = SimpleNamespace(
        schema_quoted='"public"',
        table_quoted='"test_table"',
    )
    mock_conn.fetchrow = AsyncMock(return_value=quote_result)
    # Make execute hang to simulate timeout
    async def slow_execute(*args, **kwargs):
        await asyncio.sleep(10)  # Longer than timeout
        return "VACUUM"

    mock_conn.execute = slow_execute

    # Mock acquire_connection context manager
    mock_db_manager.acquire_connection = MagicMock(return_value=create_mock_acquire_connection(mock_conn))

    # Use short timeout for test
    result = await vacuum_manager.run_vacuum(
        db_manager=mock_db_manager,
        schema_name="public",
        table_name="test_table",
        vacuum_type="analyze",
        dry_run=False,
        timeout_seconds=0.1,  # Very short timeout
    )

    assert result["success"] is False
    assert "timeout" in result or "timeout" in result.get("error", "").lower()

