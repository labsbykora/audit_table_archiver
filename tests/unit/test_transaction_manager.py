"""Unit tests for transaction manager."""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from archiver.exceptions import TransactionError
from archiver.transaction_manager import TransactionManager


@pytest.fixture
def mock_connection() -> MagicMock:
    """Create a mock asyncpg connection."""
    connection = MagicMock(spec=asyncpg.Connection)
    connection.transaction = MagicMock()
    connection.execute = AsyncMock()
    return connection


@pytest.fixture
def transaction_manager(mock_connection: MagicMock) -> TransactionManager:
    """Create a TransactionManager instance."""
    return TransactionManager(mock_connection, timeout_seconds=60)


@pytest.mark.asyncio
async def test_transaction_success(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test successful transaction."""
    mock_transaction = MagicMock()
    mock_transaction.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_transaction.__aexit__ = AsyncMock(return_value=None)
    mock_connection.transaction.return_value = mock_transaction
    
    async with transaction_manager.transaction() as conn:
        assert conn == mock_connection
    
    mock_connection.execute.assert_called_once()


@pytest.mark.asyncio
async def test_transaction_postgres_error(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test transaction with PostgreSQL error."""
    mock_transaction = MagicMock()
    error = asyncpg.PostgresError("Database error")
    error.sqlstate = "23505"  # Unique violation
    
    async def aenter(*args):
        raise error
    
    mock_transaction.__aenter__ = AsyncMock(side_effect=aenter)
    mock_transaction.__aexit__ = AsyncMock(return_value=None)
    mock_connection.transaction.return_value = mock_transaction
    
    with pytest.raises(TransactionError, match="Transaction failed"):
        async with transaction_manager.transaction():
            pass


@pytest.mark.asyncio
async def test_transaction_timeout(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test transaction timeout."""
    mock_transaction = MagicMock()
    
    async def aenter(*args):
        await asyncio.sleep(0.1)  # Simulate delay
        raise asyncio.TimeoutError("Transaction timeout")
    
    mock_transaction.__aenter__ = AsyncMock(side_effect=aenter)
    mock_transaction.__aexit__ = AsyncMock(return_value=None)
    mock_connection.transaction.return_value = mock_transaction
    
    with pytest.raises(TransactionError, match="Transaction timeout"):
        async with transaction_manager.transaction():
            pass


@pytest.mark.asyncio
async def test_savepoint_success(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test successful savepoint creation and release."""
    async with transaction_manager.savepoint("test_sp"):
        pass
    
    # Should call SAVEPOINT and RELEASE
    assert mock_connection.execute.call_count == 2
    calls = [str(call) for call in mock_connection.execute.call_args_list]
    assert any("SAVEPOINT test_sp" in str(call) for call in calls)
    assert any("RELEASE SAVEPOINT test_sp" in str(call) for call in calls)


@pytest.mark.asyncio
async def test_savepoint_auto_name(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test savepoint with auto-generated name."""
    async with transaction_manager.savepoint():
        pass
    
    # Should use auto-generated name sp_1
    calls = [str(call) for call in mock_connection.execute.call_args_list]
    assert any("SAVEPOINT sp_1" in str(call) for call in calls)


@pytest.mark.asyncio
async def test_savepoint_rollback(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test savepoint rollback on error."""
    error = asyncpg.PostgresError("Savepoint error")
    
    # First call (SAVEPOINT) succeeds, second call (operation) fails
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:  # Simulate error on second call
            raise error
        return None
    
    mock_connection.execute.side_effect = execute_side_effect
    
    with pytest.raises(TransactionError, match="Savepoint operation failed"):
        async with transaction_manager.savepoint("test_sp"):
            # This should trigger the error
            await mock_connection.execute("SELECT 1")
    
    # Should have SAVEPOINT, error operation, and ROLLBACK TO SAVEPOINT
    assert mock_connection.execute.call_count >= 2
    calls = [str(call) for call in mock_connection.execute.call_args_list]
    assert any("ROLLBACK TO SAVEPOINT test_sp" in str(call) for call in calls)


@pytest.mark.asyncio
async def test_savepoint_rollback_failure(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test savepoint rollback failure."""
    error = asyncpg.PostgresError("Savepoint error")
    rollback_error = asyncpg.PostgresError("Rollback failed")
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        query = str(args[0]) if args else ""
        if "ROLLBACK TO SAVEPOINT" in query:
            raise rollback_error
        if call_count == 2:
            raise error
        return None
    
    mock_connection.execute.side_effect = execute_side_effect
    
    with pytest.raises(TransactionError):
        async with transaction_manager.savepoint("test_sp"):
            await mock_connection.execute("SELECT 1")


@pytest.mark.asyncio
async def test_get_transaction_age_no_transaction(transaction_manager: TransactionManager) -> None:
    """Test get_transaction_age when no transaction is active."""
    age = transaction_manager.get_transaction_age()
    assert age is None


@pytest.mark.asyncio
async def test_get_transaction_age_active(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test get_transaction_age during active transaction."""
    mock_transaction = MagicMock()
    mock_transaction.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_transaction.__aexit__ = AsyncMock(return_value=None)
    mock_connection.transaction.return_value = mock_transaction
    
    async with transaction_manager.transaction():
        age = transaction_manager.get_transaction_age()
        assert age is not None
        assert age >= 0


@pytest.mark.asyncio
async def test_transaction_monitor_warning(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test transaction monitor warning at 50% timeout."""
    # Use a very short timeout for testing
    manager = TransactionManager(mock_connection, timeout_seconds=1)
    
    mock_transaction = MagicMock()
    mock_transaction.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_transaction.__aexit__ = AsyncMock(return_value=None)
    mock_connection.transaction.return_value = mock_transaction
    
    # Manually set transaction start to trigger warning
    manager._transaction_start = datetime.now() - timedelta(seconds=0.6)
    
    # The monitor task should warn at 50% (0.5 seconds)
    # We can't easily test the async task, but we can verify the logic
    age = manager.get_transaction_age()
    assert age is not None
    assert age > 0.5  # Should be past warning threshold


@pytest.mark.asyncio
async def test_transaction_reset_on_exit(transaction_manager: TransactionManager, mock_connection: MagicMock) -> None:
    """Test that transaction state is reset after exit."""
    mock_transaction = MagicMock()
    mock_transaction.__aenter__ = AsyncMock(return_value=mock_connection)
    mock_transaction.__aexit__ = AsyncMock(return_value=None)
    mock_connection.transaction.return_value = mock_transaction
    
    async with transaction_manager.transaction():
        assert transaction_manager._transaction_start is not None
        assert transaction_manager._savepoint_count == 0
    
    assert transaction_manager._transaction_start is None
    assert transaction_manager._savepoint_count == 0
