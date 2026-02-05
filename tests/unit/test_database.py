"""Unit tests for database module."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
                name="test_table",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )


@pytest.mark.asyncio
async def test_database_manager_init(db_config: DatabaseConfig) -> None:
    """Test database manager initialization."""
    manager = DatabaseManager(db_config, pool_size=5)
    assert manager.config == db_config
    assert manager.pool_size == 5
    assert manager.pool is None


@pytest.mark.asyncio
async def test_database_manager_dsn(db_config: DatabaseConfig) -> None:
    """Test DSN generation."""
    manager = DatabaseManager(db_config)
    dsn = manager.dsn
    assert "test_user" in dsn
    assert "test_password" in dsn
    assert "localhost" in dsn
    assert "test_db" in dsn


@pytest.mark.asyncio
async def test_database_manager_dsn_missing_password() -> None:
    """Test DSN generation fails when password env var missing."""
    config = DatabaseConfig(
        name="test_db",
        host="localhost",
        user="test_user",
        password_env="NONEXISTENT_ENV",
        tables=[
            TableConfig(
                schema_name="public",
                name="test_table",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )
    manager = DatabaseManager(config)

    with pytest.raises(DatabaseError, match="Environment variable.*not set"):
        _ = manager.dsn


@pytest.mark.asyncio
async def test_database_manager_connect(db_config: DatabaseConfig) -> None:
    """Test database connection."""
    manager = DatabaseManager(db_config)

    with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        # Mock pool instance
        mock_pool_instance = AsyncMock()

        # Mock connection for health check
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value="PostgreSQL 14.5")

        # Make acquire() return an async context manager
        # acquire() is NOT async - it returns an async context manager directly
        async def aenter(self):
            return mock_conn

        async def aexit(self, exc_type, exc_val, exc_tb):
            return None

        mock_acquire_context = MagicMock()  # Use MagicMock, not AsyncMock
        mock_acquire_context.__aenter__ = aenter
        mock_acquire_context.__aexit__ = aexit
        # acquire() is a regular method that returns the context manager
        mock_pool_instance.acquire = MagicMock(return_value=mock_acquire_context)

        # Make create_pool awaitable (return the pool instance when awaited)
        mock_create_pool.return_value = mock_pool_instance

        await manager.connect()

        assert manager.pool is not None
        mock_create_pool.assert_called_once()


@pytest.mark.asyncio
async def test_database_manager_disconnect(db_config: DatabaseConfig) -> None:
    """Test database disconnection."""
    manager = DatabaseManager(db_config)

    # Create mock pool
    mock_pool = AsyncMock()
    manager.pool = mock_pool

    await manager.disconnect()

    mock_pool.close.assert_called_once()
    assert manager.pool is None


@pytest.mark.asyncio
async def test_database_manager_health_check_healthy(db_config: DatabaseConfig) -> None:
    """Test health check when healthy."""
    manager = DatabaseManager(db_config)

    mock_pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=1)

    # Make acquire() return an async context manager
    # acquire() is NOT async - it returns an async context manager directly
    async def aenter(self):
        return mock_conn

    async def aexit(self, exc_type, exc_val, exc_tb):
        return None

    mock_acquire_context = MagicMock()  # Use MagicMock, not AsyncMock
    mock_acquire_context.__aenter__ = aenter
    mock_acquire_context.__aexit__ = aexit
    # acquire() is a regular method that returns the context manager
    mock_pool.acquire = MagicMock(return_value=mock_acquire_context)

    manager.pool = mock_pool

    result = await manager.health_check()
    assert result is True


@pytest.mark.asyncio
async def test_database_manager_health_check_unhealthy(db_config: DatabaseConfig) -> None:
    """Test health check when unhealthy."""
    manager = DatabaseManager(db_config)

    mock_pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(side_effect=Exception("Connection failed"))

    # Make acquire() return an async context manager
    # acquire() is NOT async - it returns an async context manager directly
    async def aenter(self):
        return mock_conn

    async def aexit(self, exc_type, exc_val, exc_tb):
        return None

    mock_acquire_context = MagicMock()  # Use MagicMock, not AsyncMock
    mock_acquire_context.__aenter__ = aenter
    mock_acquire_context.__aexit__ = aexit
    # acquire() is a regular method that returns the context manager
    mock_pool.acquire = MagicMock(return_value=mock_acquire_context)

    manager.pool = mock_pool

    result = await manager.health_check()
    assert result is False


@pytest.mark.asyncio
async def test_database_manager_acquire_connection_not_initialized(
    db_config: DatabaseConfig,
) -> None:
    """Test acquire_connection fails when pool not initialized."""
    manager = DatabaseManager(db_config)

    with pytest.raises(DatabaseError, match="Connection pool not initialized"):
        async with manager.acquire_connection():
            pass


@pytest.mark.asyncio
async def test_database_manager_fetchval(db_config: DatabaseConfig) -> None:
    """Test fetchval method."""
    manager = DatabaseManager(db_config)

    mock_pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=42)
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    result = await manager.fetchval("SELECT 42")
    assert result == 42
    mock_conn.fetchval.assert_called_once_with("SELECT 42")


@pytest.mark.asyncio
async def test_database_manager_fetchval_error(db_config: DatabaseConfig) -> None:
    """Test fetchval raises DatabaseError on failure."""
    manager = DatabaseManager(db_config)

    mock_pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(side_effect=Exception("Query failed"))
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    with pytest.raises(DatabaseError, match="Query execution failed"):
        await manager.fetchval("SELECT * FROM invalid_table")


@pytest.mark.asyncio
async def test_database_manager_execute(db_config: DatabaseConfig) -> None:
    """Test execute method."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value="INSERT 0 1")
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    result = await manager.execute("INSERT INTO test VALUES ($1)", "value")
    assert result == "INSERT 0 1"
    mock_conn.execute.assert_called_once_with("INSERT INTO test VALUES ($1)", "value")
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_execute_error(db_config: DatabaseConfig) -> None:
    """Test execute raises DatabaseError on failure."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(side_effect=Exception("Query failed"))
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    with pytest.raises(DatabaseError, match="Query execution failed"):
        await manager.execute("INSERT INTO invalid_table VALUES ($1)", "value")
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_fetch(db_config: DatabaseConfig) -> None:
    """Test fetch method."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_records = [MagicMock(), MagicMock()]
    mock_conn.fetch = AsyncMock(return_value=mock_records)
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    result = await manager.fetch("SELECT * FROM test")
    assert result == mock_records
    mock_conn.fetch.assert_called_once_with("SELECT * FROM test")
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_fetch_error(db_config: DatabaseConfig) -> None:
    """Test fetch raises DatabaseError on failure."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(side_effect=Exception("Query failed"))
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    with pytest.raises(DatabaseError, match="Query execution failed"):
        await manager.fetch("SELECT * FROM invalid_table")
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_fetchrow(db_config: DatabaseConfig) -> None:
    """Test fetchrow method."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_record = MagicMock()
    mock_conn.fetchrow = AsyncMock(return_value=mock_record)
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    result = await manager.fetchrow("SELECT * FROM test WHERE id = $1", 1)
    assert result == mock_record
    mock_conn.fetchrow.assert_called_once_with("SELECT * FROM test WHERE id = $1", 1)
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_fetchrow_none(db_config: DatabaseConfig) -> None:
    """Test fetchrow returns None when no row found."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=None)
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    result = await manager.fetchrow("SELECT * FROM test WHERE id = $1", 999)
    assert result is None
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_fetchone(db_config: DatabaseConfig) -> None:
    """Test fetchone method."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_record = MagicMock()
    mock_record.__getitem__ = lambda self, key: {"id": 1, "name": "test"}[key]
    mock_record.keys = lambda: ["id", "name"]
    mock_conn.fetchrow = AsyncMock(return_value=mock_record)
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    result = await manager.fetchone("SELECT * FROM test WHERE id = $1", 1)
    assert result is not None
    assert result == {"id": 1, "name": "test"}
    mock_conn.fetchrow.assert_called_once_with("SELECT * FROM test WHERE id = $1", 1)
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_fetchone_none(db_config: DatabaseConfig) -> None:
    """Test fetchone returns None when no row found."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=None)
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    result = await manager.fetchone("SELECT * FROM test WHERE id = $1", 999)
    assert result is None
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_get_postgres_version(db_config: DatabaseConfig) -> None:
    """Test get_postgres_version method."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value="PostgreSQL 14.5")
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    version = await manager.get_postgres_version()
    assert version == "14.5"
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_get_postgres_version_unknown(db_config: DatabaseConfig) -> None:
    """Test get_postgres_version with unknown format."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value="Unknown version string")
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    version = await manager.get_postgres_version()
    assert version == "unknown"
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_get_postgres_version_none(db_config: DatabaseConfig) -> None:
    """Test get_postgres_version when version is None."""
    manager = DatabaseManager(db_config)

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=None)
    mock_pool.acquire = AsyncMock(return_value=mock_conn)
    mock_pool.release = AsyncMock()

    manager.pool = mock_pool

    version = await manager.get_postgres_version()
    assert version == "unknown"
    mock_pool.release.assert_called_once_with(mock_conn)


@pytest.mark.asyncio
async def test_database_manager_transaction(db_config: DatabaseConfig) -> None:
    """Test transaction context manager."""
    manager = DatabaseManager(db_config)

    mock_pool = AsyncMock()
    mock_conn = AsyncMock()

    async def aenter(self):
        return mock_conn

    async def aexit(self, exc_type, exc_val, exc_tb):
        return None

    async def tenter(self):
        return mock_conn

    async def texit(self, exc_type, exc_val, exc_tb):
        return None

    mock_acquire_context = MagicMock()
    mock_acquire_context.__aenter__ = aenter
    mock_acquire_context.__aexit__ = aexit

    mock_transaction_context = MagicMock()
    mock_transaction_context.__aenter__ = tenter
    mock_transaction_context.__aexit__ = texit

    mock_conn.transaction = MagicMock(return_value=mock_transaction_context)
    mock_pool.acquire = MagicMock(return_value=mock_acquire_context)

    manager.pool = mock_pool

    async with manager.transaction() as conn:
        assert conn == mock_conn

    mock_conn.transaction.assert_called_once()
