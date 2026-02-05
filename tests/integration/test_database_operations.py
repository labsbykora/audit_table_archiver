"""Integration tests for database operations."""


import pytest

from archiver.config import DatabaseConfig
from archiver.database import DatabaseManager

# Fixtures are auto-discovered from conftest.py by pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_database_manager_connect() -> None:
    """Test database manager can connect."""
    import os

    from archiver.config import TableConfig

    os.environ["TEST_DB_PASSWORD"] = "archiver_password"

    db_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="TEST_DB_PASSWORD",
        tables=[
            TableConfig(
                name="dummy_table",
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )

    db_manager = DatabaseManager(db_config)

    await db_manager.connect()

    try:
        # Test health check
        assert await db_manager.health_check() is True

        # Test query
        version = await db_manager.fetchval("SELECT version()")
        assert version is not None
    finally:
        await db_manager.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_database_manager_transaction(db_connection, test_table: str) -> None:
    """Test database manager transaction."""
    import os

    from archiver.config import TableConfig

    os.environ["TEST_DB_PASSWORD"] = "archiver_password"

    db_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="TEST_DB_PASSWORD",
        tables=[
            TableConfig(
                name="dummy_table",
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )

    db_manager = DatabaseManager(db_config)
    await db_manager.connect()

    try:
        # Insert record in transaction
        async with db_manager.transaction() as conn:
            await conn.execute(
                f"INSERT INTO {test_table} (user_id, action, created_at) VALUES ($1, $2, NOW())",
                999,
                "test_action",
            )

        # Verify record exists
        count = await db_manager.fetchval(f"SELECT COUNT(*) FROM {test_table} WHERE user_id = 999")
        assert count == 1

        # Test rollback
        try:
            async with db_manager.transaction() as conn:
                await conn.execute(
                    f"INSERT INTO {test_table} (user_id, action, created_at) VALUES ($1, $2, NOW())",
                    998,
                    "test_action",
                )
                raise Exception("Force rollback")
        except Exception:
            pass

        # Verify record not inserted (rolled back)
        count = await db_manager.fetchval(f"SELECT COUNT(*) FROM {test_table} WHERE user_id = 998")
        assert count == 0

    finally:
        await db_manager.disconnect()

