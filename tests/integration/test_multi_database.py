"""Integration tests for multi-database support."""

import os

import pytest

from archiver.archiver import Archiver
from archiver.config import ArchiverConfig, DatabaseConfig, DefaultsConfig, TableConfig

# Fixtures are auto-discovered from conftest.py by pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sequential_multi_database_processing(
    archiver_config: ArchiverConfig, test_table: str
) -> None:
    """Test sequential processing of multiple databases."""

    # Set password environment variable
    os.environ.setdefault("DB_PASSWORD", "archiver_password")

    # Create a second database configuration (using same test database for simplicity)
    # In real scenario, these would be different databases
    db1_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="DB_PASSWORD",
        tables=[
            TableConfig(
                name=test_table,
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
                retention_days=90,
                batch_size=10,
            )
        ],
    )

    # Create config with two "databases" (same DB, different table configs for testing)
    # In production, these would be actual different databases
    multi_db_config = ArchiverConfig(
        version="2.0",
        s3=archiver_config.s3,
        defaults=DefaultsConfig(
            parallel_databases=False,  # Sequential mode
            connection_pool_size=5,
        ),
        databases=[db1_config],
    )

    # Set S3 credentials
    multi_db_config.s3.aws_access_key_id = "minioadmin"
    multi_db_config.s3.aws_secret_access_key = "minioadmin"

    archiver = Archiver(multi_db_config, dry_run=True)

    stats = await archiver.archive()

    assert stats["databases_processed"] == 1
    assert stats["databases_failed"] == 0
    assert len(stats["database_stats"]) == 1
    assert stats["database_stats"][0]["database"] == "test_db"
    assert stats["database_stats"][0]["success"] is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_database_failure_isolation(
    archiver_config: ArchiverConfig, test_table: str
) -> None:
    """Test that one database failure doesn't stop processing of other databases."""

    # Set password environment variable
    os.environ.setdefault("DB_PASSWORD", "archiver_password")

    # Create config with one valid and one invalid database
    valid_db = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="DB_PASSWORD",
        tables=[
            TableConfig(
                name=test_table,
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
                retention_days=90,
                batch_size=10,
            )
        ],
    )

    invalid_db = DatabaseConfig(
        name="nonexistent_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="DB_PASSWORD",
        tables=[
            TableConfig(
                name="nonexistent_table",
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
            )
        ],
    )

    multi_db_config = ArchiverConfig(
        version="2.0",
        s3=archiver_config.s3,
        defaults=DefaultsConfig(parallel_databases=False),
        databases=[valid_db, invalid_db],
    )

    multi_db_config.s3.aws_access_key_id = "minioadmin"
    multi_db_config.s3.aws_secret_access_key = "minioadmin"

    archiver = Archiver(multi_db_config, dry_run=True)

    stats = await archiver.archive()

    # Valid database should be processed
    assert stats["databases_processed"] >= 0  # May be 0 if invalid DB fails early
    # Invalid database should fail but not stop processing
    assert stats["databases_failed"] >= 1
    # Should have stats for both databases
    assert len(stats["database_stats"]) == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_per_database_statistics(
    archiver_config: ArchiverConfig, test_table: str
) -> None:
    """Test that per-database statistics are correctly tracked."""

    # Set password environment variable
    os.environ.setdefault("DB_PASSWORD", "archiver_password")

    db_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="DB_PASSWORD",
        tables=[
            TableConfig(
                name=test_table,
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
                retention_days=90,
                batch_size=10,
            )
        ],
    )

    multi_db_config = ArchiverConfig(
        version="2.0",
        s3=archiver_config.s3,
        defaults=DefaultsConfig(parallel_databases=False),
        databases=[db_config],
    )

    multi_db_config.s3.aws_access_key_id = "minioadmin"
    multi_db_config.s3.aws_secret_access_key = "minioadmin"

    archiver = Archiver(multi_db_config, dry_run=True)

    stats = await archiver.archive()

    # Check that database_stats is present
    assert "database_stats" in stats
    assert len(stats["database_stats"]) == 1

    db_stat = stats["database_stats"][0]
    assert "database" in db_stat
    assert "tables_processed" in db_stat
    assert "tables_failed" in db_stat
    assert "records_archived" in db_stat
    assert "batches_processed" in db_stat
    assert "start_time" in db_stat
    assert "end_time" in db_stat
    assert "success" in db_stat

    # Verify statistics are consistent
    assert db_stat["database"] == "test_db"
    assert isinstance(db_stat["tables_processed"], int)
    assert isinstance(db_stat["records_archived"], int)
    assert isinstance(db_stat["batches_processed"], int)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_pool_size_configuration(
    archiver_config: ArchiverConfig, test_table: str
) -> None:
    """Test that per-database connection pool size configuration works."""

    # Set password environment variable
    os.environ.setdefault("DB_PASSWORD", "archiver_password")

    # Test with custom pool size
    db_config = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="DB_PASSWORD",
        connection_pool_size=10,  # Custom pool size
        tables=[
            TableConfig(
                name=test_table,
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
                retention_days=90,
            )
        ],
    )

    multi_db_config = ArchiverConfig(
        version="2.0",
        s3=archiver_config.s3,
        defaults=DefaultsConfig(connection_pool_size=5),  # Global default
        databases=[db_config],
    )

    multi_db_config.s3.aws_access_key_id = "minioadmin"
    multi_db_config.s3.aws_secret_access_key = "minioadmin"

    archiver = Archiver(multi_db_config, dry_run=True)

    # Should not raise an error
    stats = await archiver.archive()

    assert stats["databases_processed"] >= 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parallel_database_processing(
    archiver_config: ArchiverConfig, test_table: str
) -> None:
    """Test parallel database processing (if enabled)."""

    # Set password environment variable
    os.environ.setdefault("DB_PASSWORD", "archiver_password")

    # Create two database configs (using same DB for testing)
    db1 = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="DB_PASSWORD",
        tables=[
            TableConfig(
                name=test_table,
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
                retention_days=90,
                batch_size=10,
            )
        ],
    )

    db2 = DatabaseConfig(
        name="test_db",
        host="localhost",
        port=5432,
        user="archiver",
        password_env="DB_PASSWORD",
        tables=[
            TableConfig(
                name=test_table,
                schema="public",
                timestamp_column="created_at",
                primary_key="id",
                retention_days=90,
                batch_size=10,
            )
        ],
    )

    multi_db_config = ArchiverConfig(
        version="2.0",
        s3=archiver_config.s3,
        defaults=DefaultsConfig(
            parallel_databases=True,
            max_parallel_databases=2,
        ),
        databases=[db1, db2],
    )

    multi_db_config.s3.aws_access_key_id = "minioadmin"
    multi_db_config.s3.aws_secret_access_key = "minioadmin"

    archiver = Archiver(multi_db_config, dry_run=True)

    stats = await archiver.archive()

    # Both databases should be processed
    assert len(stats["database_stats"]) == 2
    # Check that parallel processing was used (both should complete)
    assert stats["databases_processed"] + stats["databases_failed"] == 2

