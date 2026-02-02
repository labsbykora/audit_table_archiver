"""Pytest fixtures for performance tests."""

import asyncio
import os
import subprocess
import time
from pathlib import Path
from typing import AsyncGenerator, Generator

import asyncpg
import pytest
from botocore.client import BaseClient

from archiver.config import ArchiverConfig, DatabaseConfig, S3Config, TableConfig
from archiver.database import DatabaseManager
from archiver.s3_client import S3Client


@pytest.fixture(scope="session")
def docker_compose_file() -> Path:
    """Return path to docker-compose file."""
    return Path(__file__).parent.parent.parent / "docker" / "docker-compose.yml"


@pytest.fixture(scope="session")
def postgres_ready(docker_compose_file: Path) -> Generator[None, None, None]:
    """Wait for PostgreSQL to be ready."""
    # Check if services are running
    try:
        result = subprocess.run(
            ["docker-compose", "-f", str(docker_compose_file), "ps", "-q", "postgres"],
            capture_output=True,
            text=True,
            check=False,
        )
        if not result.stdout.strip():
            pytest.skip("PostgreSQL container not running. Start with: docker-compose up -d")

        # Wait for PostgreSQL to be ready
        max_attempts = 30
        for attempt in range(max_attempts):
            try:
                async def check_connection():
                    conn = await asyncpg.connect(
                        host="localhost",
                        port=5432,
                        user="archiver",
                        password="archiver_password",
                        database="test_db",
                    )
                    await conn.close()
                
                asyncio.run(check_connection())
                break
            except Exception:
                if attempt < max_attempts - 1:
                    time.sleep(1)
                else:
                    pytest.skip("PostgreSQL not ready after 30 seconds")

        yield
    except FileNotFoundError:
        pytest.skip("docker-compose not found. Install Docker Compose to run integration tests.")


@pytest.fixture
async def db_connection(postgres_ready: None) -> AsyncGenerator[asyncpg.Connection, None]:
    """Create database connection for testing."""
    conn = await asyncpg.connect(
        host="localhost",
        port=5432,
        user="archiver",
        password="archiver_password",
        database="test_db",
    )

    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
async def test_table(db_connection: asyncpg.Connection) -> AsyncGenerator[str, None]:
    """Create test table and return table name."""
    import uuid
    table_name = f"test_audit_logs_{uuid.uuid4().hex[:8]}"

    # Create table
    await db_connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGSERIAL PRIMARY KEY,
            user_id INTEGER,
            action TEXT NOT NULL,
            metadata JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    # Create index on timestamp
    await db_connection.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name}(created_at)"
    )

    try:
        yield table_name
    finally:
        # Cleanup
        await db_connection.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")


@pytest.fixture
def s3_config() -> S3Config:
    """Create S3 configuration for MinIO."""
    return S3Config(
        endpoint="http://localhost:9000",
        bucket="test-archives",
        prefix="test/",
        region="us-east-1",
        storage_class="STANDARD",
    )


@pytest.fixture
def archiver_config(s3_config: S3Config) -> ArchiverConfig:
    """Create archiver configuration for testing."""
    os.environ["TEST_DB_PASSWORD"] = "archiver_password"

    return ArchiverConfig(
        version="2.0",
        s3=s3_config,
        defaults={
            "retention_days": 90,
            "batch_size": 10,
            "sleep_between_batches": 0,
            "vacuum_after": False,
            "vacuum_strategy": "none",
        },
        databases=[
            DatabaseConfig(
                name="test_db",
                host="localhost",
                port=5432,
                user="archiver",
                password_env="TEST_DB_PASSWORD",
                tables=[
                    TableConfig(
                        name="test_audit_logs",
                        schema="public",
                        timestamp_column="created_at",
                        primary_key="id",
                        retention_days=90,
                        batch_size=10,
                    )
                ],
            )
        ],
    )

