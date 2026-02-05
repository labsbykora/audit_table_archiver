"""Database connection and query management using asyncpg."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Optional

import asyncpg
from structlog import BoundLogger

from archiver.config import DatabaseConfig
from archiver.exceptions import DatabaseError
from utils.logging import get_logger


class DatabaseManager:
    """Manages PostgreSQL database connections and operations."""

    def __init__(
        self,
        config: DatabaseConfig,
        pool_size: int = 5,
        logger: Optional[BoundLogger] = None,
    ) -> None:
        """Initialize database manager.

        Args:
            config: Database configuration
            pool_size: Connection pool size
            logger: Optional logger instance
        """
        self.config = config
        self.pool_size = pool_size
        self.logger = logger or get_logger("database")
        self.pool: Optional[asyncpg.Pool] = None
        self._dsn: Optional[str] = None

    @property
    def dsn(self) -> str:
        """Get database connection DSN."""
        if self._dsn is None:
            try:
                password = self.config.get_password()
            except ValueError as e:
                raise DatabaseError(
                    str(e),
                    context={"database": self.config.name},
                ) from e

            self._dsn = (
                f"postgresql://{self.config.user}:{password}@"
                f"{self.config.host}:{self.config.port}/{self.config.name}"
            )
        return self._dsn

    async def connect(self) -> None:
        """Create connection pool."""
        try:
            self.logger.debug(
                "Creating connection pool",
                database=self.config.name,
                host=self.config.host,
                pool_size=self.pool_size,
            )

            self.pool = await asyncpg.create_pool(
                self.dsn,
                min_size=1,
                max_size=self.pool_size,
                command_timeout=60,
                server_settings={
                    "application_name": "audit_archiver",
                },
            )

            # Test connection
            async with self.pool.acquire() as conn:
                version = await conn.fetchval("SELECT version()")
                self.logger.debug(
                    "Database connection established",
                    database=self.config.name,
                    version=version.split(",")[0] if version else "unknown",
                )

        except Exception as e:
            raise DatabaseError(
                f"Failed to create connection pool: {e}",
                context={"database": self.config.name, "host": self.config.host},
            ) from e

    async def disconnect(self) -> None:
        """Close connection pool."""
        if self.pool:
            self.logger.debug("Closing connection pool", database=self.config.name)
            await self.pool.close()
            self.pool = None

    async def health_check(self) -> bool:
        """Check database connection health.

        Returns:
            True if healthy, False otherwise
        """
        if not self.pool:
            return False

        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            self.logger.warning("Health check failed", error=str(e))
            return False

    @asynccontextmanager
    async def acquire_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Acquire a connection from the pool.

        Yields:
            Database connection

        Raises:
            DatabaseError: If pool is not initialized or connection fails
        """
        if not self.pool:
            raise DatabaseError(
                "Connection pool not initialized. Call connect() first.",
                context={"database": self.config.name},
            )

        conn = await self.pool.acquire()
        try:
            yield conn
        finally:
            await self.pool.release(conn)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Start a database transaction.

        Yields:
            Database connection in transaction

        Raises:
            DatabaseError: If transaction fails
        """
        if not self.pool:
            raise DatabaseError(
                "Connection pool not initialized. Call connect() first.",
                context={"database": self.config.name},
            )

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def execute(self, query: str, *args: Any) -> str:
        """Execute a query that doesn't return rows.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            Command status string

        Raises:
            DatabaseError: If execution fails
        """
        try:
            async with self.acquire_connection() as conn:
                return await conn.execute(query, *args)
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={"database": self.config.name, "query": query[:100]},
            ) from e

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        """Execute a query and return all rows.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            List of records

        Raises:
            DatabaseError: If execution fails
        """
        try:
            async with self.acquire_connection() as conn:
                return await conn.fetch(query, *args)
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={"database": self.config.name, "query": query[:100]},
            ) from e

    async def fetchrow(self, query: str, *args: Any) -> Optional[asyncpg.Record]:
        """Execute a query and return one row.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            Single record or None

        Raises:
            DatabaseError: If execution fails
        """
        try:
            async with self.acquire_connection() as conn:
                return await conn.fetchrow(query, *args)
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={"database": self.config.name, "query": query[:100]},
            ) from e

    async def fetchone(self, query: str, *args: Any) -> Optional[dict[str, Any]]:
        """Execute a query and return one row as dictionary.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            Single record as dictionary or None

        Raises:
            DatabaseError: If execution fails
        """
        try:
            async with self.acquire_connection() as conn:
                row = await conn.fetchrow(query, *args)
                return dict(row) if row else None
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={"database": self.config.name, "query": query[:100]},
            ) from e

    async def fetchval(self, query: str, *args: Any) -> Any:
        """Execute a query and return a single value.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            Single value or None

        Raises:
            DatabaseError: If execution fails
        """
        try:
            async with self.acquire_connection() as conn:
                return await conn.fetchval(query, *args)
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={"database": self.config.name, "query": query[:100]},
            ) from e

    async def get_postgres_version(self) -> str:
        """Get PostgreSQL version.

        Returns:
            PostgreSQL version string
        """
        version = await self.fetchval("SELECT version()")
        if version:
            # Extract version number (e.g., "PostgreSQL 14.5" -> "14.5")
            import re

            match = re.search(r"PostgreSQL (\d+\.\d+)", version)
            if match:
                return match.group(1)
        return "unknown"
