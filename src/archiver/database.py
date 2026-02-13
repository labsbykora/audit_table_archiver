"""Database connection and query management using asyncpg."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Optional

import asyncpg
from structlog import BoundLogger

from archiver.config import DatabaseConfig
from archiver.exceptions import DatabaseError
from utils.logging import get_logger
from utils.retry import RetryConfig, retry_async


class DatabaseManager:
    """Manages PostgreSQL database connections and operations."""

    def __init__(
        self,
        config: DatabaseConfig,
        pool_size: int = 5,
        logger: Optional[BoundLogger] = None,
        max_reconnect_attempts: int = 3,
    ) -> None:
        """Initialize database manager.

        Args:
            config: Database configuration
            pool_size: Connection pool size
            logger: Optional logger instance
            max_reconnect_attempts: Maximum reconnection attempts (default: 3)
        """
        self.config = config
        self.pool_size = pool_size
        self.logger = logger or get_logger("database")
        self.pool: Optional[asyncpg.Pool] = None
        self._dsn: Optional[str] = None
        self.max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_config = RetryConfig(
            max_attempts=max_reconnect_attempts,
            initial_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            retryable_exceptions=(
                asyncpg.PostgresConnectionError,
                asyncpg.ConnectionDoesNotExistError,
                OSError,
                ConnectionError,
            ),
        )

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
        """Create connection pool with automatic retry on failure."""
        async def _connect_internal() -> None:
            """Internal connection function for retry logic."""
            self.logger.debug(
                "Creating connection pool",
                database=self.config.name,
                host=self.config.host,
                pool_size=self.pool_size,
            )

            # Close existing pool if reconnecting
            if self.pool:
                try:
                    await self.pool.close()
                except Exception:
                    pass  # Ignore errors when closing failed pool
                self.pool = None

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

        try:
            await retry_async(
                _connect_internal,
                config=self._reconnect_config,
                logger=self.logger,
            )
        except Exception as e:
            raise DatabaseError(
                f"Failed to create connection pool after {self.max_reconnect_attempts} attempts: {e}",
                context={
                    "database": self.config.name,
                    "host": self.config.host,
                    "attempts": self.max_reconnect_attempts,
                },
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

    async def _ensure_pool_healthy(self) -> None:
        """Ensure connection pool is healthy, reconnect if needed."""
        if not self.pool:
            self.logger.warning(
                "Connection pool not initialized, attempting to reconnect",
                database=self.config.name,
            )
            await self.connect()
            return

        # Check if pool is still valid
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
        except (
            asyncpg.PostgresConnectionError,
            asyncpg.ConnectionDoesNotExistError,
            OSError,
            ConnectionError,
        ) as e:
            self.logger.warning(
                "Connection pool unhealthy, attempting to reconnect",
                database=self.config.name,
                error=str(e),
            )
            await self.connect()

    @asynccontextmanager
    async def acquire_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Acquire a connection from the pool with automatic reconnection.

        Yields:
            Database connection

        Raises:
            DatabaseError: If pool cannot be initialized or connection fails
        """
        await self._ensure_pool_healthy()

        if not self.pool:
            raise DatabaseError(
                "Connection pool not initialized. Call connect() first.",
                context={"database": self.config.name},
            )

        # pool.acquire() is an async context manager, use it properly
        async with self.pool.acquire() as conn:
            yield conn

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
        """Execute a query that doesn't return rows with automatic retry.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            Command status string

        Raises:
            DatabaseError: If execution fails after retries
        """
        async def _execute_internal() -> str:
            async with self.acquire_connection() as conn:
                return await conn.execute(query, *args)

        try:
            return await retry_async(
                _execute_internal,
                config=self._reconnect_config,
                logger=self.logger,
            )
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={
                    "database": self.config.name,
                    "query": query[:100],
                    "attempts": self.max_reconnect_attempts,
                },
            ) from e

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        """Execute a query and return all rows with automatic retry.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            List of records

        Raises:
            DatabaseError: If execution fails after retries
        """
        async def _fetch_internal() -> list[asyncpg.Record]:
            async with self.acquire_connection() as conn:
                return await conn.fetch(query, *args)

        try:
            return await retry_async(
                _fetch_internal,
                config=self._reconnect_config,
                logger=self.logger,
            )
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={
                    "database": self.config.name,
                    "query": query[:100],
                    "attempts": self.max_reconnect_attempts,
                },
            ) from e

    async def fetchrow(self, query: str, *args: Any) -> Optional[asyncpg.Record]:
        """Execute a query and return one row with automatic retry.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            Single record or None

        Raises:
            DatabaseError: If execution fails after retries
        """
        async def _fetchrow_internal() -> Optional[asyncpg.Record]:
            async with self.acquire_connection() as conn:
                return await conn.fetchrow(query, *args)

        try:
            return await retry_async(
                _fetchrow_internal,
                config=self._reconnect_config,
                logger=self.logger,
            )
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={
                    "database": self.config.name,
                    "query": query[:100],
                    "attempts": self.max_reconnect_attempts,
                },
            ) from e

    async def fetchone(self, query: str, *args: Any) -> Optional[dict[str, Any]]:
        """Execute a query and return one row as dictionary with automatic retry.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            Single record as dictionary or None

        Raises:
            DatabaseError: If execution fails after retries
        """
        async def _fetchone_internal() -> Optional[dict[str, Any]]:
            async with self.acquire_connection() as conn:
                row = await conn.fetchrow(query, *args)
                return dict(row) if row else None

        try:
            return await retry_async(
                _fetchone_internal,
                config=self._reconnect_config,
                logger=self.logger,
            )
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={
                    "database": self.config.name,
                    "query": query[:100],
                    "attempts": self.max_reconnect_attempts,
                },
            ) from e

    async def fetchval(self, query: str, *args: Any) -> Any:
        """Execute a query and return a single value with automatic retry.

        Args:
            query: SQL query
            *args: Query parameters

        Returns:
            Single value or None

        Raises:
            DatabaseError: If execution fails after retries
        """
        async def _fetchval_internal() -> Any:
            async with self.acquire_connection() as conn:
                return await conn.fetchval(query, *args)

        try:
            return await retry_async(
                _fetchval_internal,
                config=self._reconnect_config,
                logger=self.logger,
            )
        except Exception as e:
            raise DatabaseError(
                f"Query execution failed: {e}",
                context={
                    "database": self.config.name,
                    "query": query[:100],
                    "attempts": self.max_reconnect_attempts,
                },
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
