"""Database connection and query management using asyncpg."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Optional

import asyncpg
from structlog import BoundLogger

from archiver.config import DatabaseConfig
from archiver.exceptions import DatabaseError
from archiver.replica_pool import ReplicaConfig, ReplicaPool
from archiver.replica_selector import ReplicaSelector, create_selector
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
        read_replica_enabled: bool = True,
        read_replica_selection_strategy: str = "round_robin",
        read_replica_max_lag_seconds: float = 10.0,
        read_replica_health_check_interval: int = 30,
        read_replica_circuit_breaker_failure_threshold: int = 5,
        read_replica_circuit_breaker_recovery_timeout: int = 60,
        read_replica_fallback_to_primary: bool = True,
    ) -> None:
        """Initialize database manager.

        Args:
            config: Database configuration
            pool_size: Connection pool size
            logger: Optional logger instance
            max_reconnect_attempts: Maximum reconnection attempts (default: 3)
            read_replica_enabled: Enable read replica load balancing
            read_replica_selection_strategy: Replica selection strategy
            read_replica_max_lag_seconds: Maximum acceptable replication lag
            read_replica_health_check_interval: Health check interval in seconds
            read_replica_circuit_breaker_failure_threshold: Failures before excluding replica
            read_replica_circuit_breaker_recovery_timeout: Seconds before retrying excluded replica
            read_replica_fallback_to_primary: Fall back to primary if replicas unavailable
        """
        self.config = config
        self.pool_size = pool_size
        self.logger = logger or get_logger("database")
        self.pool: Optional[asyncpg.Pool] = None  # Primary pool
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

        # Read replica configuration
        self.read_replica_enabled = read_replica_enabled
        self.read_replica_selection_strategy = read_replica_selection_strategy
        self.read_replica_max_lag_seconds = read_replica_max_lag_seconds
        self.read_replica_health_check_interval = read_replica_health_check_interval
        self.read_replica_circuit_breaker_failure_threshold = (
            read_replica_circuit_breaker_failure_threshold
        )
        self.read_replica_circuit_breaker_recovery_timeout = (
            read_replica_circuit_breaker_recovery_timeout
        )
        self.read_replica_fallback_to_primary = read_replica_fallback_to_primary

        # Replica pools and selector
        self.replica_pools: list[ReplicaPool] = []
        self.replica_selector: Optional[ReplicaSelector] = None
        self._health_check_task: Optional[asyncio.Task[None]] = None

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

        # Initialize read replicas if enabled
        if self.read_replica_enabled:
            await self._initialize_replicas()
            if self.replica_pools:
                self._start_health_check_task()

    async def disconnect(self) -> None:
        """Close connection pool."""
        # Stop health check task
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None

        # Close replica pools
        for replica_pool in self.replica_pools:
            await replica_pool.disconnect()
        self.replica_pools.clear()

        # Close primary pool
        if self.pool:
            self.logger.debug("Closing connection pool", database=self.config.name)
            await self.pool.close()
            self.pool = None

    async def _initialize_replicas(self) -> None:
        """Initialize read replica pools."""
        if not self.config.read_replicas:
            return

        try:
            password = self.config.get_password()
        except ValueError as e:
            self.logger.warning(
                "Cannot initialize replicas: password not available",
                error=str(e),
            )
            return

        self.logger.info(
            "Initializing read replicas",
            database=self.config.name,
            replica_count=len(self.config.read_replicas),
        )

        for replica_config_dict in self.config.read_replicas:
            try:
                replica_config = ReplicaConfig(
                    host=replica_config_dict["host"],
                    port=replica_config_dict.get("port", self.config.port),
                    weight=replica_config_dict.get("weight", 1.0),
                    name=replica_config_dict.get("name"),
                )

                replica_pool = ReplicaPool(
                    config=replica_config,
                    database_name=self.config.name,
                    user=self.config.user,
                    password=password,
                    pool_size=self.pool_size,
                    logger=self.logger,
                )

                try:
                    await replica_pool.connect()
                    self.replica_pools.append(replica_pool)
                    self.logger.info(
                        "Replica pool initialized",
                        replica=replica_pool.identifier,
                    )
                except Exception as e:
                    self.logger.warning(
                        "Failed to initialize replica pool",
                        replica=replica_pool.identifier,
                        error=str(e),
                    )
            except Exception as e:
                self.logger.warning(
                    "Failed to create replica config",
                    error=str(e),
                )

        if self.replica_pools:
            self.replica_selector = create_selector(self.read_replica_selection_strategy)
            self.logger.info(
                "Read replica load balancing enabled",
                database=self.config.name,
                replica_count=len(self.replica_pools),
                strategy=self.read_replica_selection_strategy,
            )
        else:
            self.logger.warning(
                "No healthy replicas available, using primary only",
                database=self.config.name,
            )

    def _start_health_check_task(self) -> None:
        """Start background health check task for replicas."""
        if self._health_check_task:
            return

        async def _health_check_loop() -> None:
            """Background health check loop."""
            while True:
                try:
                    await asyncio.sleep(self.read_replica_health_check_interval)
                    for replica_pool in self.replica_pools:
                        await replica_pool.health_check()
                        if self.read_replica_selection_strategy == "lag_aware":
                            await replica_pool.check_replication_lag()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.warning(
                        "Health check loop error",
                        error=str(e),
                    )

        self._health_check_task = asyncio.create_task(_health_check_loop())

    def _is_read_query(self, query: str) -> bool:
        """Check if query is read-only (SELECT).

        Args:
            query: SQL query string

        Returns:
            True if query is read-only, False otherwise
        """
        query_upper = query.strip().upper()
        # Check for SELECT, EXPLAIN, or SHOW statements
        return (
            query_upper.startswith("SELECT")
            or query_upper.startswith("EXPLAIN")
            or query_upper.startswith("SHOW")
            or query_upper.startswith("WITH")  # CTEs are typically read-only
        )

    @asynccontextmanager
    async def _get_connection_for_query(
        self, query: str, use_replica: bool = True
    ) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get appropriate connection (replica or primary) for query.

        Args:
            query: SQL query string
            use_replica: Whether to prefer replica for read queries

        Yields:
            Database connection
        """
        # Always use primary for write operations
        if not self._is_read_query(query):
            async with self.acquire_connection() as conn:
                yield conn
            return

        # Use replica for read operations if available
        if (
            use_replica
            and self.read_replica_enabled
            and self.replica_selector
            and self.replica_pools
        ):
            replica = self.replica_selector.select_replica(
                self.replica_pools, self.read_replica_fallback_to_primary
            )

            if replica:
                # Check lag threshold
                if replica.health.lag_seconds > self.read_replica_max_lag_seconds:
                    self.logger.debug(
                        "Replica lag exceeds threshold, using primary",
                        replica=replica.identifier,
                        lag_seconds=replica.health.lag_seconds,
                        threshold=self.read_replica_max_lag_seconds,
                    )
                else:
                    try:
                        conn = await replica.acquire_connection()
                        try:
                            yield conn
                        finally:
                            replica.release_connection(conn)
                        return
                    except Exception as e:
                        self.logger.debug(
                            "Failed to acquire replica connection, falling back to primary",
                            replica=replica.identifier,
                            error=str(e),
                        )

        # Fall back to primary
        async with self.acquire_connection() as conn:
            yield conn

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
            async with self._get_connection_for_query(query) as conn:
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
            async with self._get_connection_for_query(query) as conn:
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
            async with self._get_connection_for_query(query) as conn:
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
            async with self._get_connection_for_query(query) as conn:
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

    async def fetchval_with_timeout(
        self, query: str, timeout_seconds: int, *args: Any
    ) -> Any:
        """Execute a query and return a single value with a dedicated connection and timeout.

        Use for long-running queries (e.g. verify COUNT on large tables) that would
        exceed the pool's command_timeout. Uses primary DSN only (no replica).

        Args:
            query: SQL query
            timeout_seconds: Command timeout in seconds for this query only
            *args: Query parameters

        Returns:
            Single value or None

        Raises:
            DatabaseError: If execution fails
        """
        conn = None
        try:
            conn = await asyncpg.connect(
                self.dsn,
                command_timeout=timeout_seconds,
            )
            return await conn.fetchval(query, *args)
        finally:
            if conn:
                await conn.close()

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
