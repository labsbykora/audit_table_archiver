"""Read replica connection pool and health management."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Optional

import asyncpg
from structlog import BoundLogger

from archiver.exceptions import DatabaseError
from utils.logging import get_logger


@dataclass
class ReplicaHealth:
    """Replica health status."""

    is_healthy: bool = False
    lag_seconds: float = 0.0
    avg_latency_ms: float = 0.0
    error_count: int = 0
    last_check: Optional[datetime] = None
    last_success: Optional[datetime] = None
    consecutive_failures: int = 0


@dataclass
class ReplicaConfig:
    """Read replica configuration."""

    host: str
    port: int = 5432
    weight: float = 1.0
    name: Optional[str] = None  # For identification in logs


class ReplicaPool:
    """Manages connection pool for a single read replica."""

    def __init__(
        self,
        config: ReplicaConfig,
        database_name: str,
        user: str,
        password: str,
        pool_size: int = 5,
        logger: Optional[BoundLogger] = None,
    ) -> None:
        """Initialize replica pool.

        Args:
            config: Replica configuration
            database_name: Database name
            user: Database user
            password: Database password
            pool_size: Connection pool size
            logger: Optional logger instance
        """
        self.config = config
        self.database_name = database_name
        self.user = user
        self.password = password
        self.pool_size = pool_size
        self.logger = logger or get_logger("replica_pool")
        self.pool: Optional[asyncpg.Pool] = None
        self.health = ReplicaHealth()
        self._dsn: Optional[str] = None
        self._active_connections: int = 0

    @property
    def dsn(self) -> str:
        """Get database connection DSN."""
        if self._dsn is None:
            self._dsn = (
                f"postgresql://{self.user}:{self.password}@"
                f"{self.config.host}:{self.config.port}/{self.database_name}"
            )
        return self._dsn

    @property
    def identifier(self) -> str:
        """Get replica identifier for logging."""
        return self.config.name or f"{self.config.host}:{self.config.port}"

    async def connect(self) -> None:
        """Create connection pool."""
        try:
            self.logger.debug(
                "Creating replica connection pool",
                replica=self.identifier,
                host=self.config.host,
                pool_size=self.pool_size,
            )

            if self.pool:
                try:
                    await self.pool.close()
                except Exception:
                    pass
                self.pool = None

            self.pool = await asyncpg.create_pool(
                self.dsn,
                min_size=1,
                max_size=self.pool_size,
                command_timeout=60,
                server_settings={
                    "application_name": "audit_archiver_replica",
                },
            )

            # Test connection
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                self.health.is_healthy = True
                self.health.last_success = datetime.now()
                self.health.consecutive_failures = 0

            self.logger.debug(
                "Replica connection pool established",
                replica=self.identifier,
            )
        except Exception as e:
            self.health.is_healthy = False
            self.health.consecutive_failures += 1
            self.logger.warning(
                "Failed to create replica connection pool",
                replica=self.identifier,
                error=str(e),
            )
            raise DatabaseError(
                f"Failed to create replica connection pool: {e}",
                context={
                    "replica": self.identifier,
                    "host": self.config.host,
                },
            ) from e

    async def disconnect(self) -> None:
        """Close connection pool."""
        if self.pool:
            self.logger.debug("Closing replica connection pool", replica=self.identifier)
            await self.pool.close()
            self.pool = None
            self.health.is_healthy = False

    async def health_check(self) -> bool:
        """Check replica health.

        Returns:
            True if healthy, False otherwise
        """
        if not self.pool:
            self.health.is_healthy = False
            return False

        start_time = datetime.now()
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            latency_ms = (datetime.now() - start_time).total_seconds() * 1000

            self.health.is_healthy = True
            self.health.last_success = datetime.now()
            self.health.consecutive_failures = 0
            self.health.avg_latency_ms = (
                self.health.avg_latency_ms * 0.7 + latency_ms * 0.3
            )  # Exponential moving average
            self.health.last_check = datetime.now()

            return True
        except Exception as e:
            self.health.is_healthy = False
            self.health.consecutive_failures += 1
            self.health.error_count += 1
            self.health.last_check = datetime.now()
            self.logger.debug(
                "Replica health check failed",
                replica=self.identifier,
                error=str(e),
            )
            return False

    async def check_replication_lag(self) -> float:
        """Check replication lag in seconds.

        Returns:
            Replication lag in seconds, or 0.0 if unable to determine
        """
        if not self.pool:
            return 999999.0  # Very high lag if not connected

        try:
            async with self.pool.acquire() as conn:
                # Try to get lag from pg_stat_replication (primary side)
                # For replica side, use pg_last_xact_replay_timestamp()
                lag_query = """
                    SELECT EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())) AS lag_seconds
                """
                lag = await conn.fetchval(lag_query)
                if lag is not None:
                    self.health.lag_seconds = float(lag)
                    return float(lag)
        except Exception as e:
            self.logger.debug(
                "Failed to check replication lag",
                replica=self.identifier,
                error=str(e),
            )

        return self.health.lag_seconds

    @property
    def active_connections(self) -> int:
        """Get number of active connections."""
        if self.pool:
            return self.pool.get_size() - self.pool.get_idle_size()
        return 0

    @property
    def is_available(self) -> bool:
        """Check if replica is available for queries."""
        return self.health.is_healthy and self.pool is not None

    async def acquire_connection(self) -> asyncpg.Connection:
        """Acquire a connection from the pool.

        Returns:
            Database connection

        Raises:
            DatabaseError: If pool is not available
        """
        if not self.pool:
            raise DatabaseError(
                "Replica pool not initialized. Call connect() first.",
                context={"replica": self.identifier},
            )

        try:
            conn = await self.pool.acquire()
            self._active_connections += 1
            return conn
        except Exception as e:
            self.health.error_count += 1
            self.health.consecutive_failures += 1
            raise DatabaseError(
                f"Failed to acquire replica connection: {e}",
                context={"replica": self.identifier},
            ) from e

    def release_connection(self, conn: asyncpg.Connection) -> None:
        """Release a connection back to the pool."""
        if self.pool:
            try:
                self.pool.release(conn)
            except Exception:
                pass  # Ignore errors when releasing
            self._active_connections = max(0, self._active_connections - 1)

