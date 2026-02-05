"""Transaction management for safe batch processing."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import asyncpg
import structlog

from archiver.exceptions import TransactionError
from utils.logging import get_logger


class TransactionManager:
    """Manages database transactions with timeout and savepoint support."""

    def __init__(
        self,
        connection: asyncpg.Connection,
        timeout_seconds: int = 1800,  # 30 minutes default
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize transaction manager.

        Args:
            connection: Database connection
            timeout_seconds: Transaction timeout in seconds
            logger: Optional logger instance
        """
        self.connection = connection
        self.timeout_seconds = timeout_seconds
        self.logger = logger or get_logger("transaction_manager")
        self._transaction_start: Optional[datetime] = None
        self._savepoint_count = 0

    @asynccontextmanager
    async def transaction(
        self,
    ) -> AsyncGenerator[asyncpg.Connection, None]:
        """Start a database transaction with timeout monitoring.

        Yields:
            Database connection in transaction

        Raises:
            TransactionError: If transaction fails or times out
        """
        self._transaction_start = datetime.now()
        self._savepoint_count = 0

        try:
            async with self.connection.transaction():
                # Set transaction timeout
                await self.connection.execute(
                    f"SET LOCAL statement_timeout = {self.timeout_seconds * 1000}"  # milliseconds
                )

                # Monitor transaction age
                asyncio.create_task(self._monitor_transaction())

                self.logger.debug(
                    "Transaction started",
                    timeout_seconds=self.timeout_seconds,
                )

                yield self.connection

                self.logger.debug("Transaction committed successfully")

        except asyncpg.PostgresError as e:
            self.logger.error(
                "Transaction failed",
                error=str(e),
                error_code=e.sqlstate if hasattr(e, "sqlstate") else None,
            )
            raise TransactionError(
                f"Transaction failed: {e}",
                context={"error_code": e.sqlstate if hasattr(e, "sqlstate") else None},
            ) from e
        except asyncio.TimeoutError:
            raise TransactionError(
                f"Transaction timeout after {self.timeout_seconds} seconds",
                context={"timeout_seconds": self.timeout_seconds},
            ) from None
        finally:
            self._transaction_start = None
            self._savepoint_count = 0

    @asynccontextmanager
    async def savepoint(self, name: Optional[str] = None) -> AsyncGenerator[None, None]:
        """Create a savepoint for partial rollback.

        Args:
            name: Optional savepoint name (auto-generated if not provided)

        Yields:
            None (use for context management)

        Raises:
            TransactionError: If savepoint operations fail
        """
        if name is None:
            self._savepoint_count += 1
            name = f"sp_{self._savepoint_count}"

        try:
            await self.connection.execute(f"SAVEPOINT {name}")
            self.logger.debug("Savepoint created", savepoint=name)

            yield

            # Release savepoint on success
            await self.connection.execute(f"RELEASE SAVEPOINT {name}")
            self.logger.debug("Savepoint released", savepoint=name)

        except asyncpg.PostgresError as e:
            # Rollback to savepoint on error
            try:
                await self.connection.execute(f"ROLLBACK TO SAVEPOINT {name}")
                self.logger.debug("Rolled back to savepoint", savepoint=name)
            except Exception as rollback_error:
                self.logger.error(
                    "Failed to rollback to savepoint",
                    savepoint=name,
                    error=str(rollback_error),
                )

            raise TransactionError(
                f"Savepoint operation failed: {e}",
                context={"savepoint": name},
            ) from e

    async def _monitor_transaction(self) -> None:
        """Monitor transaction age and warn if approaching timeout."""
        while self._transaction_start is not None:
            await asyncio.sleep(30)  # Check every 30 seconds

            if self._transaction_start is None:
                break

            age = (datetime.now() - self._transaction_start).total_seconds()
            threshold = self.timeout_seconds * 0.5  # Warn at 50% of timeout

            if age > threshold:
                self.logger.warning(
                    "Transaction age approaching timeout",
                    age_seconds=age,
                    timeout_seconds=self.timeout_seconds,
                    percentage=(age / self.timeout_seconds) * 100,
                )

            if age > self.timeout_seconds:
                self.logger.error(
                    "Transaction exceeded timeout",
                    age_seconds=age,
                    timeout_seconds=self.timeout_seconds,
                )
                break

    def get_transaction_age(self) -> Optional[float]:
        """Get current transaction age in seconds.

        Returns:
            Transaction age in seconds, or None if no active transaction
        """
        if self._transaction_start is None:
            return None

        return (datetime.now() - self._transaction_start).total_seconds()

