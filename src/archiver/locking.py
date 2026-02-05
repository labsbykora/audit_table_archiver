"""Distributed locking for concurrent run prevention."""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import structlog

from archiver.database import DatabaseManager
from archiver.exceptions import LockError
from utils.logging import get_logger


class Lock:
    """Represents a distributed lock."""

    def __init__(
        self,
        lock_id: str,
        acquired_at: datetime,
        expires_at: datetime,
        owner: str,
    ) -> None:
        """Initialize lock.

        Args:
            lock_id: Unique lock identifier
            acquired_at: When lock was acquired
            expires_at: When lock expires
            owner: Lock owner identifier
        """
        self.lock_id = lock_id
        self.acquired_at = acquired_at
        self.expires_at = expires_at
        self.owner = owner

    def is_expired(self) -> bool:
        """Check if lock is expired.

        Returns:
            True if lock is expired, False otherwise
        """
        return datetime.now(timezone.utc) >= self.expires_at

    def time_until_expiry(self) -> float:
        """Get seconds until lock expires.

        Returns:
            Seconds until expiry (negative if expired)
        """
        delta = self.expires_at - datetime.now(timezone.utc)
        return delta.total_seconds()


class LockManager:
    """Manages distributed locks for preventing concurrent runs."""

    def __init__(
        self,
        lock_type: str = "postgresql",  # "postgresql", "redis", or "file"
        lock_ttl_seconds: int = 3600,  # 1 hour default
        heartbeat_interval_seconds: int = 30,  # 30 seconds default
        logger: Optional[structlog.BoundLogger] = None,
    ) -> None:
        """Initialize lock manager.

        Args:
            lock_type: Lock type ("postgresql", "redis", or "file")
            lock_ttl_seconds: Lock time-to-live in seconds
            heartbeat_interval_seconds: Heartbeat interval in seconds
            logger: Optional logger instance
        """
        if lock_type not in ("postgresql", "redis", "file"):
            raise ValueError(
                f"Invalid lock_type: {lock_type}. Must be 'postgresql', 'redis', or 'file'"
            )

        self.lock_type = lock_type
        self.lock_ttl_seconds = lock_ttl_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.logger = logger or get_logger("lock_manager")
        self.current_lock: Optional[Lock] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._lock_owner: str = f"archiver_{int(time.time())}"
        self._acquired_locks: dict[str, Lock] = {}  # Track locks held by this instance

    async def acquire_lock(
        self,
        lock_key: str,
        db_manager: Optional[DatabaseManager] = None,
        redis_client: Optional[Any] = None,  # Redis client (if using Redis)
        lock_file_path: Optional[Path] = None,  # Lock file path (if using file-based)
    ) -> Lock:
        """Acquire a distributed lock.

        Args:
            lock_key: Unique lock key (e.g., "database:table" or "database")
            db_manager: Database manager (required for PostgreSQL locks)
            redis_client: Redis client (required for Redis locks)
            lock_file_path: Lock file path (required for file-based locks)

        Returns:
            Acquired Lock object

        Raises:
            LockError: If lock cannot be acquired
        """
        if self.lock_type == "postgresql":
            if db_manager is None:
                raise ValueError("db_manager is required for PostgreSQL locks")
            return await self._acquire_postgresql_lock(lock_key, db_manager)
        elif self.lock_type == "redis":
            if redis_client is None:
                raise ValueError("redis_client is required for Redis locks")
            return await self._acquire_redis_lock(lock_key, redis_client)
        else:  # file
            if lock_file_path is None:
                raise ValueError("lock_file_path is required for file-based locks")
            return await self._acquire_file_lock(lock_key, lock_file_path)

    async def release_lock(
        self,
        lock: Lock,
        db_manager: Optional[DatabaseManager] = None,
        redis_client: Optional[Any] = None,
        lock_file_path: Optional[Path] = None,
    ) -> None:
        """Release a distributed lock.

        Args:
            lock: Lock object to release
            db_manager: Database manager (required for PostgreSQL locks)
            redis_client: Redis client (required for Redis locks)
            lock_file_path: Lock file path (required for file-based locks)

        Raises:
            LockError: If lock release fails
        """
        if self.lock_type == "postgresql":
            if db_manager is None:
                raise ValueError("db_manager is required for PostgreSQL locks")
            await self._release_postgresql_lock(lock, db_manager)
        elif self.lock_type == "redis":
            if redis_client is None:
                raise ValueError("redis_client is required for Redis locks")
            await self._release_redis_lock(lock, redis_client)
        else:  # file
            if lock_file_path is None:
                raise ValueError("lock_file_path is required for file-based locks")
            await self._release_file_lock(lock, lock_file_path)

        # Stop heartbeat task
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        self.current_lock = None

    async def start_heartbeat(
        self,
        lock: Lock,
        db_manager: Optional[DatabaseManager] = None,
        redis_client: Optional[Any] = None,
        lock_file_path: Optional[Path] = None,
    ) -> None:
        """Start heartbeat task to keep lock alive.

        Args:
            lock: Lock object to keep alive
            db_manager: Database manager (required for PostgreSQL locks)
            redis_client: Redis client (required for Redis locks)
            lock_file_path: Lock file path (required for file-based locks)
        """
        self.current_lock = lock

        async def heartbeat_loop() -> None:
            while True:
                try:
                    await asyncio.sleep(self.heartbeat_interval_seconds)

                    if lock.is_expired():
                        self.logger.warning(
                            "Lock expired during heartbeat",
                            lock_id=lock.lock_id,
                            lock_key=lock.lock_id,
                        )
                        break

                    # Extend lock TTL
                    new_expires_at = datetime.now(timezone.utc) + timedelta(
                        seconds=self.lock_ttl_seconds
                    )
                    lock.expires_at = new_expires_at

                    if self.lock_type == "postgresql" and db_manager:
                        await self._extend_postgresql_lock(lock, db_manager)
                    elif self.lock_type == "redis" and redis_client:
                        await self._extend_redis_lock(lock, redis_client)
                    elif self.lock_type == "file" and lock_file_path:
                        await self._extend_file_lock(lock, lock_file_path)

                    self.logger.debug(
                        "Lock heartbeat sent",
                        lock_id=lock.lock_id,
                        expires_at=new_expires_at,
                    )

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(
                        "Heartbeat failed",
                        lock_id=lock.lock_id,
                        error=str(e),
                        exc_info=True,
                    )
                    # Continue heartbeat even if one fails
                    await asyncio.sleep(self.heartbeat_interval_seconds)

        self._heartbeat_task = asyncio.create_task(heartbeat_loop())

    async def _acquire_postgresql_lock(self, lock_key: str, db_manager: DatabaseManager) -> Lock:
        """Acquire PostgreSQL advisory lock.

        Args:
            lock_key: Lock key
            db_manager: Database manager

        Returns:
            Acquired Lock object

        Raises:
            LockError: If lock cannot be acquired
        """
        # Convert lock_key to integer for PostgreSQL advisory lock
        # Use hash of lock_key to ensure consistent mapping
        lock_id = abs(hash(lock_key)) % (2**31)  # PostgreSQL advisory locks are int4

        # Check if we already hold this lock in this instance
        # pg_try_advisory_lock is session-level, so same session can acquire multiple times
        # We need to check if we already have it tracked
        if lock_key in self._acquired_locks:
            raise LockError(
                f"Lock already held by this instance: {lock_key}",
                context={"lock_id": lock_id, "lock_key": lock_key},
            )

        query = "SELECT pg_try_advisory_lock($1) as acquired"

        try:
            acquired = await db_manager.fetchval(query, lock_id)

            if not acquired:
                # For PostgreSQL, we can't easily check lock age, so we'll just fail
                raise LockError(
                    f"Lock already held: {lock_key}",
                    context={"lock_id": lock_id, "lock_key": lock_key},
                )

            acquired_at = datetime.now(timezone.utc)
            expires_at = acquired_at + timedelta(seconds=self.lock_ttl_seconds)

            lock = Lock(
                lock_id=str(lock_id),
                acquired_at=acquired_at,
                expires_at=expires_at,
                owner=self._lock_owner,
            )

            # Track acquired lock to prevent re-acquisition from same instance
            self._acquired_locks[lock_key] = lock

            self.logger.debug(
                "PostgreSQL advisory lock acquired",
                lock_key=lock_key,
                lock_id=lock_id,
                expires_at=expires_at,
            )

            return lock

        except LockError:
            raise
        except Exception as e:
            raise LockError(
                f"Failed to acquire PostgreSQL lock: {e}",
                context={"lock_key": lock_key, "lock_id": lock_id},
            ) from e

    async def _release_postgresql_lock(self, lock: Lock, db_manager: DatabaseManager) -> None:
        """Release PostgreSQL advisory lock.

        Args:
            lock: Lock object
            db_manager: Database manager

        Raises:
            LockError: If lock release fails
        """
        lock_id = int(lock.lock_id)
        query = "SELECT pg_advisory_unlock($1) as released"

        try:
            released = await db_manager.fetchval(query, lock_id)

            if not released:
                self.logger.warning(
                    "PostgreSQL lock was not held (may have expired)",
                    lock_id=lock_id,
                )
            else:
                self.logger.info(
                    "PostgreSQL advisory lock released",
                    lock_id=lock_id,
                )

            # Remove from tracked locks
            # Find and remove by lock_id
            for key, tracked_lock in list(self._acquired_locks.items()):
                if tracked_lock.lock_id == lock.lock_id:
                    del self._acquired_locks[key]
                    break

        except Exception as e:
            raise LockError(
                f"Failed to release PostgreSQL lock: {e}",
                context={"lock_id": lock_id},
            ) from e

    async def _extend_postgresql_lock(self, lock: Lock, db_manager: DatabaseManager) -> None:
        """Extend PostgreSQL lock TTL (no-op for advisory locks, but update expires_at).

        Args:
            lock: Lock object
            db_manager: Database manager
        """
        # PostgreSQL advisory locks don't have TTL, but we track expiry in our Lock object
        # The heartbeat just updates our local expiry time
        # In practice, advisory locks are released when the session ends
        pass

    async def _acquire_redis_lock(self, lock_key: str, redis_client: Any) -> Lock:
        """Acquire Redis lock.

        Args:
            lock_key: Lock key
            redis_client: Redis client

        Returns:
            Acquired Lock object

        Raises:
            LockError: If lock cannot be acquired
        """
        # For MVP, stub implementation
        # Full Redis implementation would use SET NX EX
        raise NotImplementedError("Redis locking not yet implemented")

    async def _release_redis_lock(self, lock: Lock, redis_client: Any) -> None:
        """Release Redis lock.

        Args:
            lock: Lock object
            redis_client: Redis client
        """
        raise NotImplementedError("Redis locking not yet implemented")

    async def _extend_redis_lock(self, lock: Lock, redis_client: Any) -> None:
        """Extend Redis lock TTL.

        Args:
            lock: Lock object
            redis_client: Redis client
        """
        raise NotImplementedError("Redis locking not yet implemented")

    async def _acquire_file_lock(self, lock_key: str, lock_file_path: Path) -> Lock:
        """Acquire file-based lock.

        Args:
            lock_key: Lock key
            lock_file_path: Path to lock file

        Returns:
            Acquired Lock object

        Raises:
            LockError: If lock cannot be acquired
        """
        import json

        lock_file = lock_file_path / f"{lock_key}.lock"

        # Check if lock file exists and is not stale
        if lock_file.exists():
            try:
                lock_data = json.loads(lock_file.read_text())
                expires_at = datetime.fromisoformat(lock_data["expires_at"])

                if datetime.now(timezone.utc) < expires_at:
                    # Lock is still valid
                    raise LockError(
                        f"Lock already held: {lock_key}",
                        context={
                            "lock_file": str(lock_file),
                            "expires_at": expires_at,
                            "owner": lock_data.get("owner"),
                        },
                    )
                else:
                    # Lock is stale, remove it
                    self.logger.warning(
                        "Removing stale lock file",
                        lock_file=str(lock_file),
                        expired_at=expires_at,
                    )
                    lock_file.unlink()

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                # Invalid lock file, remove it
                self.logger.warning(
                    "Removing invalid lock file",
                    lock_file=str(lock_file),
                    error=str(e),
                )
                lock_file.unlink()

        # Create lock file
        acquired_at = datetime.now(timezone.utc)
        expires_at = acquired_at + timedelta(seconds=self.lock_ttl_seconds)

        lock_data = {
            "lock_key": lock_key,
            "acquired_at": acquired_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "owner": self._lock_owner,
        }

        try:
            lock_file.write_text(json.dumps(lock_data, indent=2))
            lock_file.touch()  # Update mtime

            lock = Lock(
                lock_id=lock_key,
                acquired_at=acquired_at,
                expires_at=expires_at,
                owner=self._lock_owner,
            )

            self.logger.info(
                "File lock acquired",
                lock_key=lock_key,
                lock_file=str(lock_file),
                expires_at=expires_at,
            )

            return lock

        except Exception as e:
            raise LockError(
                f"Failed to acquire file lock: {e}",
                context={"lock_file": str(lock_file)},
            ) from e

    async def _release_file_lock(self, lock: Lock, lock_file_path: Path) -> None:
        """Release file-based lock.

        Args:
            lock: Lock object
            lock_file_path: Path to lock file directory

        Raises:
            LockError: If lock release fails
        """
        lock_file = lock_file_path / f"{lock.lock_id}.lock"

        try:
            if lock_file.exists():
                lock_file.unlink()
                self.logger.info(
                    "File lock released",
                    lock_id=lock.lock_id,
                    lock_file=str(lock_file),
                )
            else:
                self.logger.warning(
                    "Lock file not found (may have been removed)",
                    lock_id=lock.lock_id,
                    lock_file=str(lock_file),
                )

        except Exception as e:
            raise LockError(
                f"Failed to release file lock: {e}",
                context={"lock_file": str(lock_file)},
            ) from e

    async def _extend_file_lock(self, lock: Lock, lock_file_path: Path) -> None:
        """Extend file lock TTL.

        Args:
            lock: Lock object
            lock_file_path: Path to lock file directory
        """
        import json

        lock_file = lock_file_path / f"{lock.lock_id}.lock"

        try:
            if not lock_file.exists():
                self.logger.warning(
                    "Lock file not found during heartbeat",
                    lock_id=lock.lock_id,
                )
                return

            # Update lock file with new expiry
            lock_data = json.loads(lock_file.read_text())
            lock_data["expires_at"] = lock.expires_at.isoformat()
            lock_file.write_text(json.dumps(lock_data, indent=2))
            lock_file.touch()  # Update mtime

        except Exception as e:
            self.logger.error(
                "Failed to extend file lock",
                lock_id=lock.lock_id,
                error=str(e),
            )
