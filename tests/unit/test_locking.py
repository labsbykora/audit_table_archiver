"""Unit tests for locking module."""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from archiver.database import DatabaseManager
from archiver.exceptions import LockError
from archiver.locking import Lock, LockManager


def test_lock_init() -> None:
    """Test lock initialization."""
    acquired_at = datetime.now(timezone.utc)
    expires_at = acquired_at + timedelta(seconds=3600)
    
    lock = Lock(
        lock_id="test_lock",
        acquired_at=acquired_at,
        expires_at=expires_at,
        owner="test_owner",
    )
    
    assert lock.lock_id == "test_lock"
    assert lock.acquired_at == acquired_at
    assert lock.expires_at == expires_at
    assert lock.owner == "test_owner"


def test_lock_is_expired() -> None:
    """Test lock expiration check."""
    now = datetime.now(timezone.utc)
    past = now - timedelta(seconds=100)
    future = now + timedelta(seconds=100)
    
    expired_lock = Lock("test", past, past, "owner")
    valid_lock = Lock("test", now, future, "owner")
    
    assert expired_lock.is_expired() is True
    assert valid_lock.is_expired() is False


def test_lock_time_until_expiry() -> None:
    """Test lock time until expiry calculation."""
    now = datetime.now(timezone.utc)
    future = now + timedelta(seconds=100)
    
    lock = Lock("test", now, future, "owner")
    time_until = lock.time_until_expiry()
    
    assert 90 < time_until < 110  # Allow some margin for execution time


def test_lock_manager_init() -> None:
    """Test lock manager initialization."""
    manager = LockManager(
        lock_type="postgresql",
        lock_ttl_seconds=3600,
        heartbeat_interval_seconds=30,
    )
    
    assert manager.lock_type == "postgresql"
    assert manager.lock_ttl_seconds == 3600
    assert manager.heartbeat_interval_seconds == 30


def test_lock_manager_init_invalid_type() -> None:
    """Test lock manager with invalid lock type."""
    with pytest.raises(ValueError, match="Invalid lock_type"):
        LockManager(lock_type="invalid")


@pytest.mark.asyncio
async def test_lock_manager_acquire_postgresql_lock() -> None:
    """Test acquiring PostgreSQL advisory lock."""
    manager = LockManager(lock_type="postgresql")
    
    mock_db_manager = MagicMock()
    mock_db_manager.fetchval = AsyncMock(return_value=True)  # Lock acquired
    
    lock = await manager.acquire_lock("test_db", db_manager=mock_db_manager)
    
    assert lock is not None
    assert lock.lock_id is not None
    assert not lock.is_expired()
    mock_db_manager.fetchval.assert_called_once()


@pytest.mark.asyncio
async def test_lock_manager_acquire_postgresql_lock_failed() -> None:
    """Test acquiring PostgreSQL lock when already held."""
    manager = LockManager(lock_type="postgresql")
    
    mock_db_manager = MagicMock()
    mock_db_manager.fetchval = AsyncMock(return_value=False)  # Lock not acquired
    
    with pytest.raises(LockError, match="Lock already held"):
        await manager.acquire_lock("test_db", db_manager=mock_db_manager)


@pytest.mark.asyncio
async def test_lock_manager_release_postgresql_lock() -> None:
    """Test releasing PostgreSQL advisory lock."""
    manager = LockManager(lock_type="postgresql")
    
    mock_db_manager = MagicMock()
    mock_db_manager.fetchval = AsyncMock(return_value=True)  # Lock acquired
    mock_db_manager.fetchval = AsyncMock(return_value=True)  # Lock released
    
    lock = await manager.acquire_lock("test_db", db_manager=mock_db_manager)
    
    # Release lock
    mock_db_manager.fetchval = AsyncMock(return_value=True)  # Lock released
    await manager.release_lock(lock, db_manager=mock_db_manager)
    
    assert manager.current_lock is None


@pytest.mark.asyncio
async def test_lock_manager_acquire_file_lock(tmp_path: Path) -> None:
    """Test acquiring file-based lock."""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    
    manager = LockManager(lock_type="file")
    
    lock = await manager.acquire_lock("test_db", lock_file_path=lock_dir)
    
    assert lock is not None
    assert lock.lock_id == "test_db"
    assert (lock_dir / "test_db.lock").exists()


@pytest.mark.asyncio
async def test_lock_manager_acquire_file_lock_stale(tmp_path: Path) -> None:
    """Test acquiring file lock when stale lock exists."""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    
    # Create stale lock file
    stale_lock_file = lock_dir / "test_db.lock"
    stale_data = {
        "lock_key": "test_db",
        "acquired_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        "expires_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        "owner": "old_owner",
    }
    stale_lock_file.write_text(json.dumps(stale_data))
    
    manager = LockManager(lock_type="file")
    
    lock = await manager.acquire_lock("test_db", lock_file_path=lock_dir)
    
    assert lock is not None
    assert lock.lock_id == "test_db"


@pytest.mark.asyncio
async def test_lock_manager_acquire_file_lock_already_held(tmp_path: Path) -> None:
    """Test acquiring file lock when already held."""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    
    # Create valid lock file
    lock_file = lock_dir / "test_db.lock"
    lock_data = {
        "lock_key": "test_db",
        "acquired_at": datetime.now(timezone.utc).isoformat(),
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        "owner": "other_owner",
    }
    lock_file.write_text(json.dumps(lock_data))
    
    manager = LockManager(lock_type="file")
    
    with pytest.raises(LockError, match="Lock already held"):
        await manager.acquire_lock("test_db", lock_file_path=lock_dir)


@pytest.mark.asyncio
async def test_lock_manager_release_file_lock(tmp_path: Path) -> None:
    """Test releasing file-based lock."""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    
    manager = LockManager(lock_type="file")
    
    lock = await manager.acquire_lock("test_db", lock_file_path=lock_dir)
    assert (lock_dir / "test_db.lock").exists()
    
    await manager.release_lock(lock, lock_file_path=lock_dir)
    assert not (lock_dir / "test_db.lock").exists()


@pytest.mark.asyncio
async def test_lock_manager_start_heartbeat() -> None:
    """Test starting lock heartbeat."""
    manager = LockManager(
        lock_type="postgresql",
        heartbeat_interval_seconds=0.1,  # Very short for testing
    )
    
    mock_db_manager = MagicMock()
    mock_db_manager.fetchval = AsyncMock(return_value=True)
    
    lock = await manager.acquire_lock("test_db", db_manager=mock_db_manager)
    
    await manager.start_heartbeat(lock, db_manager=mock_db_manager)
    
    # Wait a bit for heartbeat
    await asyncio.sleep(0.2)
    
    # Cancel heartbeat
    if manager._heartbeat_task:
        manager._heartbeat_task.cancel()
        try:
            await manager._heartbeat_task
        except asyncio.CancelledError:
            pass
    
    assert manager._heartbeat_task is not None or manager.current_lock is not None


@pytest.mark.asyncio
async def test_lock_manager_acquire_redis_lock_not_implemented() -> None:
    """Test that Redis locking is not yet implemented."""
    manager = LockManager(lock_type="redis")
    
    mock_redis = MagicMock()
    
    with pytest.raises(NotImplementedError, match="Redis locking not yet implemented"):
        await manager.acquire_lock("test_db", redis_client=mock_redis)


@pytest.mark.asyncio
async def test_lock_manager_acquire_lock_missing_db_manager() -> None:
    """Test acquiring lock without required db_manager."""
    manager = LockManager(lock_type="postgresql")
    
    with pytest.raises(ValueError, match="db_manager is required"):
        await manager.acquire_lock("test_db")


@pytest.mark.asyncio
async def test_lock_manager_acquire_lock_missing_redis_client() -> None:
    """Test acquiring Redis lock without required redis_client."""
    manager = LockManager(lock_type="redis")
    
    with pytest.raises(ValueError, match="redis_client is required"):
        await manager.acquire_lock("test_db")


@pytest.mark.asyncio
async def test_lock_manager_acquire_lock_missing_file_path() -> None:
    """Test acquiring file lock without required lock_file_path."""
    manager = LockManager(lock_type="file")
    
    with pytest.raises(ValueError, match="lock_file_path is required"):
        await manager.acquire_lock("test_db")

