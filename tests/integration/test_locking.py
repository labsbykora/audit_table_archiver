"""Integration tests for distributed locking."""

import asyncio

import pytest

from archiver.database import DatabaseManager
from archiver.exceptions import LockError
from archiver.locking import LockManager


@pytest.mark.asyncio
async def test_postgresql_lock_acquisition_and_release(postgres_ready, db_config):
    """Test PostgreSQL advisory lock acquisition and release."""
    db_manager = DatabaseManager(db_config, pool_size=2)
    await db_manager.connect()

    try:
        lock_manager = LockManager(lock_type="postgresql", logger=None)

        # Acquire lock
        lock = await lock_manager.acquire_lock(
            lock_key="test_lock",
            db_manager=db_manager,
        )

        assert lock is not None
        assert lock.lock_id is not None
        assert lock.owner is not None

        # Try to acquire same lock again (should fail)
        with pytest.raises(LockError):
            await lock_manager.acquire_lock(
                lock_key="test_lock",
                db_manager=db_manager,
            )

        # Release lock
        await lock_manager.release_lock(lock, db_manager=db_manager)

        # Now we should be able to acquire it again
        lock2 = await lock_manager.acquire_lock(
            lock_key="test_lock",
            db_manager=db_manager,
        )
        assert lock2 is not None

        await lock_manager.release_lock(lock2, db_manager=db_manager)

    finally:
        await db_manager.disconnect()


@pytest.mark.asyncio
async def test_lock_heartbeat(postgres_ready, db_config):
    """Test lock heartbeat mechanism."""
    db_manager = DatabaseManager(db_config, pool_size=2)
    await db_manager.connect()

    try:
        lock_manager = LockManager(
            lock_type="postgresql",
            lock_ttl_seconds=60,
            heartbeat_interval_seconds=1,  # 1 second for testing
            logger=None,
        )

        # Acquire lock
        lock = await lock_manager.acquire_lock(
            lock_key="test_heartbeat",
            db_manager=db_manager,
        )

        # Start heartbeat
        await lock_manager.start_heartbeat(lock, db_manager=db_manager)

        # Wait a bit to let heartbeat run
        await asyncio.sleep(2)

        # Stop heartbeat and release lock
        await lock_manager.release_lock(lock, db_manager=db_manager)

    finally:
        await db_manager.disconnect()

    @pytest.mark.asyncio
    async def test_concurrent_lock_prevention(postgres_ready, db_config):
        """Test that concurrent runs are prevented by locking."""
        # Use different connections from the pool to simulate different sessions
        db_manager1 = DatabaseManager(db_config, pool_size=2)
        db_manager2 = DatabaseManager(db_config, pool_size=2)
        await db_manager1.connect()
        await db_manager2.connect()

        try:
            lock_manager1 = LockManager(lock_type="postgresql", logger=None)
            lock_manager2 = LockManager(lock_type="postgresql", logger=None)

            # First instance acquires lock
            lock1 = await lock_manager1.acquire_lock(
                lock_key="concurrent_test",
                db_manager=db_manager1,
            )

            # Second instance tries to acquire same lock (should fail)
            # Note: This will only fail if using different database connections/sessions
            # PostgreSQL advisory locks are session-level, so different connections should fail
            with pytest.raises(LockError):
                await lock_manager2.acquire_lock(
                    lock_key="concurrent_test",
                    db_manager=db_manager2,
                )

            # Release lock from first instance
            await lock_manager1.release_lock(lock1, db_manager=db_manager1)

            # Now second instance should be able to acquire it
            lock2 = await lock_manager2.acquire_lock(
                lock_key="concurrent_test",
                db_manager=db_manager2,
            )
            assert lock2 is not None

            await lock_manager2.release_lock(lock2, db_manager=db_manager2)

        finally:
            await db_manager1.disconnect()
            await db_manager2.disconnect()


@pytest.mark.asyncio
async def test_file_lock_acquisition_and_release(tmp_path):
    """Test file-based lock acquisition and release."""
    lock_file_path = tmp_path / "locks"
    lock_file_path.mkdir()

    lock_manager = LockManager(lock_type="file", logger=None)

    # Acquire lock
    lock = await lock_manager.acquire_lock(
        lock_key="test_file_lock",
        lock_file_path=lock_file_path,
    )

    assert lock is not None
    assert lock.lock_id == "test_file_lock"
    assert (lock_file_path / "test_file_lock.lock").exists()

    # Try to acquire same lock again (should fail)
    with pytest.raises(LockError):
        await lock_manager.acquire_lock(
            lock_key="test_file_lock",
            lock_file_path=lock_file_path,
        )

    # Release lock
    await lock_manager.release_lock(lock, lock_file_path=lock_file_path)

    # Lock file should be removed
    assert not (lock_file_path / "test_file_lock.lock").exists()

    # Now we should be able to acquire it again
    lock2 = await lock_manager.acquire_lock(
        lock_key="test_file_lock",
        lock_file_path=lock_file_path,
    )
    assert lock2 is not None

    await lock_manager.release_lock(lock2, lock_file_path=lock_file_path)


@pytest.mark.asyncio
async def test_stale_file_lock_cleanup(tmp_path):
    """Test that stale file locks are automatically cleaned up."""
    lock_file_path = tmp_path / "locks"
    lock_file_path.mkdir()

    import json
    from datetime import datetime, timedelta, timezone

    # Create a stale lock file (expired)
    stale_lock_file = lock_file_path / "stale_lock.lock"
    stale_lock_data = {
        "lock_key": "stale_lock",
        "acquired_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        "expires_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),  # Expired
        "owner": "old_owner",
    }
    stale_lock_file.write_text(json.dumps(stale_lock_data))

    lock_manager = LockManager(lock_type="file", logger=None)

    # Should be able to acquire lock (stale lock should be removed)
    lock = await lock_manager.acquire_lock(
        lock_key="stale_lock",
        lock_file_path=lock_file_path,
    )

    assert lock is not None
    assert lock.owner != "old_owner"  # New lock with new owner

    await lock_manager.release_lock(lock, lock_file_path=lock_file_path)
