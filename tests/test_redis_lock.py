"""Tests for Redis distributed locking mechanism."""

from unittest.mock import AsyncMock

import pytest

from src.redis_lock import DistributedLock, LockAcquisitionError


@pytest.mark.asyncio
async def test_distributed_lock_acquire_and_release(mock_redis: AsyncMock) -> None:
    """Tests normal acquire and atomic release workflow."""
    lock = DistributedLock(
        redis_client=mock_redis,
        resource_key="cluster_partition_1",
        ttl_seconds=60,
    )

    acquired = await lock.acquire(timeout_seconds=0.5)
    assert acquired is True
    assert lock.is_acquired is True

    released = await lock.release()
    assert released is True
    assert lock.is_acquired is False


@pytest.mark.asyncio
async def test_distributed_lock_contention(mock_redis: AsyncMock) -> None:
    """Verifies that second lock cannot be acquired while first is active."""
    lock1 = DistributedLock(
        redis_client=mock_redis,
        resource_key="shared_resource_x",
        ttl_seconds=60,
    )
    lock2 = DistributedLock(
        redis_client=mock_redis,
        resource_key="shared_resource_x",
        ttl_seconds=60,
    )

    # First lock succeeds
    assert await lock1.acquire() is True

    # Second lock fails to acquire because key already exists in mock redis
    assert await lock2.acquire(timeout_seconds=0.2) is False

    # Once lock1 releases, lock2 can acquire
    await lock1.release()
    assert await lock2.acquire() is True
    await lock2.release()


@pytest.mark.asyncio
async def test_distributed_lock_context_manager(mock_redis: AsyncMock) -> None:
    """Tests async context manager usage."""
    async with DistributedLock(mock_redis, "auto_resource", ttl_seconds=30) as lock:
        assert lock.is_acquired is True

    # After exit, lock should be released
    assert lock.is_acquired is False


@pytest.mark.asyncio
async def test_distributed_lock_context_manager_timeout(mock_redis: AsyncMock) -> None:
    """Verifies LockAcquisitionError is raised when lock cannot be obtained in context manager."""
    # Pre-occupy resource in mock
    await mock_redis.set("lock:resource:busy_resource", "other_token")

    with pytest.raises(LockAcquisitionError):
        async with DistributedLock(mock_redis, "busy_resource", ttl_seconds=30):
            pass
