"""Unit tests for AsyncTokenBucketRateLimiter and worker batch throttling."""

import asyncio
import time

import pytest

from src.rate_limiter import AsyncTokenBucketRateLimiter
from src.schemas import TaskMessage, TaskPayload, TaskPriority
from src.worker import TaskWorker


@pytest.mark.asyncio
async def test_rate_limiter_acquire_burst() -> None:
    """Verifies that burst operations within capacity execute without delay."""
    limiter = AsyncTokenBucketRateLimiter(rate=50.0, capacity=10.0)
    start_time = time.monotonic()

    # Consuming 5 tokens should be instantaneous
    for _ in range(5):
        await limiter.acquire(1.0)

    elapsed = time.monotonic() - start_time
    assert elapsed < 0.1


@pytest.mark.asyncio
async def test_rate_limiter_throttles_cadence() -> None:
    """Verifies that consuming tokens past capacity enforces correct sleep duration."""
    # Rate: 10 tokens/sec. Refill 1 token every 0.1s.
    limiter = AsyncTokenBucketRateLimiter(rate=10.0, capacity=2.0)

    # Empty the burst capacity
    await limiter.acquire(2.0)

    start_time = time.monotonic()
    # Now acquire 2 more tokens. Needs 0.2s of accumulation
    await limiter.acquire(2.0)
    elapsed = time.monotonic() - start_time

    assert elapsed >= 0.15


@pytest.mark.asyncio
async def test_rate_limiter_try_acquire() -> None:
    """Verifies non-blocking try_acquire behavior."""
    limiter = AsyncTokenBucketRateLimiter(rate=10.0, capacity=2.0)

    assert await limiter.try_acquire(1.0) is True
    assert await limiter.try_acquire(1.0) is True
    # Capacity exhausted
    assert await limiter.try_acquire(1.0) is False

    # Wait for replenishment
    await asyncio.sleep(0.12)
    assert await limiter.try_acquire(1.0) is True


def test_rate_limiter_invalid_rate() -> None:
    """Verifies ValueError when configuring non-positive rate."""
    with pytest.raises(ValueError, match="Rate must be greater than 0"):
        AsyncTokenBucketRateLimiter(rate=0)

    with pytest.raises(ValueError, match="Rate must be greater than 0"):
        AsyncTokenBucketRateLimiter(rate=-5.0)


@pytest.mark.asyncio
async def test_worker_respects_rate_limiter() -> None:
    """Verifies that TaskWorker correctly paces chunk execution with rate limiter."""
    worker = TaskWorker(rate_limit=100.0)
    task = TaskMessage(
        task_type="demo",
        resource_id="endpoint_alpha",
        priority=TaskPriority.NORMAL,
        payload=TaskPayload(
            items=[{"id": i} for i in range(250)],
        ),
    )

    result = await worker.process_task_payload(task)
    assert result.processed_count == 250
    assert result.chunk_count == 3  # 100 + 100 + 50
    assert result.execution_time_seconds > 0.0
