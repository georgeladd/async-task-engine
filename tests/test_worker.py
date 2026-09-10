"""Unit tests for worker message handling, Redis retry tracking, and DLQ routing."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from src.schemas import TaskMessage, TaskPriority, TaskStatus
from src.worker import TaskWorker


@pytest.mark.asyncio
async def test_worker_retry_attempts_increment_and_requeue() -> None:
    """Verifies that failing task increments Redis attempt counter and requeues message."""
    worker = TaskWorker()
    worker.redis = AsyncMock()

    # Simulate first failure: Redis INCR returns 1 (< max_retries 3)
    worker.redis.incr = AsyncMock(return_value=1)
    worker.redis.expire = AsyncMock()
    worker.redis.set = AsyncMock()
    worker.redis.delete = AsyncMock()

    task = TaskMessage(
        task_id=uuid4(),
        task_type="sync_job",
        resource_id="tenant_retry_1",
        priority=TaskPriority.HIGH,
    )

    mock_msg = AsyncMock()
    mock_msg.body = task.model_dump_json().encode("utf-8")
    process_cm = MagicMock()
    process_cm.__aenter__ = AsyncMock(return_value=mock_msg)
    process_cm.__aexit__ = AsyncMock(return_value=None)
    mock_msg.process = MagicMock(return_value=process_cm)
    mock_msg.nack = AsyncMock()
    mock_msg.reject = AsyncMock()

    with (
        patch.object(worker, "process_task_payload", side_effect=RuntimeError("Transient DB connection glitch")),
        patch("src.worker.DistributedLock.acquire", new_callable=AsyncMock, return_value=True),
        patch("src.worker.DistributedLock.release", new_callable=AsyncMock, return_value=True),
    ):
        await worker.handle_incoming_message(mock_msg)

        # Verified: Redis INCR was called for task attempts and active workers
        worker.redis.incr.assert_any_call(f"task:attempts:{task.task_id}")
        worker.redis.incr.assert_any_call("metrics:active_workers")
        worker.redis.expire.assert_called_once_with(f"task:attempts:{task.task_id}", 86400)
        worker.redis.decr.assert_called_once_with("metrics:active_workers")

        # Verified: Status set back to PENDING and nack(requeue=True) was invoked
        worker.redis.set.assert_any_call(f"task:status:{task.task_id}", TaskStatus.PENDING.value, ex=86400)
        mock_msg.nack.assert_called_once_with(requeue=True)
        mock_msg.reject.assert_not_called()


@pytest.mark.asyncio
async def test_worker_exceeds_max_retries_routes_to_dlq() -> None:
    """Verifies that exceeding max retries routes message to DLQ via reject(requeue=False)."""
    worker = TaskWorker()
    worker.redis = AsyncMock()

    # Simulate 3rd failure: Redis INCR returns 3 (>= max_retries 3)
    worker.redis.incr = AsyncMock(return_value=3)
    worker.redis.expire = AsyncMock()
    worker.redis.set = AsyncMock()
    worker.redis.delete = AsyncMock()

    task = TaskMessage(
        task_id=uuid4(),
        task_type="critical_batch",
        resource_id="tenant_retry_2",
        priority=TaskPriority.CRITICAL,
        callback_url="https://webhook.internal/dlq-alerts",
    )

    mock_msg = AsyncMock()
    mock_msg.body = task.model_dump_json().encode("utf-8")
    process_cm = MagicMock()
    process_cm.__aenter__ = AsyncMock(return_value=mock_msg)
    process_cm.__aexit__ = AsyncMock(return_value=None)
    mock_msg.process = MagicMock(return_value=process_cm)
    mock_msg.nack = AsyncMock()
    mock_msg.reject = AsyncMock()

    with (
        patch.object(worker, "process_task_payload", side_effect=ValueError("Unrecoverable schema corruption")),
        patch.object(worker, "dispatch_webhook", new_callable=AsyncMock) as mock_webhook,
        patch("src.worker.DistributedLock.acquire", new_callable=AsyncMock, return_value=True),
        patch("src.worker.DistributedLock.release", new_callable=AsyncMock, return_value=True),
    ):
        await worker.handle_incoming_message(mock_msg)

        # Verified: Status set to DEAD_LETTERED
        worker.redis.set.assert_any_call(f"task:status:{task.task_id}", TaskStatus.DEAD_LETTERED.value, ex=86400)

        # Verified: Rejected to DLQ without requeueing
        mock_msg.reject.assert_called_once_with(requeue=False)
        mock_msg.nack.assert_not_called()

        # Verified: Webhook alert sent for dead lettered task
        mock_webhook.assert_called_once()
        webhook_kwargs = mock_webhook.call_args[1]
        assert webhook_kwargs["callback_url"] == str(task.callback_url)
        assert webhook_kwargs["event"] == "task.dead_lettered"
        assert webhook_kwargs["status"] == TaskStatus.DEAD_LETTERED
        assert webhook_kwargs["task"].attempts == 3
        assert webhook_kwargs["result"] is None


@pytest.mark.asyncio
async def test_worker_lock_contention_backs_off_and_requeues() -> None:
    """Verifies that worker backs off with sleep before requeuing when resource is locked."""
    worker = TaskWorker()
    worker.redis = AsyncMock()

    task = TaskMessage(
        task_id=uuid4(),
        task_type="contention_job",
        resource_id="locked_resource_42",
    )

    mock_msg = AsyncMock()
    mock_msg.body = task.model_dump_json().encode("utf-8")
    process_cm = MagicMock()
    process_cm.__aenter__ = AsyncMock(return_value=mock_msg)
    process_cm.__aexit__ = AsyncMock(return_value=None)
    mock_msg.process = MagicMock(return_value=process_cm)
    mock_msg.nack = AsyncMock()

    with (
        patch("src.worker.DistributedLock.acquire", new_callable=AsyncMock, return_value=False),
        patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        await worker.handle_incoming_message(mock_msg)

        mock_sleep.assert_called_once_with(0.5)
        mock_msg.nack.assert_called_once_with(requeue=True)


@pytest.mark.asyncio
async def test_worker_manages_in_flight_task_key() -> None:
    """Verifies that worker sets self-expiring in_flight key and deletes it in finally block."""
    worker = TaskWorker()
    worker.redis = AsyncMock()

    task = TaskMessage(
        task_id=uuid4(),
        task_type="in_flight_test",
        resource_id="tenant_in_flight",
    )

    mock_msg = AsyncMock()
    mock_msg.body = task.model_dump_json().encode("utf-8")
    process_cm = MagicMock()
    process_cm.__aenter__ = AsyncMock(return_value=mock_msg)
    process_cm.__aexit__ = AsyncMock(return_value=None)
    mock_msg.process = MagicMock(return_value=process_cm)
    mock_msg.ack = AsyncMock()

    with (
        patch.object(worker, "process_task_payload", new_callable=AsyncMock),
        patch("src.worker.DistributedLock.acquire", new_callable=AsyncMock, return_value=True),
        patch("src.worker.DistributedLock.release", new_callable=AsyncMock, return_value=True),
    ):
        await worker.handle_incoming_message(mock_msg)

        in_flight_key = f"task:in_flight:{task.task_id}"
        worker.redis.set.assert_any_call(in_flight_key, "1", ex=300)
        worker.redis.delete.assert_any_call(in_flight_key)
