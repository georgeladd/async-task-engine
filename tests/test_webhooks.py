import json
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
import pytest

from src.schemas import TaskMessage, TaskPriority, TaskResult, TaskStatus
from src.worker import TaskWorker


@pytest.mark.asyncio
async def test_webhook_delivery_success() -> None:
    """Verifies that worker dispatches valid HTTP POST notification upon completion."""
    worker = TaskWorker()
    callback_url = "https://api.external.com/callbacks/task-done"
    task = TaskMessage(
        task_id=uuid4(),
        task_type="billing_reconciliation",
        resource_id="tenant_charlie",
        priority=TaskPriority.HIGH,
        callback_url=callback_url,
    )
    result = TaskResult(
        task_id=task.task_id,
        status=TaskStatus.COMPLETED,
        processed_count=100,
        chunk_count=1,
        execution_time_seconds=0.45,
    )

    with (
        patch("src.worker.is_safe_webhook_url", return_value=(True, "")),
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
    ):
        mock_response = AsyncMock()
        mock_response.is_success = True
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        await worker.dispatch_webhook(
            callback_url=str(task.callback_url),
            event="task.completed",
            task=task,
            status=TaskStatus.COMPLETED,
            result=result,
        )

        assert mock_post.call_count == 1
        args, kwargs = mock_post.call_args
        assert args[0] == callback_url
        assert kwargs["headers"]["X-Task-ID"] == str(task.task_id)
        assert kwargs["headers"]["X-Event-Type"] == "task.completed"
        assert "X-Hub-Signature-256" in kwargs["headers"]
        assert kwargs["headers"]["X-Hub-Signature-256"].startswith("sha256=")

        body = json.loads(kwargs["content"].decode("utf-8"))
        assert body["event"] == "task.completed"
        assert body["status"] == "completed"
        assert body["result"]["processed_count"] == 100


@pytest.mark.asyncio
async def test_webhook_delivery_failure_is_isolated() -> None:
    """Verifies that webhook network failures do not throw exceptions or crash the worker."""
    worker = TaskWorker()
    task = TaskMessage(
        task_id=uuid4(),
        task_type="audit_sync",
        resource_id="tenant_delta",
        callback_url="https://api.external.com/webhook",
    )

    with (
        patch("src.worker.is_safe_webhook_url", return_value=(True, "")),
        patch("httpx.AsyncClient.post", side_effect=httpx.ConnectTimeout("Connection timed out")),
    ):
        # Must execute cleanly without raising exception
        await worker.dispatch_webhook(
            callback_url=str(task.callback_url),
            event="task.completed",
            task=task,
            status=TaskStatus.COMPLETED,
        )


@pytest.mark.asyncio
async def test_webhook_dead_letter_delivery() -> None:
    """Verifies that dead-lettered tasks dispatch failure webhooks."""
    worker = TaskWorker()
    callback_url = "https://api.external.com/dlq-alerts"
    task = TaskMessage(
        task_id=uuid4(),
        task_type="payment_sync",
        resource_id="merchant_99",
        callback_url=callback_url,
    )

    with (
        patch("src.worker.is_safe_webhook_url", return_value=(True, "")),
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
    ):
        mock_response = AsyncMock()
        mock_response.is_success = True
        mock_post.return_value = mock_response

        await worker.dispatch_webhook(
            callback_url=str(task.callback_url),
            event="task.dead_lettered",
            task=task,
            status=TaskStatus.DEAD_LETTERED,
            result=None,
        )

        assert mock_post.call_count == 1
        kwargs = mock_post.call_args[1]
        assert "X-Hub-Signature-256" in kwargs["headers"]
        body = json.loads(kwargs["content"].decode("utf-8"))
        assert body["event"] == "task.dead_lettered"
        assert body["status"] == "dead_lettered"
        assert body["result"] is None
