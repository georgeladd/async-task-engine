"""Unit and integration tests for Operations and Support API."""

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from src.api import app
from src.schemas import TaskStatus


@pytest.mark.asyncio
async def test_ops_endpoints_require_valid_token() -> None:
    """Verifies that operations endpoints reject requests without valid X-Ops-Token."""
    transport = ASGITransport(app=app)

    # 1. No token provided
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/api/v1/ops/overview")
        assert res.status_code == 401
        assert "Invalid or missing operational API key" in res.json()["detail"]

    # 2. Invalid token provided
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-Ops-Token": "wrong-secret"},
    ) as client:
        res = await client.get("/api/v1/ops/overview")
        assert res.status_code == 401
        assert "Invalid or missing operational API key" in res.json()["detail"]


@pytest.mark.asyncio
async def test_dashboard_html_view(async_client: AsyncClient) -> None:
    """Verifies that /dashboard endpoint serves the HTML console application."""
    response = await async_client.get("/dashboard")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Async Task Engine" in response.text
    assert "Operations Console" in response.text


@pytest.mark.asyncio
async def test_ops_overview_endpoint(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Verifies that /api/v1/ops/overview returns aggregated telemetry."""
    with patch("src.ops_api._get_redis_client", return_value=mock_redis):
        mock_redis.keys = AsyncMock(return_value=[])
        response = await async_client.get("/api/v1/ops/overview")
        assert response.status_code == 200
        data = response.json()
        assert "system_status" in data
        assert "queue_primary_depth" in data
        assert "active_workers" in data
        assert "tasks_completed_total" in data


@pytest.mark.asyncio
async def test_ops_overview_reads_distributed_redis_metrics(
    async_client: AsyncClient, mock_redis: AsyncMock
) -> None:
    """Verifies that ops overview correctly aggregates cross-process Redis metrics from workers."""
    with patch("src.ops_api._get_redis_client", return_value=mock_redis):
        mock_redis.keys = AsyncMock(return_value=[])

        redis_metrics = {
            "metrics:tasks_submitted": "50",
            "metrics:tasks_completed": "45",
            "metrics:tasks_dead_letter": "2",
            "metrics:items_processed": "4500",
            "metrics:active_workers": "3",
            "metrics:duration_sum": "9.0",
            "metrics:duration_count": "45",
        }

        async def redis_metric_get(key: str) -> str | None:
            return redis_metrics.get(key)

        mock_redis.get = AsyncMock(side_effect=redis_metric_get)

        response = await async_client.get("/api/v1/ops/overview")
        assert response.status_code == 200
        data = response.json()
        assert data["tasks_submitted_total"] == 50
        assert data["tasks_completed_total"] == 45
        assert data["tasks_dead_letter_total"] == 2
        assert data["items_processed_total"] == 4500
        assert data["active_workers"] == 3
        assert data["avg_duration_seconds"] == 0.2
        assert data["queue_primary_depth"] == 3  # 50 - 45 - 2


@pytest.mark.asyncio
async def test_ops_locks_listing(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Tests listing active distributed resource locks."""
    with patch("src.ops_api._get_redis_client", return_value=mock_redis):
        mock_redis.keys = AsyncMock(return_value=["lock:resource:account_99"])
        mock_redis.ttl = AsyncMock(return_value=180)
        mock_redis.get = AsyncMock(return_value="owner-token-12345")

        response = await async_client.get("/api/v1/ops/locks")
        assert response.status_code == 200
        locks = response.json()
        assert len(locks) == 1
        assert locks[0]["resource_id"] == "account_99"
        assert locks[0]["ttl_remaining"] == 180


@pytest.mark.asyncio
async def test_ops_force_unlock(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Tests manual lock release by on-call operator."""
    with patch("src.ops_api._get_redis_client", return_value=mock_redis):
        mock_redis.exists = AsyncMock(return_value=True)
        mock_redis.ttl = AsyncMock(return_value=60)
        mock_redis.delete = AsyncMock(return_value=1)

        payload = {"resource_id": "stuck_tenant_42"}
        response = await async_client.post("/api/v1/ops/unlock", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "unlocked"
        assert "stuck_tenant_42" in data["message"]


@pytest.mark.asyncio
async def test_ops_dlq_listing(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Tests querying Dead-Letter Queue items."""
    with patch("src.ops_api._get_redis_client", return_value=mock_redis):
        task_uuid = str(uuid4())
        mock_redis.keys = AsyncMock(return_value=[f"task:status:{task_uuid}"])

        async def custom_get(key: str) -> str | None:
            if key == f"task:status:{task_uuid}":
                return TaskStatus.DEAD_LETTERED.value
            return '{"error_message": "Database timeout after 3 retries"}'

        mock_redis.get = AsyncMock(side_effect=custom_get)

        response = await async_client.get("/api/v1/ops/dlq")
        assert response.status_code == 200
        items = response.json()
        assert len(items) == 1
        assert items[0]["task_id"] == task_uuid
        assert "Database timeout" in items[0]["error_reason"]


@pytest.mark.asyncio
async def test_ops_dlq_replay(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Tests replaying a failed task from DLQ into primary queue."""
    with (
        patch("src.ops_api._get_redis_client", return_value=mock_redis),
        patch("src.api.broker.publish_task", new_callable=AsyncMock) as mock_publish,
    ):
        task_uuid = str(uuid4())
        payload = {"task_id": task_uuid}
        response = await async_client.post("/api/v1/ops/dlq/replay", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "requeued"
        mock_publish.assert_called_once()


@pytest.mark.asyncio
async def test_ops_dlq_replay_restores_original_payload(
    async_client: AsyncClient, mock_redis: AsyncMock
) -> None:
    """Verifies that DLQ replay restores the original task payload, resource_id, and callback."""
    from src.schemas import TaskMessage, TaskPayload, TaskPriority

    with (
        patch("src.ops_api._get_redis_client", return_value=mock_redis),
        patch("src.api.broker.publish_task", new_callable=AsyncMock) as mock_publish,
    ):
        task_uuid = str(uuid4())
        original_task = TaskMessage(
            task_id=uuid4(),
            task_type="inventory_sync",
            resource_id="warehouse_zone_1",
            priority=TaskPriority.NORMAL,
            payload=TaskPayload(items=[{"sku": "A101", "qty": 42}], parameters={"dry_run": False}),
            callback_url="https://erp.internal/hook",
            attempts=3,
        )

        async def redis_get_side_effect(key: str) -> str | None:
            if key == f"task:data:{task_uuid}":
                return original_task.model_dump_json()
            return None

        mock_redis.get = AsyncMock(side_effect=redis_get_side_effect)

        payload = {"task_id": task_uuid}
        response = await async_client.post("/api/v1/ops/dlq/replay", json=payload)
        assert response.status_code == 200

        mock_publish.assert_called_once()
        replayed_task: TaskMessage = mock_publish.call_args[0][0]
        assert replayed_task.task_type == "inventory_sync"
        assert replayed_task.resource_id == "warehouse_zone_1"
        assert replayed_task.payload.items == [{"sku": "A101", "qty": 42}]
        assert replayed_task.priority == TaskPriority.HIGH
        assert replayed_task.attempts == 0
        assert str(replayed_task.callback_url) == "https://erp.internal/hook"


@pytest.mark.asyncio
async def test_ops_escalate_incident(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Tests compiling an incident report dossier for L3/Dev."""
    with patch("src.ops_api._get_redis_client", return_value=mock_redis):
        task_uuid = str(uuid4())
        mock_redis.get = AsyncMock(return_value="NullPointer exception in batch stream")

        payload = {
            "task_id": task_uuid,
            "operator_comment": "Customer experienced failure during midnight migration",
        }
        response = await async_client.post("/api/v1/ops/escalate", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "escalated"
        assert "INC-" in data["incident_id"]
        assert task_uuid in data["markdown_dossier"]
        assert "Customer experienced failure" in data["markdown_dossier"]


@pytest.mark.asyncio
async def test_ops_escalate_running_task(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Tests compiling an incident report for a running/stalled task."""
    with patch("src.ops_api._get_redis_client", return_value=mock_redis):
        task_uuid = str(uuid4())

        async def redis_get_side_effect(key: str) -> str | None:
            if key == f"task:status:{task_uuid}":
                return "running"
            return None

        mock_redis.get = AsyncMock(side_effect=redis_get_side_effect)

        payload = {
            "task_id": task_uuid,
            "operator_comment": "Task seems stalled in worker event loop",
        }
        response = await async_client.post("/api/v1/ops/escalate", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "escalated"
        assert "RUNNING" in data["markdown_dossier"]
        assert "stalled in worker event loop" in data["markdown_dossier"]
