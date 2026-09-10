"""Unit and integration tests for Operations and Support API."""

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from httpx import AsyncClient

from src.schemas import TaskStatus


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
