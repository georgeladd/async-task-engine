from unittest.mock import AsyncMock, PropertyMock, patch
from uuid import uuid4

import pytest
from httpx import AsyncClient

from src.broker import MessageBroker


@pytest.mark.asyncio
async def test_health_check(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Verifies that health check endpoint returns 200 OK when dependencies are operational."""
    with (
        patch("src.api.redis_client", mock_redis),
        patch.object(MessageBroker, "is_connected", new_callable=PropertyMock, return_value=True),
    ):
        mock_redis.ping = AsyncMock(return_value=True)
        response = await async_client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "async-task-engine"
        assert data["dependencies"]["redis"] == "connected"
        assert data["dependencies"]["rabbitmq"] == "connected"


@pytest.mark.asyncio
async def test_health_check_degraded_when_dependency_down(
    async_client: AsyncClient, mock_redis: AsyncMock
) -> None:
    """Verifies that health check endpoint returns 503 Service Unavailable when broker is down."""
    with (
        patch("src.api.redis_client", mock_redis),
        patch.object(MessageBroker, "is_connected", new_callable=PropertyMock, return_value=False),
    ):
        mock_redis.ping = AsyncMock(return_value=True)
        response = await async_client.get("/health")
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "degraded"
        assert data["dependencies"]["rabbitmq"] == "disconnected"


@pytest.mark.asyncio
async def test_submit_task_success(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Tests successful task submission via API."""
    with (
        patch("src.api.broker.publish_task", new_callable=AsyncMock) as mock_publish,
        patch("src.api.redis_client", mock_redis),
    ):
            payload = {
                "task_type": "cache_warmup",
                "resource_id": "redis_cluster_main",
                "priority": "high",
                "payload": {
                    "items": [{"key": "user:1", "val": 10}],
                    "parameters": {"force": True},
                },
            }
            response = await async_client.post("/api/v1/tasks", json=payload)
            assert response.status_code == 202
            data = response.json()
            assert data["status"] == "pending"
            assert "task_id" in data
            mock_publish.assert_called_once()


@pytest.mark.asyncio
async def test_submit_task_validation_error(async_client: AsyncClient) -> None:
    """Verifies that invalid task submission returns 422 Unprocessable Entity."""
    invalid_payload = {
        "task_type": "",  # Empty task_type violates min_length
        "resource_id": "test_res",
    }
    response = await async_client.post("/api/v1/tasks", json=invalid_payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_task_status_not_found(async_client: AsyncClient, mock_redis: AsyncMock) -> None:
    """Verifies 404 response for nonexistent task."""
    with patch("src.api.redis_client", mock_redis):
        random_uuid = uuid4()
        response = await async_client.get(f"/api/v1/tasks/{random_uuid}")
        assert response.status_code == 404
