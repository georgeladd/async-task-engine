"""Integration tests for FastAPI endpoints."""

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_check(async_client: AsyncClient) -> None:
    """Verifies that health check endpoint returns 200 OK."""
    response = await async_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == "async-task-engine"


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
