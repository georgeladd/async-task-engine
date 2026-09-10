"""Unit and integration tests for task submission idempotency."""

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_submit_task_without_idempotency_key(
    async_client: AsyncClient,
    mock_redis: AsyncMock,
) -> None:
    """Verifies that requests without idempotency keys create unique tasks."""
    payload = {
        "task_type": "unique_sync",
        "resource_id": "account_101",
        "payload": {"items": [{"id": 1}]},
    }
    with (
        patch("src.api.broker.publish_task", new_callable=AsyncMock) as mock_publish,
        patch("src.api.redis_client", mock_redis),
    ):
        res = await async_client.post("/api/v1/tasks", json=payload)
        assert res.status_code == 202
        data = res.json()
        assert data["is_duplicate"] is False
        mock_publish.assert_called_once()


@pytest.mark.asyncio
async def test_submit_task_with_idempotency_header_deduplication(
    async_client: AsyncClient,
    mock_redis: AsyncMock,
) -> None:
    """Verifies that duplicate requests with Idempotency-Key header return cached task without re-queuing."""
    payload = {
        "task_type": "payment_export",
        "resource_id": "billing_999",
        "payload": {"items": [{"id": 100}]},
    }
    idempotency_key = f"idempotency-{uuid4()}"
    headers = {"Idempotency-Key": idempotency_key}

    # Simulate in-memory Redis cache for the test
    redis_store: dict[str, str] = {}

    async def mock_get(key: str) -> str | None:
        return redis_store.get(key)

    async def mock_set(key: str, value: str, **kwargs) -> bool:
        if kwargs.get("nx") and key in redis_store:
            return False
        redis_store[key] = value
        return True

    mock_redis.get = AsyncMock(side_effect=mock_get)
    mock_redis.set = AsyncMock(side_effect=mock_set)

    with (
        patch("src.api.broker.publish_task", new_callable=AsyncMock) as mock_publish,
        patch("src.api.redis_client", mock_redis),
    ):
        # First submission
        res1 = await async_client.post("/api/v1/tasks", json=payload, headers=headers)
        assert res1.status_code == 202
        data1 = res1.json()
        first_task_id = data1["task_id"]
        assert data1["is_duplicate"] is False
        assert mock_publish.call_count == 1

        # Second duplicate submission with same header
        res2 = await async_client.post("/api/v1/tasks", json=payload, headers=headers)
        assert res2.status_code == 202
        data2 = res2.json()
        assert data2["task_id"] == first_task_id
        assert data2["is_duplicate"] is True
        assert "idempotent replay" in data2["message"]
        # Broker MUST NOT be called a second time
        assert mock_publish.call_count == 1


@pytest.mark.asyncio
async def test_submit_task_with_idempotency_body_field(
    async_client: AsyncClient,
    mock_redis: AsyncMock,
) -> None:
    """Verifies that idempotency_key field inside request JSON body behaves identically."""
    idempotency_key = f"idem-body-{uuid4()}"
    payload = {
        "task_type": "inventory_reconciliation",
        "resource_id": "warehouse_east",
        "idempotency_key": idempotency_key,
        "payload": {"items": []},
    }

    redis_store: dict[str, str] = {}

    async def mock_get(key: str) -> str | None:
        return redis_store.get(key)

    async def mock_set(key: str, value: str, **kwargs) -> bool:
        if kwargs.get("nx") and key in redis_store:
            return False
        redis_store[key] = value
        return True

    mock_redis.get = AsyncMock(side_effect=mock_get)
    mock_redis.set = AsyncMock(side_effect=mock_set)

    with (
        patch("src.api.broker.publish_task", new_callable=AsyncMock) as mock_publish,
        patch("src.api.redis_client", mock_redis),
    ):
        res1 = await async_client.post("/api/v1/tasks", json=payload)
        assert res1.status_code == 202
        task_id = res1.json()["task_id"]
        assert res1.json()["is_duplicate"] is False

        # Repeat submission
        res2 = await async_client.post("/api/v1/tasks", json=payload)
        assert res2.status_code == 202
        assert res2.json()["task_id"] == task_id
        assert res2.json()["is_duplicate"] is True
        assert mock_publish.call_count == 1


@pytest.mark.asyncio
async def test_idempotency_returns_actual_completed_status(
    async_client: AsyncClient,
    mock_redis: AsyncMock,
) -> None:
    """Verifies that idempotent replay reflects COMPLETED status if task has already finished."""
    idempotency_key = f"idem-status-{uuid4()}"
    task_uuid = str(uuid4())
    payload = {
        "task_type": "report_gen",
        "resource_id": "analytics_shard_1",
        "payload": {"items": []},
    }
    headers = {"Idempotency-Key": idempotency_key}

    # Simulate already executed and completed task in Redis
    redis_store: dict[str, str] = {
        f"idempotency:{idempotency_key}": task_uuid,
        f"task:status:{task_uuid}": "completed",
    }

    async def mock_get(key: str) -> str | None:
        return redis_store.get(key)

    async def mock_set(key: str, value: str, **kwargs) -> bool:
        if kwargs.get("nx") and key in redis_store:
            return False
        redis_store[key] = value
        return True

    mock_redis.get = AsyncMock(side_effect=mock_get)
    mock_redis.set = AsyncMock(side_effect=mock_set)

    with (
        patch("src.api.broker.publish_task", new_callable=AsyncMock) as mock_publish,
        patch("src.api.redis_client", mock_redis),
    ):
        res = await async_client.post("/api/v1/tasks", json=payload, headers=headers)
        assert res.status_code == 202
        data = res.json()
        assert data["task_id"] == task_uuid
        assert data["is_duplicate"] is True
        assert data["status"] == "completed"
        mock_publish.assert_not_called()
