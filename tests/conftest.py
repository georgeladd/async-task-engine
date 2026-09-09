"""Pytest test fixtures and configuration."""

from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.api import app
from src.schemas import TaskCreateRequest, TaskPayload, TaskPriority


@pytest.fixture
def sample_task_request() -> TaskCreateRequest:
    """Fixture providing a standard task creation request."""
    return TaskCreateRequest(
        task_type="inventory_sync",
        resource_id="server_cluster_alpha",
        priority=TaskPriority.HIGH,
        payload=TaskPayload(
            items=[{"id": idx, "name": f"item_{idx}"} for idx in range(250)],
            parameters={"dry_run": False},
        ),
    )


@pytest.fixture
def mock_redis() -> AsyncMock:
    """Fixture creating a simulated asynchronous Redis client."""
    client = AsyncMock()
    # Simulate internal storage dictionary
    storage: dict[str, Any] = {}

    async def mock_set(key: str, val: Any, nx: bool = False, ex: int | None = None) -> bool:
        if nx and key in storage:
            return False
        storage[key] = val
        return True

    async def mock_get(key: str) -> Any:
        return storage.get(key)

    async def mock_eval(script: str, numkeys: int, *keys_and_args: Any) -> int:
        key = keys_and_args[0]
        token = keys_and_args[1]
        if storage.get(key) == token:
            storage.pop(key, None)
            return 1
        return 0

    client.set = AsyncMock(side_effect=mock_set)
    client.get = AsyncMock(side_effect=mock_get)
    client.eval = AsyncMock(side_effect=mock_eval)
    client.close = AsyncMock()
    return client


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Fixture providing an async HTTP client for API endpoints."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
