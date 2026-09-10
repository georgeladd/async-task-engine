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

    async def mock_incr(key: str) -> int:
        val = int(storage.get(key, 0)) + 1
        storage[key] = val
        return val

    async def mock_incrby(key: str, amount: int) -> int:
        val = int(storage.get(key, 0)) + amount
        storage[key] = val
        return val

    async def mock_incrbyfloat(key: str, amount: float) -> float:
        val = float(storage.get(key, 0.0)) + float(amount)
        storage[key] = val
        return val

    async def mock_decr(key: str) -> int:
        val = max(0, int(storage.get(key, 0)) - 1)
        storage[key] = val
        return val

    async def mock_delete(*keys: str) -> int:
        deleted = 0
        for k in keys:
            if k in storage:
                storage.pop(k, None)
                deleted += 1
        return deleted

    async def mock_expire(key: str, seconds: int) -> bool:
        return key in storage

    async def mock_scan_iter(match: str | None = None, count: int | None = None) -> AsyncGenerator[str, None]:
        import fnmatch

        pattern = match or "*"
        for k in list(storage.keys()):
            if fnmatch.fnmatch(k, pattern):
                yield k

    async def mock_keys(pattern: str = "*") -> list[str]:
        import fnmatch

        return [k for k in storage if fnmatch.fnmatch(k, pattern)]

    client.set = AsyncMock(side_effect=mock_set)
    client.get = AsyncMock(side_effect=mock_get)
    client.eval = AsyncMock(side_effect=mock_eval)
    client.incr = AsyncMock(side_effect=mock_incr)
    client.incrby = AsyncMock(side_effect=mock_incrby)
    client.incrbyfloat = AsyncMock(side_effect=mock_incrbyfloat)
    client.decr = AsyncMock(side_effect=mock_decr)
    client.delete = AsyncMock(side_effect=mock_delete)
    client.expire = AsyncMock(side_effect=mock_expire)
    client.scan_iter = mock_scan_iter
    client.keys = AsyncMock(side_effect=mock_keys)
    client.close = AsyncMock()
    return client


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Fixture providing an async HTTP client for API endpoints."""
    transport = ASGITransport(app=app)
    headers = {"X-Ops-Token": "ops-dev-secret"}
    async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as client:
        yield client
