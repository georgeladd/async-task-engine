"""Unit tests for the official TaskEngineClient SDK."""

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
import pytest

from src.client import TaskEngineClient
from src.schemas import TaskStatus


@pytest.mark.asyncio
async def test_client_dispatch_task_success() -> None:
    """Verifies that dispatch sends correctly formatted JSON and parses TaskResponse."""
    task_uuid = str(uuid4())
    mock_response = httpx.Response(
        status_code=202,
        json={
            "task_id": task_uuid,
            "status": "pending",
            "message": "Task queued successfully",
            "is_duplicate": False,
        },
        request=httpx.Request("POST", "http://localhost:8000/api/v1/tasks"),
    )

    async with TaskEngineClient("http://localhost:8000") as client:
        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response

            res = await client.dispatch(
                task_type="inventory_sync",
                resource_id="wh_spb",
                items=[{"sku": "A1"}],
                idempotency_key="key-123",
            )

            assert str(res.task_id) == task_uuid
            assert res.status == TaskStatus.PENDING
            assert res.is_duplicate is False

            mock_post.assert_called_once()
            called_headers = mock_post.call_args[1]["headers"]
            assert called_headers["Idempotency-Key"] == "key-123"


@pytest.mark.asyncio
async def test_client_dispatch_raises_on_server_error() -> None:
    """Verifies that dispatch raises RuntimeError on non-2xx status."""
    mock_response = httpx.Response(
        status_code=422,
        text="Validation error",
        request=httpx.Request("POST", "http://localhost:8000/api/v1/tasks"),
    )

    async with TaskEngineClient("http://localhost:8000") as client:
        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_response

            with pytest.raises(RuntimeError) as exc_info:
                await client.dispatch(
                    task_type="bad_task",
                    resource_id="res_1",
                    items=[],
                )
            assert "Task dispatch failed with status 422" in str(exc_info.value)


@pytest.mark.asyncio
async def test_client_get_status_success() -> None:
    """Verifies querying task execution state via get_status."""
    task_uuid = str(uuid4())
    mock_response = httpx.Response(
        status_code=200,
        json={
            "task_id": task_uuid,
            "status": "completed",
            "result": {
                "task_id": task_uuid,
                "status": "completed",
                "processed_count": 50,
                "chunk_count": 1,
                "execution_time_seconds": 0.05,
            },
        },
        request=httpx.Request("GET", f"http://localhost:8000/api/v1/tasks/{task_uuid}"),
    )

    async with TaskEngineClient("http://localhost:8000") as client:
        with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            status_res = await client.get_status(task_uuid)
            assert str(status_res.task_id) == task_uuid
            assert status_res.status == TaskStatus.COMPLETED
            assert status_res.result is not None
            assert status_res.result.processed_count == 50


@pytest.mark.asyncio
async def test_client_wait_completion_success() -> None:
    """Verifies wait_completion polling until task is completed."""
    task_uuid = str(uuid4())
    pending_resp = httpx.Response(
        status_code=200,
        json={"task_id": task_uuid, "status": "running", "result": None},
        request=httpx.Request("GET", f"http://localhost:8000/api/v1/tasks/{task_uuid}"),
    )
    completed_resp = httpx.Response(
        status_code=200,
        json={
            "task_id": task_uuid,
            "status": "completed",
            "result": {
                "task_id": task_uuid,
                "status": "completed",
                "processed_count": 10,
                "chunk_count": 1,
                "execution_time_seconds": 0.02,
            },
        },
        request=httpx.Request("GET", f"http://localhost:8000/api/v1/tasks/{task_uuid}"),
    )

    async with TaskEngineClient("http://localhost:8000") as client:
        with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = [pending_resp, completed_resp]
            final_res = await client.wait_completion(task_uuid, poll_interval=0.01, timeout=1.0)
            assert final_res.status == TaskStatus.COMPLETED
            assert mock_get.call_count == 2


@pytest.mark.asyncio
async def test_client_wait_completion_timeout() -> None:
    """Verifies that wait_completion raises TimeoutError if task does not finish in time."""
    task_uuid = str(uuid4())
    running_resp = httpx.Response(
        status_code=200,
        json={"task_id": task_uuid, "status": "running", "result": None},
        request=httpx.Request("GET", f"http://localhost:8000/api/v1/tasks/{task_uuid}"),
    )

    async with TaskEngineClient("http://localhost:8000") as client:
        with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = running_resp
            with pytest.raises(TimeoutError) as exc_info:
                await client.wait_completion(task_uuid, poll_interval=0.01, timeout=0.05)
            assert f"Task {task_uuid} did not complete within 0.05s" in str(exc_info.value)
