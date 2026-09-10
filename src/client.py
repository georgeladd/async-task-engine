"""Official asynchronous Python client SDK for interacting with Async Task Engine."""

import asyncio
import types
from typing import Any, Self

import httpx

from src.schemas import TaskResponse, TaskStatus, TaskStatusResponse


class TaskEngineClient:
    """High-level asynchronous client for dispatching tasks and querying execution states.

    Example:
        async with TaskEngineClient("http://localhost:8000") as client:
            res = await client.dispatch(
                task_type="inventory_sync",
                resource_id="wh_spb",
                items=[{"sku": "A1", "price": 100}],
            )
            final_status = await client.wait_completion(res.task_id)
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        api_key: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        """Initializes the client instance.

        Args:
            base_url: URL address of the API service.
            api_key: Optional authorization token for protected operations.
            timeout: HTTP request timeout in seconds.
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Lazily creates or returns the underlying HTTPX client."""
        if self._client is None or self._client.is_closed:
            headers: dict[str, str] = {}
            if self.api_key:
                headers["X-Ops-Token"] = self.api_key
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout, headers=headers)
        return self._client

    async def close(self) -> None:
        """Closes the underlying HTTP client session."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self) -> Self:
        await self._get_client()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self.close()

    async def dispatch(
        self,
        task_type: str,
        items: list[dict[str, Any]],
        resource_id: str,
        priority: str = "normal",
        parameters: dict[str, Any] | None = None,
        callback_url: str | None = None,
        idempotency_key: str | None = None,
    ) -> TaskResponse:
        """Submits a new task for asynchronous background execution.

        Args:
            task_type: Registered task type identifier.
            items: Collection of batch records.
            resource_id: Target resource partition key for locking.
            priority: Scheduling priority (low, normal, high, critical).
            parameters: Optional execution parameters.
            callback_url: Optional completion webhook destination.
            idempotency_key: Optional deduplication key.

        Returns:
            TaskResponse containing task_id and queued status.

        Raises:
            RuntimeError: If server responds with an error code.
        """
        client = await self._get_client()
        headers: dict[str, str] = {}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        payload = {
            "task_type": task_type,
            "resource_id": resource_id,
            "priority": priority,
            "payload": {
                "items": items,
                "parameters": parameters or {},
            },
        }
        if callback_url:
            payload["callback_url"] = callback_url
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key

        response = await client.post("/api/v1/tasks", json=payload, headers=headers)
        if not response.is_success:
            raise RuntimeError(f"Task dispatch failed with status {response.status_code}: {response.text}")

        data = response.json()
        result = TaskResponse(**data)
        return result

    async def get_status(self, task_id: str) -> TaskStatusResponse:
        """Queries the current status and metrics of a task.

        Args:
            task_id: UUID of the target task.

        Returns:
            TaskStatusResponse with current execution status and result.

        Raises:
            RuntimeError: If server responds with an error code.
        """
        client = await self._get_client()
        response = await client.get(f"/api/v1/tasks/{task_id}")
        if not response.is_success:
            raise RuntimeError(f"Task status query failed with status {response.status_code}: {response.text}")

        data = response.json()
        result = TaskStatusResponse(**data)
        return result

    async def wait_completion(
        self,
        task_id: str,
        poll_interval: float = 0.5,
        timeout: float = 60.0,
    ) -> TaskStatusResponse:
        """Polls task status until it transitions into terminal status or times out.

        Args:
            task_id: UUID of the task.
            poll_interval: Seconds between subsequent poll queries.
            timeout: Maximum total seconds to wait before raising TimeoutError.

        Returns:
            Terminal TaskStatusResponse.

        Raises:
            TimeoutError: If task does not complete before timeout.
        """
        elapsed: float = 0.0
        final_status: TaskStatusResponse | None = None

        while elapsed < timeout:
            status_res = await self.get_status(task_id)
            if status_res.status in (TaskStatus.COMPLETED, TaskStatus.DEAD_LETTERED):
                final_status = status_res
                break
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        if final_status is None:
            raise TimeoutError(f"Task {task_id} did not complete within {timeout}s")

        return final_status
