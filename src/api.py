"""FastAPI application providing endpoints for task submission and health checks."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, status
from redis.asyncio import Redis

from src.broker import MessageBroker
from src.config import settings
from src.schemas import TaskCreateRequest, TaskMessage, TaskResponse, TaskStatus

logger = logging.getLogger(__name__)

broker = MessageBroker()
redis_client: Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manages application startup and shutdown lifecycle events.

    Initializes RabbitMQ and Redis connections upon startup, and performs clean disconnection.
    """
    global redis_client
    logger.info("Initializing background broker and cache connections...")
    redis_client = aioredis.from_url(
        settings.redis_uri,
        encoding="utf-8",
        decode_responses=True,
    )
    try:
        await broker.connect()
    except Exception as err:  # noqa: BLE001
        logger.warning(f"Could not eagerly connect to RabbitMQ on startup: {err}")

    yield

    logger.info("Cleaning up connections during shutdown...")
    await broker.close()
    if redis_client:
        await redis_client.close()


app = FastAPI(
    title="Async Task Engine API",
    description="Resilient, memory-safe task execution and queueing service for operations automation",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", status_code=status.HTTP_200_OK, tags=["Health"])
async def health_check() -> dict[str, str]:
    """Verifies that API service is healthy and responsive.

    Returns:
        Status summary dictionary.
    """
    return {"status": "ok", "service": settings.app_name}


@app.post(
    "/api/v1/tasks",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Tasks"],
)
async def submit_task(request: TaskCreateRequest) -> TaskResponse:
    """Submits a new operational task for asynchronous background processing.

    Validates incoming payload, wraps in TaskMessage with isolated UUID, and routes to RabbitMQ.

    Args:
        request: Task creation parameters and payload items.

    Returns:
        TaskResponse with generated task_id and queued status.

    Raises:
        HTTPException: If queuing fails.
    """
    task = TaskMessage(
        task_type=request.task_type,
        resource_id=request.resource_id,
        priority=request.priority,
        payload=request.payload,
    )

    try:
        await broker.publish_task(task)

        # Store initial task status in Redis
        if redis_client:
            task_status_key: str = f"task:status:{task.task_id}"
            await redis_client.set(task_status_key, TaskStatus.PENDING.value, ex=86400)

        response = TaskResponse(
            task_id=task.task_id,
            status=TaskStatus.PENDING,
            message="Task enqueued successfully for background processing",
        )
    except Exception as err:
        logger.error(f"Failed to submit task {task.task_id}: {err}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Message broker temporarily unavailable: {err}",
        ) from err

    return response


@app.get(
    "/api/v1/tasks/{task_id}",
    response_model=dict[str, Any],
    tags=["Tasks"],
)
async def get_task_status(task_id: UUID) -> dict[str, Any]:
    """Retrieves current execution status and result metrics for a given task ID.

    Args:
        task_id: UUID of the task.

    Returns:
        Dictionary containing task status and metrics.

    Raises:
        HTTPException: If task is not found.
    """
    if not redis_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cache backend is not initialized",
        )

    task_status_key: str = f"task:status:{task_id}"
    task_result_key: str = f"task:result:{task_id}"

    current_status: str | None = await redis_client.get(task_status_key)
    if not current_status:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found",
        )

    result_data: str | None = await redis_client.get(task_result_key)
    response_data: dict[str, Any] = {
        "task_id": str(task_id),
        "status": current_status,
        "result": result_data,
    }
    return response_data
