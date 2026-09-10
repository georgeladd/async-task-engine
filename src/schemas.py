"""Data schemas and transfer models for task operations."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, HttpUrl


class TaskPriority(str, Enum):
    """Priority levels for task scheduling."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class TaskStatus(str, Enum):
    """Lifecycle statuses of an asynchronous task."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"


class TaskPayload(BaseModel):
    """Payload definition representing data items to be processed.

    Attributes:
        items: List of raw items to stream and process in chunks.
        parameters: Arbitrary task parameters and flags.
    """

    items: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Collection of records to process",
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional execution parameters",
    )


class TaskCreateRequest(BaseModel):
    """API request schema for scheduling a new operational task.

    Attributes:
        task_type: Identifier of the business logic handler to execute.
        resource_id: Identifier of the resource being modified (used for locking).
        priority: Scheduling priority for the queue.
        payload: Task data payload.
    """

    task_type: str = Field(..., min_length=2, max_length=100, description="Task category")
    resource_id: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Target resource key for distributed locking",
    )
    priority: TaskPriority = Field(default=TaskPriority.NORMAL, description="Queue priority")
    payload: TaskPayload = Field(default_factory=TaskPayload, description="Task data")
    idempotency_key: str | None = Field(
        default=None,
        max_length=128,
        description="Optional unique idempotency token to prevent duplicate task execution",
    )
    callback_url: HttpUrl | None = Field(
        default=None,
        description="Optional webhook HTTP/HTTPS callback destination",
    )


class TaskMessage(BaseModel):
    """Internal message format routed through message queues.

    Attributes:
        task_id: Globally unique task identifier.
        task_type: Category of the task handler.
        resource_id: Target resource key for distributed locking.
        priority: Assigned priority level.
        payload: Payload records and arguments.
        attempts: Number of times this task was attempted.
        callback_url: Webhook destination URL if notifications requested.
        created_at: ISO timestamp of task generation.
    """

    task_id: UUID = Field(default_factory=uuid4, description="Unique task UUID")
    task_type: str = Field(..., description="Task category")
    resource_id: str = Field(..., description="Target resource key")
    priority: TaskPriority = Field(default=TaskPriority.NORMAL, description="Queue priority")
    payload: TaskPayload = Field(default_factory=TaskPayload, description="Task data")
    attempts: int = Field(default=0, ge=0, description="Execution attempt counter")
    callback_url: HttpUrl | None = Field(
        default=None,
        description="Optional webhook HTTP/HTTPS callback destination",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Creation timestamp",
    )


class TaskResponse(BaseModel):
    """API response schema after task submission.

    Attributes:
        task_id: Assigned UUID.
        status: Initial task status.
        enqueued_at: Timestamp when task was placed into queue.
        message: Descriptive status message.
    """

    task_id: UUID = Field(..., description="Task UUID")
    status: TaskStatus = Field(default=TaskStatus.PENDING, description="Current status")
    is_duplicate: bool = Field(default=False, description="True if response is a cached replay")
    enqueued_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    message: str = Field(..., description="User-facing summary message")


class TaskResult(BaseModel):
    """Final output metrics of a finished task.

    Attributes:
        task_id: Task UUID.
        status: Final status.
        processed_count: Total processed items count.
        chunk_count: Total chunks processed.
        execution_time_seconds: Total processing duration.
        error_message: Optional error message if execution failed.
    """

    task_id: UUID = Field(..., description="Task UUID")
    status: TaskStatus = Field(..., description="Completion status")
    processed_count: int = Field(default=0, ge=0, description="Items processed count")
    chunk_count: int = Field(default=0, ge=0, description="Chunks count")
    execution_time_seconds: float = Field(default=0.0, ge=0.0, description="Duration in seconds")
    error_message: str | None = Field(default=None, description="Failure details if any")


class WebhookDeliveryPayload(BaseModel):
    """Schema for asynchronous webhook HTTP notifications dispatched upon task resolution."""

    event: str = Field(..., description="Event identifier, e.g. task.completed or task.dead_lettered")
    task_id: UUID = Field(..., description="Task UUID")
    task_type: str = Field(..., description="Task category")
    resource_id: str = Field(..., description="Target resource key")
    status: TaskStatus = Field(..., description="Final execution status")
    result: TaskResult | None = Field(default=None, description="Task execution metrics")
    delivered_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Webhook transmission timestamp",
    )
