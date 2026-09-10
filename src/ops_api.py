"""Operations and support API router for dashboard telemetry and incident management."""

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.config import settings
from src.metrics import (
    ACTIVE_WORKER_TASKS,
    DEAD_LETTER_TASKS_TOTAL,
    ITEMS_PROCESSED_TOTAL,
    TASK_DURATION_SECONDS,
    TASKS_COMPLETED_TOTAL,
    TASKS_SUBMITTED_TOTAL,
)
from src.schemas import TaskMessage, TaskPriority, TaskStatus

logger = logging.getLogger(__name__)

ops_api_key_header = APIKeyHeader(name="X-Ops-Token", auto_error=False)


async def verify_ops_token(
    token: str | None = Security(ops_api_key_header),
) -> str:
    """Validates operational API key for access control to sensitive endpoints.

    Args:
        token: Provided API token from X-Ops-Token header.

    Returns:
        Validated token string.

    Raises:
        HTTPException: If token is missing or invalid.
    """
    if not token or token != settings.ops_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing operational API key",
        )
    return token


router = APIRouter(
    prefix="/api/v1/ops",
    tags=["Operations"],
    dependencies=[Depends(verify_ops_token)],
)


class OpsOverview(BaseModel):
    """Aggregated operational health and metrics overview."""

    system_status: str = Field(default="healthy", description="Overall health state")
    queue_primary_depth: int = Field(default=0, description="Tasks awaiting execution in primary queue")
    queue_dlq_depth: int = Field(default=0, description="Tasks currently inside Dead-Letter Queue")
    active_workers: int = Field(default=0, description="Active tasks executing right now")
    tasks_submitted_total: int = Field(default=0, description="Total tasks accepted by API")
    tasks_completed_total: int = Field(default=0, description="Total successfully finished tasks")
    tasks_dead_letter_total: int = Field(default=0, description="Total tasks routed to DLQ")
    items_processed_total: int = Field(default=0, description="Total individual data records processed")
    avg_duration_seconds: float = Field(default=0.0, description="Average batch processing time")
    active_locks_count: int = Field(default=0, description="Active distributed resource locks count")


class LockItem(BaseModel):
    """Information about an active Redis distributed lock."""

    resource_id: str = Field(..., description="Target protected resource key")
    lock_key: str = Field(..., description="Full Redis lock key")
    ttl_remaining: int = Field(..., description="Remaining seconds before auto-expiry")
    owner_token: str = Field(..., description="Unique ownership token")


class UnlockRequest(BaseModel):
    """Request schema for manually releasing a distributed lock."""

    resource_id: str = Field(..., description="Target resource key to release")


class DLQItem(BaseModel):
    """Dead-Letter Queue incident task item for inspection."""

    task_id: str = Field(..., description="Unique task identifier")
    task_type: str = Field(..., description="Category of the failing task")
    resource_id: str = Field(..., description="Guarded resource key")
    status: str = Field(..., description="Task lifecycle status")
    error_reason: str = Field(..., description="Diagnostic error traceback")
    updated_at: str = Field(..., description="Timestamp of dead-letter routing")


class DLQReplayRequest(BaseModel):
    """Request schema for replaying a failed task from DLQ back into primary queue."""

    task_id: str = Field(..., description="Task UUID to requeue")


class EscalateRequest(BaseModel):
    """Request schema for generating an incident package for L3/Development."""

    task_id: str = Field(..., description="Failing task UUID")
    operator_comment: str | None = Field(default=None, description="Observations from on-call engineer")


class EscalateResponse(BaseModel):
    """Generated incident report and dossier response."""

    incident_id: str = Field(..., description="Unique incident identifier")
    status: str = Field(default="escalated", description="Incident triage status")
    markdown_dossier: str = Field(..., description="Formatted Markdown dossier for Jira/Slack")


def _get_redis_client() -> Redis:
    """Creates an asynchronous Redis connection instance.

    Returns:
        Redis client instance.
    """
    return aioredis.from_url(
        settings.redis_uri,
        encoding="utf-8",
        decode_responses=True,
    )


@router.get("/overview", response_model=OpsOverview)
async def get_ops_overview() -> OpsOverview:
    """Aggregates real-time health and telemetry metrics across components.

    Collects data from Prometheus instruments and Redis state cache.

    Returns:
        OpsOverview schema with system counters and status.
    """
    redis = _get_redis_client()
    active_locks_count: int = 0

    r_submitted: int = 0
    r_completed: int = 0
    r_dlq: int = 0
    r_items: int = 0
    r_active: int = 0
    r_dur_sum: float = 0.0
    r_dur_cnt: int = 0
    try:
        # Count active distributed locks using non-blocking SCAN iteration
        async for _ in redis.scan_iter(match="lock:resource:*", count=100):
            active_locks_count += 1

        # Read distributed cluster-wide metrics from Redis directly (O(1) lookups)
        r_submitted = int(await redis.get("metrics:tasks_submitted") or 0)
        r_completed = int(await redis.get("metrics:tasks_completed") or 0)
        r_dlq = int(await redis.get("metrics:tasks_dead_letter") or 0)
        r_items = int(await redis.get("metrics:items_processed") or 0)
        r_active = max(0, int(await redis.get("metrics:active_workers") or 0))
        r_dur_sum = float(await redis.get("metrics:duration_sum") or 0.0)
        r_dur_cnt = int(await redis.get("metrics:duration_count") or 0)
    except (RedisError, OSError) as err:
        logger.warning(f"Error querying Redis state during overview aggregation: {err}")
    finally:
        await redis.close()

    # Prometheus telemetry calculations
    prom_active: int = int(ACTIVE_WORKER_TASKS._value.get())
    prom_submitted: int = int(sum(sample.value for sample in TASKS_SUBMITTED_TOTAL.collect()[0].samples))
    prom_completed: int = int(sum(sample.value for sample in TASKS_COMPLETED_TOTAL.collect()[0].samples))
    prom_dlq: int = int(sum(sample.value for sample in DEAD_LETTER_TASKS_TOTAL.collect()[0].samples))
    prom_items: int = int(sum(sample.value for sample in ITEMS_PROCESSED_TOTAL.collect()[0].samples))

    # Duration average
    duration_samples = TASK_DURATION_SECONDS.collect()[0].samples
    duration_sum: float = 0.0
    duration_count: float = 0.0
    for s in duration_samples:
        if s.name.endswith("_sum"):
            duration_sum += s.value
        elif s.name.endswith("_count"):
            duration_count += s.value

    # Prioritize distributed cluster-wide Redis counters; fallback to in-memory Prometheus
    submitted: int = r_submitted if r_submitted > 0 else prom_submitted
    completed: int = r_completed if r_completed > 0 else prom_completed
    dlq_metric: int = r_dlq if r_dlq > 0 else prom_dlq
    items_metric: int = r_items if r_items > 0 else prom_items
    active_workers: int = r_active if r_active > 0 else prom_active

    if r_dur_cnt > 0:
        avg_duration: float = round(r_dur_sum / r_dur_cnt, 3)
    elif duration_count > 0:
        avg_duration = round(duration_sum / duration_count, 3)
    else:
        avg_duration = 0.0

    sys_status: str = "degraded" if dlq_metric > 0 else "healthy"

    overview = OpsOverview(
        system_status=sys_status,
        queue_primary_depth=max(0, submitted - completed - dlq_metric),
        queue_dlq_depth=dlq_metric,
        active_workers=active_workers,
        tasks_submitted_total=submitted,
        tasks_completed_total=completed,
        tasks_dead_letter_total=dlq_metric,
        items_processed_total=items_metric,
        avg_duration_seconds=avg_duration,
        active_locks_count=active_locks_count,
    )
    return overview


@router.get("/locks", response_model=list[LockItem])
async def list_active_locks() -> list[LockItem]:
    """Retrieves all active Redis distributed resource locks.

    Returns:
        List of active LockItem instances with remaining TTL.
    """
    redis = _get_redis_client()
    locks: list[LockItem] = []

    try:
        async for key in redis.scan_iter(match="lock:resource:*", count=100):
            ttl = await redis.ttl(key)
            token = await redis.get(key)
            resource_name = key.replace("lock:resource:", "")
            locks.append(
                LockItem(
                    resource_id=resource_name,
                    lock_key=key,
                    ttl_remaining=max(0, ttl),
                    owner_token=str(token or "unknown"),
                )
            )
            if len(locks) >= 200:
                break
    except Exception as err:
        logger.error(f"Failed to query distributed locks: {err}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lock retrieval failed: {err}",
        ) from err
    finally:
        await redis.close()

    return locks


@router.post("/unlock")
async def force_unlock_resource(request: UnlockRequest) -> dict[str, Any]:
    """Manually releases an orphaned distributed lock key in Redis.

    Args:
        request: Target resource identifier.

    Returns:
        Status summary confirming unlock action.
    """
    redis = _get_redis_client()
    lock_key = f"lock:resource:{request.resource_id}"
    response: dict[str, Any] = {"resource_id": request.resource_id}

    try:
        exists = await redis.exists(lock_key)
        if not exists:
            response["status"] = "not_locked"
            response["message"] = f"Resource '{request.resource_id}' was not locked"
        else:
            ttl = await redis.ttl(lock_key)
            await redis.delete(lock_key)
            response["status"] = "unlocked"
            response["message"] = f"Successfully released lock for '{request.resource_id}' (evicted {ttl}s TTL)"
    except Exception as err:
        logger.error(f"Failed to force-unlock resource {request.resource_id}: {err}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Force unlock failed: {err}",
        ) from err
    finally:
        await redis.close()

    return response


@router.get("/dlq", response_model=list[DLQItem])
async def list_dead_letter_tasks() -> list[DLQItem]:
    """Retrieves list of tasks that exhausted retries and were routed to Dead-Letter Queue.

    Returns:
        List of DLQItem records with failure reasons.
    """
    redis = _get_redis_client()
    dlq_items: list[DLQItem] = []

    try:
        async for sk in redis.scan_iter(match="task:status:*", count=100):
            state = await redis.get(sk)
            if state == TaskStatus.DEAD_LETTERED.value:
                task_id = sk.replace("task:status:", "")
                result_key = f"task:result:{task_id}"
                raw_result = await redis.get(result_key)
                error_msg: str = "Max retry attempts exhausted"
                if raw_result:
                    try:
                        parsed = json.loads(raw_result)
                        if parsed.get("error_message"):
                            error_msg = parsed.get("error_message")
                    except (json.JSONDecodeError, TypeError):
                        error_msg = str(raw_result)

                # Attempt to extract original task type and resource from preserved payload
                task_data_raw = await redis.get(f"task:data:{task_id}")
                task_type_val = "batch_operation"
                resource_id_val = f"res_{task_id[:8]}"
                if task_data_raw:
                    try:
                        parsed_data = json.loads(task_data_raw)
                        task_type_val = parsed_data.get("task_type", task_type_val)
                        resource_id_val = parsed_data.get("resource_id", resource_id_val)
                    except (json.JSONDecodeError, TypeError):
                        pass

                dlq_items.append(
                    DLQItem(
                        task_id=task_id,
                        task_type=task_type_val,
                        resource_id=resource_id_val,
                        status=TaskStatus.DEAD_LETTERED.value,
                        error_reason=error_msg,
                        updated_at=datetime.now(timezone.utc).isoformat(),
                    )
                )
                if len(dlq_items) >= 200:
                    break
    except (RedisError, OSError) as err:
        logger.error(f"Failed to list dead-letter tasks: {err}")
    finally:
        await redis.close()

    return dlq_items


@router.post("/dlq/replay")
async def replay_dead_letter_task(request: DLQReplayRequest) -> dict[str, Any]:
    """Requeues a failed task from DLQ back into the primary queue for execution.

    Args:
        request: Target task UUID to replay.

    Returns:
        Confirmation dictionary with re-enqueued status.
    """
    from src.api import broker

    redis = _get_redis_client()
    status_key = f"task:status:{request.task_id}"

    try:
        # Read preserved original task payload if available
        task_data_raw = await redis.get(f"task:data:{request.task_id}")
        if task_data_raw:
            try:
                task = TaskMessage.model_validate_json(task_data_raw)
                task.priority = TaskPriority.HIGH
                task.attempts = 0
            except (ValueError, TypeError, KeyError) as parse_err:
                logger.warning(f"Could not parse preserved task data for {request.task_id}: {parse_err}")
                task = TaskMessage(
                    task_id=UUID(request.task_id),
                    task_type="replayed_operation",
                    resource_id=f"replay_{request.task_id[:8]}",
                    priority=TaskPriority.HIGH,
                    attempts=0,
                )
        else:
            task = TaskMessage(
                task_id=UUID(request.task_id),
                task_type="replayed_operation",
                resource_id=f"replay_{request.task_id[:8]}",
                priority=TaskPriority.HIGH,
                attempts=0,
            )

        # Clear previous retry failure count, reset status to pending, and decrement DLQ counter
        await redis.delete(f"task:attempts:{request.task_id}")
        await redis.set(status_key, TaskStatus.PENDING.value, ex=86400)
        current_dlq_cnt = int(await redis.get("metrics:tasks_dead_letter") or 0)
        if current_dlq_cnt > 0:
            await redis.decr("metrics:tasks_dead_letter")
        await broker.publish_task(task)

        logger.info(f"Task {request.task_id} replayed from DLQ into primary queue")
        result = {
            "task_id": request.task_id,
            "status": "requeued",
            "message": "Task re-enqueued into primary queue with high priority",
        }
    except Exception as err:
        logger.error(f"Failed to replay task {request.task_id}: {err}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Task replay failed: {err}",
        ) from err
    finally:
        await redis.close()

    return result


@router.post("/escalate", response_model=EscalateResponse)
async def escalate_task_incident(request: EscalateRequest) -> EscalateResponse:
    """Compiles an incident dossier with runtime context and diagnostics for L3/Dev escalations.

    Args:
        request: Target task UUID and optional operator notes.

    Returns:
        EscalateResponse containing unique incident ID and Markdown report.
    """
    redis = _get_redis_client()
    incident_id: str = f"INC-{str(uuid4())[:8].upper()}"
    status_key = f"task:status:{request.task_id}"
    result_key = f"task:result:{request.task_id}"

    current_status: str = "UNKNOWN / UNREGISTERED"
    error_detail: str = "No recorded error details"
    try:
        raw_status = await redis.get(status_key)
        if raw_status:
            current_status = raw_status.upper()

        raw_result = await redis.get(result_key)
        if raw_result:
            error_detail = raw_result
    except (RedisError, OSError) as err:
        logger.warning(f"Failed to query Redis context for incident {request.task_id}: {err}")
    finally:
        await redis.close()

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    operator_notes = request.operator_comment or "No additional notes provided by operator"

    if current_status == "RUNNING":
        action_recommendation = (
            "1. Inspect worker health and verify whether distributed lock is stalled\n"
            "2. Review thread activity and connection pool saturation in Grafana"
        )
    elif current_status in ("DEAD_LETTERED", "FAILED"):
        action_recommendation = (
            "1. Inspect payload format against active database schemas\n"
            "2. Verify downstream database connection pool saturation\n"
            "3. After patch deployment, execute replay via POST /api/v1/ops/dlq/replay"
        )
    elif current_status == "PENDING":
        action_recommendation = (
            "1. Verify RabbitMQ consumer queue bindings and active worker count\n"
            "2. Check if primary queue consumer is experiencing backlog throttling"
        )
    else:
        action_recommendation = (
            "1. Verify task UUID correctness with customer\n"
            "2. Inspect Redis TTL logs or archived log stream in Loki/Elasticsearch"
        )

    dossier: str = f"""### Incident Report: {incident_id}
**Service:** {settings.app_name}
**Timestamp:** {timestamp}
**Target Task UUID:** `{request.task_id}`
**Current Status:** `{current_status}`

#### Diagnostic Context
- **Environment:** `{settings.environment}`
- **RabbitMQ Main Queue:** `{settings.rabbitmq_main_queue}`
- **Dead-Letter Queue:** `{settings.rabbitmq_dlq_queue}`
- **Runtime Diagnostics:**
```text
{error_detail}
```

#### Operator Observations (L2 Support)
> {operator_notes}

#### Recommended Immediate Actions (L3 / Dev)
{action_recommendation}
"""

    return EscalateResponse(
        incident_id=incident_id,
        status="escalated",
        markdown_dossier=dossier.strip(),
    )
