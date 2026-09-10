# Developer Integration Guide

🌐 **[English](DEVELOPER_GUIDE.md)** • **[Русский](DEVELOPER_GUIDE_RU.md)**

This guide walks through integrating custom business logic and batch workloads into **Async Task Engine**: from architecture contracts and message lifecycle to registering task handlers with databases, third-party APIs, and queues

---

## 1. End-to-End Task Lifecycle

The engine decouples transport, queuing, and operational supervision from domain business logic:

```
[ Client / API Caller ]
       │  POST /api/v1/tasks (with Idempotency-Key header)
       ▼
[ FastAPI Producer ] ──(SET NX)──► [ Redis Cache ] (Deduplication Check)
       │
       ▼ (AMQP Publish)
[ RabbitMQ Exchange ]
       │
       ▼ (Primary tasks_primary queue)
[ Worker Consumer ] ──(SET NX)──► [ Redis Lock ] (Guards resource_id)
       │
       ▼
 [ Chunker / Rate Limiter ] ──► Invokes @task_handler(chunk, params)
       │
       ├─► Success: Redis COMPLETED + ACK + outgoing Webhook
       └─► Error: Retry Counter -> Retry (nack) or DLQ (reject)
```

1. **Submission:** Downstream microservices send a JSON payload to `POST /api/v1/tasks`
2. **Atomic Deduplication:** Redis claims the `Idempotency-Key` using `SET NX`. Repeated clicks return the original `task_id` with `is_duplicate: true` instantly without queue dispatch
3. **Context Preservation:** Full task payload is recorded in `task:data:{task_id}` with a 7-day TTL, allowing operators to trigger zero-loss Replays from the dashboard
4. **Queue Dispatch:** Message publishes to RabbitMQ under the designated priority level
5. **Resource Locking:** The worker acquires a distributed Redis lock on `resource_id`. If busy, it performs an asynchronous backoff and requeues the task
6. **Streaming Execution:** The generator `chunk_iterator` splits `payload.items` into bounded slices and streams them into the registered handler under Token Bucket rate limits
7. **Finalization:** On completion, the worker acknowledges (ACK) the message, updates state, and fires an HMAC-signed webhook to `callback_url`

---

## 2. Handler Registry Architecture (Strategy Pattern)

To uphold the Open-Closed Principle (SOLID), task execution avoids monolithic `if/elif/else` ladders or hardcoded dispatchers

### Creating the Registry

Create `src/handlers.py` (or a `src/handlers/` module package):

```python
"""Application business logic handlers registry."""

from collections.abc import Callable, Coroutine
from typing import Any

TaskHandler = Callable[[list[dict[str, Any]], dict[str, Any]], Coroutine[Any, Any, None]]

HANDLERS: dict[str, TaskHandler] = {}


def task_handler(task_type: str):
    """Decorator to register business logic handlers in the engine registry."""
    def decorator(func: TaskHandler) -> TaskHandler:
        HANDLERS[task_type] = func
        return func
    return decorator


def get_handler(task_type: str) -> TaskHandler:
    """Retrieves registered task handler or raises an informative ValueError."""
    handler = HANDLERS.get(task_type)
    if not handler:
        raise ValueError(f"No handler registered for task_type '{task_type}'")
    return handler
```

---

## 3. Practical Implementation Examples

### Scenario A: Bulk-Upsert in PostgreSQL

**Use Case:** Ingesting 50,000 inventory items without table locking or connection pool starvation

```python
# src/handlers/inventory.py
import asyncpg
from typing import Any
from src.handlers import task_handler

db_pool: asyncpg.Pool | None = None


@task_handler("inventory_sync")
async def handle_inventory_sync(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Performs batched relational database upserts."""
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(
            dsn="postgresql://app_user:secret@postgres:5432/app_db",
            min_size=2,
            max_size=10,
        )

    records = [
        (item["sku"], item["warehouse_id"], item["quantity"], item["price"])
        for item in chunk
    ]

    upsert_query = """
        INSERT INTO inventory_stocks (sku, warehouse_id, quantity, price, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (sku, warehouse_id) DO UPDATE
        SET quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            updated_at = NOW();
    """

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(upsert_query, records)
```

---

### Scenario B: Rate-Limited Outgoing API Integration

**Use Case:** Transmitting 10,000 payments to a payment gateway enforcing a strict 50 RPS limit

```python
# src/handlers/payments.py
import httpx
from typing import Any
from src.handlers import task_handler


@task_handler("payment_gateway_sync")
async def handle_payment_sync(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Dispatches batched payments to a third-party gateway."""
    gateway_url = parameters.get("gateway_url", "https://api.payments.internal/v2/batch")

    payload = {
        "batch_id": parameters.get("batch_reference"),
        "transactions": chunk,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            gateway_url,
            json=payload,
            headers={"Authorization": "Bearer secure-gateway-key"},
        )

        if response.status_code == 429:
            raise ConnectionError("Gateway rate limit exceeded, backing off task")

        if not response.is_success:
            raise RuntimeError(f"Payment gateway rejected batch: HTTP {response.status_code}")
```

---

### Scenario C: Report Generation with Live Redis Progress Tracking

**Use Case:** Long-running customer exports with real-time percentage progress displayed in the support console

```python
# src/handlers/reporting.py
import redis.asyncio as aioredis
from typing import Any
from src.handlers import task_handler


@task_handler("customer_report_export")
async def handle_report_export(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Streams export generation with real-time Redis progress updates."""
    redis = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    task_id = parameters.get("task_id")
    total_items = parameters.get("total_items", 1)

    try:
        for item in chunk:
            # Transform record and write to temporary export buffer
            pass

        if task_id:
            processed = await redis.incrby(f"task:progress:count:{task_id}", len(chunk))
            percent = min(100, round((processed / total_items) * 100, 1))
            await redis.set(f"task:progress:percent:{task_id}", str(percent), ex=3600)
    finally:
        await redis.close()
```

---

## 4. Connecting Handlers to Worker Core

Inside [src/worker.py](file:///home/x/Projects/resume/async-task-engine/src/worker.py), hook into `process_task_payload`:

```python
from src.handlers import get_handler

# Inside TaskWorker.process_task_payload:
handler = get_handler(task.task_type)

for chunk in chunk_iterator(items, chunk_size):
    await self.rate_limiter.acquire()
    await handler(chunk, task.payload.parameters)
    total_processed += len(chunk)
```

---

## 5. Error Handling & Fault-Tolerance Contract

The engine differentiates between **Transient Errors** and **Unrecoverable Errors**:

| Error Category | Examples | Engine Reaction |
|---|---|---|
| **Transient Error** | Network timeouts, HTTP 429/503 from partner APIs, DB deadlocks | Worker increments `task:attempts:{id}`, sets status to `PENDING`, and triggers `nack(requeue=True)` |
| **Unrecoverable Error** | Invalid payload schema, corrupted IDs, deleted accounts | Once retry quota is exhausted (`MAX_TASK_RETRIES=3`), task is marked `DEAD_LETTERED`, rejected via `reject(requeue=False)` to DLQ, and triggers escalation webhooks |

---

## 6. Local Development & Testing

### Starting Services

```bash
docker-compose up -d rabbitmq redis
```

### Running the Worker

```bash
source .venv/bin/activate
python -m src.worker
```

### Submitting a Test Job via CLI

```bash
python -m src.cli submit \
  --type inventory_sync \
  --resource warehouse_spb_1 \
  --priority high \
  --items 200
```

### Checking Status

```bash
python -m src.cli status <TASK_UUID>
```

### Unit Testing Custom Handlers

```python
# tests/test_inventory_handler.py
import pytest
from unittest.mock import AsyncMock, patch
from src.handlers.inventory import handle_inventory_sync


@pytest.mark.asyncio
async def test_inventory_handler_success():
    chunk = [{"sku": "SKU-01", "warehouse_id": "WH-1", "quantity": 10, "price": 100.0}]
    params = {"source": "unit_test"}

    mock_conn = AsyncMock()
    mock_pool = AsyncMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    with patch("src.handlers.inventory.db_pool", mock_pool):
        await handle_inventory_sync(chunk, params)
        mock_conn.executemany.assert_called_once()
```
