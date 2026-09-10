"""Asynchronous worker service consuming tasks from RabbitMQ with Redis distributed locking."""

import asyncio
import json
import logging
import signal
import time
from typing import Any

import redis.asyncio as aioredis
from aio_pika.abc import AbstractIncomingMessage
from redis.asyncio import Redis

from src.broker import MessageBroker
from src.chunker import chunk_iterator
from src.config import settings
from src.logging_config import (
    current_correlation_id,
    current_resource_id,
    setup_logging,
)
from src.metrics import (
    ACTIVE_WORKER_TASKS,
    DEAD_LETTER_TASKS_TOTAL,
    ITEMS_PROCESSED_TOTAL,
    TASK_DURATION_SECONDS,
    TASKS_COMPLETED_TOTAL,
)
from src.redis_lock import DistributedLock
from src.schemas import TaskMessage, TaskResult, TaskStatus

setup_logging(settings.log_level, json_mode=True)
logger = logging.getLogger("worker")


class TaskWorker:
    """Worker node consuming tasks from RabbitMQ, locking resources, and executing chunked batch pipelines."""

    def __init__(self) -> None:
        """Initializes worker state, connections, and shutdown event flags."""
        self.broker = MessageBroker()
        self.redis: Redis | None = None
        self.is_running: bool = False
        self.shutdown_event = asyncio.Event()

    async def initialize(self) -> None:
        """Establishes connections to RabbitMQ and Redis."""
        logger.info("Connecting worker to RabbitMQ and Redis...")
        self.redis = aioredis.from_url(
            settings.redis_uri,
            encoding="utf-8",
            decode_responses=True,
        )
        await self.broker.connect()
        logger.info("Worker connections established successfully")

    async def process_task_payload(self, task: TaskMessage) -> TaskResult:
        """Processes payload items in memory-safe batches under distributed lock.

        Args:
            task: Validated TaskMessage.

        Returns:
            TaskResult summarizing processed records and duration.
        """
        start_time: float = time.monotonic()
        items: list[dict[str, Any]] = task.payload.items
        chunk_size: int = settings.batch_chunk_size

        total_processed: int = 0
        total_chunks: int = 0

        # Execute chunked streaming batch iteration
        for chunk in chunk_iterator(items, chunk_size):
            # Simulate real batch database/API processing
            await asyncio.sleep(0.01)
            total_processed += len(chunk)
            total_chunks += 1
            ITEMS_PROCESSED_TOTAL.labels(task_type=task.task_type).inc(len(chunk))
            logger.debug(
                f"Task {task.task_id}: processed chunk #{total_chunks} "
                f"({len(chunk)} items, cumulative={total_processed})"
            )

        duration: float = time.monotonic() - start_time
        TASK_DURATION_SECONDS.labels(task_type=task.task_type).observe(duration)
        result = TaskResult(
            task_id=task.task_id,
            status=TaskStatus.COMPLETED,
            processed_count=total_processed,
            chunk_count=total_chunks,
            execution_time_seconds=round(duration, 3),
        )
        return result

    async def handle_incoming_message(self, message: AbstractIncomingMessage) -> None:
        """Handler for incoming AMQP messages.

        Enforces concurrency safety via Redis distributed lock, updates status, and handles DLQ routing.

        Args:
            message: Raw AMQP incoming message from RabbitMQ.
        """
        async with message.process(requeue=False, ignore_processed=True):
            try:
                task_data = json.loads(message.body.decode("utf-8"))
                task = TaskMessage.model_validate(task_data)
            except (json.JSONDecodeError, ValueError) as parse_err:
                logger.error(f"Malformed message body received, rejecting to DLQ: {parse_err}")
                await message.reject(requeue=False)
                return

            logger.info(
                f"Received task {task.task_id} [type={task.task_type}, "
                f"resource={task.resource_id}, items={len(task.payload.items)}]"
            )

            token_corr = current_correlation_id.set(str(task.task_id))
            token_res = current_resource_id.set(task.resource_id)

            try:
                if not self.redis:
                    logger.error("Redis client is uninitialized, requeuing task")
                    await message.nack(requeue=True)
                    return

                lock = DistributedLock(
                    self.redis,
                    resource_key=task.resource_id,
                    ttl_seconds=settings.redis_lock_ttl_seconds,
                )

                # Attempt to acquire lock on target resource
                acquired: bool = await lock.acquire(timeout_seconds=2.0)
                if not acquired:
                    logger.warning(
                        f"Resource '{task.resource_id}' is locked by another task. "
                        f"Requeuing task {task.task_id} for later retry."
                    )
                    await message.nack(requeue=True)
                    return

                # Resource successfully locked
                status_key: str = f"task:status:{task.task_id}"
                result_key: str = f"task:result:{task.task_id}"
                ACTIVE_WORKER_TASKS.inc()

                try:
                    await self.redis.set(status_key, TaskStatus.RUNNING.value, ex=86400)
                    result: TaskResult = await self.process_task_payload(task)

                    # Store completion result
                    await self.redis.set(status_key, TaskStatus.COMPLETED.value, ex=86400)
                    await self.redis.set(result_key, result.model_dump_json(), ex=86400)
                    await message.ack()
                    TASKS_COMPLETED_TOTAL.labels(task_type=task.task_type, status="completed").inc()
                    logger.info(
                        f"Task {task.task_id} completed successfully in "
                        f"{result.execution_time_seconds}s ({result.processed_count} items)"
                    )
                except Exception as exec_err:  # noqa: BLE001
                    logger.error(f"Execution failed for task {task.task_id}: {exec_err}")
                    task.attempts += 1

                    if task.attempts < settings.max_task_retries:
                        logger.info(f"Retrying task {task.task_id} (attempt {task.attempts})")
                        await self.redis.set(status_key, TaskStatus.PENDING.value, ex=86400)
                        await message.nack(requeue=True)
                    else:
                        logger.critical(
                            f"Task {task.task_id} exceeded max retries ({settings.max_task_retries}). "
                            f"Routing to Dead-Letter Queue (DLQ)"
                        )
                        await self.redis.set(status_key, TaskStatus.DEAD_LETTERED.value, ex=86400)
                        await message.reject(requeue=False)
                        TASKS_COMPLETED_TOTAL.labels(task_type=task.task_type, status="dead_lettered").inc()
                        DEAD_LETTER_TASKS_TOTAL.labels(task_type=task.task_type).inc()
                finally:
                    ACTIVE_WORKER_TASKS.dec()
                    await lock.release()
            finally:
                current_correlation_id.reset(token_corr)
                current_resource_id.reset(token_res)

    async def start(self) -> None:
        """Starts worker consumption loop."""
        self.is_running = True
        await self.initialize()

        if not self.broker.main_queue:
            raise RuntimeError("Broker main queue is not declared")

        await self.broker.main_queue.consume(self.handle_incoming_message)
        logger.info(f"Worker listening on queue: {settings.rabbitmq_main_queue}")

        await self.shutdown_event.wait()
        logger.info("Worker shutdown event triggered, stopping consumer...")
        await self.stop()

    async def stop(self) -> None:
        """Performs clean shutdown of worker connections."""
        self.is_running = False
        await self.broker.close()
        if self.redis:
            await self.redis.close()
        logger.info("Worker stopped cleanly")


async def main() -> None:
    """Worker entry point handling OS termination signals."""
    worker = TaskWorker()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.shutdown_event.set)

    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
