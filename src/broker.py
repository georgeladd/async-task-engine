"""RabbitMQ broker communication layer with Dead-Letter Queue (DLQ) support."""

import logging
from typing import Any

import aio_pika
from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractQueue

from src.config import settings
from src.schemas import TaskMessage, TaskPriority

logger = logging.getLogger(__name__)

PRIORITY_MAP: dict[TaskPriority, int] = {
    TaskPriority.LOW: 1,
    TaskPriority.NORMAL: 5,
    TaskPriority.HIGH: 8,
    TaskPriority.CRITICAL: 10,
}


class MessageBroker:
    """Asynchronous RabbitMQ broker manager handling topology, queues, and message dispatch.

    Manages primary task queue with dead-letter exchange for failed or rejected tasks.
    """

    def __init__(self, amqp_uri: str | None = None) -> None:
        """Initializes message broker instance.

        Args:
            amqp_uri: AMQP connection string. Defaults to settings.rabbitmq_uri.
        """
        self.amqp_uri: str = amqp_uri or settings.rabbitmq_uri
        self.connection: AbstractConnection | None = None
        self.channel: AbstractChannel | None = None
        self.main_queue: AbstractQueue | None = None
        self.dlq_queue: AbstractQueue | None = None

    async def connect(self) -> None:
        """Establishes connection and configures exchanges, queues, and dead-letter routing."""
        self.connection = await aio_pika.connect_robust(self.amqp_uri)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)

        # 1. Dead-Letter Exchange & Queue declaration
        dlx_exchange = await self.channel.declare_exchange(
            "tasks.dlx",
            ExchangeType.DIRECT,
            durable=True,
        )
        self.dlq_queue = await self.channel.declare_queue(
            settings.rabbitmq_dlq_queue,
            durable=True,
        )
        await self.dlq_queue.bind(dlx_exchange, routing_key="dead_letter")

        # 2. Main Exchange & Queue declaration with DLX arguments
        main_exchange = await self.channel.declare_exchange(
            "tasks.direct",
            ExchangeType.DIRECT,
            durable=True,
        )
        queue_arguments: dict[str, Any] = {
            "x-dead-letter-exchange": "tasks.dlx",
            "x-dead-letter-routing-key": "dead_letter",
            "x-max-priority": 10,
        }
        self.main_queue = await self.channel.declare_queue(
            settings.rabbitmq_main_queue,
            durable=True,
            arguments=queue_arguments,
        )
        await self.main_queue.bind(main_exchange, routing_key="task.process")

        logger.info(
            f"RabbitMQ topology initialized: queue={settings.rabbitmq_main_queue}, "
            f"dlq={settings.rabbitmq_dlq_queue}"
        )

    async def publish_task(self, task: TaskMessage) -> None:
        """Publishes a task message into the primary direct exchange.

        Args:
            task: TaskMessage data schema to serialize and enqueue.

        Raises:
            RuntimeError: If channel is not initialized.
        """
        if not self.channel:
            raise RuntimeError("Broker connection is not initialized. Call connect() first.")

        main_exchange = await self.channel.get_exchange("tasks.direct")
        priority_value: int = PRIORITY_MAP.get(task.priority, 5)

        message_body = task.model_dump_json().encode("utf-8")
        amqp_message = Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority=priority_value,
            message_id=str(task.task_id),
            correlation_id=str(task.task_id),
            headers={
                "attempts": task.attempts,
                "correlation_id": str(task.task_id),
                "resource_id": task.resource_id,
            },
        )

        await main_exchange.publish(amqp_message, routing_key="task.process")
        logger.info(f"Published task {task.task_id} [priority={task.priority.value}] to queue")

    async def close(self) -> None:
        """Gracefully closes RabbitMQ connection and channel."""
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        logger.info("RabbitMQ connection closed successfully")
