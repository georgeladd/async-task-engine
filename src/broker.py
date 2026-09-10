"""RabbitMQ broker communication layer with Dead-Letter Queue (DLQ) support."""

import logging
from typing import Any

import aio_pika
from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractExchange, AbstractQueue

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

    Manages topic exchange with alternate exchange fallback, dedicated dynamic queues,
    and dead-letter exchange (DLQ) for failed or rejected tasks.
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
        self.unrouted_queue: AbstractQueue | None = None
        self.topic_exchange: AbstractExchange | None = None
        self.ae_exchange: AbstractExchange | None = None
        self.dlx_exchange: AbstractExchange | None = None

    @property
    def is_connected(self) -> bool:
        """Indicates whether broker connection and channel are open and operational."""
        return bool(
            self.connection
            and not self.connection.is_closed
            and self.channel
            and not self.channel.is_closed
        )

    async def declare_worker_queue(
        self,
        queue_name: str,
        routing_key: str = "tasks.#",
    ) -> AbstractQueue:
        """Dynamically declares and binds a worker queue to the topic exchange.

        Args:
            queue_name: Name of the worker queue.
            routing_key: AMQP topic binding pattern (e.g. 'tasks.#' or 'tasks.heavy.*').

        Returns:
            Declared and bound AbstractQueue instance.

        Raises:
            RuntimeError: If broker channel is not initialized.
        """
        if not self.channel:
            raise RuntimeError("Broker connection is not initialized. Call connect() first.")

        queue_arguments: dict[str, Any] = {
            "x-dead-letter-exchange": "tasks.dlx",
            "x-dead-letter-routing-key": "dead_letter",
            "x-max-priority": 10,
        }

        queue = await self.channel.declare_queue(
            queue_name,
            durable=True,
            arguments=queue_arguments,
        )

        # Bind to primary topic exchange
        topic_exchange = await self.channel.get_exchange("tasks.topic")
        await queue.bind(topic_exchange, routing_key=routing_key)

        # Legacy compatibility binding to tasks.direct
        try:
            direct_exchange = await self.channel.get_exchange("tasks.direct")
            await queue.bind(direct_exchange, routing_key="task.process")
        except Exception:  # noqa: BLE001
            pass

        if queue_name == settings.rabbitmq_main_queue:
            self.main_queue = queue

        logger.info(f"Declared queue '{queue_name}' bound to 'tasks.topic' with key '{routing_key}'")
        return queue

    async def connect(self) -> None:
        """Establishes connection and configures exchanges, queues, and dead-letter routing."""
        self.connection = await aio_pika.connect_robust(self.amqp_uri)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)

        # 1. Dead-Letter Exchange & Queue declaration
        self.dlx_exchange = await self.channel.declare_exchange(
            "tasks.dlx",
            ExchangeType.DIRECT,
            durable=True,
        )
        self.dlq_queue = await self.channel.declare_queue(
            settings.rabbitmq_dlq_queue,
            durable=True,
        )
        await self.dlq_queue.bind(self.dlx_exchange, routing_key="dead_letter")

        # 2. Alternate Exchange & Unrouted Queue declaration
        self.ae_exchange = await self.channel.declare_exchange(
            "tasks.ae",
            ExchangeType.FANOUT,
            durable=True,
        )
        self.unrouted_queue = await self.channel.declare_queue(
            "tasks_unrouted",
            durable=True,
        )
        await self.unrouted_queue.bind(self.ae_exchange)

        # 3. Topic Exchange declaration with Alternate Exchange fallback
        self.topic_exchange = await self.channel.declare_exchange(
            "tasks.topic",
            ExchangeType.TOPIC,
            durable=True,
            arguments={"alternate-exchange": "tasks.ae"},
        )

        # 4. Direct Exchange declaration for backward compatibility
        await self.channel.declare_exchange(
            "tasks.direct",
            ExchangeType.DIRECT,
            durable=True,
        )

        # 5. Default Catch-All Queue declaration and binding
        self.main_queue = await self.declare_worker_queue(
            queue_name=settings.rabbitmq_main_queue,
            routing_key=settings.worker_routing_key,
        )

        logger.info(
            f"RabbitMQ topic topology initialized: queue={settings.rabbitmq_main_queue}, "
            f"dlq={settings.rabbitmq_dlq_queue}, unrouted=tasks_unrouted"
        )

    async def publish_task(
        self,
        task: TaskMessage,
        routing_key: str | None = None,
    ) -> None:
        """Publishes a task message into the topic exchange with categorized routing key.

        Args:
            task: TaskMessage data schema to serialize and enqueue.
            routing_key: Optional explicit routing key. Defaults to 'tasks.{task_type}'.

        Raises:
            RuntimeError: If channel is not initialized.
        """
        if not self.channel:
            raise RuntimeError("Broker connection is not initialized. Call connect() first.")

        # Resolve topic routing key
        if not routing_key:
            category = task.task_type
            routing_key = category if category.startswith("tasks.") else f"tasks.{category}"

        try:
            exchange = await self.channel.get_exchange("tasks.topic")
        except Exception:  # noqa: BLE001
            exchange = await self.channel.get_exchange("tasks.direct")
            routing_key = "task.process"

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

        await exchange.publish(amqp_message, routing_key=routing_key)
        logger.info(
            f"Published task {task.task_id} [priority={task.priority.value}] "
            f"to exchange with routing_key='{routing_key}'"
        )

    async def close(self) -> None:
        """Gracefully closes RabbitMQ connection and channel."""
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        logger.info("RabbitMQ connection closed successfully")
