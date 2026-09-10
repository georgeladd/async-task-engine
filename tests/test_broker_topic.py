"""Unit tests for RabbitMQ Topic Exchange topology, Alternate Exchange, and dynamic queues."""

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from aio_pika import ExchangeType

from src.broker import MessageBroker
from src.schemas import TaskMessage, TaskPriority
from src.worker import TaskWorker


@pytest.mark.asyncio
async def test_broker_connect_declares_topic_and_ae_topology() -> None:
    """Verifies that connect declares Alternate Exchange, Unrouted queue, and Topic Exchange."""
    broker = MessageBroker()
    mock_conn = AsyncMock()
    mock_channel = AsyncMock()
    mock_conn.channel.return_value = mock_channel

    mock_topic_exchange = AsyncMock()
    mock_channel.get_exchange.return_value = mock_topic_exchange

    with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_conn):
        await broker.connect()

        # 1. Verify channel QoS
        mock_channel.set_qos.assert_called_once_with(prefetch_count=10)

        # 2. Verify exchange declarations
        declared_exchanges = [call[0][0] for call in mock_channel.declare_exchange.call_args_list]
        assert "tasks.dlx" in declared_exchanges
        assert "tasks.ae" in declared_exchanges
        assert "tasks.topic" in declared_exchanges

        # Verify Alternate Exchange was configured on tasks.topic
        topic_call = next(c for c in mock_channel.declare_exchange.call_args_list if c[0][0] == "tasks.topic")
        assert topic_call[0][1] == ExchangeType.TOPIC
        assert topic_call[1]["arguments"] == {"alternate-exchange": "tasks.ae"}

        # Verify tasks.ae is FANOUT
        ae_call = next(c for c in mock_channel.declare_exchange.call_args_list if c[0][0] == "tasks.ae")
        assert ae_call[0][1] == ExchangeType.FANOUT

        # 3. Verify unrouted queue declared and bound
        declared_queues = [call[0][0] for call in mock_channel.declare_queue.call_args_list]
        assert "tasks_unrouted" in declared_queues
        assert broker.unrouted_queue is not None


@pytest.mark.asyncio
async def test_broker_declare_worker_queue_binds_to_topic() -> None:
    """Verifies dynamically declaring dedicated worker queue with custom routing key."""
    broker = MessageBroker()
    mock_channel = AsyncMock()
    mock_queue = AsyncMock()
    mock_topic_exchange = AsyncMock()

    mock_channel.declare_queue.return_value = mock_queue
    mock_channel.get_exchange.return_value = mock_topic_exchange
    broker.channel = mock_channel

    queue = await broker.declare_worker_queue(
        queue_name="tasks_heavy",
        routing_key="tasks.heavy.*",
    )

    assert queue is mock_queue
    mock_channel.declare_queue.assert_called_once_with(
        "tasks_heavy",
        durable=True,
        arguments={
            "x-dead-letter-exchange": "tasks.dlx",
            "x-dead-letter-routing-key": "dead_letter",
            "x-max-priority": 10,
        },
    )
    mock_queue.bind.assert_any_call(mock_topic_exchange, routing_key="tasks.heavy.*")


@pytest.mark.asyncio
async def test_broker_publish_task_resolves_topic_routing_key() -> None:
    """Verifies that publish_task computes tasks.<task_type> routing key for topic exchange."""
    broker = MessageBroker()
    mock_channel = AsyncMock()
    mock_topic_exchange = AsyncMock()
    mock_channel.get_exchange.return_value = mock_topic_exchange
    broker.channel = mock_channel

    task = TaskMessage(
        task_id=uuid4(),
        task_type="heavy.pdf_render",
        resource_id="doc_101",
        priority=TaskPriority.CRITICAL,
    )

    await broker.publish_task(task)

    assert mock_topic_exchange.publish.call_count == 1
    call_kwargs = mock_topic_exchange.publish.call_args[1]
    assert call_kwargs["routing_key"] == "tasks.heavy.pdf_render"


def test_task_worker_custom_queue_configuration() -> None:
    """Verifies that TaskWorker accepts specialized queue and routing key parameters."""
    worker = TaskWorker(
        rate_limit=50.0,
        queue_name="tasks_analytics",
        routing_key="tasks.analytics.*",
    )
    assert worker.queue_name == "tasks_analytics"
    assert worker.routing_key == "tasks.analytics.*"
    assert worker.rate_limiter.rate == 50.0
