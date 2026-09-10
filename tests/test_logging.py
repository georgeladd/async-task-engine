"""Unit tests for structured JSON logging and distributed tracing context."""

import json
import logging
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.broker import MessageBroker
from src.logging_config import (
    JSONLogFormatter,
    current_correlation_id,
    current_resource_id,
    setup_logging,
)
from src.schemas import TaskMessage, TaskPriority


def test_json_log_formatter_basic() -> None:
    """Verifies that JSONLogFormatter outputs standard single-line JSON with core fields."""
    formatter = JSONLogFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Processing test batch chunk",
        args=(),
        exc_info=None,
    )
    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "test_logger"
    assert parsed["message"] == "Processing test batch chunk"
    assert "timestamp" in parsed


def test_json_log_formatter_with_correlation_context() -> None:
    """Verifies that active ContextVars inject correlation_id and resource_id into JSON log."""
    formatter = JSONLogFormatter()
    test_corr_id = str(uuid4())
    test_res_id = "tenant_billing_99"

    token_corr = current_correlation_id.set(test_corr_id)
    token_res = current_resource_id.set(test_res_id)

    try:
        record = logging.LogRecord(
            name="worker",
            level=logging.WARNING,
            pathname="worker.py",
            lineno=55,
            msg="Task delayed due to lock contention",
            args=(),
            exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert parsed["correlation_id"] == test_corr_id
        assert parsed["resource_id"] == test_res_id
    finally:
        current_correlation_id.reset(token_corr)
        current_resource_id.reset(token_res)


def test_json_log_formatter_with_exception() -> None:
    """Verifies that exceptions are cleanly formatted into the exception JSON attribute."""
    formatter = JSONLogFormatter()
    try:
        raise ValueError("Simulated database timeout")
    except ValueError:
        import sys

        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="worker",
        level=logging.ERROR,
        pathname="worker.py",
        lineno=80,
        msg="Fatal failure during batch execution",
        args=(),
        exc_info=exc_info,
    )
    parsed = json.loads(formatter.format(record))
    assert parsed["level"] == "ERROR"
    assert "exception" in parsed
    assert "Simulated database timeout" in parsed["exception"]


@pytest.mark.asyncio
async def test_broker_publishes_correlation_headers() -> None:
    """Verifies that MessageBroker injects correlation_id and resource_id into AMQP message."""
    broker = MessageBroker()
    mock_channel = AsyncMock()
    mock_exchange = AsyncMock()
    mock_channel.get_exchange = AsyncMock(return_value=mock_exchange)
    broker.channel = mock_channel

    task = TaskMessage(
        task_type="audit_export",
        resource_id="tenant_audit_1",
        priority=TaskPriority.HIGH,
    )

    await broker.publish_task(task)

    assert mock_exchange.publish.call_count == 1
    published_msg = mock_exchange.publish.call_args[0][0]

    assert published_msg.correlation_id == str(task.task_id)
    assert published_msg.headers["correlation_id"] == str(task.task_id)
    assert published_msg.headers["resource_id"] == "tenant_audit_1"


def test_setup_logging_toggle() -> None:
    """Verifies setup_logging configures root handler."""
    setup_logging("DEBUG", json_mode=True)
    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert any(isinstance(h.formatter, JSONLogFormatter) for h in root.handlers)

    setup_logging("INFO", json_mode=False)
    assert not any(isinstance(h.formatter, JSONLogFormatter) for h in root.handlers)
