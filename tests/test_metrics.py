"""Tests for Prometheus metrics telemetry."""


import pytest
from httpx import AsyncClient

from src.metrics import (
    ACTIVE_WORKER_TASKS,
    ITEMS_PROCESSED_TOTAL,
    get_prometheus_metrics,
)


@pytest.mark.asyncio
async def test_metrics_endpoint(async_client: AsyncClient) -> None:
    """Verifies that /metrics endpoint returns HTTP 200 with Prometheus text format."""
    response = await async_client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    text_content = response.text
    assert "tasks_submitted_total" in text_content
    assert "tasks_completed_total" in text_content
    assert "task_duration_seconds" in text_content


def test_metrics_serialization() -> None:
    """Tests Prometheus binary metrics generation."""
    payload, content_type = get_prometheus_metrics()
    assert isinstance(payload, bytes)
    assert len(payload) > 0
    assert "text/plain" in content_type


def test_custom_metrics_increments() -> None:
    """Tests manual gauge and counter operations."""
    initial_gauge = ACTIVE_WORKER_TASKS._value.get()
    ACTIVE_WORKER_TASKS.inc()
    assert ACTIVE_WORKER_TASKS._value.get() == initial_gauge + 1
    ACTIVE_WORKER_TASKS.dec()
    assert ACTIVE_WORKER_TASKS._value.get() == initial_gauge

    ITEMS_PROCESSED_TOTAL.labels(task_type="unit_test").inc(15)
