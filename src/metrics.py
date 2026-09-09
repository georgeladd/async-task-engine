"""Prometheus metrics instruments and telemetry collectors for Async Task Engine."""

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Total tasks accepted by API producer
TASKS_SUBMITTED_TOTAL = Counter(
    "tasks_submitted_total",
    "Total number of operational tasks submitted to the API",
    ["task_type", "priority"],
)

# Total tasks completed or failed by worker
TASKS_COMPLETED_TOTAL = Counter(
    "tasks_completed_total",
    "Total number of tasks finished by the worker node",
    ["task_type", "status"],
)

# Histogram tracking execution duration
TASK_DURATION_SECONDS = Histogram(
    "task_duration_seconds",
    "Time spent executing task processing in seconds",
    ["task_type"],
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, float("inf")),
)

# Total count of individual data elements processed through chunk streams
ITEMS_PROCESSED_TOTAL = Counter(
    "items_processed_total",
    "Total number of records processed through streaming chunk generators",
    ["task_type"],
)

# Tasks rejected and rerouted to Dead-Letter Queue
DEAD_LETTER_TASKS_TOTAL = Counter(
    "dead_letter_tasks_total",
    "Total tasks exhausted retries and diverted to Dead-Letter Queue",
    ["task_type"],
)

# Active executing tasks gauge
ACTIVE_WORKER_TASKS = Gauge(
    "active_worker_tasks",
    "Number of tasks currently in-flight and processing inside the worker",
)


def get_prometheus_metrics() -> tuple[bytes, str]:
    """Serializes all registered metrics into Prometheus text presentation format.

    Returns:
        Tuple containing binary payload and content-type header string.
    """
    return generate_latest(), CONTENT_TYPE_LATEST
