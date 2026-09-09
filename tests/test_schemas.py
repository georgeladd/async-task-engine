"""Tests for Pydantic schema validation."""

import pytest
from pydantic import ValidationError

from src.schemas import TaskCreateRequest, TaskMessage, TaskPriority


def test_task_create_request_valid() -> None:
    """Tests valid task creation schema instantiation."""
    req = TaskCreateRequest(
        task_type="data_migration",
        resource_id="db_table_users",
        priority=TaskPriority.HIGH,
        payload={"items": [{"id": 1}], "parameters": {"mode": "fast"}},
    )
    assert req.task_type == "data_migration"
    assert req.resource_id == "db_table_users"
    assert req.priority == TaskPriority.HIGH
    assert len(req.payload.items) == 1


def test_task_create_request_invalid_length() -> None:
    """Verifies that empty or short resource/task strings trigger validation errors."""
    with pytest.raises(ValidationError):
        TaskCreateRequest(
            task_type="a",  # min_length is 2
            resource_id="",
        )


def test_task_message_serialization() -> None:
    """Tests JSON roundtrip serialization for TaskMessage."""
    task = TaskMessage(
        task_type="maintenance_run",
        resource_id="cache_cluster_1",
    )
    json_data = task.model_dump_json()
    reconstructed = TaskMessage.model_validate_json(json_data)

    assert reconstructed.task_id == task.task_id
    assert reconstructed.priority == TaskPriority.NORMAL
    assert reconstructed.attempts == 0
