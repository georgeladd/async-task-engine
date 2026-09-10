"""Unit tests verifying package level exports and interfaces."""

def test_package_exports() -> None:
    """Verifies that key engine primitives are exported from root package."""
    from src import (
        MessageBroker,
        TaskEngineClient,
        TaskWorker,
        get_handler,
        register_handler,
        task_handler,
    )

    assert TaskEngineClient is not None
    assert TaskWorker is not None
    assert MessageBroker is not None
    assert callable(get_handler)
    assert callable(register_handler)
    assert callable(task_handler)
