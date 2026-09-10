"""HTTP batch executor for outgoing bulk API integrations."""

from typing import Any

import httpx

from src.security import is_safe_webhook_url


async def handle_http_batch(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Dispatches a chunk of records as batch HTTP requests to target service.

    Args:
        chunk: Sliced list of data items.
        parameters: Execution parameters including target_url, http_method, headers.

    Raises:
        ValueError: If target_url is missing or unsafe.
        RuntimeError: If target API responds with non-success status code.
    """
    target_url = parameters.get("target_url")
    if not target_url:
        raise ValueError("Missing required parameter 'target_url' for http_batch handler")

    is_safe = await is_safe_webhook_url(target_url, allow_local=parameters.get("allow_local", False))
    if not is_safe:
        raise ValueError(f"SSRF violation: target_url '{target_url}' is blocked")

    http_method = parameters.get("http_method", "POST").upper()
    headers = parameters.get("headers", {})
    timeout_seconds = float(parameters.get("timeout_seconds", 10.0))

    payload = {
        "batch_reference": parameters.get("batch_reference"),
        "items": chunk,
        "count": len(chunk),
    }

    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        if http_method == "POST":
            response = await client.post(target_url, json=payload, headers=headers)
        elif http_method == "PUT":
            response = await client.put(target_url, json=payload, headers=headers)
        else:
            response = await client.request(http_method, target_url, json=payload, headers=headers)

        if not response.is_success:
            raise RuntimeError(
                f"HTTP batch dispatch failed with status {response.status_code}: {response.text[:200]}"
            )
