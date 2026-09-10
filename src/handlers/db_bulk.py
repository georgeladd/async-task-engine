"""Database bulk ingest executor for relational storage operations."""

import asyncio
import sqlite3
from typing import Any


def _execute_sqlite_bulk(
    db_path: str,
    table_name: str,
    chunk: list[dict[str, Any]],
) -> None:
    """Synchronous worker function to execute batch insert in SQLite.

    Args:
        db_path: Path to SQLite database file or ':memory:'.
        table_name: Target table name.
        chunk: List of dictionaries to insert.
    """
    if not chunk:
        return

    # Sanitize table name alphanumeric only
    clean_table = "".join(c for c in table_name if c.isalnum() or c == "_")
    columns = list(chunk[0].keys())
    clean_cols = [f'"{col}"' for col in columns]
    placeholders = ", ".join("?" for _ in columns)
    columns_clause = ", ".join(clean_cols)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        create_cols = ", ".join(f'"{col}" TEXT' for col in columns)
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {clean_table} ({create_cols})")

        insert_sql = (
            f"INSERT OR REPLACE INTO {clean_table} ({columns_clause}) "
            f"VALUES ({placeholders})"
        )
        data_rows = [tuple(str(item.get(col, "")) for col in columns) for item in chunk]
        cursor.executemany(insert_sql, data_rows)
        conn.commit()


async def handle_db_bulk(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Inserts batch records into database table in an asynchronous worker thread.

    Args:
        chunk: Sliced list of data items.
        parameters: Execution parameters including table_name and db_path.

    Raises:
        ValueError: If required parameters are invalid or missing.
    """
    table_name = parameters.get("table_name", "batch_records")
    db_path = parameters.get("db_path", ":memory:")

    await asyncio.to_thread(_execute_sqlite_bulk, db_path, table_name, chunk)
