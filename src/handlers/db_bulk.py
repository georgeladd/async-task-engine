"""Database bulk ingest executor for relational storage operations."""

import asyncio
import os
import sqlite3
from typing import Any


def _execute_sqlite_bulk(
    db_path: str,
    table_name: str,
    chunk: list[dict[str, Any]],
    primary_key: str | None = None,
) -> None:
    """Synchronous worker function to execute batch insert in SQLite.

    Args:
        db_path: Path to SQLite database file.
        table_name: Target table name.
        chunk: List of dictionaries to insert.
        primary_key: Optional explicit primary key column name.
    """
    if not chunk:
        return

    # Ensure parent directory exists for file-based database paths
    if db_path != ":memory:":
        parent_dir = os.path.dirname(db_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

    # Sanitize table and column names (alphanumeric and underscore only)
    clean_table = "".join(c for c in table_name if c.isalnum() or c == "_")
    raw_columns = list(chunk[0].keys())
    clean_columns = ["".join(c for c in col if c.isalnum() or c == "_") for col in raw_columns]

    # Resolve primary key column for deterministic upsert resolution
    pk_column = primary_key
    if not pk_column:
        for candidate in ("id", "uuid", "sku", "order_id", "key", clean_columns[0]):
            if candidate in clean_columns:
                pk_column = candidate
                break

    create_defs: list[str] = []
    for col in clean_columns:
        if col == pk_column:
            create_defs.append(f'"{col}" TEXT PRIMARY KEY')
        else:
            create_defs.append(f'"{col}" TEXT')

    clean_cols_clause = ", ".join(f'"{col}"' for col in clean_columns)
    placeholders = ", ".join("?" for _ in clean_columns)
    columns_defs_clause = ", ".join(create_defs)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {clean_table} ({columns_defs_clause})")
        if pk_column:
            cursor.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS "idx_{clean_table}_{pk_column}" '
                f'ON {clean_table} ("{pk_column}")'
            )

        insert_sql = (
            f"INSERT OR REPLACE INTO {clean_table} ({clean_cols_clause}) "
            f"VALUES ({placeholders})"
        )
        data_rows = [
            tuple(str(item.get(orig_col, "")) for orig_col in raw_columns)
            for item in chunk
        ]
        cursor.executemany(insert_sql, data_rows)
        conn.commit()


async def handle_db_bulk(
    chunk: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> None:
    """Inserts batch records into database table in an asynchronous worker thread.

    Args:
        chunk: Sliced list of data items.
        parameters: Execution parameters including table_name, db_path, and primary_key.

    Raises:
        ValueError: If required parameters are invalid or missing.
    """
    table_name = parameters.get("table_name", "batch_records")
    db_path = parameters.get("db_path", "data/bulk_records.db")
    primary_key = parameters.get("primary_key")

    await asyncio.to_thread(_execute_sqlite_bulk, db_path, table_name, chunk, primary_key)
