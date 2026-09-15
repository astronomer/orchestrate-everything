from __future__ import annotations

import os
from pathlib import Path

import duckdb

DB_PATH = os.getenv("COSMARKET_DB_PATH") or str(
    Path(os.getenv("AIRFLOW_HOME", "/usr/local/airflow")) / "include" / "cosmarket.duckdb"
)
SCHEMA_FILE = Path(__file__).parent / "sql" / "cosmarket_schema.sql"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def get_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=read_only)


def create_schema() -> None:
    with get_conn() as conn:
        conn.execute(SCHEMA_FILE.read_text())


def _foreign_key_columns(conn, table: str) -> set[str]:
    rows = conn.execute(
        "SELECT constraint_column_names FROM duckdb_constraints()"
        " WHERE table_name = ? AND constraint_type = 'FOREIGN KEY'",
        [table],
    ).fetchall()
    return {column for (names,) in rows for column in names}


def _table_columns(conn, table: str) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = ? ORDER BY ordinal_position",
            [table],
        ).fetchall()
    ]


def upsert(table: str, key: str, records: list[dict]) -> int:
    if not records:
        return 0
    columns = list(records[0])
    missing = [c for c in columns if any(c not in r for r in records)]
    if missing:
        raise ValueError(
            f"every record must have the same keys; {sorted(missing)} "
            f"missing from at least one of the {len(records)} records"
        )

    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(_quote(c) for c in columns)
    updatable = [c for c in columns if c != key]

    with get_conn() as conn:
        present = _table_columns(conn, table)
        if not present:
            raise ValueError(f"table {table!r} does not exist, run `setup` first")
        unknown = [c for c in columns if c not in present]
        if unknown:
            raise ValueError(f"{table}: no such column(s) {sorted(unknown)}")

        pinned = _foreign_key_columns(conn, table)
        assignments = [
            f"{_quote(c)} = excluded.{_quote(c)}" for c in updatable if c not in pinned
        ]
        if "updated_at" in present and "updated_at" not in columns:
            assignments.append('"updated_at" = now()')

        sql = (
            f"INSERT INTO {_quote(table)} ({column_list}) VALUES ({placeholders})"
            f" ON CONFLICT ({_quote(key)}) DO UPDATE SET {', '.join(assignments)}"
            if assignments
            else f"INSERT INTO {_quote(table)} ({column_list}) VALUES ({placeholders})"
            f" ON CONFLICT ({_quote(key)}) DO NOTHING"
        )

        conn.execute("BEGIN TRANSACTION")
        try:
            conn.executemany(sql, [[r[c] for c in columns] for r in records])
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
    return len(records)


def records(table: str) -> list[dict]:
    with get_conn(read_only=True) as conn:
        result = conn.execute(f"SELECT * FROM {_quote(table)}")
        columns = [c[0] for c in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]
