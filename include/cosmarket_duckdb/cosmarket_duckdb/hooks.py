from __future__ import annotations

from typing import Any

import duckdb

from airflow.providers.common.sql.hooks.sql import DbApiHook


SYSTEM_SCHEMAS = ("information_schema", "pg_catalog")


class _DuckDBInspector:

    def __init__(self, hook: DuckDBHook) -> None:
        self._hook = hook

    def get_table_names(self, schema: str | None = None) -> list[str]:
        sql = "SELECT table_name FROM information_schema.tables"
        parameters: list[Any] = []
        if schema:
            sql += " WHERE table_schema = ?"
            parameters.append(schema)
        else:
            placeholders = ", ".join(["?"] * len(SYSTEM_SCHEMAS))
            sql += f" WHERE table_schema NOT IN ({placeholders})"
            parameters.extend(SYSTEM_SCHEMAS)
        sql += " ORDER BY table_name"
        return [row[0] for row in self._hook.get_records(sql, parameters=parameters)]

    def get_columns(
        self, table_name: str, schema: str | None = None
    ) -> list[dict[str, str]]:
        return self._hook.get_table_schema(table_name, schema=schema)


class DuckDBHook(DbApiHook):

    conn_name_attr = "duckdb_conn_id"
    default_conn_name = "duckdb_default"
    conn_type = "duckdb"
    hook_name = "DuckDB"
    placeholder = "?"
    supports_autocommit = False

    @property
    def database_path(self) -> str:
        connection = self.get_connection(self.get_conn_id())
        return connection.extra_dejson.get("path") or connection.host or ":memory:"

    def get_conn(self) -> Any:
        return duckdb.connect(self.database_path)

    def get_uri(self) -> str:
        return f"duckdb:///{self.database_path}"

    @property
    def dialect_name(self) -> str:
        return "duckdb"

    @property
    def inspector(self) -> _DuckDBInspector:
        return _DuckDBInspector(self)

    def get_table_schema(
        self, table_name: str, schema: str | None = None
    ) -> list[dict[str, str]]:
        sql = (
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ?"
        )
        parameters: list[Any] = [table_name]
        if schema:
            sql += " AND table_schema = ?"
            parameters.append(schema)
        sql += " ORDER BY ordinal_position"

        return [
            {"name": row[0], "type": row[1]}
            for row in self.get_records(sql, parameters=parameters)
        ]
