"""DuckDB connection manager with automatic migration runner."""

import os
import threading
from pathlib import Path

import duckdb

_lock = threading.Lock()
_connection: duckdb.DuckDBPyConnection | None = None

DB_PATH = Path(os.getenv("HOUSEFLIP_DB_PATH", "data/houseflip.duckdb"))
MIGRATIONS_DIR = Path(__file__).parent / "migrations"


class Database:
    """Thin wrapper around a shared DuckDB connection."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def execute(self, query: str, params: list | None = None) -> duckdb.DuckDBPyRelation:
        with _lock:
            if params:
                return self._conn.execute(query, params)
            return self._conn.execute(query)

    def executemany(self, query: str, params: list[list]) -> None:
        with _lock:
            self._conn.executemany(query, params)

    def close(self) -> None:
        self._conn.close()


def _apply_migrations(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    VARCHAR PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    applied: set[str] = {
        row[0] for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
    }
    sql_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    for sql_file in sql_files:
        version = sql_file.stem
        if version not in applied:
            sql = sql_file.read_text(encoding="utf-8")
            conn.execute(sql)
            conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", [version])


def get_db() -> Database:
    """Return the shared Database instance, creating it on first call."""
    global _connection
    with _lock:
        if _connection is None:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            _connection = duckdb.connect(str(DB_PATH))
            _apply_migrations(_connection)
    return Database(_connection)
