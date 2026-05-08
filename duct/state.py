"""SQLite-backed state store for tracking pipeline task execution."""

import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timezone


class StateStore:
    """Thread-safe SQLite state store for task run status.

    Persists task execution state across pipeline runs, tracking status,
    duration, and attempt counts in a WAL-mode SQLite database.

    Attributes:
        db_path: Path to the SQLite database file.
    """
    _create_table_sql = """
    CREATE TABLE IF NOT EXISTS task_runs (
        run_id TEXT,
        task_name TEXT,
        status TEXT,
        started_at TEXT,
        finished_at TEXT,
        duration_ms REAL,
        attempts INTEGER DEFAULT 0,
        PRIMARY KEY (run_id, task_name)
    );
    """

    def __init__(self, db_path: Path):
        """Initialize the state store and create the schema if needed.

        Configures WAL journal mode, normal synchronous writes, and a
        64MB cache for reasonable performance.
        """
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA cache_size=-64000;")
        self._conn.execute(self._create_table_sql)
        self._conn.commit()

    def set_status(self, run_id: str, task_name: str, status: str,
                   duration_ms: float = 0.0, attempts: int = 0) -> None:
        """Update or insert task run status.

        Uses INSERT OR REPLACE for RUNNING and terminal statuses alike.
        All timestamps are in UTC ISO format.

        Args:
            run_id: Unique run identifier.
            task_name: Name of the task.
            status: One of "RUNNING", "SUCCESS", "FAILED".
            duration_ms: Execution time in milliseconds.
            attempts: Number of attempts made.
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO task_runs"
                "(run_id, task_name, status, started_at, finished_at, duration_ms, attempts)"
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (run_id, task_name, status,
                 now if status == "RUNNING" else None,
                 now if status != "RUNNING" else None,
                 duration_ms if status != "RUNNING" else 0,
                 attempts),
            )
            self._conn.commit()

    def is_completed(self, run_id: str, task_name: str) -> bool:
        """Check whether a task completed successfully in a given run.

        Args:
            run_id: Unique run identifier.
            task_name: Name of the task.

        Returns:
            True if the task has a SUCCESS status in this run, False otherwise.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM task_runs WHERE run_id = ? AND task_name = ?",
                (run_id, task_name),
            ).fetchone()
        return row is not None and row[0] == "SUCCESS"

    def get_all_task_statuses(self, run_id: str):
        """Retrieve all task statuses for a given run.

        Args:
            run_id: Unique run identifier.

        Returns:
            List of (task_name, status, duration_ms, attempts) tuples.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_name, status, duration_ms, attempts FROM task_runs WHERE run_id = ?",
                (run_id,)
            ).fetchall()
        return rows

    def close(self):
        """Close the database connection and release resources."""
        self._conn.close()