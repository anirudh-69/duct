import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timezone

class StateStore:
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
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA cache_size=-64000;")
        self._conn.execute(self._create_table_sql)
        self._conn.commit()
    
    def set_status(self, run_id: str, task_name: str, status: str,
                   duration_ms: float = 0.0, attempts: int = 0) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            if status == "RUNNING":
                self._conn.execute(
                    "INSERT OR REPLACE INTO task_runs"
                    "(run_id, task_name, status, started_at, finished_at, duration_ms, attempts)"
                    "VALUES (?, ?, ?, ?, NULL, 0, ?)",
                    (run_id, task_name, status, now, attempts),
                )
            else:
                self._conn.execute(
                    "UPDATE task_runs SET status = ?, finished_at = ?, duration_ms = ?"
                    "WHERE run_id = ? AND task_name = ?",
                    (status, now, duration_ms, run_id, task_name),
                )
            self._conn.commit()
    
    def is_completed(self, run_id: str, task_name: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM task_runs WHERE run_id = ? AND task_name = ?",
                (run_id, task_name),
            ).fetchone()
        return row is not None and row[0] == "SUCCESS"
    
    def get_all_task_statuses(self, run_id: str) -> list[tuple]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_name, status, duration_ms, attempts FROM task_runs WHERE run_id = ?",
                (run_id,)
            ).fetchall()
        return rows

    def close(self):
        self._conn.close()