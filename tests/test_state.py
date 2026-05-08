"""Unit tests for duct.state module."""

import os
import tempfile
import unittest
from pathlib import Path
from duct.state import StateStore


class TestStateStore(unittest.TestCase):
    def setUp(self):
        self.db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = Path(self.db.name)
        self.db.close()
        self.store = StateStore(self.db_path)

    def tearDown(self):
        self.store.close()
        self.db_path.unlink()

    def test_initial_status(self):
        result = self.store.is_completed("run1", "task1")
        self.assertFalse(result)

    def test_set_running(self):
        self.store.set_status("run1", "task1", "RUNNING", attempts=1)
        self.assertFalse(self.store.is_completed("run1", "task1"))

    def test_set_success(self):
        self.store.set_status("run1", "task1", "SUCCESS", duration_ms=42.5, attempts=1)
        self.assertTrue(self.store.is_completed("run1", "task1"))

    def test_set_failed(self):
        self.store.set_status("run1", "task1", "FAILED", duration_ms=100.0, attempts=3)
        self.assertFalse(self.store.is_completed("run1", "task1"))

    def test_replaces_running_on_reexecute(self):
        self.store.set_status("run1", "task1", "RUNNING", attempts=1)
        self.store.set_status("run1", "task1", "RUNNING", attempts=2)
        statuses = self.store.get_all_task_statuses("run1")
        self.assertEqual(len(statuses), 1)
        name, status, duration, attempts = statuses[0]
        self.assertEqual(attempts, 2)

    def test_multiple_runs_independent(self):
        self.store.set_status("run1", "task1", "SUCCESS", attempts=1)
        self.store.set_status("run2", "task1", "RUNNING", attempts=1)
        self.assertTrue(self.store.is_completed("run1", "task1"))
        self.assertFalse(self.store.is_completed("run2", "task1"))

    def test_get_all_task_statuses(self):
        self.store.set_status("run1", "task_a", "SUCCESS", duration_ms=10.0, attempts=1)
        self.store.set_status("run1", "task_b", "FAILED", duration_ms=50.0, attempts=3)
        self.store.set_status("run1", "task_c", "RUNNING", attempts=2)
        rows = self.store.get_all_task_statuses("run1")
        self.assertEqual(len(rows), 3)

    def test_get_all_task_statuses_empty_run(self):
        rows = self.store.get_all_task_statuses("nonexistent_run")
        self.assertEqual(rows, [])

    def test_concurrent_writes(self):
        import threading
        errors = []

        def writer(run_id):
            try:
                for i in range(20):
                    self.store.set_status(run_id, f"task_{i}", "SUCCESS", attempts=1)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer, args=("concurrent_1",))
        t2 = threading.Thread(target=writer, args=("concurrent_2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(self.store.get_all_task_statuses("concurrent_1")), 20)
        self.assertEqual(len(self.store.get_all_task_statuses("concurrent_2")), 20)

    def test_different_tasks_same_run(self):
        for name in ["alpha", "beta", "gamma"]:
            self.store.set_status("run_x", name, "SUCCESS", attempts=1)
        rows = self.store.get_all_task_statuses("run_x")
        self.assertEqual(len(rows), 3)
        names = {r[0] for r in rows}
        self.assertEqual(names, {"alpha", "beta", "gamma"})


if __name__ == "__main__":
    unittest.main()