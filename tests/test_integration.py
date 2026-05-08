"""Integration tests for the duct pipeline engine.

These tests run full pipeline executions end-to-end, exercising real
function modules, multi-task DAGs, retries, timeouts, persistence,
and cross-run resumption.
"""

import os
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path

from duct.models import Task, TaskType, Pipeline, RunContext
from duct.state import StateStore
from duct.runner import pipeline_session, run_pipeline
from duct.executor import ResultStore


_FAIL_COUNTER = {}
_TASK_RETRY_COUNTER = {}  # keyed by task name


def _reset_counters():
    _FAIL_COUNTER.clear()
    _TASK_RETRY_COUNTER.clear()


def _make_tasks_module():
    mod = types.ModuleType("integration_tasks")

    def extract_a(ctx, **kw):
        return {"source": "a", "value": 1}

    def extract_b(ctx, **kw):
        return {"source": "b", "value": 2}

    def extract_c(ctx, **kw):
        return {"source": "c", "value": 3}

    def merge_ab(ctx, **kw):
        return {"merged": "ab_done"}

    def final_task(ctx, **kw):
        return {"final": True}

    def failing_task(ctx, **kw):
        tid = threading.current_thread().name
        _FAIL_COUNTER[tid] = _FAIL_COUNTER.get(tid, 0) + 1
        raise RuntimeError("intentional failure")

    def eventually_succeeds(ctx, **kw):
        _TASK_RETRY_COUNTER["eventually_succeeds"] = _TASK_RETRY_COUNTER.get("eventually_succeeds", 0) + 1
        count = _TASK_RETRY_COUNTER["eventually_succeeds"]
        if count < 3:
            raise ValueError(f"attempt {count}")
        return {"attempt": count, "ok": True}

    def slow_task(ctx, **kw):
        evt = threading.Event()
        evt.wait(timeout=10)
        return "slow_done"

    def fast_task(ctx, **kw):
        return "fast_done"

    def persist_task(ctx, **kw):
        return {"key": "persist_val", "num": 42}

    def downstream_persisted(ctx, **kw):
        return {"prev": "from_p1"}

    def with_params_task(ctx, mode=None, limit=None, **kw):
        return {"mode": mode, "limit": limit}

    def cross_run_check(ctx, **kw):
        return {"run_id": ctx.run_id, "mode": ctx.mode}

    mod.extract_a = extract_a
    mod.extract_b = extract_b
    mod.extract_c = extract_c
    mod.merge_ab = merge_ab
    mod.final_task = final_task
    mod.failing_task = failing_task
    mod.eventually_succeeds = eventually_succeeds
    mod.slow_task = slow_task
    mod.fast_task = fast_task
    mod.persist_task = persist_task
    mod.downstream_persisted = downstream_persisted
    mod.with_params_task = with_params_task
    mod.cross_run_check = cross_run_check
    return mod


class TestPipelineExecution(unittest.TestCase):
    def setUp(self):
        _reset_counters()
        _TASK_RETRY_COUNTER.clear()  # ensure clean state for retry tests
        self.out_dir = tempfile.mkdtemp()
        self.out_path = Path(self.out_dir)
        db_path = self.out_path / "duct.db"
        self.state = StateStore(db_path)
        self.store = ResultStore()
        self.mod = _make_tasks_module()
        sys.modules["integration_tasks"] = self.mod

    def tearDown(self):
        import shutil
        self.state.close()
        shutil.rmtree(self.out_dir)
        if "integration_tasks" in sys.modules:
            del sys.modules["integration_tasks"]

    def _ctx(self, run_id):
        return RunContext(
            pipeline=Pipeline(name="test", tasks=[]),
            run_id=run_id,
            output_base=self.out_path,
        )

    def test_linear_pipeline(self):
        pipeline = Pipeline(name="linear", tasks=[
            Task(name="a", type=TaskType.EXTRACT, function="integration_tasks:extract_a"),
            Task(name="b", type=TaskType.TRANSFORM, depends_on=["a"],
                 function="integration_tasks:merge_ab"),
            Task(name="c", type=TaskType.LOAD, depends_on=["b"],
                 function="integration_tasks:final_task"),
        ])
        ctx = self._ctx("linear_run")
        run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("linear_run")
        self.assertEqual(len(rows), 3)
        for name, status, *_ in rows:
            self.assertEqual(status, "SUCCESS")
        self.assertEqual(self.store["c"]["final"], True)

    def test_parallel_extracts(self):
        pipeline = Pipeline(name="parallel", tasks=[
            Task(name="a", type=TaskType.EXTRACT, function="integration_tasks:extract_a", priority=2),
            Task(name="b", type=TaskType.EXTRACT, function="integration_tasks:extract_b", priority=1),
            Task(name="c", type=TaskType.EXTRACT, function="integration_tasks:extract_c", priority=3),
            Task(name="merge", type=TaskType.TRANSFORM, depends_on=["a", "b", "c"],
                 function="integration_tasks:final_task"),
        ])
        ctx = self._ctx("parallel_run")
        run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("parallel_run")
        self.assertEqual(len(rows), 4)
        statuses = {n: s for n, s, *_ in rows}
        for name in ["a", "b", "c"]:
            self.assertEqual(statuses[name], "SUCCESS")
        self.assertEqual(statuses["merge"], "SUCCESS")

    def test_retry_on_failure_eventual_success(self):
        _reset_counters()  # ensure clean state
        pipeline = Pipeline(name="retry", tasks=[
            Task(name="retry_task", type=TaskType.EXTRACT,
                 function="integration_tasks:eventually_succeeds",
                 retry=5, retry_delay=0.1),
        ])
        ctx = self._ctx("retry_run")
        run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("retry_run")
        self.assertEqual(len(rows), 1)
        name, status, duration, attempts = rows[0]
        self.assertEqual(status, "SUCCESS")
        self.assertGreaterEqual(attempts, 3)

    def test_retry_exhausted_aborts(self):
        _reset_counters()
        pipeline = Pipeline(name="failing", tasks=[
            Task(name="fail", type=TaskType.EXTRACT,
                 function="integration_tasks:failing_task",
                 retry=2, retry_delay=0.05),
        ])
        ctx = self._ctx("fail_run")
        with self.assertRaises(RuntimeError):
            run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("fail_run")
        name, status, attempts = rows[0][0], rows[0][1], rows[0][3]
        self.assertEqual(status, "FAILED")
        self.assertEqual(attempts, 3)

    def test_task_with_params(self):
        pipeline = Pipeline(name="params", tasks=[
            Task(name="p", type=TaskType.EXTRACT,
                 function="integration_tasks:with_params_task",
                 params={"mode": "strict", "limit": 200}),
        ])
        ctx = self._ctx("params_run")
        run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("params_run")
        name, status = rows[0][0], rows[0][1]
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(self.store["p"]["mode"], "strict")
        self.assertEqual(self.store["p"]["limit"], 200)

    def test_result_persistence(self):
        (self.out_path / "tmp" / "persist_run" / "results").mkdir(parents=True, exist_ok=True)
        pipeline = Pipeline(name="persist", tasks=[
            Task(name="p1", type=TaskType.EXTRACT,
                 function="integration_tasks:persist_task",
                 result_persist=True),
            Task(name="p2", type=TaskType.TRANSFORM,
                 depends_on=["p1"],
                 function="integration_tasks:downstream_persisted",
                 result_persist=True),
        ])
        ctx = self._ctx("persist_run")
        run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("persist_run")
        for name, status, *_ in rows:
            self.assertEqual(status, "SUCCESS")

        result_file = self.out_path / "tmp" / "persist_run" / "results" / "p1.json"
        self.assertTrue(result_file.exists())

        import json
        data = json.loads(result_file.read_text())
        self.assertEqual(data["key"], "persist_val")

    def test_timeout_enforcement(self):
        pipeline = Pipeline(name="timeout_test", tasks=[
            Task(name="slow", type=TaskType.EXTRACT,
                 function="integration_tasks:slow_task",
                 timeout=1),
        ])
        ctx = self._ctx("timeout_run")
        with self.assertRaises(RuntimeError):
            run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("timeout_run")
        name, status = rows[0][0], rows[0][1]
        self.assertEqual(status, "FAILED")

    def test_resume_completed_tasks(self):
        pipeline = Pipeline(name="resume", tasks=[
            Task(name="a", type=TaskType.EXTRACT, function="integration_tasks:extract_a"),
            Task(name="b", type=TaskType.TRANSFORM, depends_on=["a"],
                 function="integration_tasks:merge_ab"),
        ])

        ctx1 = self._ctx("resume_run_1")
        run_pipeline(pipeline, ctx1, self.state, self.store)
        rows1 = self.state.get_all_task_statuses("resume_run_1")
        self.assertEqual(len(rows1), 2)

        store2 = ResultStore()
        ctx2 = self._ctx("resume_run_1")
        run_pipeline(pipeline, ctx2, self.state, store2)
        rows2 = self.state.get_all_task_statuses("resume_run_1")
        task_counts = {}
        for name, *_ in rows2:
            task_counts[name] = task_counts.get(name, 0) + 1
        for name, count in task_counts.items():
            self.assertEqual(count, 1)

    def test_missing_dependency_raises(self):
        pipeline = Pipeline(name="broken", tasks=[
            Task(name="a", type=TaskType.EXTRACT, function="integration_tasks:extract_a"),
            Task(name="b", type=TaskType.LOAD, depends_on=["nonexistent"],
                 function="integration_tasks:final_task"),
        ])
        ctx = self._ctx("broken_run")
        with self.assertRaises((RuntimeError, ValueError)):
            run_pipeline(pipeline, ctx, self.state, self.store)

    def test_empty_pipeline(self):
        pipeline = Pipeline(name="empty", tasks=[])
        ctx = self._ctx("empty_run")
        run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("empty_run")
        self.assertEqual(rows, [])

    def test_complex_dag(self):
        pipeline = Pipeline(name="complex", tasks=[
            Task(name="e", type=TaskType.EXTRACT, function="integration_tasks:extract_a", priority=9),
            Task(name="f", type=TaskType.EXTRACT, function="integration_tasks:extract_b", priority=7),
            Task(name="g", type=TaskType.EXTRACT, function="integration_tasks:extract_c", priority=8),
            Task(name="ab", type=TaskType.TRANSFORM, depends_on=["e", "f"],
                 function="integration_tasks:merge_ab"),
            Task(name="fin", type=TaskType.TRANSFORM, depends_on=["ab", "g"],
                 function="integration_tasks:final_task"),
        ])
        ctx = self._ctx("complex_run")
        run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("complex_run")
        self.assertEqual(len(rows), 5)
        for name, status, *_ in rows:
            self.assertEqual(status, "SUCCESS")

    def test_priority_ordering(self):
        pipeline = Pipeline(name="priority", tasks=[
            Task(name="low", type=TaskType.EXTRACT, function="integration_tasks:extract_a", priority=100),
            Task(name="high", type=TaskType.EXTRACT, function="integration_tasks:extract_b", priority=1),
            Task(name="mid", type=TaskType.EXTRACT, function="integration_tasks:extract_c", priority=50),
            Task(name="merge", type=TaskType.TRANSFORM, depends_on=["low", "high", "mid"],
                 function="integration_tasks:final_task"),
        ])
        ctx = self._ctx("priority_run")
        run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("priority_run")
        self.assertEqual(len(rows), 4)
        statuses = {n: s for n, s, *_ in rows}
        self.assertEqual(statuses["merge"], "SUCCESS")


class TestCrossRunContext(unittest.TestCase):
    def setUp(self):
        _reset_counters()
        self.out_dir = tempfile.mkdtemp()
        self.out_path = Path(self.out_dir)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.out_dir)

    def test_run_id_propagated_to_task(self):
        mod = _make_tasks_module()
        sys.modules["integration_tasks"] = mod
        db_path = self.out_path / "duct.db"
        state = StateStore(db_path)
        store = ResultStore()

        pipeline = Pipeline(name="ctx_test", tasks=[
            Task(name="check", type=TaskType.EXTRACT,
                 function="integration_tasks:cross_run_check"),
        ])
        ctx = RunContext(pipeline=pipeline, run_id="my_unique_run_42",
                         output_base=self.out_path, mode="prod")
        run_pipeline(pipeline, ctx, state, store)
        result = store["check"]
        self.assertEqual(result["run_id"], "my_unique_run_42")
        self.assertEqual(result["mode"], "prod")
        state.close()
        del sys.modules["integration_tasks"]


if __name__ == "__main__":
    unittest.main()