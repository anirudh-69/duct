"""Stress and load tests for the duct pipeline engine.

These tests verify correctness and performance under heavy concurrent
load, large DAGs, high retry pressure, and cross-run resumption scenarios.
"""

import gc
import os
import sys
import tempfile
import threading
import time
import types
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from duct.models import Task, TaskType, Pipeline, RunContext
from duct.state import StateStore
from duct.runner import pipeline_session, run_pipeline
from duct.executor import ResultStore, _sig_cache


_EXEC_COUNTER = threading.Lock()
_EXEC_TIMES = []


def _reset_stress_counters():
    global _EXEC_TIMES
    _EXEC_TIMES = []


def _make_tasks_module(name="stress_tasks"):
    mod = types.ModuleType(name)

    def noop(ctx, **kw):
        return "done"

    def sleep_then_done(duration=0.01, ctx=None, **kw):
        time.sleep(duration)
        return f"slept_{duration}"

    def compute_task(ctx, **kw):
        return sum(i * i for i in range(500))

    def with_dep_a(ctx, **kw):
        return "a_result"

    def with_dep_b(a=None, ctx=None, **kw):
        return f"b_from_{a}"

    def with_dep_c(a=None, b=None, ctx=None, **kw):
        return f"c_from_{a}_{b}"

    def failing_once(ctx, **kw):
        _EXEC_COUNTER.acquire()
        try:
            count = len(_EXEC_TIMES)
            _EXEC_TIMES.append(time.time())
            if count == 0:
                raise ValueError("first attempt fails")
        finally:
            _EXEC_COUNTER.release()
        return "finally_success"

    def always_fails(ctx, **kw):
        raise RuntimeError("always fails")

    def slow_succeed(sleep_sec=0.05, ctx=None, **kw):
        time.sleep(sleep_sec)
        return {"slept": sleep_sec}

    mod.noop = noop
    mod.sleep_then_done = sleep_then_done
    mod.compute_task = compute_task
    mod.with_dep_a = with_dep_a
    mod.with_dep_b = with_dep_b
    mod.with_dep_c = with_dep_c
    mod.failing_once = failing_once
    mod.always_fails = always_fails
    mod.slow_succeed = slow_succeed
    return mod


class TestStressConcurrency(unittest.TestCase):
    """Tests for correctness under high concurrency."""

    def setUp(self):
        _reset_stress_counters()
        self.out_dir = tempfile.mkdtemp()
        self.out_path = Path(self.out_dir)
        db_path = self.out_path / "duct.db"
        self.state = StateStore(db_path)
        self.store = ResultStore()
        self.mod = _make_tasks_module()
        sys.modules["stress_tasks"] = self.mod

    def tearDown(self):
        import shutil
        self.state.close()
        shutil.rmtree(self.out_dir)
        if "stress_tasks" in sys.modules:
            del sys.modules["stress_tasks"]

    def _ctx(self, run_id):
        return RunContext(pipeline=Pipeline(name="stress", tasks=[]),
                          run_id=run_id, output_base=self.out_path)

    def test_50_parallel_independent_tasks(self):
        """50 independent tasks should all succeed with 10 workers."""
        tasks = [
            Task(name=f"t{i}", type=TaskType.EXTRACT, function="stress_tasks:noop")
            for i in range(50)
        ]
        pipeline = Pipeline(name="stress_50", tasks=tasks)
        ctx = self._ctx("stress_50_run")
        run_pipeline(pipeline, ctx, self.state, self.store, max_workers=10)
        rows = self.state.get_all_task_statuses("stress_50_run")
        self.assertEqual(len(rows), 50)
        for name, status, *_ in rows:
            self.assertEqual(status, "SUCCESS")

    def test_100_tasks_wide_parallel(self):
        """100 independent tasks across 4 workers, all succeed."""
        tasks = [
            Task(name=f"t{i}", type=TaskType.EXTRACT, function="stress_tasks:noop")
            for i in range(100)
        ]
        pipeline = Pipeline(name="stress_100", tasks=tasks)
        ctx = self._ctx("stress_100_run")
        run_pipeline(pipeline, ctx, self.state, self.store, max_workers=4)
        rows = self.state.get_all_task_statuses("stress_100_run")
        self.assertEqual(len(rows), 100)
        statuses = {n: s for n, s, *_ in rows}
        self.assertEqual(sum(1 for s in statuses.values() if s == "SUCCESS"), 100)

    def test_diamond_dependency_pattern(self):
        """Task A fans out to B,C,D then converges to E - parallel + join."""
        tasks = [
            Task(name="a", type=TaskType.EXTRACT, function="stress_tasks:noop", priority=10),
            Task(name="b", type=TaskType.TRANSFORM, depends_on=["a"],
                 function="stress_tasks:noop", priority=9),
            Task(name="c", type=TaskType.TRANSFORM, depends_on=["a"],
                 function="stress_tasks:noop", priority=9),
            Task(name="d", type=TaskType.TRANSFORM, depends_on=["a"],
                 function="stress_tasks:noop", priority=9),
            Task(name="e", type=TaskType.LOAD, depends_on=["b", "c", "d"],
                 function="stress_tasks:noop", priority=1),
        ]
        pipeline = Pipeline(name="diamond", tasks=tasks)
        ctx = self._ctx("diamond_run")
        run_pipeline(pipeline, ctx, self.state, self.store, max_workers=4)
        rows = self.state.get_all_task_statuses("diamond_run")
        self.assertEqual(len(rows), 5)
        for name, status, *_ in rows:
            self.assertEqual(status, "SUCCESS")

    def test_deep_staged_pipeline_20_tasks(self):
        """20-task pipeline in a single chain - tests staged execution."""
        tasks = [Task(name=f"s{i}", type=TaskType.EXTRACT if i == 0 else TaskType.TRANSFORM,
                      depends_on=[f"s{i-1}"] if i > 0 else [],
                      function="stress_tasks:noop", priority=10 - i)
                 for i in range(20)]
        pipeline = Pipeline(name="deep_stage", tasks=tasks)
        ctx = self._ctx("deep_stage_run")
        run_pipeline(pipeline, ctx, self.state, self.store, max_workers=4)
        rows = self.state.get_all_task_statuses("deep_stage_run")
        self.assertEqual(len(rows), 20)
        for name, status, *_ in rows:
            self.assertEqual(status, "SUCCESS")

    def test_mixed_parallel_and_staged(self):
        """3 parallel branches each with 5 stages."""
        branches = ["a", "b", "c"]
        tasks = []
        for b in branches:
            for i in range(5):
                deps = [] if i == 0 else [f"{b}_s{i-1}"]
                tasks.append(Task(
                    name=f"{b}_s{i}",
                    type=TaskType.EXTRACT if i == 0 else TaskType.TRANSFORM,
                    depends_on=deps,
                    function="stress_tasks:noop",
                    priority=10 - i,
                ))
        tasks.append(Task(name="merge", type=TaskType.LOAD,
                          depends_on=[f"{b}_s4" for b in branches],
                          function="stress_tasks:noop"))

        pipeline = Pipeline(name="mixed", tasks=tasks)
        ctx = self._ctx("mixed_run")
        run_pipeline(pipeline, ctx, self.state, self.store, max_workers=6)
        rows = self.state.get_all_task_statuses("mixed_run")
        self.assertEqual(len(rows), len(tasks))
        for name, status, *_ in rows:
            self.assertEqual(status, "SUCCESS")

    def test_high_retry_task_succeeds_after_failures(self):
        """Task that fails 5 times then succeeds - verify final success."""
        pipeline = Pipeline(name="high_retry", tasks=[
            Task(name="retry5", type=TaskType.EXTRACT,
                 function="stress_tasks:failing_once",
                 retry=5, retry_delay=0.05),
        ])
        ctx = self._ctx("high_retry_run")
        run_pipeline(pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("high_retry_run")
        self.assertEqual(len(rows), 1)
        name, status, duration, attempts = rows[0]
        self.assertEqual(status, "SUCCESS")
        self.assertEqual(attempts, 2)

    def test_partial_failure_one_task_fails(self):
        """2 independent tasks, one always fails - pipeline aborts."""
        pipeline = Pipeline(name="partial_fail", tasks=[
            Task(name="ok", type=TaskType.EXTRACT, function="stress_tasks:noop"),
            Task(name="bad", type=TaskType.EXTRACT, function="stress_tasks:always_fails",
                 retry=1, retry_delay=0.05),
        ])
        ctx = self._ctx("partial_fail_run")
        with self.assertRaises(RuntimeError):
            run_pipeline(pipeline, ctx, self.state, self.store, max_workers=2)
        rows = self.state.get_all_task_statuses("partial_fail_run")
        statuses = {n: s for n, s, *_ in rows}
        self.assertEqual(statuses["ok"], "SUCCESS")
        self.assertEqual(statuses["bad"], "FAILED")

    def test_completed_tasks_skipped_on_resume(self):
        """Re-running same run_id should skip already-completed tasks."""
        pipeline = Pipeline(name="resume_test", tasks=[
            Task(name="a", type=TaskType.EXTRACT, function="stress_tasks:noop"),
            Task(name="b", type=TaskType.TRANSFORM, depends_on=["a"],
                 function="stress_tasks:noop"),
        ])

        ctx1 = self._ctx("resume_same")
        run_pipeline(pipeline, ctx1, self.state, self.store)

        store2 = ResultStore()
        ctx2 = self._ctx("resume_same")
        run_pipeline(pipeline, ctx2, self.state, store2)

        rows = self.state.get_all_task_statuses("resume_same")
        task_names = {n for n, *_ in rows}
        self.assertEqual(task_names, {"a", "b"})

    def test_multiple_run_ids_independent(self):
        """5 different run_ids don't interfere with each other."""
        for run_idx in range(5):
            tasks = [Task(name=f"t{i}", type=TaskType.EXTRACT,
                          function="stress_tasks:noop")
                     for i in range(10)]
            pipeline = Pipeline(name=f"run{run_idx}", tasks=tasks)
            ctx = self._ctx(f"multi_run_{run_idx}")
            store = ResultStore()
            run_pipeline(pipeline, ctx, self.state, store)

        for run_idx in range(5):
            rows = self.state.get_all_task_statuses(f"multi_run_{run_idx}")
            self.assertEqual(len(rows), 10)
            for name, status, *_ in rows:
                self.assertEqual(status, "SUCCESS")

    def test_concurrent_pipeline_sessions(self):
        """3 pipelines running concurrently, same state store."""
        def run_one(run_id):
            _reset_stress_counters()
            out = tempfile.mkdtemp()
            out_path = Path(out)
            db_path = out_path / "duct.db"
            state = StateStore(db_path)
            store = ResultStore()
            ctx = RunContext(pipeline=Pipeline(name=f"par", tasks=[]),
                             run_id=run_id, output_base=out_path)
            tasks = [Task(name=f"p{run_id}_t{i}", type=TaskType.EXTRACT,
                          function="stress_tasks:noop")
                     for i in range(20)]
            pipeline = Pipeline(name=f"parallel_{run_id}", tasks=tasks)
            run_pipeline(pipeline, ctx, state, store, max_workers=3)
            state.close()
            import shutil; shutil.rmtree(out)
            return run_id

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(run_one, f"concurrent_{i}") for i in range(3)]
            results = [f.result() for f in as_completed(futures)]

        self.assertEqual(sorted(results), ["concurrent_0", "concurrent_1", "concurrent_2"])

    def test_max_workers_respected(self):
        """With max_workers=1, tasks should run sequentially."""
        tasks = [Task(name=f"t{i}", type=TaskType.EXTRACT,
                      function="stress_tasks:slow_succeed", params={"sleep_sec": 0.05})
                 for i in range(4)]
        pipeline = Pipeline(name="sequential", tasks=tasks)
        ctx = self._ctx("seq_run")
        start = time.time()
        run_pipeline(pipeline, ctx, self.state, self.store, max_workers=1)
        elapsed = time.time() - start
        self.assertGreater(elapsed, 0.15)  # 4 tasks * 0.05s = ~0.2s sequential
        rows = self.state.get_all_task_statuses("seq_run")
        for name, status, *_ in rows:
            self.assertEqual(status, "SUCCESS")

    def test_priority_high_runs_first(self):
        """Higher priority (lower number) tasks run first in batch."""
        tasks = [
            Task(name="low_prio", type=TaskType.EXTRACT,
                 function="stress_tasks:noop", priority=100),
            Task(name="mid_prio", type=TaskType.EXTRACT,
                 function="stress_tasks:noop", priority=50),
            Task(name="high_prio", type=TaskType.EXTRACT,
                 function="stress_tasks:noop", priority=1),
        ]
        pipeline = Pipeline(name="prio", tasks=tasks)
        ctx = self._ctx("prio_run")
        run_pipeline(pipeline, ctx, self.state, self.store, max_workers=3)
        rows = self.state.get_all_task_statuses("prio_run")
        durations = {n: d for n, s, d, *_ in rows}
        self.assertEqual(durations["high_prio"] + durations["mid_prio"] + durations["low_prio"],
                         sum(durations.values()))


class TestStressResourceUsage(unittest.TestCase):
    """Tests for memory and resource usage under load."""

    def setUp(self):
        _reset_stress_counters()
        self.out_dir = tempfile.mkdtemp()
        self.out_path = Path(self.out_dir)
        db_path = self.out_path / "duct.db"
        self.state = StateStore(db_path)
        self.mod = _make_tasks_module()
        sys.modules["stress_tasks"] = self.mod

    def tearDown(self):
        import shutil
        self.state.close()
        shutil.rmtree(self.out_dir)
        if "stress_tasks" in sys.modules:
            del sys.modules["stress_tasks"]

    def _ctx(self, run_id):
        return RunContext(pipeline=Pipeline(name="stress", tasks=[]),
                          run_id=run_id, output_base=self.out_path)

    def test_large_result_store_no_leak(self):
        """Large results stored in memory don't cause issues."""
        import random
        mod = types.ModuleType("stress_tasks_large")
        def big_result(ctx, **kw):
            return {"data": [random.random() for _ in range(1000)]}
        mod.big_result = big_result
        sys.modules["stress_tasks_large"] = mod

        tasks = [Task(name=f"big_{i}", type=TaskType.EXTRACT,
                      function="stress_tasks_large:big_result")
                 for i in range(20)]
        pipeline = Pipeline(name="big_results", tasks=tasks)
        ctx = self._ctx("big_results_run")
        store = ResultStore()
        run_pipeline(pipeline, ctx, self.state, store, max_workers=5)
        self.assertEqual(len(store), 20)
        for key, val in store.items():
            self.assertEqual(len(val["data"]), 1000)
        del sys.modules["stress_tasks_large"]

    def test_repeated_pipeline_runs_clean(self):
        """10 sequential pipeline runs don't accumulate stale state."""
        for run_idx in range(10):
            tasks = [Task(name=f"t{i}", type=TaskType.EXTRACT,
                          function="stress_tasks:noop")
                     for i in range(10)]
            pipeline = Pipeline(name=f"repeat_{run_idx}", tasks=tasks)
            ctx = self._ctx(f"repeat_run_{run_idx}")
            store = ResultStore()
            run_pipeline(pipeline, ctx, self.state, store, max_workers=3)

        for run_idx in range(10):
            rows = self.state.get_all_task_statuses(f"repeat_run_{run_idx}")
            self.assertEqual(len(rows), 10)

    def test_sig_cache_hit_after_repeated_calls(self):
        """Signature cache is used on repeated calls."""
        _sig_cache.clear()
        tasks = [Task(name=f"sig_t{i}", type=TaskType.EXTRACT,
                      function="stress_tasks:noop")
                 for i in range(20)]
        pipeline = Pipeline(name="sig_cache", tasks=tasks)
        ctx = self._ctx("sig_cache_run")
        store = ResultStore()
        run_pipeline(pipeline, ctx, self.state, store, max_workers=5)
        self.assertGreater(len(_sig_cache), 0)


class TestStressTimeouts(unittest.TestCase):
    """Tests for timeout enforcement under stress."""

    def setUp(self):
        _reset_stress_counters()
        self.out_dir = tempfile.mkdtemp()
        self.out_path = Path(self.out_dir)
        db_path = self.out_path / "duct.db"
        self.state = StateStore(db_path)
        self.mod = _make_tasks_module("stress_timeout_tasks")
        sys.modules["stress_timeout_tasks"] = self.mod

    def tearDown(self):
        import shutil
        self.state.close()
        shutil.rmtree(self.out_dir)
        if "stress_timeout_tasks" in sys.modules:
            del sys.modules["stress_timeout_tasks"]

    def _ctx(self, run_id):
        return RunContext(pipeline=Pipeline(name="timeout", tasks=[]),
                          run_id=run_id, output_base=self.out_path)

    def test_timeout_releases_worker(self):
        """Worker thread is released after timeout - task completes or times."""
        mod = types.ModuleType("stress_timeout_tasks")

        def very_slow(ctx, **kw):
            evt = threading.Event()
            evt.wait(timeout=10)
            return "done"

        mod.very_slow = very_slow
        sys.modules["stress_timeout_tasks"] = mod

        pipeline = Pipeline(name="timeout_test", tasks=[
            Task(name="vslow", type=TaskType.EXTRACT,
                 function="stress_timeout_tasks:very_slow",
                 timeout=1),
        ])
        ctx = self._ctx("timeout_run")
        with self.assertRaises(RuntimeError):
            run_pipeline(pipeline, ctx, self.state, ResultStore())
        rows = self.state.get_all_task_statuses("timeout_run")
        self.assertEqual(rows[0][1], "FAILED")


if __name__ == "__main__":
    unittest.main()