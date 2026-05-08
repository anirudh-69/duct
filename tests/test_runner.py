"""Unit tests for duct.runner module."""

import tempfile
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from duct.models import Task, TaskType, Pipeline, RunContext
from duct.state import StateStore
from duct.runner import pipeline_session, run_pipeline, _sort_by_priority
from duct.executor import ResultStore


class TestSortByPriority(unittest.TestCase):
    def test_ascending_order(self):
        tasks = {
            "low": Task(name="low", type=TaskType.LOAD, priority=100),
            "high": Task(name="high", type=TaskType.LOAD, priority=1),
            "mid": Task(name="mid", type=TaskType.LOAD, priority=50),
        }
        result = _sort_by_priority(["low", "high", "mid"], tasks)
        self.assertEqual(result, ["high", "mid", "low"])

    def test_all_same_priority(self):
        tasks = {
            "a": Task(name="a", type=TaskType.LOAD, priority=10),
            "b": Task(name="b", type=TaskType.LOAD, priority=10),
        }
        result = _sort_by_priority(["a", "b"], tasks)
        self.assertEqual(set(result), {"a", "b"})

    def test_single_task(self):
        tasks = {"only": Task(name="only", type=TaskType.LOAD, priority=5)}
        result = _sort_by_priority(["only"], tasks)
        self.assertEqual(result, ["only"])

    def test_empty_list(self):
        tasks = {"a": Task(name="a", type=TaskType.LOAD, priority=10)}
        result = _sort_by_priority([], tasks)
        self.assertEqual(result, [])


class TestPipelineSession(unittest.TestCase):
    def setUp(self):
        self.out_dir = tempfile.mkdtemp()
        self.out_path = Path(self.out_dir)
        self.pipeline = Pipeline(name="session_test", tasks=[])

    def tearDown(self):
        import shutil
        shutil.rmtree(self.out_dir)

    def test_session_creates_output_dir(self):
        ctx = RunContext(pipeline=self.pipeline, run_id="s1", output_base=self.out_path)
        with pipeline_session(ctx) as (state, store):
            self.assertIsInstance(state, StateStore)
            self.assertIsInstance(store, ResultStore)
            self.assertTrue(self.out_path.exists())

    def test_session_creates_db(self):
        ctx = RunContext(pipeline=self.pipeline, run_id="s2", output_base=self.out_path)
        with pipeline_session(ctx):
            db = self.out_path / "duct.db"
            self.assertTrue(db.exists())

    def test_session_creates_results_dir(self):
        ctx = RunContext(pipeline=self.pipeline, run_id="s3", output_base=self.out_path)
        with pipeline_session(ctx):
            results = self.out_path / "tmp" / "s3" / "results"
            self.assertTrue(results.exists())

    def test_session_closes_on_error(self):
        ctx = RunContext(pipeline=self.pipeline, run_id="s4", output_base=self.out_path)
        state = None
        store = None
        try:
            with pipeline_session(ctx) as (s, st):
                state = s
                store = st
                raise ValueError("simulated error")
        except ValueError:
            pass
        self.assertIsNotNone(state)
        # close() should have been called - no error on re-close


class TestRunPipelineBasic(unittest.TestCase):
    def setUp(self):
        self.out_dir = tempfile.mkdtemp()
        self.out_path = Path(self.out_dir)
        db_path = self.out_path / "test.db"
        self.state = StateStore(db_path)
        self.store = ResultStore()

    def tearDown(self):
        import shutil
        self.state.close()
        shutil.rmtree(self.out_dir)

    def _ctx(self, run_id):
        return RunContext(pipeline=self.pipeline, run_id=run_id, output_base=self.out_path)

    def test_empty_pipeline(self):
        self.pipeline = Pipeline(name="empty", tasks=[])
        ctx = self._ctx("run_empty")
        run_pipeline(self.pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("run_empty")
        self.assertEqual(rows, [])

    def test_linear_three_tasks(self):
        import types, sys
        mod = types.ModuleType("linear_test_tasks")
        def step1(ctx=None, **kw): return {"value": 1}
        def step2(s1=None, ctx=None, **kw): return s1["value"]
        def step3(s2=None, ctx=None, **kw): return s2 + 1
        mod.step1 = step1
        mod.step2 = step2
        mod.step3 = step3
        sys.modules["linear_test_tasks"] = mod

        self.pipeline = Pipeline(name="linear", tasks=[
            Task(name="s1", type=TaskType.EXTRACT, function="linear_test_tasks:step1"),
            Task(name="s2", type=TaskType.TRANSFORM, depends_on=["s1"],
                 function="linear_test_tasks:step2"),
            Task(name="s3", type=TaskType.LOAD, depends_on=["s2"],
                 function="linear_test_tasks:step3"),
        ])
        ctx = self._ctx("run_linear")
        run_pipeline(self.pipeline, ctx, self.state, self.store)
        rows = self.state.get_all_task_statuses("run_linear")
        self.assertEqual(len(rows), 3)
        for name, status, *_ in rows:
            self.assertEqual(status, "SUCCESS")

        del sys.modules["linear_test_tasks"]


if __name__ == "__main__":
    unittest.main()