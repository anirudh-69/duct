"""Unit tests for duct.executor module."""

import inspect
import tempfile
import unittest
from pathlib import Path
from duct.executor import (
    ResultStore, _resolve_function, _validate_params,
    _persist_result, _load_persisted_result, execute_task,
)
from duct.models import Task, TaskType, Pipeline, RunContext
from duct.state import StateStore


# Test module with various function signatures
module_code = '''
def no_deps(ctx):
    return {"result": "ok"}

def single_dep(dep_a, ctx):
    return {"dep_a": dep_a}

def multi_deps(dep_a, dep_b, ctx):
    return {"a": dep_a, "b": dep_b}

def with_params(mode, limit, ctx):
    return {"mode": mode, "limit": limit}

def with_kwarg(ctx, **kwargs):
    return kwargs

def missing_required(ctx):
    return None

def combine(dep_a, extra_arg=None, ctx=None):
    return {"a": dep_a, "extra": extra_arg}
'''
exec(compile(module_code, "<test_module>", "exec"))


class TestResultStore(unittest.TestCase):
    def test_set_and_get(self):
        store = ResultStore()
        store["key1"] = "value1"
        self.assertEqual(store["key1"], "value1")

    def test_concurrent_set(self):
        import threading
        store = ResultStore()
        results = []

        def writer(key, value):
            store[key] = value

        threads = [threading.Thread(target=writer, args=(f"k{i}", i)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(store), 20)
        self.assertEqual(store["k10"], 10)

    def test_collision_protected(self):
        store = ResultStore()
        store["key"] = "first"
        store["key"] = "second"
        self.assertEqual(store["key"], "second")


class TestResolveFunction(unittest.TestCase):
    def test_colon_separator(self):
        # "os.path:join" -> os.path.join
        fn = _resolve_function("os.path:join")
        import os.path
        self.assertEqual(fn, os.path.join)

    def test_dot_separator(self):
        fn = _resolve_function("json.load")
        import json
        self.assertEqual(fn, json.load)

    def test_nonexistent_module(self):
        with self.assertRaises(ImportError):
            _resolve_function("nonexistent_module:func")

    def test_nonexistent_attribute(self):
        with self.assertRaises(AttributeError):
            _resolve_function("os.path:nonexistent_func")


class TestValidateParams(unittest.TestCase):
    def test_all_params_provided(self):
        sig = inspect.signature(lambda a, b, ctx: None)
        provided = {"a": 1, "b": 2, "ctx": None}
        _validate_params(sig, provided, Task(name="t1", type=TaskType.EXTRACT))

    def test_missing_required(self):
        sig = inspect.signature(lambda a, b, ctx: None)
        provided = {"a": 1, "ctx": None}
        with self.assertRaises(TypeError) as cm:
            _validate_params(sig, provided, Task(name="t1", type=TaskType.EXTRACT))
        self.assertIn("b", str(cm.exception))

    def test_var_keyword_allows_extra(self):
        sig = inspect.signature(lambda a, ctx, **kwargs: None)
        provided = {"a": 1, "b": 2, "ctx": None}
        _validate_params(sig, provided, Task(name="t1", type=TaskType.EXTRACT))

    def test_default_params_not_required(self):
        sig = inspect.signature(lambda a, b=None, ctx=None: None)
        provided = {"a": 1, "ctx": None}
        _validate_params(sig, provided, Task(name="t1", type=TaskType.EXTRACT))


class TestPersistAndLoad(unittest.TestCase):
    def setUp(self):
        self.out_dir = tempfile.mkdtemp()
        self.ctx = RunContext(
            pipeline=Pipeline(name="p", tasks=[]),
            run_id="test_run",
            output_base=Path(self.out_dir),
        )
        self.out_dir_path = Path(self.out_dir)
        (self.out_dir_path / "tmp" / "test_run" / "results").mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.out_dir)

    def test_persist_and_load_json(self):
        data = {"key": "value", "num": 42, "list": [1, 2]}
        path = _persist_result(data, "test_task", self.ctx)
        self.assertTrue(path.exists())
        self.assertEqual(path.suffix, ".json")

        loaded = _load_persisted_result("test_task", self.ctx)
        self.assertEqual(loaded, data)

    def test_persist_none_returns_none(self):
        path = _persist_result(None, "none_task", self.ctx)
        self.assertTrue(path.exists())
        loaded = _load_persisted_result("none_task", self.ctx)
        self.assertIsNone(loaded)

    def test_load_nonexistent_raises(self):
        with self.assertRaises(FileNotFoundError):
            _load_persisted_result("does_not_exist", self.ctx)

    def test_persist_overwrites(self):
        _persist_result({"v": 1}, "overwrite_test", self.ctx)
        _persist_result({"v": 2}, "overwrite_test", self.ctx)
        loaded = _load_persisted_result("overwrite_test", self.ctx)
        self.assertEqual(loaded, {"v": 2})


class TestExecuteTaskIntegration(unittest.TestCase):
    def setUp(self):
        self.out_dir = tempfile.mkdtemp()
        self.out_path = Path(self.out_dir)
        db_path = self.out_path / "test.db"
        self.state = StateStore(db_path)
        self.store = ResultStore()
        self.ctx = RunContext(
            pipeline=Pipeline(name="p", tasks=[]),
            run_id="exec_test",
            output_base=self.out_path,
        )
        (self.out_path / "tmp" / "exec_test" / "results").mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        import shutil
        self.state.close()
        shutil.rmtree(self.out_dir)

    def test_execute_function_resolves_and_runs(self):
        import sys
        import types
        test_module = types.ModuleType("test_exec_module")
        def my_func(ctx):
            return "executed"
        test_module.my_func = my_func
        sys.modules["test_exec_module"] = test_module

        task = Task(name="exec_task", type=TaskType.EXTRACT, function="test_exec_module:my_func")
        result = execute_task(task, self.ctx, self.state, self.store)

        self.assertEqual(result, "executed")
        self.assertEqual(self.store["exec_task"], "executed")
        del sys.modules["test_exec_module"]


if __name__ == "__main__":
    unittest.main()