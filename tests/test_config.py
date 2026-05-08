"""Unit tests for duct.config module."""

import os
import tempfile
import unittest
from pathlib import Path
from duct.config import load_pipeline, _substitute_env, _process_params
from duct.models import TaskType


class TestSubstituteEnv(unittest.TestCase):
    def test_substitute_single_var(self):
        os.environ["TEST_VAR"] = "hello"
        result = _substitute_env("${TEST_VAR}")
        self.assertEqual(result, "hello")
        del os.environ["TEST_VAR"]

    def test_substitute_multiple_vars(self):
        os.environ["FOO"] = "foo"
        os.environ["BAR"] = "bar"
        result = _substitute_env("${FOO}_${BAR}")
        self.assertEqual(result, "foo_bar")
        del os.environ["FOO"]
        del os.environ["BAR"]

    def test_undefined_var_unchanged(self):
        result = _substitute_env("${UNDEFINED_VAR}")
        self.assertEqual(result, "${UNDEFINED_VAR}")

    def test_no_var_unchanged(self):
        result = _substitute_env("plain_string")
        self.assertEqual(result, "plain_string")


class TestProcessParams(unittest.TestCase):
    def test_nested_env_sub(self):
        os.environ["NESTED_VAL"] = "resolved"
        params = {"key": {"nested": "${NESTED_VAL}"}}
        result = _process_params(params)
        self.assertEqual(result, {"key": {"nested": "resolved"}})
        del os.environ["NESTED_VAL"]

    def test_non_string_unchanged(self):
        params = {"num": 42, "flag": True, "list": [1, 2]}
        result = _process_params(params)
        self.assertEqual(result, params)

    def test_mixed_types(self):
        os.environ["STR_VAL"] = "hello"
        params = {"a": "${STR_VAL}", "b": 123, "c": True, "d": {"e": "world"}}
        result = _process_params(params)
        self.assertEqual(result["a"], "hello")
        self.assertEqual(result["b"], 123)
        self.assertEqual(result["c"], True)
        self.assertEqual(result["d"]["e"], "world")
        del os.environ["STR_VAL"]


class TestLoadPipeline(unittest.TestCase):
    def test_minimal_pipeline(self):
        config = """
[name]
name = "test_pipeline"

[tasks.task_a]
type = "extract"
function = "mymodule:myfunc"
"""
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False, mode="w") as f:
            f.write(config)
            f.flush()
            path = Path(f.name)

        pipeline = load_pipeline(path)
        path.unlink()

        self.assertEqual(pipeline.name, "test_pipeline")
        self.assertEqual(len(pipeline.tasks), 1)
        task = pipeline.tasks[0]
        self.assertEqual(task.name, "task_a")
        self.assertEqual(task.type, TaskType.EXTRACT)
        self.assertEqual(task.function, "mymodule:myfunc")
        self.assertEqual(task.retry, 0)
        self.assertEqual(task.retry_delay, 1.0)
        self.assertEqual(task.priority, 10)
        self.assertEqual(task.timeout, 3600)
        self.assertFalse(task.result_persist)

    def test_full_task_config(self):
        config = """
[name]
name = "full_pipeline"

[tasks.task_b]
type = "transform"
depends_on = ["task_a"]
function = "tasks:transform_fn"
retry = 3
retry_delay = 2.0
priority = 5
timeout = 60
result_persist = true
params = { mode = "strict", limit = 100 }
"""
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False, mode="w") as f:
            f.write(config)
            f.flush()
            path = Path(f.name)

        pipeline = load_pipeline(path)
        path.unlink()

        self.assertEqual(pipeline.name, "full_pipeline")
        task = pipeline.tasks[0]
        self.assertEqual(task.name, "task_b")
        self.assertEqual(task.type, TaskType.TRANSFORM)
        self.assertEqual(task.depends_on, ["task_a"])
        self.assertEqual(task.retry, 3)
        self.assertEqual(task.retry_delay, 2.0)
        self.assertEqual(task.priority, 5)
        self.assertEqual(task.timeout, 60)
        self.assertTrue(task.result_persist)
        self.assertEqual(task.params, {"mode": "strict", "limit": 100})

    def test_env_var_substitution_in_params(self):
        os.environ["DB_HOST"] = "localhost"
        config = """
[name]
name = "env_pipeline"

[tasks.task_c]
type = "load"
function = "tasks:load_data"
params = { host = "${DB_HOST}" }
"""
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False, mode="w") as f:
            f.write(config)
            f.flush()
            path = Path(f.name)

        pipeline = load_pipeline(path)
        path.unlink()
        self.assertEqual(pipeline.tasks[0].params["host"], "localhost")
        del os.environ["DB_HOST"]

    def test_multiple_tasks(self):
        config = """
[name]
name = "multi"

[tasks.a]
type = "extract"
function = "tasks:a"

[tasks.b]
type = "transform"
depends_on = ["a"]
function = "tasks:b"

[tasks.c]
type = "load"
depends_on = ["b"]
function = "tasks:c"
"""
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False, mode="w") as f:
            f.write(config)
            f.flush()
            path = Path(f.name)

        pipeline = load_pipeline(path)
        path.unlink()

        self.assertEqual(len(pipeline.tasks), 3)
        names = {t.name for t in pipeline.tasks}
        self.assertEqual(names, {"a", "b", "c"})


if __name__ == "__main__":
    unittest.main()