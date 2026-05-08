"""Unit tests for duct.models module."""

import unittest
from pathlib import Path
from duct.models import Task, TaskType, Pipeline, RunContext


class TestTaskType(unittest.TestCase):
    def test_all_types_exist(self):
        self.assertEqual(TaskType.EXTRACT.value, "extract")
        self.assertEqual(TaskType.TRANSFORM.value, "transform")
        self.assertEqual(TaskType.QUALITY.value, "quality")
        self.assertEqual(TaskType.LOAD.value, "load")

    def test_type_from_string(self):
        self.assertEqual(TaskType("extract"), TaskType.EXTRACT)
        self.assertEqual(TaskType("load"), TaskType.LOAD)

    def test_invalid_type_raises(self):
        with self.assertRaises(ValueError):
            TaskType("invalid")


class TestTask(unittest.TestCase):
    def test_immutable(self):
        task = Task(name="t1", type=TaskType.EXTRACT)
        with self.assertRaises(AttributeError):
            task.name = "t2"

    def test_defaults(self):
        task = Task(name="t1", type=TaskType.EXTRACT)
        self.assertEqual(task.depends_on, [])
        self.assertEqual(task.function, "")
        self.assertEqual(task.params, {})
        self.assertEqual(task.retry, 0)
        self.assertEqual(task.retry_delay, 1.0)
        self.assertEqual(task.priority, 10)
        self.assertEqual(task.timeout, 3600)
        self.assertFalse(task.result_persist)

    def test_full_constructor(self):
        task = Task(
            name="t1",
            type=TaskType.TRANSFORM,
            depends_on=["a", "b"],
            function="mymodule.func",
            params={"k": "v"},
            retry=5,
            retry_delay=3.0,
            priority=1,
            timeout=300,
            result_persist=True,
        )
        self.assertEqual(task.name, "t1")
        self.assertEqual(task.type, TaskType.TRANSFORM)
        self.assertEqual(task.depends_on, ["a", "b"])
        self.assertEqual(task.params, {"k": "v"})
        self.assertEqual(task.retry, 5)
        self.assertEqual(task.retry_delay, 3.0)
        self.assertEqual(task.priority, 1)
        self.assertEqual(task.timeout, 300)
        self.assertTrue(task.result_persist)


class TestPipeline(unittest.TestCase):
    def test_empty_pipeline(self):
        p = Pipeline(name="p1", tasks=[])
        self.assertEqual(p.name, "p1")
        self.assertEqual(p.tasks, [])
        self.assertEqual(p.config, {})

    def test_pipeline_with_tasks(self):
        tasks = [
            Task(name="t1", type=TaskType.EXTRACT),
            Task(name="t2", type=TaskType.LOAD),
        ]
        p = Pipeline(name="p1", tasks=tasks, config={"version": 1})
        self.assertEqual(len(p.tasks), 2)
        self.assertEqual(p.config["version"], 1)


class TestRunContext(unittest.TestCase):
    def test_defaults(self):
        pipeline = Pipeline(name="p1", tasks=[])
        ctx = RunContext(pipeline=pipeline, run_id="r1", output_base=Path("."))
        self.assertEqual(ctx.mode, "test")
        self.assertIsNone(ctx.timeout_override)

    def test_full_constructor(self):
        pipeline = Pipeline(name="p1", tasks=[])
        ctx = RunContext(
            pipeline=pipeline,
            run_id="r2",
            output_base=Path("/tmp/out"),
            mode="prod",
            timeout_override=60,
        )
        self.assertEqual(ctx.run_id, "r2")
        self.assertEqual(ctx.mode, "prod")
        self.assertEqual(ctx.timeout_override, 60)


if __name__ == "__main__":
    unittest.main()