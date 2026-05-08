"""Unit tests for duct.graph module."""

import unittest
from graphlib import CycleError
from duct.graph import build_sorter
from duct.models import Pipeline, Task, TaskType


class TestBuildSorter(unittest.TestCase):
    def test_single_task_no_deps(self):
        pipeline = Pipeline(name="p1", tasks=[
            Task(name="t1", type=TaskType.EXTRACT),
        ])
        sorter = build_sorter(pipeline)
        sorter.prepare()
        self.assertEqual(set(sorter.get_ready()), {"t1"})
        sorter.done("t1")
        self.assertFalse(sorter.is_active())

    def test_linear_chain(self):
        pipeline = Pipeline(name="p1", tasks=[
            Task(name="a", type=TaskType.EXTRACT),
            Task(name="b", type=TaskType.TRANSFORM, depends_on=["a"]),
            Task(name="c", type=TaskType.LOAD, depends_on=["b"]),
        ])
        sorter = build_sorter(pipeline)
        sorter.prepare()
        self.assertEqual(set(sorter.get_ready()), {"a"})
        sorter.done("a")
        self.assertEqual(set(sorter.get_ready()), {"b"})
        sorter.done("b")
        self.assertEqual(set(sorter.get_ready()), {"c"})

    def test_parallel_tasks(self):
        pipeline = Pipeline(name="p1", tasks=[
            Task(name="a", type=TaskType.EXTRACT),
            Task(name="b", type=TaskType.EXTRACT),
            Task(name="c", type=TaskType.LOAD, depends_on=["a", "b"]),
        ])
        sorter = build_sorter(pipeline)
        sorter.prepare()
        ready = set(sorter.get_ready())
        self.assertEqual(ready, {"a", "b"})

    def test_complex_dag(self):
        pipeline = Pipeline(name="p1", tasks=[
            Task(name="e", type=TaskType.EXTRACT),
            Task(name="f", type=TaskType.EXTRACT),
            Task(name="g", type=TaskType.TRANSFORM, depends_on=["e"]),
            Task(name="h", type=TaskType.TRANSFORM, depends_on=["e", "f"]),
            Task(name="i", type=TaskType.LOAD, depends_on=["g", "h"]),
        ])
        sorter = build_sorter(pipeline)
        sorter.prepare()
        first = set(sorter.get_ready())
        self.assertEqual(first, {"e", "f"})

    def test_missing_dependency_raises(self):
        pipeline = Pipeline(name="p1", tasks=[
            Task(name="a", type=TaskType.EXTRACT),
            Task(name="b", type=TaskType.TRANSFORM, depends_on=["nonexistent"]),
        ])
        with self.assertRaises(ValueError) as cm:
            build_sorter(pipeline)
        self.assertIn("nonexistent", str(cm.exception))
        self.assertIn("b", str(cm.exception))

    def test_all_tasks_complete(self):
        pipeline = Pipeline(name="p1", tasks=[
            Task(name="a", type=TaskType.EXTRACT),
            Task(name="b", type=TaskType.LOAD, depends_on=["a"]),
        ])
        sorter = build_sorter(pipeline)
        sorter.prepare()
        self.assertEqual(set(sorter.get_ready()), {"a"})
        sorter.done("a")
        self.assertEqual(set(sorter.get_ready()), {"b"})
        sorter.done("b")
        self.assertFalse(sorter.is_active())

    def test_cyclic_dependency_raises(self):
        pipeline = Pipeline(name="p1", tasks=[
            Task(name="a", type=TaskType.TRANSFORM, depends_on=["b"]),
            Task(name="b", type=TaskType.TRANSFORM, depends_on=["a"]),
        ])
        sorter = build_sorter(pipeline)
        with self.assertRaises(Exception):
            sorter.prepare()


if __name__ == "__main__":
    unittest.main()