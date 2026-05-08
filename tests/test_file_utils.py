"""Unit tests for duct.file_utils module."""

import os
import tempfile
import unittest
from pathlib import Path
from duct.file_utils import (
    get_results_dir, ensure_partition_path,
    atomic_write, cleanup_partitions,
)
from duct.models import Pipeline, RunContext


class TestGetResultsDir(unittest.TestCase):
    def test_path_constructed_correctly(self):
        pipeline = Pipeline(name="p", tasks=[])
        ctx = RunContext(pipeline=pipeline, run_id="abc123", output_base=Path("/base"))
        path = get_results_dir(ctx)
        self.assertEqual(path, Path("/base/tmp/abc123/results"))

    def test_different_run_ids_produce_different_paths(self):
        pipeline = Pipeline(name="p", tasks=[])
        ctx1 = RunContext(pipeline=pipeline, run_id="run1", output_base=Path("/out"))
        ctx2 = RunContext(pipeline=pipeline, run_id="run2", output_base=Path("/out"))
        self.assertNotEqual(get_results_dir(ctx1), get_results_dir(ctx2))


class TestEnsurePartitionPath(unittest.TestCase):
    def test_creates_directory(self):
        base = Path(tempfile.mkdtemp())
        path = ensure_partition_path(base, "2025-01-01")
        self.assertTrue(path.exists())
        self.assertTrue(path.is_dir())
        self.assertEqual(path.name, "partition=2025-01-01")

    def test_idempotent(self):
        base = Path(tempfile.mkdtemp())
        p1 = ensure_partition_path(base, "2025-01-01")
        p2 = ensure_partition_path(base, "2025-01-01")
        self.assertEqual(p1, p2)

    def test_nested_partition(self):
        base = Path(tempfile.mkdtemp())
        path = ensure_partition_path(base, "type=2025-01-01/hour=00")
        self.assertTrue(path.exists())


class TestAtomicWrite(unittest.TestCase):
    def test_writes_file(self):
        src = Path(tempfile.mktemp(suffix=".txt"))
        dest = Path(tempfile.mktemp(suffix=".txt"))
        src.write_text("test content")
        atomic_write(src, dest)
        self.assertEqual(dest.read_text(), "test content")
        src.unlink()
        if dest.exists():
            dest.unlink()

    def test_creates_parent_dirs(self):
        base = Path(tempfile.mkdtemp())
        dest = base / "sub" / "deep"
        dest.mkdir(parents=True, exist_ok=True)
        dest_file = dest / "file.txt"
        src = Path(tempfile.mktemp())
        src.write_text("data")
        atomic_write(src, dest_file)
        self.assertTrue(dest_file.exists())
        self.assertEqual(dest_file.read_text(), "data")
        src.unlink()


class TestCleanupPartitions(unittest.TestCase):
    def test_keeps_last_n(self):
        base = Path(tempfile.mkdtemp())
        for i in range(10):
            (base / f"partition=2025-01-{i+1:02d}").mkdir()
        cleanup_partitions(base, keep_last=3)
        remaining = sorted(base.glob("partition=*"))
        self.assertEqual(len(remaining), 3)

    def test_empty_partition_dir(self):
        base = Path(tempfile.mkdtemp())
        cleanup_partitions(base, keep_last=7)
        self.assertEqual(list(base.glob("partition=*")), [])

    def test_fewer_partitions_than_keep(self):
        base = Path(tempfile.mkdtemp())
        for i in range(3):
            (base / f"partition=2025-01-{i+1:02d}").mkdir()
        cleanup_partitions(base, keep_last=7)
        remaining = list(base.glob("partition=*"))
        self.assertEqual(len(remaining), 3)


if __name__ == "__main__":
    unittest.main()