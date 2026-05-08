"""File and directory utilities for pipeline operations."""

import shutil
from pathlib import Path

from .models import RunContext


def get_results_dir(ctx: RunContext) -> Path:
    """Return the directory path for persisting task results.

    Path: ``{output_base}/tmp/{run_id}/results``

    Args:
        ctx: The pipeline run context.

    Returns:
        Path to the results directory for this run.
    """
    return ctx.output_base / "tmp" / ctx.run_id / "results"


def ensure_partition_path(base: Path, partition: str) -> Path:
    """Create and return a path for a date-partitioned directory.

    The directory is created with ``parents=True, exist_ok=True``.

    Args:
        base: Root directory for partitions.
        partition: Partition identifier (e.g. "2025-01-01").

    Returns:
        Path to the created partition directory.
    """
    p = base / f"partition={partition}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def atomic_write(src: Path, dest: Path) -> None:
    """Atomically write a file by writing to a temp file then renaming.

    Uses ``shutil.copy2`` to preserve metadata. On platforms that support
    rename-overwrite (all major OSes), this is atomic.

    Args:
        src: Source file path.
        dest: Destination file path.
    """
    tmp = dest.with_suffix(dest.suffix + '.tmp')
    shutil.copy2(src, tmp)
    tmp.replace(dest)


def cleanup_partitions(base: Path, keep_last: int = 7) -> None:
    """Delete old partition directories, keeping the most recent N.

    Partitions are identified by the ``partition=*`` glob pattern.

    Args:
        base: Root directory containing partition subdirectories.
        keep_last: Number of recent partitions to retain (default 7).
    """
    partitions = sorted(base.glob("partition=*"))
    for old in partitions[:-keep_last]:
        shutil.rmtree(old)