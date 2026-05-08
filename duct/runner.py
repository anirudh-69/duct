"""Pipeline execution engine and task scheduling.

Manages the lifecycle of a pipeline run: context setup, task scheduling,
retry logic, timeout enforcement, graceful shutdown, and result reporting.
"""

import signal
import sys
import heapq
import time
import logging
import threading
from pathlib import Path
from contextlib import contextmanager
from concurrent.futures import (ThreadPoolExecutor, Future,
                                wait, FIRST_COMPLETED)
from typing import Optional
from .models import Pipeline, RunContext, Task
from .state import StateStore
from .executor import execute_task, ResultStore
from .graph import build_sorter
from .formatting import format_summary
from .file_utils import get_results_dir

logger = logging.getLogger(__name__)

_shutdown_requested = False


def _signal_handler(signum, frame):
    """Signal handler that sets the global shutdown flag.

    Catches SIGINT (Ctrl+C) and SIGTERM to allow graceful pipeline cancellation.
    """
    global _shutdown_requested
    _shutdown_requested = True
    logger.warning("Received shutdown signal. Cancelling pending tasks...")

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


@contextmanager
def pipeline_session(ctx: RunContext):
    """Context manager that sets up and tears down a pipeline execution session.

    Creates the output directory, initializes the StateStore and ResultStore,
    and ensures all resources are released on exit.

    Usage::

        with pipeline_session(ctx) as (state, store):
            run_pipeline(pipeline, ctx, state, store)

    Args:
        ctx: The RunContext for this pipeline execution.

    Yields:
        A tuple of (StateStore, ResultStore) ready for use.
    """
    ctx.output_base.mkdir(parents=True, exist_ok=True)
    db_path = ctx.output_base / "duct.db"
    state = StateStore(db_path)
    store = ResultStore()
    get_results_dir(ctx).mkdir(parents=True, exist_ok=True)
    try:
        yield state, store
    finally:
        state.close()

def _sort_by_priority(ready_tasks: list[str], task_map: dict) -> list[str]:
    """Sort ready tasks by ascending priority (lower value = higher priority).

    Uses a min-heap for O(n log n) sorting.

    Args:
        ready_tasks: List of task names that are ready to execute.
        task_map: Mapping of task names to Task objects.

    Returns:
        Task names sorted by priority.
    """
    heap = [(task_map[name].priority, name) for name in ready_tasks]
    heapq.heapify(heap)
    return [heapq.heappop(heap)[1] for _ in range(len(heap))]


def _calc_duration_ms(start: float) -> float:
    """Calculate elapsed time in milliseconds from a perf_counter start value."""
    return (time.perf_counter() - start) * 1000


def run_pipeline(pipeline: Pipeline, ctx: RunContext,
                 state: StateStore, store: ResultStore,
                 max_workers: int = 4,
                 timeout_override: Optional[int] = None) -> None:
    """Execute a pipeline, scheduling tasks as their dependencies complete.

    Tasks are executed in a ThreadPoolExecutor with at most ``max_workers``
    concurrent tasks. The dependency graph is traversed using a topological
    sorter, with tasks sorted by priority within each batch of ready tasks.

    Completed tasks from prior runs are pre-marked in the sorter to enable
    incremental execution when resuming.

    On any task failure, all pending futures are cancelled and a
    RuntimeError is raised.

    Args:
        pipeline: The Pipeline to execute.
        ctx: The RunContext for this run.
        state: StateStore for tracking task status.
        store: ResultStore for sharing task results.
        max_workers: Maximum number of concurrent task workers (default 4).
        timeout_override: Override all task timeouts in seconds.
    """
    task_map = {t.name: t for t in pipeline.tasks}
    sorter = build_sorter(pipeline)
    sorter.prepare()

    completed_tasks: set[str] = set()
    for row in state.get_all_task_statuses(ctx.run_id):
        task_name, status, _, _ = row
        if status == "SUCCESS":
            completed_tasks.add(task_name)
            try:
                sorter.done(task_name)
            except (RuntimeError, ValueError):
                pass

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        running_futures: dict[Future, str] = {}

        def submit_ready():
            nonlocal running_futures
            if sorter.is_active():
                ready = list(sorter.get_ready())
            else:
                ready = []
            ready = [n for n in ready
                     if n not in completed_tasks
                     and n not in running_futures.values()]
            if not ready:
                return
            ready = _sort_by_priority(ready, task_map)
            for name in ready:
                if _shutdown_requested:
                    break
                future = executor.submit(run_single_task, name, task_map, ctx, state, store, timeout_override)
                running_futures[future] = name

        # Main loop
        while sorter.is_active() or running_futures:
            if _shutdown_requested:
                for f in running_futures:
                    f.cancel()
                sys.exit(1)

            submit_ready()

            if not running_futures:
                break

            done, _ = wait(running_futures, timeout=None, return_when=FIRST_COMPLETED)
            for future in done:
                name = running_futures.pop(future)
                try:
                    success, duration_ms, attempts = future.result()
                except Exception as e:
                    logger.error(f"Unexpected future error for task '{name}': {e}")
                    state.set_status(ctx.run_id, name, "FAILED")
                    for f in running_futures:
                        f.cancel()
                    raise RuntimeError(f"Pipeline aborted: unexpected error in task '{name}'") from e

                if success:
                    completed_tasks.add(name)
                    sorter.done(name)
                else:
                    logger.error(f"Pipeline aborted due to failure in task '{name}'")
                    for f in running_futures:
                        f.cancel()
                    raise RuntimeError(f"Pipeline aborted: task '{name}' failed after {attempts} attempts")

    _print_summary(ctx, state)


def run_single_task(task_name: str, task_map: dict, ctx: RunContext, state: StateStore,
                    store: ResultStore, timeout_override: Optional[int] = None) -> tuple:
    """Run a single task with timeout enforcement and retry logic.

    Executes the task in a worker thread with a timeout. On failure, retries
    are attempted with exponential backoff (delay doubles each retry). Timeout
    and non-retryable errors result in immediate failure.

    Args:
        task_name: Name of the task to run.
        task_map: Mapping of task names to Task objects.
        ctx: The RunContext for this pipeline run.
        state: StateStore for recording task status.
        store: ResultStore for sharing task results.
        timeout_override: Override this task's timeout (in seconds).

    Returns:
        A tuple of (success: bool, duration_ms: float, attempts: int).
        duration_ms is the execution time of the successful attempt,
        or the last failed attempt on failure.
    """
    task = task_map[task_name]
    task_logger = logging.getLogger(f"duct.task.{task.name}")
    effective_timeout = timeout_override if timeout_override is not None else task.timeout

    attempts = 0
    error = None

    for attempt in range(task.retry + 1):
        attempts += 1
        state.set_status(ctx.run_id, task.name, "RUNNING", attempts=attempts)
        task_logger.info(f"Attempt {attempt+1}/{task.retry+1} starting")

        attempt_start = time.perf_counter()
        result_container: list = [None]
        exc_container: list = [None]

        def target():
            try:
                execute_task(task, ctx, state, store)
            except Exception as e:
                exc_container[0] = e

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout=effective_timeout)

        if thread.is_alive():
            attempt_duration = _calc_duration_ms(attempt_start)
            task_logger.error(f"Task '{task_name}' timed out after {effective_timeout}s")
            state.set_status(ctx.run_id, task.name, "FAILED",
                             duration_ms=attempt_duration, attempts=attempts)
            return False, attempt_duration, attempts

        error = exc_container[0]
        if error is not None:
            attempt_duration = _calc_duration_ms(attempt_start)
            task_logger.error(f"Attempt {attempt+1} failed after {attempt_duration:.0f}ms: {error}")
            if attempt < task.retry:
                sleep_time = task.retry_delay * (2 ** attempt)
                task_logger.info(f"Retrying in {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                continue
            state.set_status(ctx.run_id, task.name, "FAILED",
                             duration_ms=attempt_duration, attempts=attempts)
            return False, attempt_duration, attempts

        attempt_duration = _calc_duration_ms(attempt_start)
        state.set_status(ctx.run_id, task.name, "SUCCESS",
                         duration_ms=attempt_duration, attempts=attempts)
        task_logger.info(f"Completed successfully in {attempt_duration:.0f}ms")
        return True, attempt_duration, attempts

    return False, 0, attempts


def _print_summary(ctx: RunContext, state: StateStore):
    """Print a formatted summary of the pipeline execution results.

    Queries the StateStore for all task statuses from the current run and
    prints a human-readable table grouped by status (success vs failed).
    """
    rows = state.get_all_task_statuses(ctx.run_id)
    details = []
    total = 0
    success = 0
    for name, status, duration, attempts in rows:
        total += 1
        if status == "SUCCESS":
            success += 1
            details.append(f"  {name}: SUCCESS ({duration:.0f}ms, {attempts} attempts)")
        else:
            details.append(f"  {name}: {status} ({attempts} attempts)")
    summary = f"Pipeline '{ctx.pipeline.name}' total tasks: {total}, succeeded: {success}"
    details.insert(0, summary)
    logger.info(format_summary("Pipeline Summary", details))