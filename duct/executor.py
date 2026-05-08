"""Task execution engine for the duct pipeline runner.

Handles function resolution, parameter validation, result persistence,
and single-shot task execution. Timeout and retry logic lives in runner.py.
"""

import importlib
import inspect
import json
import logging
from pathlib import Path
from typing import Any, Dict
import threading
from .models import Task, RunContext
from .state import StateStore
from .logging_utils import setup_task_logger
from .file_utils import get_results_dir

logger = logging.getLogger(__name__)

_sig_cache: Dict[str, inspect.Signature] = {}


class ResultStore(dict):
    """Thread-safe dictionary for storing task results in memory.

    All reads and writes are protected by a lock, making it safe to share
    across multiple threads executing tasks concurrently.
    """
    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()

    def __setitem__(self, key, value):
        with self._lock:
            super().__setitem__(key, value)

    def __getitem__(self, key):
        with self._lock:
            return super().__getitem__(key)


def _resolve_function(func_str: str):
    """Resolve a task function string to an actual callable.

    Supports two notation styles:
      - "module.path:func_name" (colon separator)
      - "module.path.func_name"  (dot separator, last component is the function)

    Args:
        func_str: Fully qualified function identifier.

    Returns:
        The callable function object.

    Raises:
        ImportError: If the module cannot be loaded.
        AttributeError: If the function does not exist in the module.
    """
    if ':' in func_str:
        module_name, func_name = func_str.split(':')
    else:
        module_name, func_name = func_str.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


def _validate_params(sig: inspect.Signature, provided: Dict[str, Any], task: Task) -> None:
    """Validate that provided arguments satisfy the function's signature.

    Args:
        sig: The inspect.Signature of the target function.
        provided: Dictionary of argument names to values being passed.
        task: The Task being validated (used only for error messages).

    Raises:
        TypeError: If a required argument is missing or an unknown argument
            is passed and the function does not accept **kwargs.
    """
    has_var_keyword = False
    required = set()
    for p in sig.parameters.values():
        if p.kind == inspect.Parameter.VAR_KEYWORD:
            has_var_keyword = True
            break
        if (p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
                and p.default is inspect.Parameter.empty):
            required.add(p.name)

    if not has_var_keyword:
        missing = required - provided.keys()
        if missing:
            raise TypeError(
                f"Task '{task.name}' expects arguments {missing} "
                f"but they are not provided by dependencies or params."
            )


def _persist_result(result: Any, task_name: str, ctx: RunContext) -> Path:
    """Serialize a task result to disk.

    Attempts JSON serialization first. Falls back to pickle for non-JSON-serializable
    objects (e.g. numpy arrays, custom classes). The directory must already exist.

    Args:
        result: The task return value to persist.
        task_name: Name of the task (used in the file name).
        ctx: The pipeline run context.

    Returns:
        Path to the file where the result was written.
    """
    result_dir = get_results_dir(ctx)
    json_path = result_dir / f"{task_name}.json"
    try:
        with open(json_path, "w") as f:
            json.dump(result, f, default=str)
    except (TypeError, OverflowError) as e:
        logger.warning(f"JSON serialization failed for '{task_name}', falling back to pickle: {e}")
        import pickle
        pickle_path = result_dir / f"{task_name}.pkl"
        with open(pickle_path, "wb") as f:
            pickle.dump(result, f)
        return pickle_path
    return json_path


def _load_persisted_result(task_name: str, ctx: RunContext) -> Any:
    """Load a persisted task result from disk.

    Tries JSON first, then pickle. The caller is responsible for ensuring
    the result has been persisted (e.g. via result_persist=True in the task config).

    Args:
        task_name: Name of the task whose result to load.
        ctx: The pipeline run context.

    Returns:
        The deserialized result object.

    Raises:
        FileNotFoundError: If neither JSON nor pickle file exists for this task.
    """
    result_dir = get_results_dir(ctx)
    json_path = result_dir / f"{task_name}.json"
    if json_path.exists():
        with open(json_path, "r") as f:
            return json.load(f)
    pickle_path = result_dir / f"{task_name}.pkl"
    if pickle_path.exists():
        import pickle
        with open(pickle_path, "rb") as f:
            return pickle.load(f)
    raise FileNotFoundError(f"No persisted result for '{task_name}'")


def execute_task(task: Task, ctx: RunContext, state: StateStore, store: ResultStore) -> Any:
    """Execute a single task once (no timeout/retry - handled by the runner).

    Checks if the task is already completed (for resumed runs) and loads a
    persisted result if available. Resolves the target function, injects
    dependency results and ctx, validates parameters, calls the function,
    stores the result in the ResultStore, and optionally persists to disk.

    Args:
        task: The Task definition to execute.
        ctx: The pipeline run context.
        state: The StateStore for recording task status.
        store: The ResultStore for sharing results with dependent tasks.

    Returns:
        The task's return value.

    Raises:
        RuntimeError: If a dependency result is not available.
        TypeError: If task arguments don't match the function signature.
    """
    task_logger = setup_task_logger(task.name, ctx.run_id, ctx.output_base)

    if state.is_completed(ctx.run_id, task.name):
        task_logger.info(f"Skipping completed task (run_id={ctx.run_id})")
        if task.result_persist:
            try:
                result = _load_persisted_result(task.name, ctx)
                store[task.name] = result
                return result
            except Exception as e:
                task_logger.warning(f"Persisted result missing/corrupt: {e}. Re-executing.")
        else:
            task_logger.info("Result not in memory; re-running to satisfy downstream.")

    func = _resolve_function(task.function)
    if task.function not in _sig_cache:
        _sig_cache[task.function] = inspect.signature(func)
    sig = _sig_cache[task.function]

    call_kwargs: Dict[str, Any] = {**task.params}
    call_kwargs['ctx'] = ctx
    for dep_name in task.depends_on:
        if dep_name not in store:
            raise RuntimeError(f"Dependency '{dep_name}' for task '{task.name}' has no result.")
        call_kwargs[dep_name] = store[dep_name]

    _validate_params(sig, call_kwargs, task)

    result = func(**call_kwargs)

    store[task.name] = result
    if task.result_persist:
        _persist_result(result, task.name, ctx)

    return result