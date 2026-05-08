"""Data models for the duct pipeline engine."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum
from pathlib import Path


class TaskType(Enum):
    """Classification categories for pipeline tasks.

    Used for logical grouping and filtering of tasks.
    """
    EXTRACT = "extract"
    TRANSFORM = "transform"
    QUALITY = "quality"
    LOAD = "load"


@dataclass(frozen=True)
class Task:
    """Immutable description of a single pipeline task.

    Attributes:
        name: Unique identifier for the task within the pipeline.
        type: Classification of the task (extract, transform, quality, load).
        depends_on: List of task names that must complete before this task runs.
        function: Fully qualified function to call (e.g. "module.submodule:func_name"
            or "module.func_name").
        params: Static parameters passed to the function.
        retry: Number of times to retry on failure (0 = no retries).
        retry_delay: Base delay in seconds between retry attempts (doubles each retry).
        priority: Lower values run first among tasks with satisfied dependencies.
        timeout: Maximum seconds to wait for task completion (None = no limit).
        result_persist: Whether to save the result to disk for cross-run reuse.
    """
    name: str
    type: TaskType
    depends_on: List[str] = field(default_factory=list)
    function: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    retry: int = 0
    retry_delay: float = 1.0
    priority: int = 10
    timeout: Optional[int] = 3600
    result_persist: bool = False


@dataclass
class Pipeline:
    """Top-level pipeline definition containing a collection of tasks.

    Attributes:
        name: Human-readable pipeline identifier.
        tasks: Ordered list of Task definitions (execution order is determined
            by the dependency graph).
        config: Arbitrary configuration dictionary for pipeline-level settings.
    """
    name: str
    tasks: List[Task]
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RunContext:
    """Runtime context passed to every task function.

    Provides access to pipeline metadata and output directories.

    Attributes:
        pipeline: The Pipeline being executed.
        run_id: Unique identifier for this execution run.
        output_base: Root directory for all pipeline output (logs, results, etc.).
        mode: Execution mode ("prod" or "test").
        timeout_override: Override all task timeouts (in seconds) for this run.
    """
    pipeline: Pipeline
    run_id: str
    output_base: Path
    mode: str = "test"
    timeout_override: Optional[int] = None