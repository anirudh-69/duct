from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum
from pathlib import Path

class TaskType(Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    QUALITY = "quality"
    LOAD = "load"

@dataclass(frozen=True)
class Task:
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
    name: str
    tasks: List[Task]
    config: Dict[str, Any] = field(default_factory=dict)

@dataclass
class RunContext:
    pipeline: Pipeline
    run_id: str
    output_base: Path
    mode: str = "dev"
    timeout_override: Optional[int] = None