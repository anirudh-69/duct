"""Pipeline configuration loader.

Loads TOML pipeline definitions and constructs Pipeline and Task objects.
Supports environment variable substitution in string parameter values using
the `${VAR_NAME}` syntax.
"""

import os
import re
import tomllib
from pathlib import Path
from .models import Pipeline, Task, TaskType

_ENV_VAR_RE = re.compile(r"\$\{(\w+)\}")

def _substitute_env(value: str) -> str:
    def replacer(match):
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))
    return _ENV_VAR_RE.sub(replacer, value)

def _process_params(params: dict) -> dict:
    """Recursively substitute environment variables in string parameter values."""
    new = {}
    for k, v in params.items():
        if isinstance(v, str):
            new[k] = _substitute_env(v)
        elif isinstance(v, dict):
            new[k] = _process_params(v)
        else:
            new[k] = v
    return new

def load_pipeline(config_path: Path) -> Pipeline:
    """Load a pipeline definition from a TOML configuration file.

    Parses the TOML file, validates task definitions, and returns a
    Pipeline object with all tasks configured. Environment variable
    substitution is applied to string parameter values.

    Args:
        config_path: Path to the pipeline TOML file.

    Returns:
        A Pipeline instance with tasks loaded.

    Raises:
        KeyError: If required pipeline fields are missing.
        ValueError: If task configuration is invalid.
    """
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    pipeline_name = config["name"]["name"]
    tasks = []
    for t_name, t_data in config["tasks"].items():
        tasks.append(Task(
            name=t_name,
            type=TaskType(t_data["type"]),
            depends_on=t_data.get("depends_on", []),
            function=t_data.get("function", ""),
            params=_process_params(t_data.get("params", {})),
            retry=t_data.get("retry", 0),
            retry_delay=t_data.get("retry_delay", 1.0),
            priority=t_data.get("priority", 10),
            timeout=t_data.get("timeout", 3600),
            result_persist=t_data.get("result_persist", False),
        ))
    return Pipeline(
        name=pipeline_name,
        tasks=tasks,
        config=config.get("config", {})
    )