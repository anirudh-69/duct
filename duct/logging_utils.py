"""Logging utilities for pipeline tasks."""

import logging
from pathlib import Path


def setup_task_logger(task_name: str, run_id: str, output_base: Path) -> logging.Logger:
    """Configure a task logger with both file and console handlers.

    Logs are written to ``{output_base}/logs/{run_id}/{task_name}.log`` and
    also forwarded to stderr. The logger name follows the pattern
    ``duct.task.{task_name}``.

    If the logger already has a FileHandler (e.g. from a previous run in the
    same process), no additional handlers are added.

    Args:
        task_name: Name of the task, used in the log file name.
        run_id: Unique run identifier, used to group logs under a run directory.
        output_base: Root output directory for the pipeline.

    Returns:
        A configured Logger instance.
    """
    log_dir = output_base / "logs" / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{task_name}.log"

    logger = logging.getLogger(f"duct.task.{task_name}")
    logger.setLevel(logging.DEBUG)
    if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(console)
    return logger