import logging
from pathlib import Path

def setup_task_logger(task_name: str, run_id: str, output_base: Path) -> logging.Logger:
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