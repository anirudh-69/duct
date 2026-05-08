"""Command-line entry point for the duct pipeline engine."""

import argparse
import logging
from pathlib import Path
from .config import load_pipeline
from .models import RunContext
from .secrets import generate_run_id
from .runner import pipeline_session, run_pipeline
from .formatting import wrap_log


def main():
    """Parse arguments, load the pipeline, and execute it.

    Supports specifying a pipeline TOML file, execution mode (prod/test),
    concurrency settings, run ID for resumption, and log verbosity.
    """
    parser = argparse.ArgumentParser(description="duct - plug-and-play data pipeline engine")
    parser.add_argument("pipeline_file", type=Path, help="TOML pipeline definition")
    parser.add_argument("--mode", choices=["prod", "test"], default="prod")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--run-id", type=str, default=None,
                        help="Specify a run ID to resume a previous run")
    parser.add_argument("--timeout", type=int, default=None,
                        help="Override all task timeouts (seconds)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    pipeline = load_pipeline(args.pipeline_file)
    run_id = args.run_id or generate_run_id()
    output_base = Path("./output") if args.mode == "prod" else Path("./test_output")

    ctx = RunContext(
        pipeline=pipeline,
        run_id=run_id,
        output_base=output_base,
        mode=args.mode,
        timeout_override=args.timeout,
    )

    logger = logging.getLogger(__name__)
    logger.info(wrap_log(f"Pipeline '{pipeline.name}' started (run_id: {run_id})"))

    with pipeline_session(ctx) as (state, store):
        run_pipeline(pipeline, ctx, state, store,
                     max_workers=args.max_workers,
                     timeout_override=args.timeout)

    logger.info(wrap_log(f"Pipeline '{pipeline.name}' completed successfully"))

if __name__ == "__main__":
    main()