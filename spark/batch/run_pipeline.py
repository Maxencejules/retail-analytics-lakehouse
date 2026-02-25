"""CLI entrypoint for the retail transactions batch ETL pipeline."""

from __future__ import annotations

import sys
from typing import Sequence

if __package__ in {None, ""}:
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from spark.batch.config import parse_config
from spark.batch.logging_utils import configure_logging
from spark.batch.pipeline import run_pipeline


def main(argv: Sequence[str] | None = None) -> int:
    config = parse_config(list(argv) if argv is not None else None)
    logger = configure_logging(config.log_level)
    logger.info(
        "batch_cli_start",
        extra={
            "context": {
                "app_name": config.app_name,
                "ingestion_date": config.ingestion_date,
            }
        },
    )
    run_pipeline(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
