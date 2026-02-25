"""CLI entrypoint for synthetic retail transaction generation."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

if __package__ in {None, ""}:
    # Allows: python ingestion/generator/generate.py
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from ingestion.generator.generator import TransactionGenerator
from ingestion.generator.io import write_batch_outputs
from ingestion.generator.logging_utils import configure_logging
from ingestion.generator.streaming import stream_transactions


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate synthetic retail transactions (batch or stream)."
    )
    parser.add_argument("--mode", choices=("batch", "stream"), required=True)
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for determinism."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Log verbosity level.",
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of rows to generate in batch mode.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/generated",
        help="Batch output directory for CSV and Parquet artifacts.",
    )
    parser.add_argument(
        "--csv-filename",
        default="transactions.csv.gz",
        help="Compressed CSV filename for batch output.",
    )
    parser.add_argument(
        "--parquet-dirname",
        default="transactions_parquet",
        help="Directory name for partitioned Parquet output.",
    )

    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Events per second in stream mode.",
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default="localhost:9092",
        help="Comma-separated Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topic",
        default="transactions",
        help="Kafka topic for stream mode.",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=1000,
        help="Progress log interval for stream mode.",
    )
    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.mode == "batch" and args.rows <= 0:
        parser.error("--rows must be > 0 in batch mode")

    if args.mode == "stream" and args.rate <= 0:
        parser.error("--rate must be > 0 in stream mode")

    if args.mode == "stream" and args.log_every <= 0:
        parser.error("--log-every must be > 0 in stream mode")

    if args.mode == "stream" and not args.kafka_bootstrap_servers.strip():
        parser.error("--kafka-bootstrap-servers cannot be empty in stream mode")


def _run_batch(args: argparse.Namespace) -> None:
    logger = configure_logging(args.log_level)
    logger.info(
        "batch_generation_start",
        extra={
            "context": {
                "rows": args.rows,
                "seed": args.seed,
                "output_dir": args.output_dir,
            }
        },
    )

    generator = TransactionGenerator(seed=args.seed)
    events = generator.generate_events(args.rows)
    result = write_batch_outputs(
        events=events,
        output_dir=Path(args.output_dir),
        csv_filename=args.csv_filename,
        parquet_dirname=args.parquet_dirname,
    )

    logger.info(
        "batch_generation_complete",
        extra={
            "context": {
                "rows": result.rows,
                "csv_path": str(result.csv_path),
                "parquet_path": str(result.parquet_path),
            }
        },
    )


def _run_stream(args: argparse.Namespace) -> None:
    configure_logging(args.log_level)
    stream_transactions(
        rate_per_second=args.rate,
        seed=args.seed,
        bootstrap_servers=args.kafka_bootstrap_servers,
        topic=args.topic,
        log_every=args.log_every,
    )


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _validate_args(parser, args)

    if args.mode == "batch":
        _run_batch(args)
    else:
        _run_stream(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
