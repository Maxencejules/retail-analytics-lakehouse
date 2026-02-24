"""Runtime configuration for the batch ETL pipeline."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """Pipeline runtime options."""

    input_path: str
    input_format: str
    output_base_path: str
    output_target: str
    ingestion_date: str
    table_format: str
    log_level: str
    app_name: str
    fail_fast_quality: bool

    def validate(self) -> None:
        if not self.input_path.strip():
            raise ValueError("input_path cannot be empty")

        if self.input_format not in {"csv", "json", "parquet"}:
            raise ValueError("input_format must be csv, json, or parquet")

        if self.output_target not in {"local", "s3"}:
            raise ValueError("output_target must be local or s3")

        if not self.output_base_path.strip():
            raise ValueError("output_base_path cannot be empty")

        if self.output_target == "s3" and not self.output_base_path.startswith("s3://"):
            raise ValueError("s3 output_target requires output_base_path starting with s3://")

        if self.output_target == "local" and self.output_base_path.startswith("s3://"):
            raise ValueError("local output_target cannot use an s3:// output_base_path")

        try:
            date.fromisoformat(self.ingestion_date)
        except ValueError as exc:
            raise ValueError("ingestion_date must be in YYYY-MM-DD format") from exc

        if self.table_format not in {"parquet", "delta"}:
            raise ValueError("table_format must be parquet or delta")

    @property
    def base_path(self) -> str:
        if self.output_target == "local":
            return Path(self.output_base_path).as_posix().rstrip("/")
        return self.output_base_path.rstrip("/")

    @property
    def bronze_transactions_path(self) -> str:
        return f"{self.base_path}/bronze/transactions"

    @property
    def silver_transactions_path(self) -> str:
        return f"{self.base_path}/silver/transactions"

    @property
    def gold_store_daily_path(self) -> str:
        return f"{self.base_path}/gold/daily_revenue_by_store"

    @property
    def gold_top_products_path(self) -> str:
        return f"{self.base_path}/gold/top_10_products_by_day"

    @property
    def gold_customer_ltv_path(self) -> str:
        return f"{self.base_path}/gold/customer_lifetime_value"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Retail transactions batch ETL pipeline.")
    parser.add_argument("--input-path", required=True, help="Raw input path (local path or s3://).")
    parser.add_argument(
        "--input-format",
        default="csv",
        choices=("csv", "json", "parquet"),
        help="Raw input format.",
    )
    parser.add_argument(
        "--output-base-path",
        default="data/lakehouse",
        help="Output lakehouse base path (local path or s3://bucket/prefix).",
    )
    parser.add_argument(
        "--output-target",
        default="local",
        choices=("local", "s3"),
        help="Whether output is written locally or to S3.",
    )
    parser.add_argument(
        "--ingestion-date",
        default=date.today().isoformat(),
        help="Ingestion partition date in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--table-format",
        default="parquet",
        choices=("parquet", "delta"),
        help="Write format for Bronze/Silver/Gold layers.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Pipeline log verbosity.",
    )
    parser.add_argument(
        "--app-name",
        default="retail-transactions-batch-etl",
        help="Spark application name.",
    )
    parser.add_argument(
        "--no-fail-fast-quality",
        action="store_true",
        help="Disable fail-fast behavior for critical quality checks.",
    )
    return parser


def parse_config(argv: list[str] | None = None) -> PipelineConfig:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = PipelineConfig(
        input_path=args.input_path,
        input_format=args.input_format,
        output_base_path=args.output_base_path,
        output_target=args.output_target,
        ingestion_date=args.ingestion_date,
        table_format=args.table_format,
        log_level=args.log_level,
        app_name=args.app_name,
        fail_fast_quality=not args.no_fail_fast_quality,
    )
    config.validate()
    return config

