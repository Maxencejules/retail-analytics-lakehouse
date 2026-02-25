"""Integration test for generator + batch ETL + Gold output validation."""

from __future__ import annotations

import argparse
import csv
from datetime import date
import logging
from pathlib import Path
import shutil
import subprocess
import sys
import uuid

LOGGER = logging.getLogger("scripts.ci_integration_test")


def _run(
    cmd: list[str], *, expect_success: bool = True
) -> subprocess.CompletedProcess[str]:
    LOGGER.info("running_command command=%s", " ".join(cmd))
    completed = subprocess.run(cmd, check=False, text=True)
    if expect_success and completed.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {completed.returncode}: {' '.join(cmd)}"
        )
    if not expect_success and completed.returncode == 0:
        raise RuntimeError(f"Command unexpectedly succeeded: {' '.join(cmd)}")
    return completed


def _assert_gold_outputs_exist(gold_root: Path) -> None:
    expected = (
        gold_root / "daily_revenue_by_store",
        gold_root / "top_10_products_by_day",
        gold_root / "customer_lifetime_value",
    )

    try:
        import pyarrow.dataset as ds
    except ImportError as exc:
        raise RuntimeError(
            "pyarrow is required for integration output validation"
        ) from exc

    for dataset_path in expected:
        parquet_files = list(dataset_path.rglob("*.parquet"))
        if not parquet_files:
            raise RuntimeError(
                f"No Parquet files found for expected Gold dataset: {dataset_path}"
            )

        rows = ds.dataset(str(dataset_path), format="parquet").count_rows()
        if rows <= 0:
            raise RuntimeError(f"Gold dataset has no rows: {dataset_path}")


def _write_invalid_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "transaction_id",
                "ts_utc",
                "store_id",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price",
                "currency",
                "payment_method",
                "channel",
                "promo_id",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "transaction_id": str(uuid.uuid4()),
                "ts_utc": "2026-02-24T10:00:00Z",
                "store_id": "STORE-0001",
                "customer_id": "CUST-00000001",
                "product_id": "PROD-000001",
                "quantity": -1,  # intentionally invalid to verify fail-fast quality behavior
                "unit_price": 15.99,
                "currency": "CAD",
                "payment_method": "credit_card",
                "channel": "store",
                "promo_id": "",
            }
        )


def run_integration_test(rows: int) -> None:
    root = Path(".tmp") / "ci_integration"
    generated = root / "generated"
    lakehouse = root / "lakehouse"

    if root.exists():
        shutil.rmtree(root)
    generated.mkdir(parents=True, exist_ok=True)
    lakehouse.mkdir(parents=True, exist_ok=True)

    ingestion_date = date.today().isoformat()
    csv_path = generated / "transactions.csv.gz"

    # Happy-path integration: generator -> batch ETL -> Gold validation.
    _run(
        [
            sys.executable,
            "ingestion/generator/generate.py",
            "--mode",
            "batch",
            "--rows",
            str(rows),
            "--seed",
            "42",
            "--output-dir",
            str(generated),
            "--csv-filename",
            csv_path.name,
            "--parquet-dirname",
            "transactions_parquet",
        ]
    )

    _run(
        [
            sys.executable,
            "spark/batch/run_pipeline.py",
            "--input-path",
            str(csv_path),
            "--input-format",
            "csv",
            "--output-target",
            "local",
            "--output-base-path",
            str(lakehouse),
            "--ingestion-date",
            ingestion_date,
            "--table-format",
            "parquet",
        ]
    )

    _assert_gold_outputs_exist(lakehouse / "gold")

    # Validation guard integration: pipeline must fail on critical quality violations.
    invalid_csv = root / "invalid" / "transactions_invalid.csv"
    invalid_output = root / "invalid_lakehouse"
    _write_invalid_csv(invalid_csv)
    _run(
        [
            sys.executable,
            "spark/batch/run_pipeline.py",
            "--input-path",
            str(invalid_csv),
            "--input-format",
            "csv",
            "--output-target",
            "local",
            "--output-base-path",
            str(invalid_output),
            "--ingestion-date",
            ingestion_date,
            "--table-format",
            "parquet",
        ],
        expect_success=False,
    )

    LOGGER.info("integration_test_passed rows=%s", rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run CI integration checks for ETL pipeline."
    )
    parser.add_argument(
        "--rows", type=int, default=1000, help="Number of generated rows."
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = parse_args(argv)
    if args.rows <= 0:
        raise ValueError("--rows must be > 0")
    run_integration_test(rows=args.rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
