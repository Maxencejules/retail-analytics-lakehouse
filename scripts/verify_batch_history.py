"""Bounded real-Spark replay proof against an independent Python/Decimal oracle.

All inputs are synthetic; this proves batch semantics, not production capacity.
Only this invocation's temporary lakehouse is written and cleaned up.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import csv
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pyarrow.dataset as ds  # noqa: E402
import psutil  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from spark.batch.config import PipelineConfig  # noqa: E402
from spark.batch.exceptions import DataQualityError  # noqa: E402
from spark.batch.pipeline import run_pipeline  # noqa: E402

FIELDS = (
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
)


def sale(
    identifier,
    *,
    ts="2025-01-01T12:00:00Z",
    quantity="2",
    price="10.00",
    currency="CAD",
):
    return dict(
        zip(
            FIELDS,
            (
                identifier,
                ts,
                "store-1",
                "customer-1",
                identifier,
                quantity,
                price,
                currency,
                "credit_card",
                "store",
                "",
            ),
        )
    )


def write_csv(path, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def oracle(batches):
    # No production schema/transformation/aggregation helper is used here.
    latest = {}
    for ingestion_date, rows in batches.items():
        for row in rows:
            timestamp = datetime.fromisoformat(row["ts_utc"].replace("Z", "+00:00"))
            version = timestamp, ingestion_date
            prior = latest.get(row["transaction_id"])
            if prior is None or version > prior[0]:
                latest[row["transaction_id"]] = version, row
            elif version == prior[0]:
                assert row == prior[1], "Ambiguous oracle input"
    daily, products, customers = {}, {}, {}
    for identifier, ((timestamp, ingestion_date), row) in latest.items():
        quantity = int(row["quantity"])
        revenue = Decimal(row["unit_price"]) * quantity
        day, currency = timestamp.date().isoformat(), row["currency"]
        for groups, key in (
            (daily, (day, row["store_id"], currency)),
            (products, (day, row["product_id"], currency)),
            (customers, (row["customer_id"], currency)),
        ):
            values = groups.setdefault(
                key,
                {
                    "revenue": Decimal(0),
                    "units": 0,
                    "count": 0,
                    "first": timestamp,
                    "last": timestamp,
                },
            )
            values["revenue"] += revenue
            values["units"] += quantity
            values["count"] += 1
            values["first"] = min(values["first"], timestamp)
            values["last"] = max(values["last"], timestamp)
    return latest, daily, products, customers


def records(path):
    rows = (
        ds.dataset(path, format="parquet", partitioning="hive").to_table().to_pylist()
    )
    return [
        {k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in row.items()}
        for row in rows
    ]


def money(value):
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def verify(lakehouse, batches):
    latest, daily, products, customers = oracle(batches)
    silver = records(lakehouse / "silver/transactions")
    assert len(silver) == len(latest)
    assert {row["transaction_id"] for row in silver} == set(latest)
    for row in silver:
        (timestamp, ingestion_date), source = latest[row["transaction_id"]]
        assert row["currency"] == source["currency"]
        assert row["quantity"] == int(source["quantity"])
        assert (
            abs(
                row["revenue"]
                - float(Decimal(source["unit_price"]) * int(source["quantity"]))
            )
            < 1e-8
        )
        assert row["event_date"] == timestamp.date().isoformat()
        assert row["ingestion_date"] == ingestion_date
    outputs = {
        name: records(lakehouse / "gold" / name)
        for name in (
            "daily_revenue_by_store",
            "top_10_products_by_day",
            "customer_lifetime_value",
        )
    }
    actual_daily = {
        (r["event_date"], r["store_id"], r["currency"]): (
            r["daily_revenue"],
            r["units_sold"],
            r["transaction_count"],
        )
        for r in outputs["daily_revenue_by_store"]
    }
    assert len(outputs["daily_revenue_by_store"]) == len(daily)
    assert actual_daily == {
        key: (money(v["revenue"]), v["units"], v["count"]) for key, v in daily.items()
    }
    product_groups = defaultdict(list)
    for (day, product, currency), values in products.items():
        product_groups[day, currency].append((product, values))
    expected_products = {}
    for (day, currency), group in product_groups.items():
        ordered = sorted(
            group, key=lambda v: (-money(v[1]["revenue"]), -v[1]["units"], v[0])
        )[:10]
        for rank, (product, values) in enumerate(ordered, 1):
            expected_products[day, currency, rank, product] = (
                money(values["revenue"]),
                values["units"],
                values["count"],
            )
    actual_products = {
        (r["event_date"], r["currency"], r["rank"], r["product_id"]): (
            r["daily_revenue"],
            r["units_sold"],
            r["transaction_count"],
        )
        for r in outputs["top_10_products_by_day"]
    }
    assert len(outputs["top_10_products_by_day"]) == len(expected_products)
    assert actual_products == expected_products
    actual_ltv = {
        (r["customer_id"], r["currency"]): (
            r["lifetime_value"],
            r["transaction_count"],
            r["snapshot_date"],
            r["first_purchase_ts_utc"],
            r["last_purchase_ts_utc"],
        )
        for r in outputs["customer_lifetime_value"]
    }
    assert len(outputs["customer_lifetime_value"]) == len(customers)
    expected_ltv = {
        key: (
            money(v["revenue"]),
            v["count"],
            max(batches),
            v["first"].replace(tzinfo=None).isoformat(),
            v["last"].replace(tzinfo=None).isoformat(),
        )
        for key, v in customers.items()
    }
    assert actual_ltv == expected_ltv
    canonical = json.dumps(
        {
            name: sorted(rows, key=lambda r: json.dumps(r, sort_keys=True))
            for name, rows in {"silver": silver, **outputs}.items()
        },
        sort_keys=True,
        allow_nan=False,
    )
    return {
        "silver_rows": len(silver),
        "gold_rows": {name: len(rows) for name, rows in outputs.items()},
        "semantic_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
    }


def run(output):
    scratch = ROOT / ".tmp"
    scratch.mkdir(exist_ok=True)
    old = sale("old")
    moving = sale("moving", ts="2025-01-02T12:00:00Z", quantity="2", price="3.00")
    first = [old, moving, sale("usd", quantity="1", price="5.00", currency="USD")]
    second = [old, sale("new", quantity="3", price="7.00")]
    correction = [sale("moving", ts="2025-01-03T12:00:00Z", quantity="1", price="9.00")]
    batches, stages = {}, []
    session = (
        SparkSession.builder.master("local[2]")
        .appName("batch-history-proof")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    spark_version = session.version
    start = time.monotonic()
    try:
        with tempfile.TemporaryDirectory(
            prefix="history-proof-", dir=scratch
        ) as directory:
            root = Path(directory)
            lakehouse, source = root / "lakehouse", root / "source.csv"

            def execute(label, ingestion_date, rows):
                write_csv(source, rows)
                config = PipelineConfig(
                    str(source),
                    "csv",
                    str(lakehouse),
                    "local",
                    ingestion_date,
                    "parquet",
                    "INFO",
                    "batch-history-proof",
                    True,
                )
                stage_start = time.monotonic()
                run_pipeline(config, spark_session=session)
                batches[ingestion_date] = rows
                result = verify(lakehouse, batches)
                result.update(
                    stage=label,
                    ingestion_date=ingestion_date,
                    input_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                    runtime_seconds=time.monotonic() - stage_start,
                )
                stages.append(result)
                return result

            execute("initial", "2025-02-01", first)
            execute("overlapping_event_date_and_retry", "2025-02-02", second)
            corrected = execute("cross_date_correction", "2025-02-03", correction)
            repeated = execute("exact_rerun", "2025-02-03", correction)
            assert corrected["semantic_sha256"] == repeated["semantic_sha256"]
            backfilled = execute(
                "replace_earlier_ingestion_remove_currency", "2025-02-01", [moving]
            )
            changed_schema = root / "changed-schema.jsonl"
            changed_schema.write_text(
                json.dumps({**moving, "quantity": 2}) + "\n", encoding="utf-8"
            )
            incompatible = PipelineConfig(
                str(changed_schema),
                "json",
                str(lakehouse),
                "local",
                "2025-02-04",
                "parquet",
                "INFO",
                "batch-history-proof",
                True,
            )
            try:
                run_pipeline(incompatible, spark_session=session)
            except DataQualityError as exc:
                assert "raw schema changed" in str(exc)
            else:
                raise AssertionError(
                    "Incompatible raw schema must fail before Bronze replacement"
                )
            assert {
                r["ingestion_date"] for r in records(lakehouse / "bronze/transactions")
            } == set(batches)
            assert (
                verify(lakehouse, batches)["semantic_sha256"]
                == backfilled["semantic_sha256"]
            )
            write_csv(source, [sale("invalid", price="NaN")])
            invalid_config = PipelineConfig(
                str(source),
                "csv",
                str(lakehouse),
                "local",
                "2025-02-01",
                "parquet",
                "INFO",
                "batch-history-proof",
                True,
            )
            try:
                run_pipeline(invalid_config, spark_session=session)
            except DataQualityError:
                pass
            else:
                raise AssertionError("Invalid batch must raise DataQualityError")
            assert (
                verify(lakehouse, batches)["semantic_sha256"]
                == backfilled["semantic_sha256"]
            )
            restored = execute(
                "repair_rejected_bronze_partition", "2025-02-01", [moving]
            )
            assert restored["semantic_sha256"] == backfilled["semantic_sha256"]
    finally:
        session.stop()

    def git(*args):
        return subprocess.check_output(
            ["git", "-c", f"safe.directory={ROOT}", "-C", str(ROOT), *args], text=True
        ).strip()

    proof = {
        "mode": "synthetic-real-spark-independent-oracle",
        "source_head": git("rev-parse", "HEAD"),
        "source_dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "spark": spark_version,
        "logical_cpu_count": psutil.cpu_count(),
        "host_memory_mb": psutil.virtual_memory().total / 1024**2,
        "runtime_seconds": time.monotonic() - start,
        "stages": stages,
        "checks": [
            "all Silver and Gold rows agree with independent Decimal oracle",
            "same event date retains prior batches",
            "currencies remain separate",
            "exact replay is semantically identical",
            "obsolete event/currency partitions are removed",
            "backfill retains latest cumulative LTV snapshot",
            "invalid Bronze never changes prior Silver/Gold",
            "corrected Bronze partition repairs replay",
            "incompatible raw schema is rejected before changing Bronze",
        ],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(proof, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(json.dumps(proof, indent=2, allow_nan=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path(".tmp/batch-history-proof.json")
    )
    run(parser.parse_args().output)
