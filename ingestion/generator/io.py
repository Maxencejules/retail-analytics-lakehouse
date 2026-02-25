"""Batch output helpers for CSV and Parquet generation."""

from __future__ import annotations

from dataclasses import dataclass
import csv
import gzip
from pathlib import Path
from typing import Iterable

from ingestion.generator.models import TRANSACTION_FIELD_ORDER, TransactionEvent


@dataclass(slots=True, frozen=True)
class BatchWriteResult:
    """Represents persisted batch artifacts."""

    rows: int
    csv_path: Path
    parquet_path: Path


def write_batch_outputs(
    events: Iterable[TransactionEvent],
    output_dir: Path,
    csv_filename: str = "transactions.csv.gz",
    parquet_dirname: str = "transactions_parquet",
) -> BatchWriteResult:
    """Write generated events to compressed CSV and partitioned Parquet."""
    event_list = list(events)
    if not event_list:
        raise ValueError("events cannot be empty")

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / csv_filename
    parquet_path = output_dir / parquet_dirname

    _write_compressed_csv(event_list, csv_path)
    _write_partitioned_parquet(event_list, parquet_path)

    return BatchWriteResult(
        rows=len(event_list), csv_path=csv_path, parquet_path=parquet_path
    )


def _write_compressed_csv(events: list[TransactionEvent], csv_path: Path) -> None:
    with gzip.open(csv_path, mode="wt", encoding="utf-8", newline="") as csv_handle:
        writer = csv.DictWriter(csv_handle, fieldnames=list(TRANSACTION_FIELD_ORDER))
        writer.writeheader()
        for event in events:
            writer.writerow(event.to_serializable_dict())


def _write_partitioned_parquet(
    events: list[TransactionEvent], parquet_path: Path
) -> None:
    try:
        import pyarrow as pa
        import pyarrow.dataset as ds
    except ImportError as exc:
        raise RuntimeError(
            "pyarrow is required for Parquet output. Install with: pip install pyarrow"
        ) from exc

    records: list[dict[str, object]] = []
    for event in events:
        record = event.as_dict()
        record["event_date"] = event.ts_utc.date().isoformat()
        records.append(record)

    table = pa.Table.from_pylist(records)
    parquet_path.mkdir(parents=True, exist_ok=True)
    ds.write_dataset(
        data=table,
        base_dir=str(parquet_path),
        format="parquet",
        partitioning=["event_date", "channel"],
        existing_data_behavior="overwrite_or_ignore",
    )
