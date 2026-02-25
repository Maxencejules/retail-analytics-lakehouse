"""Automated Silver/Gold dataset compaction for file-size optimization."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
import logging
import math
import sys
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.common.lineage import OpenLineageConfig, apply_openlineage
from spark.common.performance import SparkPerformanceProfile, apply_performance_profile

LOGGER = logging.getLogger("spark.optimization.compaction")


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    dataset_name: str
    relative_path: str
    partition_column: str | None


DATASET_CATALOG: dict[str, DatasetSpec] = {
    "silver/transactions": DatasetSpec(
        dataset_name="silver/transactions",
        relative_path="silver/transactions",
        partition_column="event_date",
    ),
    "gold/daily_revenue_by_store": DatasetSpec(
        dataset_name="gold/daily_revenue_by_store",
        relative_path="gold/daily_revenue_by_store",
        partition_column="event_date",
    ),
    "gold/top_10_products_by_day": DatasetSpec(
        dataset_name="gold/top_10_products_by_day",
        relative_path="gold/top_10_products_by_day",
        partition_column="event_date",
    ),
    "gold/customer_lifetime_value": DatasetSpec(
        dataset_name="gold/customer_lifetime_value",
        relative_path="gold/customer_lifetime_value",
        partition_column="snapshot_date",
    ),
}


DEFAULT_DATASETS = tuple(DATASET_CATALOG.keys())


@dataclass(frozen=True, slots=True)
class CompactionConfig:
    base_path: str
    table_format: str
    datasets: tuple[str, ...]
    target_file_size_mb: int
    min_file_count: int
    max_file_count: int
    max_records_per_file: int
    full_dataset: bool
    continue_on_error: bool
    app_name: str
    log_level: str
    shuffle_partitions: int | None

    def validate(self) -> None:
        if not self.base_path.strip():
            raise ValueError("base_path cannot be empty")
        if self.table_format not in {"parquet", "delta"}:
            raise ValueError("table_format must be parquet or delta")
        if not self.datasets:
            raise ValueError("at least one dataset is required")

        invalid = [
            dataset for dataset in self.datasets if dataset not in DATASET_CATALOG
        ]
        if invalid:
            raise ValueError(f"unknown datasets: {', '.join(invalid)}")

        if self.target_file_size_mb <= 0:
            raise ValueError("target_file_size_mb must be > 0")
        if self.min_file_count <= 0:
            raise ValueError("min_file_count must be > 0")
        if self.max_file_count < self.min_file_count:
            raise ValueError("max_file_count must be >= min_file_count")
        if self.max_records_per_file <= 0:
            raise ValueError("max_records_per_file must be > 0")
        if self.shuffle_partitions is not None and self.shuffle_partitions <= 0:
            raise ValueError("shuffle_partitions must be > 0")

    @property
    def target_file_size_bytes(self) -> int:
        return self.target_file_size_mb * 1024 * 1024


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compact Silver/Gold datasets to control small-file proliferation."
    )
    parser.add_argument("--base-path", default="data/lakehouse")
    parser.add_argument(
        "--table-format", default="parquet", choices=("parquet", "delta")
    )
    parser.add_argument(
        "--datasets",
        default=",".join(DEFAULT_DATASETS),
        help=f"Comma-separated dataset names: {', '.join(DEFAULT_DATASETS)}",
    )
    parser.add_argument("--target-file-size-mb", type=int, default=256)
    parser.add_argument("--min-file-count", type=int, default=1)
    parser.add_argument("--max-file-count", type=int, default=128)
    parser.add_argument("--max-records-per-file", type=int, default=5_000_000)
    parser.add_argument(
        "--full-dataset",
        action="store_true",
        help="Compact the full dataset instead of only the latest partition.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue to remaining datasets if one compaction task fails.",
    )
    parser.add_argument("--app-name", default="retail-lakehouse-compaction")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=None,
        help="Optional override for spark.sql.shuffle.partitions during compaction.",
    )
    return parser


def parse_config(argv: list[str] | None = None) -> CompactionConfig:
    args = build_arg_parser().parse_args(argv)
    datasets = tuple(
        value.strip() for value in args.datasets.split(",") if value.strip()
    )
    config = CompactionConfig(
        base_path=args.base_path.rstrip("/"),
        table_format=args.table_format,
        datasets=datasets,
        target_file_size_mb=args.target_file_size_mb,
        min_file_count=args.min_file_count,
        max_file_count=args.max_file_count,
        max_records_per_file=args.max_records_per_file,
        full_dataset=args.full_dataset,
        continue_on_error=args.continue_on_error,
        app_name=args.app_name,
        log_level=args.log_level,
        shuffle_partitions=args.shuffle_partitions,
    )
    config.validate()
    return config


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_spark_session(config: CompactionConfig) -> SparkSession:
    profile = SparkPerformanceProfile.from_env()
    lineage = OpenLineageConfig.from_env(default_parent_job_name=config.app_name)
    if config.shuffle_partitions is not None:
        profile = replace(profile, shuffle_partitions=config.shuffle_partitions)
        profile.validate()

    builder = (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    builder = apply_performance_profile(builder, profile)
    builder = apply_openlineage(builder, lineage)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    LOGGER.info("spark_performance_profile_applied %s", profile.as_dict())
    return spark


def _fs_and_path(spark: SparkSession, path: str) -> tuple[Any, Any]:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hconf)
    return fs, jpath


def _path_exists(spark: SparkSession, path: str) -> bool:
    fs, jpath = _fs_and_path(spark, path)
    return bool(fs.exists(jpath))


def _delete_path(spark: SparkSession, path: str) -> None:
    fs, jpath = _fs_and_path(spark, path)
    if fs.exists(jpath):
        fs.delete(jpath, True)


def _rename_path(spark: SparkSession, source: str, destination: str) -> None:
    source_fs, source_jpath = _fs_and_path(spark, source)
    destination_fs, destination_jpath = _fs_and_path(spark, destination)
    if source_fs.getUri().toString() != destination_fs.getUri().toString():
        raise RuntimeError("source and destination filesystems must match for rename")

    parent = destination_jpath.getParent()
    if parent is not None and not destination_fs.exists(parent):
        destination_fs.mkdirs(parent)

    if not destination_fs.rename(source_jpath, destination_jpath):
        raise RuntimeError(f"Failed to rename {source} to {destination}")


def _replace_path(spark: SparkSession, source: str, destination: str) -> None:
    _delete_path(spark, destination)
    _rename_path(spark, source, destination)


def _path_size_bytes(spark: SparkSession, path: str) -> int:
    fs, jpath = _fs_and_path(spark, path)
    if not fs.exists(jpath):
        return 0
    return int(fs.getContentSummary(jpath).getLength())


def _latest_partition_value(df: DataFrame, partition_column: str) -> str | None:
    row = df.select(F.max(F.col(partition_column)).alias("latest_value")).collect()[0]
    latest_value = row["latest_value"]
    if latest_value is None:
        return None
    return str(latest_value)


def _find_partition_output_path(
    spark: SparkSession,
    base_path: str,
    partition_column: str,
) -> str | None:
    fs, base_jpath = _fs_and_path(spark, base_path)
    if not fs.exists(base_jpath):
        return None
    prefix = f"{partition_column}="
    for status in fs.listStatus(base_jpath):
        path_name = status.getPath().getName()
        if path_name.startswith(prefix):
            return status.getPath().toString()
    return None


def compute_target_file_count(
    total_size_bytes: int,
    *,
    target_file_size_bytes: int,
    min_file_count: int,
    max_file_count: int,
) -> int:
    if total_size_bytes <= 0:
        return min_file_count
    raw = math.ceil(total_size_bytes / target_file_size_bytes)
    return min(max(raw, min_file_count), max_file_count)


def _write_compacted_output(
    df: DataFrame,
    *,
    table_format: str,
    output_path: str,
    partition_column: str | None,
    max_records_per_file: int,
) -> None:
    writer = (
        df.write.format(table_format)
        .mode("overwrite")
        .option("maxRecordsPerFile", str(max_records_per_file))
    )
    if partition_column is not None:
        writer = writer.partitionBy(partition_column)
    if table_format == "delta":
        writer = writer.option("overwriteSchema", "true")
    writer.save(output_path)


def _build_temp_path(dataset_path: str) -> str:
    return f"{dataset_path.rstrip('/')}_tmp_compaction_{uuid4().hex}"


@dataclass(frozen=True, slots=True)
class CompactionResult:
    dataset_name: str
    status: str
    scope: str
    input_size_bytes: int
    target_file_count: int
    row_count: int


def _compact_full_dataset(
    spark: SparkSession,
    config: CompactionConfig,
    dataset_path: str,
    spec: DatasetSpec,
) -> CompactionResult:
    input_size_bytes = _path_size_bytes(spark, dataset_path)
    target_file_count = compute_target_file_count(
        input_size_bytes,
        target_file_size_bytes=config.target_file_size_bytes,
        min_file_count=config.min_file_count,
        max_file_count=config.max_file_count,
    )

    df = spark.read.format(config.table_format).load(dataset_path).cache()
    row_count = df.count()
    if row_count == 0:
        df.unpersist()
        return CompactionResult(
            dataset_name=spec.dataset_name,
            status="skipped_empty",
            scope="full_dataset",
            input_size_bytes=input_size_bytes,
            target_file_count=target_file_count,
            row_count=row_count,
        )

    if spec.partition_column:
        compact_df = df.repartition(target_file_count, F.col(spec.partition_column))
    else:
        compact_df = df.repartition(target_file_count)

    temp_path = _build_temp_path(dataset_path)
    _write_compacted_output(
        compact_df,
        table_format=config.table_format,
        output_path=temp_path,
        partition_column=spec.partition_column,
        max_records_per_file=config.max_records_per_file,
    )
    _replace_path(spark, temp_path, dataset_path)
    df.unpersist()

    return CompactionResult(
        dataset_name=spec.dataset_name,
        status="compacted",
        scope="full_dataset",
        input_size_bytes=input_size_bytes,
        target_file_count=target_file_count,
        row_count=row_count,
    )


def _compact_latest_partition(
    spark: SparkSession,
    config: CompactionConfig,
    dataset_path: str,
    spec: DatasetSpec,
) -> CompactionResult:
    if spec.partition_column is None:
        return _compact_full_dataset(spark, config, dataset_path, spec)

    df = spark.read.format(config.table_format).load(dataset_path)
    latest_value = _latest_partition_value(df, spec.partition_column)
    if latest_value is None:
        return CompactionResult(
            dataset_name=spec.dataset_name,
            status="skipped_empty",
            scope="latest_partition",
            input_size_bytes=0,
            target_file_count=config.min_file_count,
            row_count=0,
        )

    partition_path = (
        f"{dataset_path.rstrip('/')}/{spec.partition_column}={latest_value}"
    )
    input_size_bytes = _path_size_bytes(spark, partition_path)
    target_file_count = compute_target_file_count(
        input_size_bytes,
        target_file_size_bytes=config.target_file_size_bytes,
        min_file_count=config.min_file_count,
        max_file_count=config.max_file_count,
    )

    partition_df = df.where(F.col(spec.partition_column) == F.lit(latest_value)).cache()
    row_count = partition_df.count()
    if row_count == 0:
        partition_df.unpersist()
        return CompactionResult(
            dataset_name=spec.dataset_name,
            status="skipped_empty",
            scope=f"partition:{latest_value}",
            input_size_bytes=input_size_bytes,
            target_file_count=target_file_count,
            row_count=row_count,
        )

    compact_df = partition_df.repartition(target_file_count)
    temp_root = _build_temp_path(dataset_path)
    _write_compacted_output(
        compact_df,
        table_format=config.table_format,
        output_path=temp_root,
        partition_column=spec.partition_column,
        max_records_per_file=config.max_records_per_file,
    )
    source_partition_output = _find_partition_output_path(
        spark,
        temp_root,
        spec.partition_column,
    )
    if source_partition_output is None:
        raise RuntimeError(
            f"Unable to resolve compacted partition output in {temp_root}"
        )

    _replace_path(spark, source_partition_output, partition_path)
    _delete_path(spark, temp_root)
    partition_df.unpersist()

    return CompactionResult(
        dataset_name=spec.dataset_name,
        status="compacted",
        scope=f"partition:{latest_value}",
        input_size_bytes=input_size_bytes,
        target_file_count=target_file_count,
        row_count=row_count,
    )


def run_compaction(config: CompactionConfig) -> list[CompactionResult]:
    config.validate()
    spark = build_spark_session(config)
    results: list[CompactionResult] = []
    failures: list[str] = []

    try:
        for dataset_name in config.datasets:
            spec = DATASET_CATALOG[dataset_name]
            dataset_path = f"{config.base_path.rstrip('/')}/{spec.relative_path}"

            if not _path_exists(spark, dataset_path):
                LOGGER.warning(
                    "dataset_path_missing dataset=%s path=%s",
                    dataset_name,
                    dataset_path,
                )
                results.append(
                    CompactionResult(
                        dataset_name=dataset_name,
                        status="skipped_missing",
                        scope="n/a",
                        input_size_bytes=0,
                        target_file_count=config.min_file_count,
                        row_count=0,
                    )
                )
                continue

            try:
                if config.full_dataset:
                    result = _compact_full_dataset(spark, config, dataset_path, spec)
                else:
                    result = _compact_latest_partition(
                        spark, config, dataset_path, spec
                    )
                results.append(result)
                LOGGER.info(
                    "compaction_result dataset=%s status=%s scope=%s rows=%s "
                    "size_bytes=%s target_files=%s",
                    result.dataset_name,
                    result.status,
                    result.scope,
                    result.row_count,
                    result.input_size_bytes,
                    result.target_file_count,
                )
            except Exception as exc:
                failure = f"{dataset_name}: {exc}"
                failures.append(failure)
                LOGGER.exception("compaction_failure dataset=%s", dataset_name)
                if not config.continue_on_error:
                    raise

        if failures:
            raise RuntimeError(
                "Compaction completed with failures: " + "; ".join(failures)
            )
        return results
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    config = parse_config(argv)
    configure_logging(config.log_level)
    run_compaction(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
