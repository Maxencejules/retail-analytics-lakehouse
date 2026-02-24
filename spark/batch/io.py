"""I/O utilities for reading raw data and writing lakehouse layers."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

LOGGER = logging.getLogger("spark.batch.io")


def ensure_delta_available(spark: SparkSession) -> None:
    """Fail early when Delta format is requested but runtime is not configured."""
    try:
        spark._jvm.java.lang.Class.forName("io.delta.tables.DeltaTable")
    except Exception as exc:  # pragma: no cover - depends on runtime setup
        raise RuntimeError(
            "Delta format requested but Delta runtime jars are not available."
        ) from exc

    extensions = spark.conf.get("spark.sql.extensions", "")
    catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "")
    if "io.delta.sql.DeltaSparkSessionExtension" not in extensions:
        raise RuntimeError("Delta format requested but spark.sql.extensions is not configured.")
    if "org.apache.spark.sql.delta.catalog.DeltaCatalog" not in catalog:
        raise RuntimeError(
            "Delta format requested but spark.sql.catalog.spark_catalog is not configured."
        )


def read_raw_transactions(
    spark: SparkSession,
    *,
    input_path: str,
    input_format: str,
) -> DataFrame:
    """Read raw transaction records without business transformation."""
    if input_format == "csv":
        return (
            spark.read.format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .load(input_path)
        )

    return spark.read.format(input_format).load(input_path)


def write_dataset(
    df: DataFrame,
    *,
    path: str,
    table_format: str,
    partition_by: list[str],
    mode: str = "overwrite",
) -> None:
    """Write dataset with partitioning in an idempotent overwrite mode."""
    (
        df.write.format(table_format)
        .mode(mode)
        .partitionBy(*partition_by)
        .save(path)
    )
    LOGGER.info(
        "dataset_written",
        extra={
            "context": {
                "path": path,
                "table_format": table_format,
                "partition_by": partition_by,
                "mode": mode,
            }
        },
    )


def read_dataset(spark: SparkSession, *, path: str, table_format: str) -> DataFrame:
    """Read an existing Bronze/Silver/Gold dataset."""
    return spark.read.format(table_format).load(path)

