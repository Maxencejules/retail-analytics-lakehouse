"""Benchmark Spark batch ETL runtime, memory usage, and throughput."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import logging
import math
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import time
from timeit import default_timer

try:
    import psutil
except ImportError as exc:  # pragma: no cover - dependency gate
    raise RuntimeError(
        "psutil is required; install development dependencies first."
    ) from exc

LOGGER = logging.getLogger("scripts.benchmark_etl")


@dataclass(frozen=True, slots=True)
class CommandBenchmark:
    runtime_seconds: float
    peak_memory_mb: float


@dataclass(frozen=True, slots=True)
class IterationBenchmark:
    iteration: int
    mode: str
    rows: int
    generator: CommandBenchmark
    spark_etl: CommandBenchmark
    spark_throughput_rows_per_sec: float
    total_runtime_seconds: float


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    rows: int
    iterations: int
    warmup_iterations: int
    seed: int
    ingestion_date: str
    work_dir: str
    output: str
    sample_interval_seconds: float
    keep_artifacts: bool

    def validate(self) -> None:
        if self.rows <= 0:
            raise ValueError("--rows must be > 0")
        if self.iterations <= 0:
            raise ValueError("--iterations must be > 0")
        if self.warmup_iterations < 0:
            raise ValueError("--warmup-iterations must be >= 0")
        if self.sample_interval_seconds <= 0:
            raise ValueError("--sample-interval-ms must be > 0")
        try:
            date.fromisoformat(self.ingestion_date)
        except ValueError as exc:
            raise ValueError("--ingestion-date must be in YYYY-MM-DD format") from exc


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark generator + Spark batch ETL runtime and memory."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5000,
        help="Number of synthetic rows per benchmark run.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Measured benchmark iterations.",
    )
    parser.add_argument(
        "--warmup-iterations",
        type=int,
        default=1,
        help="Warmup iterations excluded from aggregate summary.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed passed to synthetic data generator.",
    )
    parser.add_argument(
        "--ingestion-date",
        default=date.today().isoformat(),
        help="Ingestion date for ETL output partitioning (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--work-dir",
        default=".tmp/benchmarks/etl",
        help="Temporary benchmark workspace root.",
    )
    parser.add_argument(
        "--output",
        default="perf/results/latest/etl-benchmark.json",
        help="Output JSON path for benchmark results.",
    )
    parser.add_argument(
        "--sample-interval-ms",
        type=int,
        default=200,
        help="Memory sampling interval in milliseconds.",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Keep generated benchmark data under work-dir for debugging.",
    )
    return parser


def _resolve_repo_path(path_str: str, *, repo_root: Path) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else (repo_root / path)


def _collect_process_tree_rss_bytes(process: psutil.Process) -> int:
    try:
        candidates = [process, *process.children(recursive=True)]
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0

    rss_total = 0
    for candidate in candidates:
        try:
            rss_total += candidate.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return rss_total


def _run_command_with_metrics(
    command: list[str],
    *,
    cwd: Path,
    sample_interval_seconds: float,
) -> CommandBenchmark:
    LOGGER.info("running_command command=%s", " ".join(command))
    start = default_timer()
    process = subprocess.Popen(command, cwd=str(cwd))
    ps_process = psutil.Process(process.pid)
    peak_rss_bytes = 0

    while process.poll() is None:
        peak_rss_bytes = max(
            peak_rss_bytes, _collect_process_tree_rss_bytes(ps_process)
        )
        time.sleep(sample_interval_seconds)

    peak_rss_bytes = max(peak_rss_bytes, _collect_process_tree_rss_bytes(ps_process))
    runtime_seconds = default_timer() - start

    if process.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {process.returncode}: {' '.join(command)}"
        )

    return CommandBenchmark(
        runtime_seconds=runtime_seconds,
        peak_memory_mb=peak_rss_bytes / (1024 * 1024),
    )


def _assert_gold_outputs_exist(gold_root: Path) -> None:
    expected = (
        gold_root / "daily_revenue_by_store",
        gold_root / "top_10_products_by_day",
        gold_root / "customer_lifetime_value",
    )
    for dataset_path in expected:
        if not dataset_path.exists():
            raise RuntimeError(f"Missing expected Gold dataset folder: {dataset_path}")
        parquet_files = list(dataset_path.rglob("*.parquet"))
        if not parquet_files:
            raise RuntimeError(
                f"No Parquet files found for Gold dataset: {dataset_path}"
            )


def _summary_stats(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    count = len(ordered)
    if count == 0:
        raise ValueError("No benchmark values provided for summary.")
    p95_index = max(0, math.ceil(0.95 * count) - 1)
    return {
        "min": round(ordered[0], 4),
        "mean": round(statistics.fmean(ordered), 4),
        "median": round(statistics.median(ordered), 4),
        "p95": round(ordered[p95_index], 4),
        "max": round(ordered[-1], 4),
    }


def _to_result_dict(
    config: BenchmarkConfig,
    iterations: list[IterationBenchmark],
) -> dict[str, object]:
    measured = [item for item in iterations if item.mode == "measured"]

    generator_runtimes = [item.generator.runtime_seconds for item in measured]
    spark_runtimes = [item.spark_etl.runtime_seconds for item in measured]
    spark_memory_mb = [item.spark_etl.peak_memory_mb for item in measured]
    spark_throughput = [item.spark_throughput_rows_per_sec for item in measured]
    total_runtimes = [item.total_runtime_seconds for item in measured]

    return {
        "benchmark": "spark_batch_etl",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "executable": sys.executable,
        },
        "config": {
            "rows": config.rows,
            "iterations": config.iterations,
            "warmup_iterations": config.warmup_iterations,
            "seed": config.seed,
            "ingestion_date": config.ingestion_date,
            "sample_interval_seconds": config.sample_interval_seconds,
        },
        "runs": [
            {
                "iteration": item.iteration,
                "mode": item.mode,
                "rows": item.rows,
                "generator": {
                    "runtime_seconds": round(item.generator.runtime_seconds, 4),
                    "peak_memory_mb": round(item.generator.peak_memory_mb, 4),
                },
                "spark_etl": {
                    "runtime_seconds": round(item.spark_etl.runtime_seconds, 4),
                    "peak_memory_mb": round(item.spark_etl.peak_memory_mb, 4),
                    "throughput_rows_per_sec": round(
                        item.spark_throughput_rows_per_sec,
                        4,
                    ),
                },
                "total_runtime_seconds": round(item.total_runtime_seconds, 4),
            }
            for item in iterations
        ],
        "summary": {
            "generator_runtime_seconds": _summary_stats(generator_runtimes),
            "spark_runtime_seconds": _summary_stats(spark_runtimes),
            "spark_peak_memory_mb": _summary_stats(spark_memory_mb),
            "spark_throughput_rows_per_sec": _summary_stats(spark_throughput),
            "total_runtime_seconds": _summary_stats(total_runtimes),
        },
    }


def _render_markdown(results: dict[str, object]) -> str:
    config = results["config"]
    summary = results["summary"]
    runs = results["runs"]
    spark_runtime = summary["spark_runtime_seconds"]
    spark_memory = summary["spark_peak_memory_mb"]
    spark_throughput = summary["spark_throughput_rows_per_sec"]

    lines = [
        "# Spark ETL Benchmark Report",
        "",
        f"- Generated at (UTC): {results['generated_at_utc']}",
        f"- Rows per run: {config['rows']}",
        f"- Measured iterations: {config['iterations']}",
        f"- Warmup iterations: {config['warmup_iterations']}",
        "",
        "## Aggregate Metrics (Measured Iterations)",
        "",
        "| Metric | Min | Mean | Median | P95 | Max |",
        "|---|---:|---:|---:|---:|---:|",
        (
            f"| Spark ETL runtime (s) | {spark_runtime['min']} | "
            f"{spark_runtime['mean']} | {spark_runtime['median']} | "
            f"{spark_runtime['p95']} | {spark_runtime['max']} |"
        ),
        (
            f"| Spark ETL peak memory (MB) | {spark_memory['min']} | "
            f"{spark_memory['mean']} | {spark_memory['median']} | "
            f"{spark_memory['p95']} | {spark_memory['max']} |"
        ),
        (
            f"| Spark ETL throughput (rows/s) | "
            f"{spark_throughput['min']} | {spark_throughput['mean']} | "
            f"{spark_throughput['median']} | {spark_throughput['p95']} | "
            f"{spark_throughput['max']} |"
        ),
        "",
        "## Iteration Details",
        "",
        (
            "| Iteration | Mode | Spark Runtime (s) | Spark Peak Memory (MB) | "
            "Throughput (rows/s) |"
        ),
        "|---|---|---:|---:|---:|",
    ]

    for run in runs:
        lines.append(
            f"| {run['iteration']} | {run['mode']} | "
            f"{run['spark_etl']['runtime_seconds']} | "
            f"{run['spark_etl']['peak_memory_mb']} | "
            f"{run['spark_etl']['throughput_rows_per_sec']} |"
        )
    lines.append("")
    return "\n".join(lines)


def _build_iteration_commands(
    *,
    rows: int,
    seed: int,
    generated_dir: Path,
    lakehouse_dir: Path,
    ingestion_date: str,
) -> tuple[list[str], list[str]]:
    csv_path = generated_dir / "transactions.csv.gz"
    generator_cmd = [
        sys.executable,
        "ingestion/generator/generate.py",
        "--mode",
        "batch",
        "--rows",
        str(rows),
        "--seed",
        str(seed),
        "--output-dir",
        str(generated_dir),
        "--csv-filename",
        csv_path.name,
        "--parquet-dirname",
        "transactions_parquet",
    ]

    spark_cmd = [
        sys.executable,
        "spark/batch/run_pipeline.py",
        "--input-path",
        str(csv_path),
        "--input-format",
        "csv",
        "--output-target",
        "local",
        "--output-base-path",
        str(lakehouse_dir),
        "--ingestion-date",
        ingestion_date,
        "--table-format",
        "parquet",
    ]
    return generator_cmd, spark_cmd


def run_benchmark(config: BenchmarkConfig) -> dict[str, object]:
    config.validate()
    repo_root = Path(__file__).resolve().parents[1]
    workspace_root = _resolve_repo_path(config.work_dir, repo_root=repo_root)
    workspace_root.mkdir(parents=True, exist_ok=True)

    all_iterations = config.warmup_iterations + config.iterations
    results: list[IterationBenchmark] = []

    for idx in range(all_iterations):
        iteration_number = idx + 1
        mode = "warmup" if idx < config.warmup_iterations else "measured"
        run_dir = workspace_root / f"run_{iteration_number:02d}_{mode}"
        generated_dir = run_dir / "generated"
        lakehouse_dir = run_dir / "lakehouse"

        if run_dir.exists():
            shutil.rmtree(run_dir)
        generated_dir.mkdir(parents=True, exist_ok=True)
        lakehouse_dir.mkdir(parents=True, exist_ok=True)

        LOGGER.info(
            "benchmark_iteration_start iteration=%s mode=%s rows=%s",
            iteration_number,
            mode,
            config.rows,
        )
        generator_cmd, spark_cmd = _build_iteration_commands(
            rows=config.rows,
            seed=config.seed,
            generated_dir=generated_dir,
            lakehouse_dir=lakehouse_dir,
            ingestion_date=config.ingestion_date,
        )

        iteration_start = default_timer()
        generator_metrics = _run_command_with_metrics(
            generator_cmd,
            cwd=repo_root,
            sample_interval_seconds=config.sample_interval_seconds,
        )
        spark_metrics = _run_command_with_metrics(
            spark_cmd,
            cwd=repo_root,
            sample_interval_seconds=config.sample_interval_seconds,
        )
        _assert_gold_outputs_exist(lakehouse_dir / "gold")
        total_runtime_seconds = default_timer() - iteration_start
        throughput = config.rows / spark_metrics.runtime_seconds

        results.append(
            IterationBenchmark(
                iteration=iteration_number,
                mode=mode,
                rows=config.rows,
                generator=generator_metrics,
                spark_etl=spark_metrics,
                spark_throughput_rows_per_sec=throughput,
                total_runtime_seconds=total_runtime_seconds,
            )
        )

        LOGGER.info(
            (
                "benchmark_iteration_complete iteration=%s mode=%s "
                "spark_runtime_seconds=%.4f spark_peak_memory_mb=%.2f "
                "throughput_rows_per_sec=%.2f"
            ),
            iteration_number,
            mode,
            spark_metrics.runtime_seconds,
            spark_metrics.peak_memory_mb,
            throughput,
        )

        if not config.keep_artifacts:
            shutil.rmtree(run_dir, ignore_errors=True)

    return _to_result_dict(config, results)


def parse_args(argv: list[str] | None = None) -> BenchmarkConfig:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    return BenchmarkConfig(
        rows=args.rows,
        iterations=args.iterations,
        warmup_iterations=args.warmup_iterations,
        seed=args.seed,
        ingestion_date=args.ingestion_date,
        work_dir=args.work_dir,
        output=args.output,
        sample_interval_seconds=args.sample_interval_ms / 1000.0,
        keep_artifacts=args.keep_artifacts,
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = parse_args(argv)
    results = run_benchmark(config)

    repo_root = Path(__file__).resolve().parents[1]
    output_json_path = _resolve_repo_path(config.output, repo_root=repo_root)
    output_md_path = output_json_path.with_suffix(".md")

    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    output_md_path.write_text(_render_markdown(results), encoding="utf-8")

    LOGGER.info("benchmark_results_json=%s", output_json_path)
    LOGGER.info("benchmark_results_markdown=%s", output_md_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
