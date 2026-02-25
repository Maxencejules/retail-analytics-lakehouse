# Performance Benchmarks

This folder contains benchmark tooling and runtime outputs for key data jobs.

## Spark ETL Benchmark

Local run:

```bash
make benchmark-etl
```

With custom settings:

```bash
make benchmark-etl \
  BENCHMARK_ROWS=10000 \
  BENCHMARK_ITERATIONS=3 \
  BENCHMARK_WARMUP_ITERATIONS=1 \
  BENCHMARK_OUTPUT=perf/results/latest/etl-benchmark.json
```

The benchmark runner:
- generates sample transactions,
- executes `spark/batch/run_pipeline.py`,
- records runtime using Python `timeit` timers,
- samples peak memory (RSS) for the Spark process tree,
- calculates ETL throughput (`rows/sec`).

Generated outputs:
- `perf/results/latest/etl-benchmark.json` (machine-readable)
- `perf/results/latest/etl-benchmark.md` (human-readable report)

CI also runs a sample benchmark and uploads artifacts:
- `perf/results/ci/etl-benchmark.json`
- `perf/results/ci/etl-benchmark.md`
