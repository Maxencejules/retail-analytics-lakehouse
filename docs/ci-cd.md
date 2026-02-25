# CI/CD Quality Gates

## What the CI Pipeline Runs

GitHub Actions workflow:
- [ci.yml](C:/Users/USER/retail-analytics-lakehouse/.github/workflows/ci.yml)

It runs these stages in order:
1. Pre-commit checks
2. Lint checks
3. dbt slim selection validation (`state:modified+`)
4. dbt governance contract validation
5. Phase 3 policy artifact validation
6. Unit tests
7. Integration test:
   - generate 1,000 synthetic transactions
   - run batch ETL
   - validate Gold outputs exist and contain rows
   - verify ETL fails on invalid data (fail-fast quality gate)
8. ETL benchmark on sample dataset:
   - runs timed Spark ETL iterations
   - captures peak process-tree memory
   - computes throughput (`rows/sec`)
   - uploads JSON/Markdown benchmark report as CI artifact

## Makefile Targets

Defined in:
- [Makefile](C:/Users/USER/retail-analytics-lakehouse/Makefile)

Main targets:
- `make init`: create virtualenv and install CI/dev dependencies.
- `make precommit`: run all pre-commit hooks.
- `make lint`: run Ruff and Black checks.
- `make test-unit`: run unit tests.
- `make test-integration`: run end-to-end integration validation.
- `make benchmark-etl`: run ETL benchmark and emit JSON/Markdown results in `perf/results/`.
- `make ml-train`: train a PyTorch sales predictor on Gold-layer daily revenue data.
- `make ml-score`: score Gold-layer examples using latest trained model artifact.
- `make ci`: run lint + unit + integration checks.
- `make airflow-dag-validate`: compile-check Airflow DAG Python files.
- `make dbt-build DBT_TARGET=dev`: run dbt models + tests for a target.
- `make dbt-docs DBT_TARGET=dev`: generate dbt docs/lineage artifacts.
- `make dbt-source-freshness DBT_TARGET=dev`: enforce source freshness SLAs.
- `make dbt-phase2-gate DBT_TARGET=dev`: run governed build + freshness + docs gate.
- `make dbt-governance-validate`: enforce semantic/exposure/governance contract metadata.
- `make dbt-slim-ci DBT_TARGET=dev`: resolve state-aware dbt slim CI selection.
- `make phase3-policy-validate`: validate compaction/scaling/WLM/lifecycle/budget policy artifacts.
- `make compact-lakehouse`: run Spark compaction for Silver/Gold targets.
- `make soda-scan TARGET_ENV=dev`: run Soda quality checks and alert on failure.
- `make monitoring-up` / `make monitoring-down`: control local Prometheus/Grafana/Marquez stack.

## Run Checks Locally

```bash
make init
make precommit
make lint
make test-unit
make test-integration
make airflow-dag-validate
```

Or run the full suite:

```bash
make ci
```

Run data-platform operational checks locally:

```bash
pip install -r requirements-dbt.txt
pip install -r requirements-quality.txt
make dbt-build DBT_TARGET=dev
make dbt-source-freshness DBT_TARGET=dev
make dbt-phase2-gate DBT_TARGET=dev
make dbt-governance-validate
make phase3-policy-validate
make soda-scan TARGET_ENV=dev
```

## Why This Protects Production Quality

- **Static quality gates** prevent style and basic correctness issues from entering mainline code.
- **Unit tests** guard transformation correctness and schema validation behavior.
- **Integration test** proves real pipeline interoperability (generator -> ETL -> Gold).
- **Fail-fast validation check** ensures data quality violations stop the pipeline before bad data reaches Gold.
- **Pre-commit parity** keeps local developer checks aligned with CI expectations, reducing merge-time surprises.
- **Airflow DAG validation** catches orchestration syntax regressions before deployment.
- **dbt build discipline** enforces model tests and lineage consistency before environment promotion.
- **dbt freshness + governance selectors** prevent stale or out-of-scope assets from promotion.
- **dbt governance validation** enforces semantic-layer and exposure ownership contracts in CI.
- **Phase 3 policy validation** prevents invalid cost/performance controls from deployment.
- **Soda alert routing** creates fast feedback for production data quality incidents.
- **Run metadata artifacts** provide auditable DAG run traces for incident response and release governance.
- **Benchmark artifacts** provide objective runtime/memory/throughput visibility for ETL trend tracking.
