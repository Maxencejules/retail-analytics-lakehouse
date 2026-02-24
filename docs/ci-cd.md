# CI/CD Quality Gates

## What the CI Pipeline Runs

GitHub Actions workflow:
- [ci.yml](C:/Users/USER/retail-analytics-lakehouse/.github/workflows/ci.yml)

It runs these stages in order:
1. Pre-commit checks
2. Lint checks
3. Unit tests
4. Integration test:
   - generate 1,000 synthetic transactions
   - run batch ETL
   - validate Gold outputs exist and contain rows
   - verify ETL fails on invalid data (fail-fast quality gate)

## Makefile Targets

Defined in:
- [Makefile](C:/Users/USER/retail-analytics-lakehouse/Makefile)

Main targets:
- `make init`: create virtualenv and install CI/dev dependencies.
- `make precommit`: run all pre-commit hooks.
- `make lint`: run Ruff and Black checks.
- `make test-unit`: run unit tests.
- `make test-integration`: run end-to-end integration validation.
- `make ci`: run lint + unit + integration checks.
- `make airflow-dag-validate`: compile-check Airflow DAG Python files.
- `make dbt-build DBT_TARGET=dev`: run dbt models + tests for a target.
- `make dbt-docs DBT_TARGET=dev`: generate dbt docs/lineage artifacts.
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
- **Soda alert routing** creates fast feedback for production data quality incidents.
