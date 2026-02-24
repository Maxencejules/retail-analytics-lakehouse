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

## Run Checks Locally

```bash
make init
make precommit
make lint
make test-unit
make test-integration
```

Or run the full suite:

```bash
make ci
```

## Why This Protects Production Quality

- **Static quality gates** prevent style and basic correctness issues from entering mainline code.
- **Unit tests** guard transformation correctness and schema validation behavior.
- **Integration test** proves real pipeline interoperability (generator -> ETL -> Gold).
- **Fail-fast validation check** ensures data quality violations stop the pipeline before bad data reaches Gold.
- **Pre-commit parity** keeps local developer checks aligned with CI expectations, reducing merge-time surprises.

