# dbt Warehouse Project

This dbt project manages warehouse transformations and metric models for:

- Staging normalization
- Star schema marts (`dim_*`, `fact_sales`)
- Executive metric-serving models
- Tests, docs, and lineage graph generation

## Project Structure

- `models/sources.yml`: source table declarations for `staging.*`
- `models/staging/`: standardized staging models
- `models/marts/`: star schema dimensions and fact model
- `models/metrics/`: metric-serving aggregate models
- `macros/`: surrogate key and custom data-quality test macros

## Local Run

```bash
pip install -r requirements-dbt.txt
cp warehouse/dbt/profiles/profiles.yml.example warehouse/dbt/profiles/profiles.yml
dbt deps --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles
dbt build --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target dev
dbt docs generate --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target dev
```

Or run via Make:

```bash
make dbt-build DBT_TARGET=dev
make dbt-docs DBT_TARGET=dev
```

## Lineage and Documentation

Generate model docs and lineage graph:

```bash
dbt docs generate --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target dev
dbt docs serve --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles
```

## Deployment Discipline

- Use `dbt build` in CI/CD for run + test gates.
- Promote by environment target (`dev`, `stage`, `prod`) with isolated credentials.
- Store generated `manifest.json` as deployment artifact for traceability.
