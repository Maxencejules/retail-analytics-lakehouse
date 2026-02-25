.RECIPEPREFIX := >

PYTHON ?= python
VENV ?= .venv
PROJECT_DIRS := ingestion spark warehouse dashboard infra tests scripts
DBT ?= dbt
DOCKER_COMPOSE ?= docker compose
SODA_CONFIG ?= quality/soda/configuration.yml
SODA_CHECKS ?= quality/soda/checks/gold_quality.yml
DBT_TARGET ?= dev
TARGET_ENV ?= dev
DBT_STATE_DIR ?= .tmp/dbt-state
DBT_SLIM_SELECTION_FILE ?= .tmp/dbt-state/selection.txt

ifeq ($(OS),Windows_NT)
VENV_BIN := $(VENV)/Scripts
else
VENV_BIN := $(VENV)/bin
endif

PIP := $(VENV_BIN)/pip
PY := $(VENV_BIN)/python

.PHONY: init format lint test-unit test-integration test precommit ci clean \
	airflow-dag-validate dbt-build dbt-docs soda-scan monitoring-up monitoring-down \
	dbt-source-freshness dbt-phase2-gate dbt-governance-validate \
	compact-lakehouse phase3-policy-validate phase3-gate platform-check \
	dbt-slim-ci

init:
>$(PYTHON) -m venv $(VENV)
>$(PY) -m pip install --upgrade pip
>$(PIP) install -r requirements-dev.txt
>$(PY) -m pre_commit install

format:
>$(PY) -m ruff format $(PROJECT_DIRS)
>$(PY) -m black $(PROJECT_DIRS)

lint:
>$(PY) -m ruff check $(PROJECT_DIRS)
>$(PY) -m black --check $(PROJECT_DIRS)

test-unit:
>$(PY) -m pytest tests/ingestion tests/spark/batch -q

test-integration:
>$(PY) scripts/ci_integration_test.py --rows 1000

test: test-unit test-integration

precommit:
>$(PY) -m pre_commit run --all-files

ci: lint test-unit test-integration

airflow-dag-validate:
>$(PY) -m py_compile infra/airflow/dags/common/notifications.py \
>	infra/airflow/dags/common/run_metadata.py \
>	infra/airflow/dags/common/datasets.py \
>	infra/airflow/dags/batch_etl_orchestration.py \
>	infra/airflow/dags/batch_etl_backfill.py \
>	infra/airflow/dags/environment_promotion_workflow.py \
>	infra/airflow/dags/cost_performance_optimization.py

dbt-build:
>$(DBT) build --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target $(DBT_TARGET)

dbt-docs:
>$(DBT) docs generate --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target $(DBT_TARGET)

dbt-source-freshness:
>$(DBT) source freshness --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target $(DBT_TARGET)

dbt-phase2-gate:
>$(DBT) build --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target $(DBT_TARGET) --selector phase2_governed_models
>$(DBT) source freshness --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target $(DBT_TARGET)
>$(DBT) docs generate --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target $(DBT_TARGET)

dbt-governance-validate:
>$(PY) scripts/validate_dbt_governance.py --repo-root .

dbt-slim-ci:
>$(PY) scripts/dbt_slim_ci.py --repo-root . --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target $(DBT_TARGET) --dbt-bin $(VENV_BIN)/dbt --state-dir $(DBT_STATE_DIR) --output-file $(DBT_SLIM_SELECTION_FILE)

compact-lakehouse:
>$(PY) spark/optimization/compact_tables.py --base-path data/lakehouse --table-format parquet --target-file-size-mb 256

phase3-policy-validate:
>$(PY) scripts/validate_phase3_policies.py --repo-root .

phase3-gate: phase3-policy-validate compact-lakehouse

soda-scan:
>$(PY) scripts/run_soda_scan.py --configuration $(SODA_CONFIG) --checks $(SODA_CHECKS) --target $(TARGET_ENV)

monitoring-up:
>$(DOCKER_COMPOSE) -f infra/monitoring/docker-compose.monitoring.yml up -d

monitoring-down:
>$(DOCKER_COMPOSE) -f infra/monitoring/docker-compose.monitoring.yml down

platform-check: airflow-dag-validate

clean:
>$(PYTHON) -c "import pathlib, shutil; shutil.rmtree(pathlib.Path('$(VENV)'), ignore_errors=True)"
