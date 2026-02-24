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

ifeq ($(OS),Windows_NT)
VENV_BIN := $(VENV)/Scripts
else
VENV_BIN := $(VENV)/bin
endif

PIP := $(VENV_BIN)/pip
PY := $(VENV_BIN)/python

.PHONY: init format lint test-unit test-integration test precommit ci clean \
	airflow-dag-validate dbt-build dbt-docs soda-scan monitoring-up monitoring-down \
	platform-check

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
>	infra/airflow/dags/batch_etl_orchestration.py \
>	infra/airflow/dags/environment_promotion_workflow.py

dbt-build:
>$(DBT) build --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target $(DBT_TARGET)

dbt-docs:
>$(DBT) docs generate --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles --target $(DBT_TARGET)

soda-scan:
>$(PY) scripts/run_soda_scan.py --configuration $(SODA_CONFIG) --checks $(SODA_CHECKS) --target $(TARGET_ENV)

monitoring-up:
>$(DOCKER_COMPOSE) -f infra/monitoring/docker-compose.monitoring.yml up -d

monitoring-down:
>$(DOCKER_COMPOSE) -f infra/monitoring/docker-compose.monitoring.yml down

platform-check: airflow-dag-validate

clean:
>$(PYTHON) -c "import pathlib, shutil; shutil.rmtree(pathlib.Path('$(VENV)'), ignore_errors=True)"
