.RECIPEPREFIX := >

PYTHON ?= python
VENV ?= .venv
PROJECT_DIRS := ingestion spark warehouse dashboard infra tests scripts

ifeq ($(OS),Windows_NT)
VENV_BIN := $(VENV)/Scripts
else
VENV_BIN := $(VENV)/bin
endif

PIP := $(VENV_BIN)/pip
PY := $(VENV_BIN)/python

.PHONY: init format lint test-unit test-integration test precommit ci clean

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

clean:
>$(PYTHON) -c "import pathlib, shutil; shutil.rmtree(pathlib.Path('$(VENV)'), ignore_errors=True)"
