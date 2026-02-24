.RECIPEPREFIX := >

PYTHON ?= python
VENV ?= .venv

ifeq ($(OS),Windows_NT)
VENV_BIN := $(VENV)/Scripts
else
VENV_BIN := $(VENV)/bin
endif

.PHONY: init format lint test clean

init:
>$(PYTHON) -m venv $(VENV)
>$(VENV_BIN)/python -m pip install --upgrade pip
>$(VENV_BIN)/pip install pre-commit black ruff pytest
>$(VENV_BIN)/pre-commit install

format:
>$(VENV_BIN)/ruff format ingestion spark warehouse scripts tests
>$(VENV_BIN)/black ingestion spark warehouse scripts tests

lint:
>$(VENV_BIN)/ruff check ingestion spark warehouse scripts tests

test:
>$(VENV_BIN)/pytest tests

clean:
>$(PYTHON) -c "import pathlib, shutil; shutil.rmtree(pathlib.Path('$(VENV)'), ignore_errors=True)"
