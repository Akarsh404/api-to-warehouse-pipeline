VENV ?= .venv
PYTHON ?= $(VENV)/bin/python
PIP ?= $(VENV)/bin/pip
PYTEST ?= $(VENV)/bin/pytest
UVICORN ?= $(VENV)/bin/uvicorn

ifeq ($(wildcard $(PYTHON)),)
    PYTHON = python3
    PIP = pip3
    PYTEST = pytest
    UVICORN = uvicorn
endif

.PHONY: help setup data api run test clean

help:
	@echo "API-to-Warehouse Pipeline Makefile"
	@echo "Usage:"
	@echo "  make setup   - Create .venv and install all dependencies"
	@echo "  make data    - Generate reproducible synthetic datasets"
	@echo "  make api     - Launch mock FastAPI server on port 8000"
	@echo "  make run     - Execute end-to-end ingestion pipeline"
	@echo "  make test    - Run offline pytest validation suite"
	@echo "  make clean   - Clean caches, WAL files, and test databases"

setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@if [ ! -f .env ]; then cp .env.example .env && echo "Created .env from .env.example"; fi

data:
	$(PYTHON) data/generate_data.py --customers 50 --orders 120 --seed 42

api:
	$(UVICORN) src.mock_api:app --host 127.0.0.1 --port 8000 --reload

run:
	$(PYTHON) -m src.pipeline

test:
	$(PYTEST) -v

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -f data/*.duckdb data/*.duckdb.wal
