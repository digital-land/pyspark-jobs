# Makefile for PySpark Jobs Development
#
# ENVIRONMENT controls venv behaviour:
#   local (default) — requires an active virtual environment
#   anything else    — runs commands directly (CI, Docker, etc.)
#
# MIN_PYTHON_VERSION can be overridden: make check-env MIN_PYTHON_VERSION=3.10

.PHONY: help check-env init clean test test-unit test-integration test-acceptance lint format security pre-commit build package upload-s3 clean-cache clean-logs clean-build clean-all

.DEFAULT_GOAL := help

# Variables
SRC_DIR := src
TESTS_DIR := tests
VENV_DIR := .venv
ENVIRONMENT ?= local
MIN_PYTHON_VERSION ?= 3.9

# Colors
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m

# -- Environment gate --------------------------------------------------------
# Targets that need Python depend on check-env.
# ENVIRONMENT=local  → error unless a virtual environment is active
# Anything else      → pass through (CI, Docker, etc.)
# Also verifies the Python version meets the minimum requirement.
check-env:
	@if [ "$(ENVIRONMENT)" = "local" ] && [ -z "$$VIRTUAL_ENV" ]; then \
		echo "$(RED)No active virtual environment.$(NC)"; \
		echo "Create one with:"; \
		echo "  python3 -m venv $(VENV_DIR) && source $(VENV_DIR)/bin/activate"; \
		exit 1; \
	fi
	@python3 -c "\
	import sys; \
	req = tuple(int(x) for x in '$(MIN_PYTHON_VERSION)'.split('.')); \
	cur = sys.version_info[:2]; \
	sys.exit(0) if cur >= req else (\
	    print(f'\033[0;31mPython {sys.version} does not meet minimum {\"$(MIN_PYTHON_VERSION)\"}.\033[0m'), \
	    sys.exit(1) \
	)" 2>&1

# -- Help --------------------------------------------------------------------
help: ## Show this help message
	@echo "PySpark Jobs Development Makefile"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(BLUE)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# -- Initialisation ----------------------------------------------------------
init: check-env ## Install dependencies
	@echo "$(BLUE)Initialising (ENVIRONMENT=$(ENVIRONMENT))...$(NC)"
	pip install --upgrade pip setuptools wheel
	pip install -r requirements-local.txt
	pip install -e .
	@if [ "$(ENVIRONMENT)" = "local" ] && command -v pre-commit >/dev/null 2>&1; then \
		pre-commit install; \
	fi
	@echo "$(GREEN)Environment ready!$(NC)"

# -- Testing -----------------------------------------------------------------
test: check-env ## Run all tests with coverage
	pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

test-unit: check-env ## Run unit tests only
	pytest tests/unit/ -v

test-integration: check-env ## Run integration tests
	pytest tests/integration/ -v

test-acceptance: check-env ## Run acceptance tests
	pytest tests/acceptance/ -v

test-coverage: check-env ## Run tests with HTML coverage report
	pytest tests/ --cov=src --cov-report=html

test-quick: check-env ## Run quick tests (unit tests only, no coverage)
	pytest tests/unit/ -v --tb=short

test-parallel: check-env ## Run tests in parallel
	pytest tests/ -n auto --cov=src

# -- Code Quality ------------------------------------------------------------
lint: check-env ## Run all linting checks
	black --check src/ tests/ 2>&1 || (echo "$(YELLOW)Black formatting issues found. Run 'make format' to fix.$(NC)" && exit 1)
	flake8 src/ tests/
	@echo "$(GREEN)All linting checks passed!$(NC)"

format: check-env ## Format code with black and isort
	black src/ tests/
	isort src/ tests/
	@echo "$(GREEN)Code formatting complete!$(NC)"

security: check-env ## Run security scanning
	bandit -r $(SRC_DIR)
	safety check

pre-commit: check-env ## Run pre-commit on all files
	pre-commit run --all-files

# -- Build and Package -------------------------------------------------------
build: check-env ## Build the package
	python setup.py sdist bdist_wheel

package: ## Create AWS deployment package
	./bin/build_aws_package.sh

upload-s3: package ## Build and upload package to S3
	./bin/build_aws_package.sh --upload

# -- Clean -------------------------------------------------------------------
clean-cache: ## Clean Python cache files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

clean-logs: ## Clean log files
	rm -rf logs/*.log
	rm -rf tests/logs/*.log

clean-build: ## Clean build artifacts
	rm -rf build/ dist/ build_output/ *.egg-info/
	rm -rf .pytest_cache/ htmlcov/ .coverage .mypy_cache/ .tox/

clean: clean-cache clean-logs ## Clean cache and log files

clean-all: clean clean-build ## Clean all generated files and artifacts
	rm -rf $(VENV_DIR)
