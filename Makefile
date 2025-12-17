# Makefile for PySpark Jobs Development
# This file provides common development tasks and workflows

.PHONY: help init init-dev init-prod init-emr clean test test-unit test-integration test-acceptance lint format type-check security docs build package upload-s3 install-hooks pre-commit run-notebook clean-cache clean-logs clean-build clean-all

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
VENV_DIR := .venv
VENV_ACTIVATE := $(VENV_DIR)/bin/activate
PROJECT_DIR := $(shell pwd)
SRC_DIR := src
TESTS_DIR := tests
DOCS_DIR := docs

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m

# Auto set ENV to local if not specified
ENVIRONMENT ?= local

# If ENV is local then check for VENV

# Help target
help: ## Show this help message
	@echo "PySpark Jobs Development Makefile"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(BLUE)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Virtual Environment Setup
init: init-local ## Default: Initialize local testing environment
	@if [ "$(ENVIRONMENT)" == "local" ] ; then \
		pre-commit install ; \
	fi

init-local: ## Initialize local testing environment (lightweight)
	@echo "$(BLUE)Setting up local testing environment...$(NC)"
	./setup_venv.sh --type local
	@echo "$(GREEN)Local testing environment ready!$(NC)"

check-venv: ## Check if virtual environment is set up
	@if [ "$(ENVIRONMENT)" = "local" ] ; then \
		if [ -z "$(VIRTUAL_ENV)" ] ; then \
			echo "$(RED)Error: Virtual environment is not activated in local environment. Run make init to activate or create it"; \
			exit 1; \
		fi ; \
	fi

# Testing
test: ## Run all tests
	@echo "$(BLUE)Running all tests...$(NC)"
	@if [ -f $(VENV_ACTIVATE) ]; then \
		source $(VENV_ACTIVATE) && ./tests/utils/test_runner --all; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

test-unit: ## Run unit tests only
	@echo "$(BLUE)Running unit tests...$(NC)"
	@if [ -f $(VENV_ACTIVATE) ]; then \
		source $(VENV_ACTIVATE) && ./tests/utils/test_runner --unit; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

test-integration: ## Run integration tests
	@echo "$(BLUE)Running integration tests...$(NC)"
	@if [ -f $(VENV_ACTIVATE) ]; then \
		source $(VENV_ACTIVATE) && ./tests/utils/test_runner --integration; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

test-acceptance: ## Run acceptance tests
	@echo "$(BLUE)Running acceptance tests...$(NC)"
	@if [ -f $(VENV_ACTIVATE) ]; then \
		source $(VENV_ACTIVATE) && ./tests/utils/test_runner --acceptance; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

test-coverage: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	@if [ -f $(VENV_ACTIVATE) ]; then \
		source $(VENV_ACTIVATE) && ./tests/utils/test_runner --all --coverage; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

test-smoke: ## Run smoke tests (quick validation)
	@echo "$(BLUE)Running smoke tests...$(NC)"
	@if [ -f $(VENV_ACTIVATE) ]; then \
		source $(VENV_ACTIVATE) && ./tests/utils/test_runner --smoke; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

# Code Quality
lint: ## Run all linting checks
	@if [ -f $(VENV_ACTIVATE) ]; then \
		. $(VENV_ACTIVATE) && (black --check src/ tests/ || echo "Black formatting issues found") && flake8 src/ tests/; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

format: ## Format code with black and isort
	@if [ -f $(VENV_ACTIVATE) ]; then \
		. $(VENV_ACTIVATE) && black src/ tests/ && isort src/ tests/; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

# TODO: implement type checking
# type-check: ## Run type checking with mypy
# 	@echo "$(BLUE)Running type checks...$(NC)"
# 	@if [ -f $(VENV_ACTIVATE) ]; then \
# 		source $(VENV_ACTIVATE) && mypy $(SRC_DIR); \
# 	else \
# 		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
# 		exit 1; \
# 	fi

security: ## Run security scanning
	@echo "$(BLUE)Running security scans...$(NC)"
	@if [ -f $(VENV_ACTIVATE) ]; then \
		source $(VENV_ACTIVATE) && \
		bandit -r $(SRC_DIR) && \
		safety check; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

pre-commit: check-venv ## Run pre-commit on all files
	pre-commit run --all-files; \

# Documentation
docs: ## Generate documentation
	@echo "$(BLUE)Generating documentation...$(NC)"
		sphinx-build -b html $(DOCS_DIR) $(DOCS_DIR)/_build/html; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

# TODO: do we really need this? I haven't seen any notebooks in the repo
# Development Tools
run-notebook: ## Start Jupyter Lab for development
	@echo "$(BLUE)Starting Jupyter Lab...$(NC)"
	@if [ -f $(VENV_ACTIVATE) ]; then \
		source $(VENV_ACTIVATE) && jupyter lab --notebook-dir=.; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

# Build and Package
build: check-venv ## Build the package
	@echo "$(BLUE)Building package...$(NC)"
	python setup.py sdist bdist_wheel; \

package: ## Create AWS deployment package
	@echo "$(BLUE)Creating AWS deployment package...$(NC)"
	./build_aws_package.sh

upload-s3: package ## Build and upload package to S3
	@echo "$(BLUE)Building and uploading package to S3...$(NC)"
	./build_aws_package.sh --upload

# Clean targets
clean-cache: ## Clean Python cache files
	@echo "$(BLUE)Cleaning Python cache files...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

clean-logs: ## Clean log files
	@echo "$(BLUE)Cleaning log files...$(NC)"
	rm -rf logs/*.log
	rm -rf tests/logs/*.log

clean-build: ## Clean build artifacts
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	rm -rf build/
	rm -rf dist/
	rm -rf build_output/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf .mypy_cache/
	rm -rf .tox/

clean: clean-cache clean-logs ## Clean cache and log files

clean-all: clean clean-build ## Clean all generated files and artifacts
	@echo "$(BLUE)Cleaning virtual environment...$(NC)"
	rm -rf $(VENV_DIR)

# TBD do we need these?
# Database targets (if applicable)
# db-upgrade: ## Upgrade database schema
# 	@echo "$(BLUE)Upgrading database schema...$(NC)"
# 	@if [ -f $(VENV_ACTIVATE) ]; then \
# 		source $(VENV_ACTIVATE) && alembic upgrade head; \
# 	else \
# 		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
# 		exit 1; \
# 	fi

# db-downgrade: ## Downgrade database schema
# 	@echo "$(BLUE)Downgrading database schema...$(NC)"
# 	@if [ -f $(VENV_ACTIVATE) ]; then \
# 		source $(VENV_ACTIVATE) && alembic downgrade -1; \
# 	else \
# 		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
# 		exit 1; \
# 	fi

# Utility targets
install-deps: check-venv ## Install/update dependencies
	@echo "$(BLUE)Installing/updating dependencies...$(NC)"
	pip install --upgrade pip setuptools wheel
	pip install -r requirements-dev.txt
	pip install -e .;

freeze: ## Freeze current dependencies
	@echo "$(BLUE)Freezing current dependencies...$(NC)"
	@if [ -f $(VENV_ACTIVATE) ]; then \
		source $(VENV_ACTIVATE) && pip freeze > requirements-frozen.txt; \
		echo "$(GREEN)Dependencies frozen to requirements-frozen.txt$(NC)"; \
	else \
		echo "$(RED)Virtual environment not found. Run 'make init' first.$(NC)"; \
		exit 1; \
	fi

# TODO: remove these as the CI/CD should use the same tagets as development
# CI/CD targets)
ci-test: ## Run tests for CI environment
	@echo "$(BLUE)Running CI tests...$(NC)"
	pytest $(TESTS_DIR) -v --junitxml=test-results.xml --cov=$(SRC_DIR) --cov-report=xml

ci-lint: ## Run linting for CI environment
	@echo "$(BLUE)Running CI linting...$(NC)"
	flake8 $(SRC_DIR) $(TESTS_DIR) --format=junit-xml --output-file=lint-results.xml

ci-security: ## Run security checks for CI environment
	@echo "$(BLUE)Running CI security checks...$(NC)"
	bandit -r $(SRC_DIR) -f json -o security-results.json
