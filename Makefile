# DeFi BI-ETL Project Makefile
# Common development tasks and shortcuts

.PHONY: help install install-dev test test-cov lint format type-check clean setup run-etl run-extract run-transform run-load docs

# Default target
help:
	@echo "DeFi BI-ETL Project - Available Commands"
	@echo "=========================================="
	@echo ""
	@echo "Setup & Installation:"
	@echo "  install      Install production dependencies"
	@echo "  install-dev  Install development dependencies"
	@echo "  setup        Run complete setup script"
	@echo ""
	@echo "Development:"
	@echo "  test         Run tests"
	@echo "  test-cov     Run tests with coverage"
	@echo "  lint         Run linting checks"
	@echo "  format       Format code with Black and isort"
	@echo "  type-check   Run type checking with mypy"
	@echo "  clean        Clean generated files and caches"
	@echo ""
	@echo "ETL Pipeline:"
	@echo "  run-etl      Run complete ETL pipeline"
	@echo "  run-extract  Run data extraction only"
	@echo "  run-transform Run data transformation only"
	@echo "  run-load     Run data loading only"
	@echo ""
	@echo "Documentation:"
	@echo "  docs         Generate documentation"
	@echo ""
	@echo "Quality Checks:"
	@echo "  check-all    Run all quality checks"

# Installation
install:
	@echo "Installing production dependencies..."
	poetry install --only main

install-dev:
	@echo "Installing development dependencies..."
	poetry install

setup:
	@echo "Running setup script..."
	./setup.sh

# Testing
test:
	@echo "Running tests..."
	poetry run pytest

test-cov:
	@echo "Running tests with coverage..."
	poetry run pytest --cov=src --cov-report=html --cov-report=term

# Code Quality
lint:
	@echo "Running linting checks..."
	poetry run flake8 src/ tests/
	poetry run bandit -r src/

format:
	@echo "Formatting code..."
	poetry run black src/ tests/
	poetry run isort src/ tests/

type-check:
	@echo "Running type checks..."
	poetry run mypy src/

# ETL Pipeline
run-etl:
	@echo "Running complete ETL pipeline..."
	poetry run python -m src.cli quickrun

run-extract:
	@echo "Running data extraction..."
	poetry run python -m src.cli extract coingecko --all
	poetry run python -m src.cli extract defillama --all
	poetry run python -m src.cli extract dexscreener --all

run-transform:
	@echo "Running data transformation..."
	poetry run python -m src.cli transform all

run-load:
	@echo "Running data loading..."
	poetry run python -m src.cli load tableau

# Documentation
docs:
	@echo "Generating documentation..."
	@echo "Documentation is in markdown format in the root directory"

# Quality Checks
check-all: lint type-check test
	@echo "All quality checks completed!"



# Cleaning
clean:
	@echo "Cleaning generated files and caches..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type d -name "dist" -exec rm -rf {} +
	find . -type d -name "build" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type f -name "coverage.xml" -delete
	@echo "Cleanup completed!"

# Data Management
clean-data:
	@echo "Cleaning data directories..."
	rm -rf data/raw/*
	rm -rf data/processed/*
	rm -rf dashboards/tableau/*
	@echo "Data cleanup completed!"

backup-data:
	@echo "Creating data backup..."
	tar -czf "data_backup_$(date +%Y%m%d_%H%M%S).tar.gz" data/ dashboards/
	@echo "Data backup created!"

# Development Workflow
dev-setup: install-dev setup
	@echo "Development environment setup completed!"

quick-test: lint type-check test
	@echo "Quick quality check completed!"

# Docker Support (if needed in the future)
docker-build:
	@echo "Building Docker image..."
	docker build -t defi-bi-seminar .

docker-run:
	@echo "Running Docker container..."
	docker run -it --rm defi-bi-seminar

# Git Helpers
git-hooks:
	@echo "Git hooks are disabled for this project"

git-clean:
	@echo "Cleaning git repository..."
	git clean -fd
	git reset --hard HEAD

# Performance
profile:
	@echo "Running performance profiling..."
	poetry run python -m cProfile -o profile.stats -m src.cli quickrun

# Security
security-check:
	@echo "Running security checks..."
	poetry run bandit -r src/ -f json -o bandit-report.json
	poetry run safety check

# Dependencies
update-deps:
	@echo "Updating dependencies..."
	poetry update

lock-deps:
	@echo "Locking dependency versions..."
	poetry lock

# Environment
env-check:
	@echo "Checking environment..."
	@echo "Python version: $(shell poetry run python --version)"
	@echo "Poetry version: $(shell poetry --version)"
	@echo "Current directory: $(PWD)"
	@echo "Virtual environment: $(shell poetry env info --path)"

# Help for specific targets
test-help:
	@echo "Test options:"
	@echo "  test                    # Run all tests"
	@echo "  test-cov               # Run tests with coverage"
	@echo "  poetry run pytest -k 'test_name'  # Run specific test"
	@echo "  poetry run pytest tests/test_file.py  # Run specific test file"

etl-help:
	@echo "ETL Pipeline options:"
	@echo "  run-etl                # Complete pipeline"
	@echo "  run-extract            # Data extraction only"
	@echo "  run-transform          # Data transformation only"
	@echo "  run-load               # Data loading only"
	@echo "  poetry run python -m src.cli --help  # More options"
