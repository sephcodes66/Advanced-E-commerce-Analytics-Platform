# E-commerce Analytics Platform Makefile
# =====================================

.PHONY: help install setup test run-dashboard deploy clean docs

# Default target
help:
	@echo "🚀 E-commerce Analytics Platform"
	@echo "================================="
	@echo ""
	@echo "Available targets:"
	@echo "  install     - Install all dependencies"
	@echo "  setup       - Set up the development environment"
	@echo "  test        - Run all tests"
	@echo "  dbt-run     - Run dbt models"
	@echo "  dbt-test    - Run dbt tests"
	@echo "  dashboard   - Launch the analytics dashboard"
	@echo "  quality     - Run data quality checks"
	@echo "  deploy      - Deploy to production"
	@echo "  docs        - Generate documentation"
	@echo "  clean       - Clean up generated files"
	@echo ""

# Install dependencies
install:
	@echo "📦 Installing dependencies..."
	pip install -r requirements.txt
	dbt deps

# Setup development environment
setup: install
	@echo "🔧 Setting up development environment..."
	pre-commit install
	mkdir -p logs
	mkdir -p data/raw
	mkdir -p data/processed
	@echo "✅ Development environment ready!"

# Run dbt models
dbt-run:
	@echo "🏗️  Running dbt models..."
	dbt run --profiles-dir .

# Run dbt tests
dbt-test:
	@echo "🧪 Running dbt tests..."
	dbt test --profiles-dir .

# Run all tests
test: dbt-test
	@echo "🧪 Running Python tests..."
	pytest python_analytics/tests/ -v --cov=python_analytics --cov-report=html

# Launch analytics dashboard
dashboard:
	@echo "🚀 Launching analytics dashboard..."
	streamlit run python_analytics/advanced_dashboard.py

# Run data quality checks
quality:
	@echo "🔍 Running data quality checks..."
	dbt run-operation data_quality_check
	python python_analytics/data_quality_monitor.py

# Full pipeline execution
pipeline: dbt-run dbt-test quality
	@echo "✅ Pipeline execution completed!"

# Generate documentation
docs:
	@echo "📚 Generating documentation..."
	dbt docs generate --profiles-dir .
	dbt docs serve --profiles-dir .

# Deploy to production
deploy:
	@echo "🚀 Deploying to production..."
	dbt run --profiles-dir . --target prod
	dbt test --profiles-dir . --target prod
	@echo "✅ Production deployment completed!"

# Clean up generated files
clean:
	@echo "🧹 Cleaning up..."
	rm -rf target/
	rm -rf dbt_packages/
	rm -rf logs/*
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	@echo "✅ Cleanup completed!"

# Development workflow
dev-workflow: clean setup dbt-run dbt-test dashboard
	@echo "🎯 Development workflow completed!"

# CI/CD workflow
ci-workflow: clean install dbt-run dbt-test quality
	@echo "🎯 CI/CD workflow completed!"

# Lint and format code
lint:
	@echo "🔍 Linting code..."
	black python_analytics/
	isort python_analytics/
	flake8 python_analytics/
	sqlfluff lint models/

# Security scan
security:
	@echo "🔐 Running security scan..."
	bandit -r python_analytics/
	safety check

# Performance monitoring
monitor:
	@echo "📊 Starting performance monitoring..."
	python python_analytics/performance_monitor.py

# Backup data
backup:
	@echo "💾 Backing up data..."
	mkdir -p backups/$(shell date +%Y%m%d)
	cp -r data/ backups/$(shell date +%Y%m%d)/
	@echo "✅ Backup completed!"

# Restore data
restore:
	@echo "🔄 Restoring data..."
	@read -p "Enter backup date (YYYYMMDD): " backup_date; \
	cp -r backups/$$backup_date/data/ ./
	@echo "✅ Data restored!"

# Initialize new environment
init:
	@echo "🌟 Initializing new environment..."
	git init
	git add .
	git commit -m "Initial commit: E-commerce Analytics Platform"
	@echo "✅ Environment initialized!"