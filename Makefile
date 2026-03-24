.PHONY: help setup start stop demo test lint format deploy teardown validate-cfn clean logs

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*##"}; {printf "  %-20s %s\n", $$1, $$2}'

setup: ## Install dependencies and set up local environment
	pip install -r requirements.txt -r requirements-dev.txt
	@echo "Setup complete. Copy .env.example to .env and fill in your values."

start: ## Start all Docker Compose services
	docker compose -f docker/docker-compose.yaml up -d

stop: ## Stop all Docker Compose services
	docker compose -f docker/docker-compose.yaml down

demo: ## Run a quick demo (limited iterations)
	MAX_ITERATIONS=5 docker compose -f docker/docker-compose.yaml up

test: ## Run unit tests with coverage
	pytest tests/unit -v --cov=src --cov-report=term-missing --cov-fail-under=80

lint: ## Run linters (ruff, mypy)
	ruff check src/ tests/
	mypy src/ --ignore-missing-imports

format: ## Auto-format code with black and ruff
	black src/ tests/
	ruff check --fix src/ tests/

deploy: ## Deploy AWS infrastructure via CloudFormation
	bash cloudformation/deploy-all.sh

teardown: ## Destroy all AWS CloudFormation stacks
	bash cloudformation/teardown-all.sh

validate-cfn: ## Validate CloudFormation templates
	cfn-lint cloudformation/*.yaml

clean: ## Remove build artifacts and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .coverage htmlcov/

logs: ## Tail logs from all Docker Compose services
	docker compose -f docker/docker-compose.yaml logs -f
