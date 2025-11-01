.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: format
format: ## Format code with ruff
	ruff format .

.PHONY: lint
lint: ## Run ruff linter
	ruff check .

.PHONY: check
check: format lint ## Run linting and formatting with ruff

.DEFAULT_GOAL := help
