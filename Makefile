.DEFAULT_GOAL := help
.PHONY: help install install-dev lint format format-check typecheck check \
        test test-unit test-cov up down down-volumes logs \
        migrate migrate-new db-shell clean

UV  := uv
SRC := src/hookrelay
TST := tests

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Dependencies ─────────────────────────────────────────────────────────────

install: ## Install runtime dependencies only
	$(UV) sync --no-dev

install-dev: ## Install all dependencies (runtime + dev + test + lint) and pre-commit hooks
	$(UV) sync --group dev --group test --group lint
	$(UV) run pre-commit install

# ─── Code quality ─────────────────────────────────────────────────────────────

lint: ## Run ruff linter
	$(UV) run ruff check $(SRC) $(TST)

format: ## Auto-format code with ruff
	$(UV) run ruff format $(SRC) $(TST)

format-check: ## Check formatting without modifying files (used in CI)
	$(UV) run ruff format --check $(SRC) $(TST)

typecheck: ## Run mypy type checker on source only
	$(UV) run mypy $(SRC)

check: lint format-check typecheck ## Run all static analysis (no file modification)

# ─── Tests ────────────────────────────────────────────────────────────────────

test: ## Run all tests (testcontainers spins up required infrastructure)
	$(UV) run pytest $(TST) -v

test-unit: ## Run unit tests only — fast, no Docker required
	$(UV) run pytest $(TST) -m "not integration" -v

test-cov: ## Run all tests with coverage report
	$(UV) run pytest $(TST) \
		--cov=$(SRC) --cov-report=term-missing --cov-report=html

# ─── Infrastructure ───────────────────────────────────────────────────────────

up: ## Build and start all services, then run database migrations
	docker compose up -d --build --wait
	$(UV) run alembic upgrade head

down: ## Stop infrastructure services
	docker compose down

down-volumes: ## Stop services and delete all persistent data (destructive)
	docker compose down -v

logs: ## Tail logs (use SVC="api worker" to filter by service)
	docker compose logs -f $(SVC)

# ─── Database ─────────────────────────────────────────────────────────────────

migrate: ## Run pending Alembic migrations
	$(UV) run alembic upgrade head

migrate-new: ## Create a new Alembic migration (usage: make migrate-new MSG="describe change")
	$(UV) run alembic revision --autogenerate -m "$(MSG)"

db-shell: ## Open a psql shell in the local Docker PostgreSQL container
	docker compose exec postgres psql -U hookrelay -d hookrelay

# ─── Cleanup ──────────────────────────────────────────────────────────────────

clean: ## Remove Python caches and build artifacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage dist build
