# Contributing to HookRelay

## Prerequisites

- Python 3.12+
- [UV](https://docs.astral.sh/uv/) — `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Docker and Docker Compose

## Setup

```bash
git clone https://github.com/rezilobz/hook-relay.git
cd hook-relay
cp .env.example .env

# Install all dependencies and set up pre-commit hooks
make install-dev

# Start infrastructure (postgres, kafka, prometheus, grafana)
make up

# Run database migrations
make migrate
```

## Project Structure

```
src/hookrelay/       Package source (src layout)
  api/               FastAPI routers and Pydantic schemas
  db/                SQLAlchemy models, engine, and session factory
  kafka/             aiokafka producer and consumer wrappers
  worker/            Delivery workers, retry scheduler, DLQ handler
  migrations/        Alembic migration scripts
tests/
  unit/              Pure unit tests — no infrastructure required
  integration/       Tests requiring live PostgreSQL/Kafka
infra/
  prometheus/        Prometheus scrape config
  grafana/           Dashboard definitions and provisioning config
```

## Code Style

Formatting and linting are handled by **ruff**. Type correctness is enforced by **mypy** at strict level.

```bash
make format      # auto-fix formatting
make lint        # check for lint violations
make typecheck   # run mypy on src/
make check       # all of the above without modifying files
```

Pre-commit hooks run automatically on every commit after `make install-dev`.

## Testing

Unit tests require no infrastructure. Integration tests spin up real services via Docker.

```bash
make test                # unit tests only
make test-integration    # integration tests (requires make up first)
make test-cov            # unit tests with HTML coverage report
```

New features must include tests. Aim to keep unit test coverage above 80%.

## Branching

- Branch from `main` — never commit directly to it.
- Branch naming: `feat/short-description`, `fix/short-description`, `chore/short-description`.
- PRs require passing CI (lint + unit tests) before merge.

## Migrations

```bash
# After changing a SQLAlchemy model:
make migrate-new MSG="add delivery_attempts table"
make migrate

# Always review the generated script in src/hookrelay/migrations/versions/ before committing.
```

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `chore:`, `docs:`, `test:`, `refactor:`.

Example: `feat: add exponential backoff retry scheduler`

## Opening Issues

For significant new features, open an issue before writing code to discuss scope and approach. For bugs, include: steps to reproduce, expected vs. actual behaviour, and relevant log output.
