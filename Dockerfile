# ─── Build stage ──────────────────────────────────────────────────────────────
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

# Install dependencies first (layer cache stays valid until pyproject.toml/uv.lock change)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Install the project itself
COPY src/ src/
RUN uv sync --frozen --no-dev

# ─── Runtime stage ────────────────────────────────────────────────────────────
FROM python:3.12-slim-bookworm AS runtime

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "hookrelay.main:app", "--host", "0.0.0.0", "--port", "8000"]
