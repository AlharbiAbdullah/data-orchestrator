FROM python:3.11-slim AS builder

# Install UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set environment variables
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock .python-version ./

# Install dependencies
RUN uv sync --frozen --no-dev --no-install-project

# Production stage
FROM python:3.11-slim

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Set PATH to use virtual environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app:$PYTHONPATH"

# Copy application code and config
COPY pyproject.toml /app/
COPY dagster_project/ /app/dagster_project/
COPY config/ /app/config/
COPY data/ /app/data/

# Create dagster home directory with empty config
ENV DAGSTER_HOME=/app/.dagster
RUN mkdir -p $DAGSTER_HOME && touch $DAGSTER_HOME/dagster.yaml

# Expose Dagster webserver port
EXPOSE 3000

# Default command
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
