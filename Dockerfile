# --- Builder Stage ---
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

# Optimize build performance
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

# Copy only metadata first to leverage layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies (without dev)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# --- Final Stage ---
FROM python:3.13-slim-bookworm AS final

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy app source code
COPY src ./src

# Ensure the venv is used by default
ENV PATH="/app/.venv/bin:$PATH"

# Expose FastAPI default port
EXPOSE 8000

# Use Uvicorn as the entrypoint
# Replace `main:app` with your actual module and app variable if different
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
