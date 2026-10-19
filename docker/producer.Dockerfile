# Producer image: runs the Alpaca -> Kafka producer.
FROM python:3.12-slim

# Copy the uv binary from its official image instead of installing it with pip.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Precompile Python files for faster startup; copy (not link) files into the venv.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Install dependencies BEFORE copying the source code. Docker caches each step
# (layer), so editing code later doesn't trigger a slow reinstall of everything.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project

# Now copy the code and install the project itself.
COPY src ./src
RUN uv sync --locked --no-dev

# Run as a regular user, not root: if the container were ever compromised,
# the attacker would have far fewer permissions.
RUN useradd --create-home appuser
USER appuser

ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "-m", "marketpulse.producer.main"]
