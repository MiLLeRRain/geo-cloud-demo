FROM python:3.9-slim

# Ensure Python output is sent directly to terminal (container logs) without buffering
ENV PYTHONUNBUFFERED=1

# Install uv for faster package installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install Java (required for Spark) - using headless and no recommends to save space
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
# Generate requirements from pyproject.toml and install
RUN uv pip compile pyproject.toml -o requirements.txt && \
    uv pip install --system --no-cache -r requirements.txt

COPY src/ src/
COPY configs/ configs/
COPY run_job.py .

# We do NOT copy data/ to keep the image small. Data should be injected via Blob Storage.
# COPY data/ data/
COPY run_job.py .

ENV PYTHONPATH="${PYTHONPATH}:/app/src"

# Use ENTRYPOINT to run the wrapper script
ENTRYPOINT ["python", "run_job.py"]
