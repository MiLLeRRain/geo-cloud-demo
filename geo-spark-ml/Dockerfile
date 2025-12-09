FROM python:3.9-slim

# Install uv for faster package installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean;

WORKDIR /app

COPY pyproject.toml .
# Generate requirements from pyproject.toml and install
RUN uv pip compile pyproject.toml -o requirements.txt && \
    uv pip install --system --no-cache -r requirements.txt

COPY src/ src/
COPY configs/ configs/
COPY data/ data/

ENV PYTHONPATH="${PYTHONPATH}:/app/src"

CMD ["python", "-m", "geo_clustering.main", "--config", "configs/local.yaml"]
