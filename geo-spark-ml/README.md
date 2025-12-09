# Geo Spark ML Demo

This is a demo project for processing geospatial data using PySpark and Spark MLlib. It calculates the distance of points to a reference location and clusters them using KMeans.

## Project Structure

```
geo-spark-ml/
  data/                # Sample data
  src/                 # Source code
    geo_clustering/    # Main package
  configs/             # Configuration files
  Dockerfile           # Docker configuration
  .github/             # CI/CD workflows
```

## Prerequisites

- Python 3.8+
- Java 8+ (Required for Spark)

## Setup and Run Locally

### Option A: Using `uv` (Recommended)

[uv](https://github.com/astral-sh/uv) is a modern, extremely fast Python package manager that handles virtual environments automatically. This avoids dependency conflicts (like the one you might see with `pandas-ta` or `numpy`).

1.  **Install uv:**
    ```powershell
    # Windows (PowerShell)
    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

2.  **Run with uv:**
    `uv` will automatically create a virtual environment, install dependencies from `pyproject.toml`, and run the script in isolation.

    ```powershell
    # Run the script directly (uv handles the venv and python path)
    uv run src/geo_clustering/main.py --config configs/local.yaml
    ```
    *Note: We point directly to the file so uv adds the current directory to PYTHONPATH automatically.*

### Option B: Using standard pip

1.  **Create a virtual environment (Optional but recommended):**
    ```bash
    python -m venv .venv
    .venv\Scripts\activate
    ```

2.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

3.  **Run the application:**

    Make sure you are in the `geo-spark-ml` directory.

    ```bash
    export PYTHONPATH=$PYTHONPATH:$(pwd)/src
    # On Windows PowerShell:
    # $env:PYTHONPATH = "$env:PYTHONPATH;$(Get-Location)\src"
    
    python -m geo_clustering.main --config configs/local.yaml
    ```

## Run with Docker

1.  **Build the image:**

    ```bash
    docker build -t geo-spark-ml .
    ```

2.  **Run the container:**

    ```bash
    docker run geo-spark-ml
    ```

## Cloud Adaptation

To adapt this project for a cloud environment like Databricks, AWS EMR, or Azure Synapse:

1.  **Storage:** Change the `input_path` and `output_path` in the configuration file (e.g., `configs/cloud_example.yaml`) to point to object storage:
    -   AWS S3: `s3://my-bucket/data/points.csv`
    -   Azure Blob/ADLS: `abfss://my-container@my-account.dfs.core.windows.net/data/points.csv`

2.  **Cluster Configuration:** In a managed Spark environment, the `SparkSession` is usually created automatically or managed by the platform. You might need to remove the `.getOrCreate()` call or adjust the builder configuration to use the cluster's resources.

3.  **Deployment:**
    -   **Databricks:** You can package the `src` directory into a Python Wheel or Egg and install it as a library on the cluster. You can then run `main.py` as a job.
    -   **EMR/Dataproc:** You can submit the job using `spark-submit`, passing the main file and the zipped dependencies.

## CI/CD

A simple GitHub Actions workflow is included in `.github/workflows/ci.yml` which installs dependencies and checks for syntax errors on every push.
