# Geo Cloud Demo

A production-style demo showcasing a distributed ETL and ML pipeline using **C# Minimal API** and **PySpark**.

## Overview

This project demonstrates how to build a scalable data processing pipeline where a lightweight API orchestrates heavy compute jobs.

- **API Layer**: ASP.NET Core Minimal API (C#) handles file uploads and job management.
- **Compute Layer**: PySpark (Python) performs ETL and KMeans clustering on point-cloud data.
- **Architecture**: Designed to mimic a cloud-native pattern (e.g., API Gateway -> Lambda -> EMR/Databricks).

See [Architecture Docs](docs/Architecture.md) for details.

## Prerequisites

- **.NET 8 SDK**: For the API.
- **Python 3.8+**: For the Spark job.
- **Java 8+**: Required for Apache Spark.
- **uv** (Optional but recommended): For Python dependency management.

## Project Structure

```
/api            # C# ASP.NET Core Minimal API
/geo-spark-ml   # PySpark ETL & ML project
/docs           # Architecture documentation
```

## How to Run End-to-End

### 1. Setup Python Environment

Navigate to `geo-spark-ml` and install dependencies.

```powershell
cd geo-spark-ml
# Using uv (Recommended)
uv sync
# OR using pip
pip install -r requirements.txt
```

### 2. Start the API

Open a new terminal in the `api` folder.

**Important:** Ensure your Python environment is active or `python` is in your PATH with `pyspark` installed.
If using `uv`, you might want to activate the virtual environment first:
```powershell
# From geo-spark-ml folder
.venv\Scripts\activate
```

Then run the API:
```powershell
cd ../api
dotnet run
```
The API will start (usually at `http://localhost:5000` or similar). Note the port.

### 3. Submit a Job

Use `curl` or Postman to upload a CSV file.

**Sample CSV Format (`sample.csv`):**
```csv
x,y,z,intensity
1.0,2.0,3.0,0.5
1.1,2.1,3.1,0.6
10.0,10.0,10.0,0.9
...
```

**Upload Command:**
```powershell
# Replace PORT with your running port
curl -X POST -F "file=@path/to/your/sample.csv" http://localhost:5000/upload
```

Response:
```json
{
  "jobId": "guid-1234...",
  "status": "Queued"
}
```

### 4. Check Status & Results

Poll the status using the `jobId` from the previous step.

```powershell
curl http://localhost:5000/result/guid-1234...
```

Response (when complete):
```json
{
  "jobId": "guid-1234...",
  "status": "Completed",
  "resultPath": "E:\\Trae_projs\\geo-cloud-demo\\api\\outputs\\guid-1234...",
  "error": null,
  "createdAt": "..."
}
```

Check the `resultPath` for the processed CSV files.

## Development

- **C# API**: Located in `api/`. Run with `dotnet run`.
- **PySpark**: Located in `geo-spark-ml/`. Run manually with:
  ```powershell
  python -m geo_clustering.main --input data/sample.csv --output output/test_run
  ```

## Running with Docker (Optional)

To simulate a more production-like environment where the compute layer is containerized:

1.  **Build the Docker Image**:
    ```powershell
    cd geo-spark-ml
    docker build -t geo-spark-ml:latest .
    ```

2.  **Enable Docker in API**:
    Edit `api/appsettings.json` and set `"UseDocker": true`.

3.  **Run the API**:
    Now when you submit a job, the API will spawn a Docker container instead of a local Python process. This ensures the job runs in an isolated environment with all dependencies pre-installed.
