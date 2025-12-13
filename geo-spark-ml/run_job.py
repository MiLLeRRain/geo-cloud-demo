import os
import sys
import urllib.request
import subprocess
import json
from urllib.parse import urlparse

def download_file(url, local_path):
    print(f"Downloading input from {url} to {local_path}...")
    urllib.request.urlretrieve(url, local_path)

def upload_file(local_path, url):
    if not os.path.exists(local_path):
        print(f"Warning: File {local_path} does not exist, skipping upload.")
        return
    
    print(f"Uploading {local_path} to {url}...")
    # Create a PUT request with the file data
    with open(local_path, 'rb') as f:
        data = f.read()
    
    req = urllib.request.Request(url, data=data, method='PUT')
    req.add_header('x-ms-blob-type', 'BlockBlob')
    urllib.request.urlopen(req)

def main():
    # 1. Get configuration from Environment Variables
    input_sas = os.environ.get("INPUT_SAS_URL")
    output_sas = os.environ.get("OUTPUT_SAS_URL")
    metrics_sas = os.environ.get("METRICS_SAS_URL")
    
    if not input_sas:
        # Fallback to Local/Passthrough Mode if arguments are provided
        if len(sys.argv) > 1:
            print("Running in Local/Passthrough Mode...")
            # Pass all arguments directly to the underlying script
            cmd = ["python", "-m", "geo_clustering.main"] + sys.argv[1:]
            try:
                subprocess.run(cmd, check=True)
                sys.exit(0)
            except subprocess.CalledProcessError as e:
                sys.exit(e.returncode)
        else:
            print("Error: INPUT_SAS_URL environment variable is missing and no arguments provided.")
            sys.exit(1)

    # Local paths
    input_file = "input.csv"
    output_dir = "output_dir"
    os.makedirs(output_dir, exist_ok=True)

    # 2. Download Input
    try:
        download_file(input_sas, input_file)
    except Exception as e:
        print(f"Failed to download input file: {e}")
        sys.exit(1)

    # 3. Run Spark Job
    print("Starting Spark Job...")
    cmd = [
        "python", "-m", "geo_clustering.main",
        "--input", input_file,
        "--output", output_dir
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
    
    if result.returncode != 0:
        print("Spark job failed.")
        sys.exit(result.returncode)

    # 4. Upload Results
    if output_sas:
        try:
            upload_file(os.path.join(output_dir, "results.csv"), output_sas)
        except Exception as e:
            print(f"Failed to upload results: {e}")
            sys.exit(1) # Fail the job if upload fails

    if metrics_sas:
        try:
            upload_file(os.path.join(output_dir, "metrics.json"), metrics_sas)
        except Exception as e:
            print(f"Failed to upload metrics: {e}")
            # Metrics failure might be non-critical, but let's log it. 
            # If you want strict failure, uncomment the next line:
            # sys.exit(1) 

    print("Job completed successfully.")

if __name__ == "__main__":
    main()
