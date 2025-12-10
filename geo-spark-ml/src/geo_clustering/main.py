import argparse
import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from geo_clustering.config import Config
from geo_clustering.features import add_distance_feature, add_magnitude_feature
from geo_clustering.clustering import run_kmeans, evaluate_clustering

def main():
    # Ensure PySpark uses the same Python interpreter as the driver (this venv)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    parser = argparse.ArgumentParser(description="Geo Spark ML Job")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--input", help="Path to input CSV file")
    parser.add_argument("--output", help="Path to output directory")
    args = parser.parse_args()

    # Load config if provided, otherwise use defaults or args
    config = None
    if args.config:
        config = Config(args.config)
    
    # Determine input/output paths
    input_path = args.input if args.input else (config.input_path if config else None)
    output_path = args.output if args.output else (config.output_path if config else None)

    if not input_path:
        print("Error: Input path must be provided via --input or --config")
        sys.exit(1)

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("GeoClustering") \
        .getOrCreate()
    
    print("-" * 30)
    print(f"Spark Master: {spark.sparkContext.master}")
    print(f"Reading data from {input_path}")
    print("-" * 30)

    # Load data
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Basic Cleaning: Remove rows with nulls in critical columns
    # Check which columns we have
    columns = df.columns
    print(f"Columns detected: {columns}")

    df_clean = df.dropna()

    # Feature Engineering & Clustering
    feature_cols = []
    
    if "x" in columns and "y" in columns and "z" in columns:
        print("Point cloud data detected (x, y, z). Calculating magnitude...")
        df_features = add_magnitude_feature(df_clean)
        feature_cols = ["x", "y", "z", "magnitude"]
        if "intensity" in columns:
            feature_cols.append("intensity")
    elif "lat" in columns and "lon" in columns:
        print("Geo data detected (lat, lon). Calculating distance to center...")
        # Fallback to config for reference point if available, else default
        ref_lat = config.reference_point['lat'] if config and hasattr(config, 'reference_point') else 0.0
        ref_lon = config.reference_point['lon'] if config and hasattr(config, 'reference_point') else 0.0
        df_features = add_distance_feature(df_clean, ref_lat, ref_lon)
        feature_cols = ["distance_to_center_km"]
        if "elevation" in columns:
            feature_cols.append("elevation")
    else:
        print("Unknown schema. Using all numeric columns for clustering.")
        # simple fallback
        feature_cols = [c for c, t in df.dtypes if t in ('int', 'double', 'float')]
        df_features = df_clean

    print(f"Clustering using features: {feature_cols}")
    
    # Clustering
    df_clustered = run_kmeans(df_features, feature_cols, k=3)
    
    # Evaluate Clustering
    try:
        silhouette_score = evaluate_clustering(df_clustered)
        print(f"Silhouette Score: {silhouette_score}")
        
        # Save metrics to JSON
        if output_path:
            import json
            metrics = {"silhouette_score": silhouette_score}
            with open(os.path.join(output_path, "metrics.json"), "w") as f:
                json.dump(metrics, f)
                
    except Exception as e:
        print(f"Could not evaluate clustering (maybe only 1 cluster or error): {e}")

    # Show results
    print("Clustering Results Sample:")
    df_clustered.show(5)
    
    # Save output
    if output_path:
        print(f"Saving results to {output_path}")
        
        # Drop 'features' column because CSV datasource doesn't support Vector types
        df_save = df_clustered.drop("features")

        # On Windows without Hadoop/Winutils, Spark's write.csv fails.
        if os.name == 'nt' and 'HADOOP_HOME' not in os.environ:
            print("Windows detected without HADOOP_HOME. Saving via Pandas to avoid winutils error.")
            os.makedirs(output_path, exist_ok=True)
            output_file = os.path.join(output_path, "results.csv")
            # Select all columns plus cluster
            df_save.toPandas().to_csv(output_file, index=False)
        else:
            df_save.write.mode("overwrite").csv(output_path, header=True)

    spark.stop()

if __name__ == "__main__":
    main()
