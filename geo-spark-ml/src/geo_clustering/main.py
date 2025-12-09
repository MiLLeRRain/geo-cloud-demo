import argparse
from pyspark.sql import SparkSession
from geo_clustering.config import Config
from geo_clustering.features import add_distance_feature
from geo_clustering.clustering import run_kmeans

def main():
    parser = argparse.ArgumentParser(description="Geo Spark ML Job")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    args = parser.parse_args()

    # Load config
    config = Config(args.config)
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("GeoClustering") \
        .getOrCreate()
    
    print(f"Running in {config.mode} mode")
    print(f"Reading data from {config.input_path}")

    # Load data
    df = spark.read.csv(config.input_path, header=True, inferSchema=True)
    
    # Feature Engineering
    ref_point = config.reference_point
    df_features = add_distance_feature(df, ref_point['lat'], ref_point['lon'])
    
    # Clustering
    feature_cols = ["distance_to_center_km", "elevation"]
    df_clustered = run_kmeans(df_features, feature_cols, k=3)
    
    # Show results
    result_cols = ["id", "lat", "lon", "elevation", "distance_to_center_km", "cluster"]
    df_result = df_clustered.select(result_cols)
    
    print("Clustering Results:")
    df_result.show()
    
    # Save output if needed (optional for this demo, but good practice)
    if config.output_path:
        print(f"Saving results to {config.output_path}")
        df_result.write.mode("overwrite").csv(config.output_path, header=True)

    spark.stop()

if __name__ == "__main__":
    main()
