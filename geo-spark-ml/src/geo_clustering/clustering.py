from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

def run_kmeans(df, input_cols, k=3, prediction_col="cluster"):
    """
    Runs KMeans clustering on the input dataframe.
    """
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    df_vector = assembler.transform(df)
    
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features").setPredictionCol(prediction_col)
    model = kmeans.fit(df_vector)
    
    predictions = model.transform(df_vector)
    
    # Return predictions with features column (needed for evaluation)
    return predictions

def evaluate_clustering(predictions, prediction_col="cluster", features_col="features"):
    """
    Evaluates clustering quality using Silhouette score.
    """
    evaluator = ClusteringEvaluator(predictionCol=prediction_col, featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean")
    silhouette = evaluator.evaluate(predictions)
    return silhouette
