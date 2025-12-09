from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

def run_kmeans(df, input_cols, k=3, prediction_col="cluster"):
    """
    Runs KMeans clustering on the input dataframe.
    """
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    df_vector = assembler.transform(df)
    
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features").setPredictionCol(prediction_col)
    model = kmeans.fit(df_vector)
    
    predictions = model.transform(df_vector)
    
    # Drop the features vector column to keep output clean
    return predictions.drop("features")
