import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
import math

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371 # Radius of earth in kilometers
    return c * r

def add_distance_feature(df, ref_lat, ref_lon, lat_col="lat", lon_col="lon", output_col="distance_to_center_km"):
    """
    Adds a column with the distance to a reference point in km.
    """
    # Register UDF
    distance_udf = F.udf(lambda lat, lon: haversine_distance(lat, lon, ref_lat, ref_lon), DoubleType())
    
    return df.withColumn(output_col, distance_udf(F.col(lat_col), F.col(lon_col)))

def add_magnitude_feature(df, x_col="x", y_col="y", z_col="z", output_col="magnitude"):
    """
    Adds a column with the magnitude of the vector (distance from origin).
    """
    return df.withColumn(output_col, F.sqrt(F.col(x_col)**2 + F.col(y_col)**2 + F.col(z_col)**2))
