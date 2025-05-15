from pyspark.sql import SparkSession
import sys

def get_spark_session(app_name="NetflixApp"):
    spark = SparkSession.builder \
    .appName(app_name) \
    .config("spark.pyspark.python", sys.executable) \
    .config("spark.pyspark.driver.python", sys.executable) \
    .config("spark.python.profile", "true") \
    .getOrCreate()

    
    return spark

if __name__ == "__main__":
    spark = get_spark_session()
    print("Spark session initialized successfully.")
