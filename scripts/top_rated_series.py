from scripts.spark_session import get_spark_session
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = get_spark_session()
spark.conf.set("spark.hadoop.io.nativeio.enabled", "false")

# Load the dataset
# Assuming you have a CSV file
df = spark.read.csv("C:\\Users\\Dell\\OneDrive\\Desktop\\Chandana\\Netflix-Data-Analysis-PySpark\\data\\netflix_series_10_columns_data.csv", header=True, inferSchema=True)

# Select relevant columns
df_selected = df.select("Series Name", "Rating", "Genre")

# Define a window specification
# Partition by Genre and order by Rating in descending order
window_spec = Window.partitionBy("Genre").orderBy(col("Rating").desc())

# Use row_number() to rank the series within each genre
df_ranked = df_selected.withColumn("rank", row_number().over(window_spec))

# Filter to keep only the top 3 series in each genre
top_rated_series = df_ranked.filter(col("rank") <= 3)

# Show the results
top_rated_series.select("Series Name", "Rating", "Genre", "rank").show()


# Stop the Spark session

