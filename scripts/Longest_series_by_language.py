from scripts.spark_session import get_spark_session
from pyspark.sql.functions import col
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark session
spark = get_spark_session()

# Load the dataset
df = spark.read.csv("data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

# Select relevant columns
long_series = df.select(['Series Name', 'Total Seasons', 'Language'])

# Define a window to rank series within each language by Total Seasons
window_spec = Window.partitionBy("Language").orderBy(F.col('Total Seasons').desc())

# Add rank column and filter top-ranked series
long_series_ranked = long_series.withColumn("Rank", F.row_number().over(window_spec))
long_running = long_series_ranked.filter(F.col('Rank') == 1)

# Show the results
long_running.show()