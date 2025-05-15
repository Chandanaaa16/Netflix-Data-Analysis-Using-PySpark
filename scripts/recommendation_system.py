from scripts.spark_session import get_spark_session
from pyspark.sql import functions as F

spark = get_spark_session("Recommendation System")
df = spark.read.csv("data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

print("Available Genres:")
df.select("Genre").distinct().show(truncate=False)

print("Available Languages:")
df.select("Language").distinct().show(truncate=False)

genre = input("Select Your Genre: ").strip().title()
language = input("Select Your Language: ").strip().title()

ds = df.filter(F.col("Genre") == genre).filter(F.col("Language") == language).orderBy(F.col("Rating").desc())

count = ds.count()
print(f"Matching records found: {count}")

if count > 0:
    ds.show(3)
else:
    print("No matching records found.")
