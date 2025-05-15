from scripts.spark_session import get_spark_session
from pyspark.sql import functions as F
from matplotlib import pyplot as plt

# Initialize Spark session
spark = get_spark_session()

# Load the dataset
df = spark.read.csv("C:\\Users\\Dell\\OneDrive\\Desktop\\Chandana\\Netflix-Data-Analysis-PySpark\\data\\netflix_series_10_columns_data.csv", header=True, inferSchema=True)

# Group and calculate average ratings by total seasons
season_rating_analysis = df.groupBy("Total Seasons").agg(F.avg("Rating").alias("Avg_Rating"))

# Sort by Total Seasons
forplot = season_rating_analysis.orderBy("Total Seasons")

# Collect data to Python lists
data = forplot.collect()
seasons = [row["Total Seasons"] for row in data]
avg_ratings = [row["Avg_Rating"] for row in data]

# Set figure size
plt.figure(figsize=(10, 6))

# Set title and labels
plt.title("Average Rating for Shows by Total Seasons")
plt.xlabel("Total Seasons")
plt.ylabel("Average Rating")

# Plot the data
plt.plot(seasons, avg_ratings, marker='o', linestyle='-', color='blue')

# Display the plot
plt.grid(True)
plt.tight_layout()
plt.show()

# Stop the Spark session

