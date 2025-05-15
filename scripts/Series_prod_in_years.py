from scripts.spark_session import get_spark_session
import matplotlib.pyplot as plt
import streamlit as st

# Initialize Spark session
spark = get_spark_session()

# Load the dataset
df = spark.read.csv(
    "C:\\Users\\Dell\\OneDrive\\Desktop\\Chandana\\Netflix-Data-Analysis-PySpark\\data\\netflix_series_10_columns_data.csv",
    header=True, inferSchema=True
)

# Group by 'Release Year' and count number of series released
ds = df.groupBy('Release Year').count()

# Sort by Release Year to ensure plot order
ds_sorted = ds.orderBy("Release Year")

# Collect data to Python lists
data = ds_sorted.collect()
years = [row["Release Year"] for row in data]
counts = [row["count"] for row in data]

# Plot
fig, ax = plt.subplots(figsize=(12, 6))  # Specify the figure size
ax.plot(years, counts, marker='o', linestyle='-', color='b')
ax.set_title('Number of Series Released Each Year')
ax.set_xlabel('Year')
ax.set_ylabel('Count')
ax.tick_params(axis='x', rotation=45)  # Rotate x labels for better readability
ax.grid(True)
plt.tight_layout()  # Adjust plot to fit labels

# Pass the figure to Streamlit
st.pyplot(fig)

# Stop the Spark session
spark.stop()
