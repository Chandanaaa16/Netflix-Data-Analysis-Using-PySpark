import pandas as pd
from pyspark.sql import functions as F

def get_most_watched_country(df):
    # Calculate the total watches per country
    most_watched_country_df = df.groupBy("Country of Origin").agg(F.sum("Total Watches").alias("Total_Watches")).orderBy(F.col("Total_Watches").desc())

    # Limit to top 5 countries
    results = most_watched_country_df.limit(5).collect()
    most_watched_country_pd = pd.DataFrame(results, columns=["Country of Origin", "Total_Watches"])

    return most_watched_country_pd
