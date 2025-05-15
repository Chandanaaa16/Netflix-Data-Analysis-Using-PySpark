from pyspark.sql import functions as F
import pandas as pd

def show_most_watched_content(df):
    """Calculate and display the most watched content globally based on total time watched."""
    
    # Calculate total time watched overall for each series
    df = df.withColumn(
        'Total Time Watched Overall',
        F.col('Total Watches') * F.col('Average Watch Time (minutes)')
    )

    # Select relevant columns and order by 'Total Time Watched Overall'
    most_watched = (
        df.select('Series Name', 'Genre', 'Country of Origin', 'Total Time Watched Overall')  # Selecting required columns
        .orderBy(F.col('Total Time Watched Overall').desc())  # Sorting by total time watched in descending order
    )

    # Collect the top 10 most-watched content as a list of Row objects
    most_watched_top_10 = most_watched.limit(10).collect()

    # Convert the result into a Pandas DataFrame manually
    most_watched_pd = pd.DataFrame(
        [(row['Series Name'], row['Genre'], row['Country of Origin'], row['Total Time Watched Overall']) 
         for row in most_watched_top_10],
        columns=['Series Name', 'Genre', 'Country of Origin', 'Total Time Watched Overall']
    )

    return most_watched_pd