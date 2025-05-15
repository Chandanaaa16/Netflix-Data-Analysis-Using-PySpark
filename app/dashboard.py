import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from scripts.spark_session import get_spark_session
from scripts.correlation_analysis import run_dynamic_correlation
from scripts.Impact_lang_watchT import show_language_watchtime_impact
from scripts.most_watched_content import show_most_watched_content
from scripts.most_watched_country import get_most_watched_country
from scripts.Top_5_Most_Watched import show_top_5_most_watched_genres

# Initialize Spark session
spark = get_spark_session("Netflix Data Analysis")

# Load dataset
data_path = "data/netflix_series_10_columns_data.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

def recommendation_system(df):
    """Recommendation system based on Genre and Language."""
    st.subheader("Recommendation System")
    genres = df.select("Genre").distinct().rdd.flatMap(lambda x: x).collect()
    languages = df.select("Language").distinct().rdd.flatMap(lambda x: x).collect()

    genre = st.selectbox("Select Your Genre", genres)
    language = st.selectbox("Select Your Language", languages)

    filtered_df = df.filter(F.col("Genre") == genre).filter(F.col("Language") == language).orderBy(F.col("Rating").desc())
    count = filtered_df.count()

    if count > 0:
        st.write(f"Found {count} matching records.")
        try:
            results = filtered_df.select("Series Name", "Rating", "Total Watches").limit(10).collect()
            results_df = pd.DataFrame(results, columns=["Series Name", "Rating", "Total Watches"])
            st.dataframe(results_df)
        except Exception as e:
            st.write(f"Error converting data to Pandas DataFrame: {e}")
    else:
        st.write("No matching records found.")

def dashboard_page(spark):
    st.title("Netflix Data Analysis Dashboard")
    st.subheader("Project Objective")
    st.write(""" 
    The aim of this project is to analyze Netflix series data using PySpark to uncover insights into viewing behavior, content trends, and generate personalized recommendations.
    """)

    with st.expander("View Dataset Details"):
        row_count = df.count()
        column_count = len(df.columns)
        st.write(f"Total Rows: {row_count}")
        st.write(f"Total Columns: {column_count}")

    tab = st.sidebar.radio("Select Analysis", [
        "Dynamic Correlation Analysis", "Impact of Language on Watch Time", 
        "Longest Series by Language", "Most Watched Content", "Most Watched Country", 
        "Top 5 Most Watched Genres", "Top Rated Series by Genre", 
        "Trends in Series Production", "Recommendation System"])

    if tab == "Dynamic Correlation Analysis":
        run_dynamic_correlation(df)

    elif tab == "Impact of Language on Watch Time":
        show_language_watchtime_impact(df)
    

    elif tab == "Most Watched Content":
        st.subheader("Most Watched Content Globally")
        most_watched_pd = show_most_watched_content(df)
        st.write(most_watched_pd)

    elif tab == "Most Watched Country":
        st.subheader("Most Active Viewer Countries")
        most_watched_country_pd = get_most_watched_country(df)

        # Scatter plot for Most Watched Countries
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(most_watched_country_pd["Country of Origin"], most_watched_country_pd["Total_Watches"], color='b')
        ax.set_title("Top 5 Countries by Viewership")
        ax.set_xlabel("Country of Origin")
        ax.set_ylabel("Total Watches")
        ax.grid(True)
        plt.xticks(rotation=45, ha='right')
        st.pyplot(fig)

    elif tab == "Top 5 Most Watched Genres":
        st.subheader("Top 5 Most Watched Genres Globally")
        show_top_5_most_watched_genres(df, st)

    elif tab == "Top Rated Series by Genre":
        st.subheader("Top Rated Series by Genre")
        df_selected = df.select("Series Name", "Rating", "Genre")
        window_spec = Window.partitionBy("Genre").orderBy(F.col("Rating").desc())
        df_ranked = df_selected.withColumn("rank", F.row_number().over(window_spec))
        top_rated_series = df_ranked.filter(F.col("rank") <= 3)

        try:
            # Ensure DataFrame is not empty
            top_rated_series_count = top_rated_series.count()
            if top_rated_series_count > 0:
                # Limit data to prevent memory issues
                top_rated_series = top_rated_series.limit(100)

                # Collect the data and convert to Pandas DataFrame
                results = top_rated_series.select("Series Name", "Rating", "Genre", "rank").collect()
                results_df = pd.DataFrame(results, columns=["Series Name", "Rating", "Genre", "Rank"])
                st.dataframe(results_df)
            else:
                st.write("No data available for the top rated series by genre.")
        except Exception as e:
            st.write(f"Error converting data to Pandas DataFrame: {e}")

    elif tab == "Longest Series by Language":
        st.subheader("Longest Series by Language")
        Long_Series = df.select(['Series Name', 'Total Seasons', 'Language'])
        Window4 = Window.partitionBy("Language").orderBy(F.col('Total Seasons').desc())
        Long_Series = Long_Series.withColumn("Rank", F.row_number().over(Window4))
        long_running = Long_Series.filter(F.col("Rank") == 1)

        try:
            # Limit data to prevent memory issues
            long_running = long_running.limit(100)

            # Collect the data and convert to Pandas DataFrame
            results = long_running.collect()
            results_df = pd.DataFrame(results, columns=["Series Name", "Total Seasons", "Language", "Rank"])
            st.dataframe(results_df)
        except Exception as e:
            st.write(f"Error converting data to Pandas DataFrame: {e}")

    elif tab == "Trends in Series Production":
        st.subheader("Trends in Series Production (by Release Year)")
        ds = df.groupBy('Release Year').count().orderBy("Release Year")
        data = ds.limit(20).collect()
        years = [row["Release Year"] for row in data]
        counts = [row["count"] for row in data]

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(years, counts, marker='o', linestyle='-', color='b')
        ax.set_title('Number of Series Released Each Year')
        ax.set_xlabel('Year')
        ax.set_ylabel('Count')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True)
        fig.tight_layout()
        st.pyplot(fig)

    elif tab == "Recommendation System":
        recommendation_system(df)

if __name__ == "__main__":
    dashboard_page(spark)
