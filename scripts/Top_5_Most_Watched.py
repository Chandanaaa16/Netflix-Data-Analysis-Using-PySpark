import matplotlib.pyplot as plt
import pyspark.sql.functions as F

def show_top_5_most_watched_genres(df, st):
    """Function to display top 5 most watched genres globally as a donut chart."""
    
    # Aggregate watch counts by genre
    most_watched = df.groupBy('Genre').sum('Total Watches')
    most_watched_sorted = most_watched.sort(F.col('sum(Total Watches)').desc()).limit(5)

    # Collect results and convert to Pandas DataFrame
    results = most_watched_sorted.collect()
    genres = [row['Genre'] for row in results]
    watches = [row['sum(Total Watches)'] for row in results]

    # Create Donut Chart
    fig, ax = plt.subplots(figsize=(8, 8))
    wedges, texts, autotexts = ax.pie(watches, labels=genres, autopct='%1.1f%%', startangle=140, 
                                      colors=plt.cm.Set3.colors, wedgeprops=dict(width=0.4))

    # Draw circle for donut hole
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig.gca().add_artist(centre_circle)

    # Set title
    ax.set_title("Top 5 Most Watched Genres Globally")

    # Display chart in Streamlit
    st.pyplot(fig)
