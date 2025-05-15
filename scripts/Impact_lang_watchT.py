# scripts/impact_lang_watchT.py
import streamlit as st
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import pandas as pd

def show_language_watchtime_impact(df):
    st.subheader("Impact of Language on Average Watch Time")

    # Group by language and get average watch time
    ds = df.groupBy("Language").avg('Average Watch Time (minutes)')
    ds_sorted = ds.sort(F.col('avg(Average Watch Time (minutes))').desc())

    # Collect all sorted rows (not just top 10)
    all_rows = ds_sorted.collect()

    # Extract language and watch time values
    languages = [row['Language'] for row in all_rows]
    watch_times = [row['avg(Average Watch Time (minutes))'] for row in all_rows]

    # Show table view
    st.write("### Languages by Average Watch Time (Descending Order)")
    table_data = pd.DataFrame({
        "Language": languages,
        "Average Watch Time (minutes)": [round(t, 2) for t in watch_times]
    })
    st.dataframe(table_data)

    # Color palette
    colors = plt.cm.get_cmap('tab20', len(languages))
    bar_colors = [colors(i) for i in range(len(languages))]

    # Create a bar chart using Matplotlib
    fig, ax = plt.subplots(figsize=(max(12, len(languages) // 2), 6))
    bars = ax.bar(languages, watch_times, color=bar_colors)
    ax.set_title("Average Watch Time by Language")
    ax.set_xlabel("Language")
    ax.set_ylabel("Average Watch Time (minutes)")
    ax.set_xticks(range(len(languages)))
    ax.set_xticklabels(languages, rotation=45, ha='right', fontsize=8)

    # Annotate each bar with value
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval, f'{yval:.1f}', va='bottom', ha='center', fontsize=6)

    st.pyplot(fig)
