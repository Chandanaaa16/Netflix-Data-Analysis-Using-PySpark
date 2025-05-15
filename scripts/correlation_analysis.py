import streamlit as st
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

def run_dynamic_correlation(df):
    st.subheader("Dynamic Correlation Analyzer")

    # Choose features
    numeric_columns = [col for col, dtype in df.dtypes if dtype in ['int', 'double', 'float', 'bigint'] and col != 'Release Year']
    selected_cols = st.multiselect("Select Columns for Correlation Analysis", numeric_columns)

    if len(selected_cols) < 2:
        st.warning("Please select at least two numerical columns.")
        return

    # Assemble features
    assembler = VectorAssembler(inputCols=selected_cols, outputCol="features")
    vector_df = assembler.transform(df.select(*selected_cols)).select("features")
    correlation_matrix = Correlation.corr(vector_df, "features").head()[0].toArray()

    # Convert the correlation matrix to a pandas DataFrame
    correlation_df = pd.DataFrame(correlation_matrix, columns=selected_cols, index=selected_cols)

    # Show correlation matrix as a dataframe
    st.write("### Correlation Matrix")
    st.dataframe(correlation_df)

    # Plot the heatmap
    st.write("### Heatmap Visualization")
    fig, ax = plt.subplots(figsize=(10, 8))  # Create the figure and axes
    sns.heatmap(correlation_df, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5, ax=ax)
    ax.set_title("Correlation Heatmap")  # Set title for the heatmap
    st.pyplot(fig)  # Pass the figure explicitly to st.pyplot()

    # Explain pairwise correlations
    st.write("### Interpretation")
    for i in range(len(selected_cols)):
        for j in range(i + 1, len(selected_cols)):
            corr_val = correlation_matrix[i][j]
            if corr_val > 0:
                direction = "positive correlation"
            elif corr_val < 0:
                direction = "negative correlation"
            else:
                direction = "no correlation"
            st.write(f"Between **{selected_cols[i]}** and **{selected_cols[j]}**: {direction} ({corr_val:.2f})")
