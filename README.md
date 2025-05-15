# Netflix Data Analysis with PySpark

## Overview

This project leverages PySpark to analyze a dataset of Netflix series. The analysis includes exploring trends in series production, popular actors across genres, and other insights into Netflix's content offerings.

## Project Structure

The project directory structure is organized as follows:
````bash
Netflix-Data-Analysis-PySpark/
├── data/
│   └── netflix_series_10_columns_data.csv
├── scripts/
│   ├── correlation_analysis.py
│   ├── Impact_lang_watchT.py
│   ├── Longest_series_by_language.py
│   ├── most_watched_content.py
│   ├── most_watched_country.py
│   ├── popular_actor.py
│   ├── recommendation_system.py
│   ├── Series_Duration.py
│   ├── Series_prod_in_years.py
│   ├── Top_5_Most_Watched.py
│   └── top_rated_series.py
└── README.md
````


- **data/**: Contains the dataset used for analysis.
- **scripts/**: Includes Python scripts for various analyses.
- **README.md**: This file, providing an overview and instructions for the project.

## Technology Used
- PySpark for data processing and analysis
- Pandas for data manipulation
- Matplotlib for visualizations



## Features

- **Trend Analysis**: Examine trends in the number of series released each year.
- **Actor Popularity**: Identify the most popular actors across different genres.
- **Data Visualization**: Generate clear and insightful visualizations to understand trends and patterns.
- **Correlation Analysis**: Explore correlations between various features in the dataset.
- **Language Impact**: Analyze the impact of language on average watch time.
- **Series Duration**: Evaluate the duration of series by language.
- **Top Watched Content**: Identify the most-watched series and content.
- **Series Production by Country**: Analyze series production trends by country.
- **Recommendation System**: Develop a recommendation system based on series data.
- **Top Rated Series**: Find and visualize the top-rated series.

## Dataset

The dataset used in this project contains information about Netflix series with the following columns:
- `Series Name`
- `Rating`
- `Total Watches`
- `Genre`
- `Release Year`
- `Average Watch Time (minutes)`
- `Total Seasons`
- `Country of Origin`
- `Language`
- `Lead Actor`

The dataset is located at `data/netflix_series_10_columns_data.csv`.

## Setup

1. **Install Dependencies**

   Ensure you have Python and PySpark installed. You can install PySpark via pip:

   ```bash
   pip install pyspark
   pip install pandas matplotlib
   ```
   
2. **Set Up Hadoop**

   If running on Windows, ensure Hadoop is properly configured with the winutils.exe file. Set up the HADOOP_HOME environment variable and add it to the PATH:
   ```bash
    set HADOOP_HOME=C:\hadoop
    set PATH=%PATH%;%HADOOP_HOME%\bin
   ```
3. **Verify the Setup**

   Ensure winutils.exe is properly installed by running the following command in the command prompt:
   ```bash
   C:\hadoop\bin\winutils.exe
   ```
4. **Run the Analysis**

   You can run any of the analysis scripts directly if you have a proper PySpark environment. Replace <script_name> with the name of the script you want to run:
   ```bash
   python <path_to_your_script>/<script_name>.py
   ```

## Scripts
- `correlation_analysis.py`: Analyzes correlations between various features in the dataset.
- `Impact_lang_watchT.py`: Analyzes the impact of language on average watch time.
- `Longest_series_by_language.py`: Evaluates the duration of series by language.
- `most_watched_content.py`: Identifies the most-watched series and content.
- `most_watched_country.py`: Analyzes series production trends by country.
- `popular_actor.py`: Identifies popular actors across different genres.
- `recommendation_system.py`: Develops a recommendation system based on series data.
- `Series Duration.py`: Analyzes the duration of series in the dataset.
- `Series_prod_in_years.py`: Examines trends in the number of series released each year.
- `Top 5 Most Watched.py`: Finds and visualizes the top 5 most-watched series.
- `top_rated_series.py`: Finds and visualizes the top-rated series.

## Examples
   Below is an example of a script to analyze the number of series released each year:

   ```bash
      from pyspark.sql import SparkSession
      import pandas as pd
      import matplotlib.pyplot as plt
      
      # Initialize Spark session
      spark = SparkSession.builder.appName("Trends in Series Production").getOrCreate()
      
      # Load the dataset
      df = spark.read.csv("data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)
      
      # Group by 'Release Year' and count the number of series released
      ds = df.groupBy('Release Year').count()
      
      # Convert to pandas DataFrame for plotting
      ds_pandas = ds.toPandas()
      
      # Sort by 'Release Year'
      ds_pandas.sort_values('Release Year', inplace=True)
      
      # Plot
      plt.figure(figsize=(12, 6))
      plt.plot(ds_pandas['Release Year'], ds_pandas['count'], marker='o', linestyle='-', color='b')
      plt.title('Number of Series Released Each Year')
      plt.xlabel('Year')
      plt.ylabel('Count')
      plt.xticks(rotation=45)
      plt.grid(True)
      plt.tight_layout()
      plt.show()
```


## Contributing
   Feel free to open issues or submit pull requests if you have suggestions or improvements.

## Contact
   For any questions or feedback, please contact atulrao15@gmail.com.