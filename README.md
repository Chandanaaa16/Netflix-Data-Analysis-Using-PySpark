# 🍿 Netflix Data Analysis & Recommendation System

An end-to-end data analytics and recommendation system built using PySpark for big data processing and TF-IDF for content-based filtering. The project provides both insights into viewing patterns and intelligent content suggestions, displayed via an interactive Streamlit dashboard.

## 📌 Overview
This project processes Netflix dataset at scale using **Apache Spark**, performs deep analysis of genres, titles, and user preferences, and builds a **content-based recommendation engine** using **TF-IDF** on show descriptions. A **Streamlit dashboard** allows users to explore data and get personalized show suggestions.

## 🎯 Features
- Big data processing with **PySpark**
- Data cleaning, preprocessing, and EDA
- TF-IDF based content recommendation
- Interactive visualizations with Streamlit
- Insights into popular genres, directors, release trends

## 🛠️ Tech Stack
- **Languages:** Python
- **Frameworks/Tools:** PySpark, Pandas, Streamlit, NumPy
- **Visualization:** Matplotlib, Seaborn, Streamlit Charts

## 🎥 Demo Preview

![Netflix Vibe](https://i.pinimg.com/originals/bb/74/04/bb74046420c4c992b8cabc6e667abe40.gif)

> *This project captures the vibe of intelligent content discovery on Netflix, enhanced by big data technologies and user-friendly visuals.*

## 📁 Installation

### ⚙️ Prerequisites
- Python 3.x
- Apache Spark setup locally or on cloud (e.g., Databricks, Colab via findspark)

### 📦 Setup
```bash
git clone https://github.com/Chandanaaa16/netflix-recommender
cd netflix-recommender
pip install -r requirements.txt
streamlit run app.py
