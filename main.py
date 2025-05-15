import streamlit as st
from app.welcome import welcome_page
from app.dashboard import dashboard_page

from scripts.spark_session import get_spark_session

# Initialize centralized Spark session
spark = get_spark_session()

# Page layout
st.set_page_config(page_title="Netflix Data Analysis with PySpark", layout="wide")

# Sidebar menu
menu = ["Welcome", "Dashboard", "Recommendation"]
choice = st.sidebar.selectbox("Select an option", menu)

if choice == "Welcome":
    welcome_page()
elif choice == "Dashboard":
    dashboard_page(spark)