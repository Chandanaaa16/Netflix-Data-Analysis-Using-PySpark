import streamlit as st
import os

def welcome_page():
    # Set full black background and modern font
    st.markdown("""
        <style>
        .stApp {
            background-color: black;
            color: white;
            font-family: 'Helvetica Neue', sans-serif;
        }
        .title {
            font-size: 48px;
            font-weight: bold;
            text-align: center;
            color: white;
            margin-top: 20px;
        }
        .video-container {
            display: flex;
            justify-content: center;
            margin-top: 40px;
        }
        </style>
    """, unsafe_allow_html=True)

    # Title
    st.markdown('<div class="title">Netflix Data Analysis Using PySpark</div>', unsafe_allow_html=True)

    # Load and display video (clean and centered)
    video_path = "assets/netflix_intro.mp4"
    if os.path.exists(video_path):
        with open(video_path, "rb") as video_file:
            st.video(video_file.read())
    else:
        st.error("Netflix intro video not found.")

    # Optional: subtle prompt or call to action
    st.markdown(
        "<p style='text-align: center; margin-top: 20px;'>Use the sidebar to explore the dashboard and features.</p>",
        unsafe_allow_html=True
    )
    st.markdown("<br><hr><br>", unsafe_allow_html=True)
    st.write("**Credits**: Chandana P, Akshay Raghavendra")