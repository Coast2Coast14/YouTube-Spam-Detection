# Things to add on eventually: airflow for data orchestration, add function to return quota limits,
# function to get all comments from a playlist

from dotenv import load_dotenv
import googleapiclient.discovery
import googleapiclient.errors
from google.oauth2 import service_account
import os
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from Spam_Detection_Algorithm import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import streamlit as st

st.title("YouTube Comment Spam Detector")
url = st.text_input("Enter YouTube Video URL")

# Load environment variables from .env file
load_dotenv()

# youtube credentials
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")
SERVICE_ACCOUNT_INFO = st.secrets["google_service_account"]
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
credentials = service_account.Credentials.from_service_account_info(
    SERVICE_ACCOUNT_INFO, scopes=SCOPES
)


# inputting youtube credentials
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY, credentials=credentials
)


# Function to get replies for a specific comment
def get_replies(youtube, parent_id, video_id):  # Added video_id as an argument
    replies = []
    next_page_token = None

    while True:
        reply_request = youtube.comments().list(
            part="snippet",
            parentId=parent_id,
            textFormat="plainText",
            maxResults=100,
            pageToken=next_page_token,
        )
        reply_response = reply_request.execute()

        for item in reply_response["items"]:
            comment = item["snippet"]
            replies.append(
                [
                    comment["channelId"],
                    comment["textDisplay"],
                    comment["textOriginal"],
                    comment["viewerRating"],
                    comment["likeCount"],
                    comment["publishedAt"],
                    comment["updatedAt"],
                ]
            )

        next_page_token = reply_response.get("nextPageToken")
        if not next_page_token:
            break

    return replies


# Function to extract the video id from a URL
def extract_video_id(url):
    """Extracts the video ID from a YouTube URL"""
    pattern = r"(?:v=|\/)([0-9A-Za-z_-]{11}).*"
    match = re.search(pattern, url)
    video_id = match.group(1) if match else None
    return video_id


# Function to get all comments (including replies) for a single video
def get_comments_for_video(youtube, video_id):
    comments = []
    next_page_token = None

    while True:
        comment_request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            pageToken=next_page_token,
            textFormat="plainText",
            maxResults=100,
        )
        comment_response = comment_request.execute()

        for item in comment_response["items"]:
            comment = item["snippet"]["topLevelComment"]["snippet"]
            comments.append(
                [
                    item["id"],
                    comment["channelId"],
                    comment["textDisplay"],
                    comment["textOriginal"],
                    comment["viewerRating"],
                    comment["likeCount"],
                    comment["publishedAt"],
                    comment["updatedAt"],
                ]
            )

            # Fetch replies if there are any
            if item["snippet"]["totalReplyCount"] > 0:
                comments.extend(
                    get_replies(
                        youtube, item["snippet"]["topLevelComment"]["id"], video_id
                    )
                )

        next_page_token = comment_response.get("nextPageToken")
        if not next_page_token:
            break

    return comments


# creates a datafame out of the columns
def create_dataframe(comments):

    df = pd.DataFrame(
        comments,
        columns=[
            "id",
            "channelId",
            "textDisplay",
            "textOriginal",
            "viewerRating",
            "likeCount",
            "publishedAt",
            "updatedAt",
        ],
    )
    return df


DATABASE_URL = "sqlite:///youtube_comments.db"
db_connection = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=db_connection)


# Creates the database connection for the data to be inserted into
# def create_db_connection():

#    try:
#        DATABASE_URL = "sqlite:///youtube_comments.db"
#        db_connection = create_engine(DATABASE_URL)
#        SessionLocal = sessionmaker(bind=db_connection)

#        print("Database connection successful ✅")

#   except:
#      print("Error creating the database ❌")

# return db_connection


# Inserts raw data into the database
def insert_table(db_connection, df):

    try:
        df.to_sql("YouTube_Comments", db_connection, if_exists="replace")
        print("Data inserted successfully into database ✅")

    except:
        print("Error loading data into database ❌")


def fetch_new_comments():
    """Fetch all YouTube comments from the database."""
    session = SessionLocal()
    df_new_comments = pd.read_sql(
        "SELECT textDisplay FROM youtube_comments", session.bind
    )
    session.close()
    return df_new_comments


def clean_new_comments():
    df_new_comments = fetch_new_comments()

    # Clean the new comments
    df_new_comments["textDisplay"] = df_new_comments["textDisplay"].apply(clean_text)

    return df_new_comments


def classify_comments(model, vectorizer):
    """Apply the trained spam detection model to classify comments."""
    df_new_comments = clean_new_comments()

    # Convert new comments into TF-IDF using the same vectorizer
    X_test_tfidf = vectorizer.transform(df_new_comments["textDisplay"])

    if df_new_comments.empty:
        return df_new_comments  # Return empty if no comments exist

    df_new_comments["is_spam"] = model.predict(
        X_test_tfidf
    )  # Predict spam (1) or not spam (0)

    return df_new_comments["textDisplay"][df_new_comments["is_spam"] == 1]


def update_database_with_spam_status(df_new_comments):
    """Update the SQLAlchemy database with spam classification results."""
    session = SessionLocal()
    for _, row in df_new_comments.iterrows():
        session.execute(
            f"UPDATE youtube_comments SET is_spam = {int(row['is_spam'])} WHERE index = {row['index']}"
        )
    session.commit()
    session.close()


def fetch_spam_comments():
    """Retrieve only spam messages from the database."""
    session = SessionLocal()
    df_spam = pd.read_sql(
        "SELECT * FROM YouTube_Comments WHERE is_spam = 1", session.bind
    )
    session.close()
    return df_spam


# Runs the data pipeline
def run_data_pipeline():

    video_id = extract_video_id(url=url)

    comments = get_comments_for_video(youtube=youtube, video_id=video_id)

    df = create_dataframe(comments=comments)

    insert_table(db_connection=db_connection, df=df)

    try:
        model, vectorizer = train_random_forest_model()
        print("Model Trained")

    except:
        print("Model Not Trained")

    # df_new_comments = classify_comments(model=model, vectorizer=vectorizer)

    # update_database_with_spam_status(df_new_comments=df_new_comments)

    # df_spam = fetch_spam_comments()

    return st.table(classify_comments(model=model, vectorizer=vectorizer))


# if __name__ == "__main__":
if st.button("Check for Spam"):
    if url:
        run_data_pipeline()
