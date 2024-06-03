# Things to add on eventually: airflow, add to github add function to return quota limits, include replies
# to comments as well in comments returned, function to get all comments from a playlist

import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# youtube credentials
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = os.getenv("DEVELOPER_KEY")


# inputting youtube credentials
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY
)

# function that asks which YouTube video to get comments from
request = youtube.commentThreads().list(
    part="snippet", videoId=input("Enter the video's YouTube ID: "), maxResults=100
)
response = request.execute()


# function that gets the specified columns from the youtube api
def process_youtube_comments(response):

    comments = []
    for item in response["items"]:
        comment = item["snippet"]["topLevelComment"]["snippet"]
        comments.append(
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
    return comments


# creates a datafame out of the columns
def create_dataframe(comments):

    df = pd.DataFrame(
        comments,
        columns=[
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


# Creates the database connection for the data to be inserted into
def create_db_connection():

    try:
        db_connection = create_engine("sqlite:///youtube_comments.db")
        print("Database connection successful ✅")

    except:
        print("Error creating the database ❌")

    return db_connection


# Inserts the data into the database
def insert_table(db_connection, df):

    try:
        df.to_sql("YouTube_Comments", db_connection, if_exists="replace")
        print("Data inserted successfully into database ✅")

    except:
        print("Error loading data into database ❌")


# Runs the data pipeline
def run_data_pipeline():
    comments = process_youtube_comments(response=response)

    df = create_dataframe(comments=comments)

    db_connection = create_db_connection()

    insert_table(db_connection=db_connection, df=df)


if __name__ == "__main__":
    run_data_pipeline()
