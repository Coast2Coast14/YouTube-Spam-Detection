# Things to add on eventually: airflow for data orchestration, add function to return quota limits,
# function to get all comments from a playlist

import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
import boto3
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


# Function to get the channel ID from a channel name
def get_channel_id(channel_name):
    request = youtube.search().list(
        part="snippet", q=channel_name, type="channel", maxResults=1
    )
    response = request.execute()
    channel_id = response["items"][0]["snippet"]["channelId"]
    return channel_id


# get the most recent video from a youtube channel
def get_most_recent_video(channel_id):
    request = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        order="date",  # Orders the results by date
        maxResults=1,
    )
    response = request.execute()
    video_id = response["items"][0]["id"]["videoId"]
    return video_id


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


# Uploads the data to S3
def upload_file_to_s3(df_csv, bucket_name, object_name=None):
    s3_client = boto3.client("s3")

    if object_name is None:
        object_name = df_csv

    key = "s3://youtubedatademo/youtubedata/Test_YouTube_Comments.csv"
    s3_client.upload_file("Test_YouTube_Comments.csv", bucket_name, object_name)


# Runs the data pipeline
def run_data_pipeline():

    channel_name = input("What YouTube channel do you want to use? ")
    channel_id = get_channel_id(channel_name)
    most_recent_video_id = get_most_recent_video(channel_id=channel_id)

    comments = get_comments_for_video(youtube=youtube, video_id=most_recent_video_id)

    df = create_dataframe(comments=comments)

    db_connection = create_db_connection()

    insert_table(db_connection=db_connection, df=df)

    df_csv = df.to_csv("Test_YouTube_Comments.csv")
    test_path = "/Users/elijahwooten/Desktop/Side_Projects/YouTube Videos/Youtube_Spam_Detection/Test_YouTube_Comments.csv"
    # bucket_name = "your-s3-bucket-name"
    bucket_name = "youtubedatademo"
    upload_file_to_s3(df_csv=test_path, bucket_name=bucket_name)


if __name__ == "__main__":
    run_data_pipeline()
