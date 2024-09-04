import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from sqlalchemy import create_engine

# youtube credentials
api_service_name = "youtube"
api_version = "v3"
developerkey = "AIzaSyBDf02q8D5Cpxf2GJSRN41dB6lIP0PbRA0"

# inputting youtube credentials
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=developerkey
)


def get_replies(youtube, parent_id, video_id):
    replies = []
    next_page_token = None

    while True:
        reply_request = youtube.comments().list(
            part="snippet",
            parentId=parent_id,
            pageToken=next_page_token,
            textFormat="plainText",
            maxResults=100,
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


# Function to get all comments and replies from a video
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

            # fetch replies if there are any
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


# create a dataframe out of the columns
def createDataFrame(comments):

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


# Creates the database that we insert our data into
def create_db_connection():

    try:
        db_connection = create_engine("sqlite:///youtube_comments.db")
        print("Database connection successful")

    except:
        print("Database connection unsuccessful")

    return db_connection


# Inserts the data into the database
def insert_table(db_connection, df):

    try:
        df.to_sql("YouTube_Comments", db_connection, if_exists="replace")
        print("Data inserted succesfully into SQL")

    except:
        print("Data was not successfully inserted")


# Runs the data pipeline
def run_data_pipeline():
    comments = get_comments_for_video(
        youtube=youtube, video_id=input("Enter the Youtube video's ID: ")
    )

    df = createDataFrame(comments=comments)

    db_connection = create_db_connection()

    insert_table(db_connection=db_connection, df=df)


run_data_pipeline()
