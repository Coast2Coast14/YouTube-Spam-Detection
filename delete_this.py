import pandas as pd
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy import create_engine
from YouTube_Comments_ETL import *

nltk.download("stopwords")

# Load manually labeled dataset (500 comments)
df_train = pd.read_csv(
    "Classified_Comments_For_Training.csv"
)  # Ensure it has 'comment' & 'label' columns


# Text cleaning function
def clean_text(text):
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)  # Remove URLs
    text = re.sub(r"@\w+|#\w+", "", text)  # Remove mentions and hashtags
    text = re.sub(r"[^\w\s]", "", text)  # Remove punctuation
    return text.lower()  # Convert to lowercase


df_train["Text"] = df_train["Text"].apply(clean_text)

# Convert text into TF-IDF numerical representation
vectorizer = TfidfVectorizer(stop_words=stopwords.words("english"))
X_train_tfidf = vectorizer.fit_transform(
    df_train["Text"]
)  # Train TF-IDF on training data
y_train = df_train["label"]  # Labels (1 = spam, 0 = not spam)

# Train a Random Forest model
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train_tfidf, y_train)

# Connect to the SQLite database
conn = sqlite3.connect("youtube_comments.db")


def get_all_comments():
    # Query all comments from the database
    query = "SELECT textDisplay FROM YouTube_Comments"  # Replace 'comments_table' with your actual table name
    df_test = pd.read_sql_query(query, conn)

    return df_test


# Close the database connection
conn.close()


def clean_comments():
    # Clean the new comments
    df_test["textDisplay"] = df_test["textDisplay"].apply(clean_text)

    # Convert new comments into TF-IDF using the same vectorizer
    X_test_tfidf = vectorizer.transform(df_test["textDisplay"])


get_all_comments()
