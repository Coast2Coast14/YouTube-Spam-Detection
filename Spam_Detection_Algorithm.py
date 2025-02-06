import pandas as pd
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy import create_engine

nltk.download("stopwords")


# Text cleaning function
def clean_text(text):
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)  # Remove URLs
    text = re.sub(r"@\w+|#\w+", "", text)  # Remove mentions and hashtags
    text = re.sub(r"[^\w\s]", "", text)  # Remove punctuation
    return text.lower()  # Convert to lowercase


def read_classified_comments():
    # Load manually labeled dataset (500 comments)
    df_train = pd.read_csv(
        "Classified_Comments_For_Training.csv"
    )  # Ensure it has 'comment' & 'label' columns

    df_train["Text"] = df_train["Text"].apply(clean_text)

    return df_train


# Convert text into TF-IDF numerical representation
def vectorize_text():
    df_train = read_classified_comments()

    vectorizer = TfidfVectorizer(stop_words=stopwords.words("english"))
    try:
        X_train_tfidf = vectorizer.fit_transform(
            df_train["Text"]
        )  # Train TF-IDF on training data
        print("X_train worked")
    except:
        print("X_train did not work")
    y_train = df_train["label"]  # Labels (1 = spam, 0 = not spam)

    return X_train_tfidf, y_train, vectorizer


def train_random_forest_model():
    X_train_tfidf, y_train, vectorizer = vectorize_text()

    # Train a Random Forest model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train_tfidf, y_train)

    return model, vectorizer
