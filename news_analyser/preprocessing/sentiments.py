#!/usr/bin/env python
# coding: utf-8

from textblob import TextBlob
from multiprocessing import Pool
from csv import QUOTE_NONNUMERIC
import pandas as pd

from ..helper import chunk_read_csv


def sentiment_analysis(df: pd.DataFrame) -> pd.DataFrame:
    def getSubjectivity(text):
        return TextBlob(text).sentiment.subjectivity

    def getPolarity(text):
        return TextBlob(text).sentiment.polarity

    df["headline_subjectivity"] = df["headline_text"].apply(getSubjectivity)
    df["headline_polarity"] = df["headline_text"].apply(getPolarity)

    def getAnalysis(score):
        if score < 0:
            return -1
        elif score == 0:
            return 0
        else:
            return 1

    df["headline_sentiment"] = df["headline_polarity"].apply(getAnalysis)

    return df


def main():
    news = chunk_read_csv("./data/processed_news.csv")

    with Pool(8) as p:
        processed_chunks = p.map(sentiment_analysis, news)

    processed_news = pd.concat(processed_chunks, axis=0)
    processed_news.to_csv(
        "./data/news_sentiments.csv", quoting=QUOTE_NONNUMERIC, index=False, encoding="utf-8"
    )


if __name__ == "__main__":
    main()

