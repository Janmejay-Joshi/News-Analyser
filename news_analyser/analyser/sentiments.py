#!/usr/bin/env python
# coding: utf-8

from functools import partial
from multiprocessing import Pool
from sys import argv

import pandas as pd

from ..helper import chunk_read_csv, get_news_from_tokens, yearly_split


def get_yearly_sentiments(
    yearlyNews: dict, types: list = ["neutral", "positive", "negative"]
) -> None:
    if yearlyNews == {}:
        print("Empty Sentiments List")
        return

    df = pd.DataFrame()

    for year, year_df in yearlyNews.items():
        dic = {"year": year} | dict(year_df["headline_sentiment"].value_counts())
        df = pd.concat([df, pd.DataFrame(dic, index=[0])])

    df.fillna(0, inplace=True)
    df.rename({0: "neutral", 1: "positive", -1: "negative"}, axis=1, inplace=True)

    return df[["year"] + types]


def main(token: str = "cricket") -> None:
    news = chunk_read_csv(
        "./data/news_sentiments.csv",
        usecols=["publish_date", "headline_tokens", "headline_sentiment"],
    )

    with Pool(8) as p:
        processed_chunks = p.map(partial(get_news_from_tokens, token=token), news)

    processed_df = pd.concat(processed_chunks, axis=0)

    yearly = yearly_split(processed_df)
    token_news = get_yearly_sentiments(yearly)

    print(token_news)


if __name__ == "__main__":
    if len(argv) > 1:
        main(argv[1].lower())

    else:
        print("Missing Arguments")
