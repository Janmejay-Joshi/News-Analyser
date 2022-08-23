#!/usr/bin/env python
# coding: utf-8

from collections import Counter
from functools import partial
from multiprocessing import Pool
from sys import argv

import pandas as pd

from ..helper import chunk_read_csv, get_news_from_tokens, yearly_split


def get_catagories_from_token(tokenNews: pd.DataFrame, token: str) -> dict:
    def token_exists(token: str, ref: str) -> bool:
        refs = set(ref.split(" "))
        tokens = set(token.split())

        return refs <= tokens

    return tokenNews[tokenNews["headline_tokens"].apply(token_exists, ref=token)][
        "headline_category"
    ]


def get_catagory_freq(tokenNews: list, catagory: str) -> dict:
    for i in tokenNews:
        print(Counter(list(i))[catagory])
    return {}


def get_token_catagory_freq(tokenNews: list) -> dict:
    for i in tokenNews:
        print(Counter(list(i)).most_common(10))
    return {}


def main(token: str = "cricket") -> None:
    news = chunk_read_csv(
        "./data/processed_news.csv",
        usecols=["publish_date", "headline_category", "headline_tokens"],
    )

    with Pool(8) as p:
        processed_chunks = p.map(partial(get_news_from_tokens, token=token), news)

    processed_df = pd.concat(processed_chunks, axis=0)

    yearly = yearly_split(processed_df)
    del processed_chunks, processed_df

    with Pool(8) as p:
        token_news = p.map(partial(get_catagories_from_token, token=token), yearly.values())

    print(token_news)
    print(get_token_catagory_freq(token_news))


if __name__ == "__main__":
    if len(argv) > 1:
        main(argv[1].lower())

    else:
        print("Missing Arguments")
