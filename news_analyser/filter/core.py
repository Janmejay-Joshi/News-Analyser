#!/usr/bin/env python
# coding: utf-8

import re
from functools import partial
from multiprocessing import Pool

import pandas as pd

from ..helper import chunk_read_csv


def filter_news(df: pd.DataFrame, filter: dict) -> pd.DataFrame:
    def from_headline(headline: str, ref: str) -> bool:
        return headline.find(ref) != -1

    def from_tokens(tokens: str, ref: list) -> bool:
        return set(tokens.split(" ")) >= set(ref)

    def from_years(datestring: int, years: list):
        return (datestring // 10000) in years

    def from_catagories(catagories: str, ref: list) -> bool:
        return bool(set(catagories.split(".")) & set(ref))

    # def from_regex():
    #     pass

    # def from_datestring():
    #     pass

    try:
        if "years" in filter:
            df = df[df["publish_date"].apply(from_years, years=filter["years"])]

        if "catagories" in filter:
            df = df[df["headline_category"].apply(from_catagories, ref=filter["catagories"])]

        if "headline" in filter:
            df = df[df["headline_text"].apply(from_headline, ref=filter["headline"])]

        if "tokens" in filter:
            df = df[df["headline_tokens"].apply(from_tokens, ref=filter["tokens"])]

    except Exception as err:
        print(err)

    return df


def main():
    news = chunk_read_csv("./data/processed_news.csv")

    test_filter = {"catagories": ["city", "sports"], "years": [2002, 2011], "tokens": ["cup"]}

    with Pool(8) as p:
        processed_chunks = p.map(partial(filter_news, filter=test_filter), news)

    processed_df = pd.concat(processed_chunks, axis=0)
    print(processed_df)


if __name__ == "__main__":
    main()
