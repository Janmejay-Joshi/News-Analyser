#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pandas.io.parsers import TextFileReader


def chunk_read_csv(path: str, chunksize: int = 500000, usecols=None) -> TextFileReader:
    return pd.read_csv(path, chunksize=chunksize, usecols=usecols)


def yearly_split(df: pd.DataFrame) -> dict:
    yearly = {}

    for year in range(2001, 2022):
        temp = df.loc[
            (df["publish_date"] > year * 10000) & (df["publish_date"] < (year + 1) * 10000)
        ]

        if not temp.empty:
            yearly[year] = temp

    return yearly


def get_news_from_tokens(df: pd.DataFrame, token: str) -> pd.DataFrame:
    def refExists(token: str, ref: str) -> bool:
        refs = ref.split(" ")
        tokens = token.split()

        for ref in refs:
            if ref not in tokens:
                return False

        return True

    return df[df["headline_tokens"].apply(refExists, ref=token)]
