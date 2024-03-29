#!/usr/bin/env python
# coding: utf-8

import re
from csv import QUOTE_NONNUMERIC
from multiprocessing import Pool

import pandas as pd
import swachhdata.text as sdt

from ..helper import chunk_read_csv


def cleanNewsText(df: pd.DataFrame) -> pd.DataFrame:
    def sdtPipeline(df: pd.DataFrame) -> pd.DataFrame:
        pipeline = [
            sdt.CaseRecast(process="lower"),
            sdt.PunctuationRecast(),
            sdt.StopWordsRecast(),
            sdt.NumberRecast(process="remove"),
        ]

        df["headline_tokens"] = sdt.RecastPipeline(df["headline_text"], pipeline)
        df.dropna(inplace=True)

        return df

    def remove_extra_spaces(text: str) -> str:
        return re.sub(" +", " ", text)

    def is_empty(text: str) -> bool:
        return bool(text)

    processed_df = sdtPipeline(df)
    processed_df["headline_tokens"] = processed_df["headline_tokens"].apply(remove_extra_spaces)
    processed_df = processed_df[processed_df["headline_tokens"].apply(is_empty)]

    return processed_df


def main():
    news = chunk_read_csv("./data/india-news-headlines.csv")

    with Pool(8) as p:
        processed_chunks = p.map(cleanNewsText, news)

    processed_df = pd.concat(processed_chunks, axis=0)
    processed_df.to_csv(
        "./data/processed_news.csv",
        quoting=QUOTE_NONNUMERIC,
        index=False,
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
