#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from multiprocessing import Pool
import itertools
import nltk
import json

from ..helper import chunk_read_csv

def split_news_by_year(df: pd.DataFrame) -> list:
    yearly_tokens = []

    for year in range(2001, 2022):
        temp = df.loc[
            (df["publish_date"] > year * 10000)
            & (df["publish_date"] < (year + 1) * 10000)
        ]

        yearly_tokens.append(temp)

    return yearly_tokens


def dataframe_to_fdist(df: pd.DataFrame) -> dict:
    def tokenize(token_s: str) -> list:
        return token_s.split()

    df["headline_tokens"] = df["headline_tokens"].apply(tokenize)
    combined_tokens = list(itertools.chain.from_iterable(df["headline_tokens"]))

    return dict(nltk.FreqDist(combined_tokens))



def main() -> None:
    news = chunk_read_csv(
        "./datasets/processed_news.csv", usecols=["publish_date", "headline_tokens"]
    )

    combined_df = pd.concat(news, axis=0)
    yearly_news = split_news_by_year(combined_df)
    del combined_df

    with Pool(8) as p:
        mono_trend = p.map(dataframe_to_fdist, yearly_news)

        with open('./datasets/MonoGramTrends.json','w') as f:
            json.dump(mono_trend, f, sort_keys=True, indent=4)
            del mono_trend

        bi_trend = p.map(dataframe_to_fdist, yearly_news)

        with open('./datasets/BiGramTrends.json','w') as f:
            json.dump(bi_trend, f, sort_keys=True, indent=4)
            del bi_trend

        tri_trend = p.map(dataframe_to_fdist, yearly_news)
        
        with open('./datasets/TriGramTrends.json','w') as f:
            json.dump(tri_trend, f, sort_keys=True, indent=4)
            del tri_trend 



if __name__ == "__main__":
    main()
