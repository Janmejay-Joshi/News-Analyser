#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from multiprocessing import Pool
from functools import partial
from sys import argv
from ..helper import chunk_read_csv, get_news_from_tokens, yearly_split


def getCloseRefsFromNews(tokenNews: pd.DataFrame, token: str) -> dict:
    closeRefs = {}

    def countRefs(token: str, ref: str) -> None:
        refs = ref.split(" ")
        tokens = token.split()

        for key in tokens:
            if key not in refs:
                closeRefs[key] = closeRefs.get(key, 0) + 1

    tokenNews["headline_tokens"].apply(countRefs, ref=token)

    return closeRefs


def main(token:str="cricket") -> None:
    news = chunk_read_csv(
        "./data/processed_news.csv", usecols=["publish_date", "headline_tokens"]
    )

    with Pool(8) as p:
        processed_chunks = p.map(partial(get_news_from_tokens, token=token), news)
        processed_df = pd.concat(processed_chunks, axis=0)

        yearly = yearly_split(processed_df)

        del processed_chunks, processed_df

        token_news = p.map(
            partial(getCloseRefsFromNews, token=token), yearly.values()
        )

    for i, x in enumerate(token_news):
        sort_by_value = dict(
            sorted(x.items(), key=lambda item: item[1], reverse=True)
        )
        print(list(yearly.keys())[i], list(sort_by_value.items())[:7])


if __name__ == "__main__":
    if len(argv) > 1:
        main(argv[1].lower())
    
    else:
        print("Missing Arguments")
