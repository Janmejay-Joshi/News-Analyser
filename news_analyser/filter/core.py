#!/usr/bin/env python
# coding: utf-8

from multiprocessing import Pool
import pandas as pd
import re

from ..helper import chunk_read_csv


def filterNews(df: pd.DataFrame, filter:dict) -> pd.DataFrame:
    def from_headline(headline: str, ref:str):
        return headline.find(ref)

    def from_tokens(tokens:str, ref:list):
        return

    def from_years(datestring:str, years:list):
        pass

    def from_catagories(catagories:str, ref:list):
        pass

    def from_regex():
        pass

    def from_datestring():
        pass

    return filtered_df



def main():
    news = chunk_read_csv("./data/processed_news.csv")

    with Pool(8) as p:
        processed_chunks = p.map(filterNews, news)

    processed_df = pd.concat(processed_chunks, axis=0)


if __name__ == "__main__":
    main()
