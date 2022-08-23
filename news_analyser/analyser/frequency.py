#!/usr/bin/env python
# coding: utf-8

import json
from collections import Counter
from sys import argv

import pandas as pd


def get_freq(yearlyTrend: list, n: int = 10) -> pd.DataFrame:
    trend = pd.DataFrame()

    for yearDist in yearlyTrend[:-1]:
        a = Counter(yearDist).most_common(n)
        print(a)

    return trend


def get_cumm_freq(yearlyTrend: list, n: int = 10) -> pd.DataFrame:
    def merge_dict(a: dict, b: dict) -> dict:
        return {x: a.get(x, 0) + b.get(x, 0) for x in set(a).union(b)}

    trend = pd.DataFrame()
    cumm_yearDist = {}

    for yearDist in yearlyTrend[:-1]:
        cumm_yearDist = merge_dict(cumm_yearDist, yearDist)
        a = Counter(cumm_yearDist)
        print(a.most_common(n))

    return trend


def main(n: int) -> None:

    if n == 1:
        with open("./data/MonoGramTrends.json") as f:
            monogram_yearly_trend = json.load(f)

        result1 = get_freq(monogram_yearly_trend, 10)
        print("_______________________")
        result2 = get_cumm_freq(monogram_yearly_trend, 10)

    elif n == 2:
        with open("./data/BiGramTrends.json") as f:
            bigram_yearly_trend = json.load(f)

        result1 = get_cumm_freq(bigram_yearly_trend)
        print("_______________________")
        result2 = get_cumm_freq(bigram_yearly_trend)

    else:
        print("Only n = 1 or 2")


if __name__ == "__main__":
    if len(argv) > 1:
        main(int(argv[1]))

    else:
        print("Missing Arguments")
