#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import json
from sys import argv


def get_monogram_trend(
    Monogram_yearlyTrend: list, token_arr: list, partial: bool = False
) -> pd.DataFrame:
    def stringRefrences(fdist_mono: dict, reference: str) -> int:
        res = [key for key, _ in fdist_mono.items() if reference in key]

        string_ref = 0
        for match in res:
            string_ref += fdist_mono[match]

        return string_ref

    trend = pd.DataFrame()
    i = 0

    for yearDist in Monogram_yearlyTrend[:-1]:
        temp = {"year": (2001 + i)}

        for token in token_arr:
            try:
                temp[token] = [stringRefrences(yearDist, token) if partial else yearDist[token]]

            except KeyError:
                temp[token] = [0]

            # temp[token] = [ yearDist[token]]
            # temp[token + '_p'] = [stringRefrences(yearDist,token)]

        trend = pd.concat([trend, pd.DataFrame(temp, index=[0])], ignore_index=True, axis=0)
        i += 1

    return trend


def get_bigram_trend(Bigram_yearlyTrend: list, token_arr: list) -> pd.DataFrame:
    trend = pd.DataFrame()
    i = 0

    for yearDist in Bigram_yearlyTrend[:-1]:
        temp = {"year": (2001 + i)}

        for token in token_arr:
            try:
                temp[token] = [yearDist[token]]

            except KeyError:
                temp[token] = [0]

        trend = pd.concat([trend, pd.DataFrame(temp)], ignore_index=True, axis=0)
        i += 1

    return trend


def main(n: int, tokens: list) -> None:

    if n == 1:
        with open("./data/MonoGramTrends.json") as f:
            monogram_yearly_trend = json.load(f)

        result = get_monogram_trend(monogram_yearly_trend, token_arr=tokens, partial=False)
        print(result)

    elif n == 2:
        with open("./data/BiGramTrends.json") as f:
            bigram_yearly_trend = json.load(f)

        result = get_bigram_trend(bigram_yearly_trend, token_arr=tokens)
        print(result)

    else:
        print("Only n = 1 or 2")


if __name__ == "__main__":
    if len(argv) > 2:
        main(int(argv[1]), argv[2:])

    else:
        print("Missing Arguments")
