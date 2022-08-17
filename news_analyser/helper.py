#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pandas.io.parsers import TextFileReader


def chunk_read_csv(path: str, chunksize: int = 500000, usecols=None) -> TextFileReader:
    return pd.read_csv(path, delimiter="~", chunksize=chunksize, usecols=usecols)

