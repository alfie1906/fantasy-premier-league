import ssl

import pandas as pd
import polars as pl

ssl._create_default_https_context = ssl._create_unverified_context

def get_github_data(url: str) -> pl.DataFrame:
    data = pd.read_csv(url, index_col=0)

    return pl.from_pandas(data)