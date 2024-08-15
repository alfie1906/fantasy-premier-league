import ssl

import pandas as pd
import polars as pl

ssl._create_default_https_context = ssl._create_unverified_context

def get_github_data(url: str) -> pl.DataFrame:
    """
    Function for pulling data from a GitHub CSV into a Polars DataFrame.

    Parameters
    ----------
    url : str
        The URL of the CSV file on GitHub

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing the data from the CSV file.
    """
    url += "?raw=true"
    data = pd.read_csv(url, index_col=0)

    return pl.from_pandas(data)