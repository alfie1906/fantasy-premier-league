import ssl

import pandas as pd

ssl._create_default_https_context = ssl._create_unverified_context

def get_github_data(url: str) -> pd.DataFrame:
    """
    Function for pulling data from a GitHub CSV into a Polars DataFrame.

    Parameters
    ----------
    url : str
        The URL of the CSV file on GitHub

    Returns
    -------
    pd.DataFrame
        A Polars DataFrame containing the data from the CSV file.
    """
    url += "?raw=true"
    data = pd.read_csv(url)

    return data