import pandas as pd


def test_dataframe(data:pd.DataFrame)->bool:

    is_dataframe = isinstance(data, pd.DataFrame)
    contains_data = not data.empty

    return (is_dataframe & contains_data)

def test_series(data:pd.Series)->bool:

    return

