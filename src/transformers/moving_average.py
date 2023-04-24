import numpy as np
import pandas as pd
from typing import List


def calc(df:pd.DataFrame, index_cols:List[str], n:int=5):
    # Group by the specified index columns
    groups = df.groupby(index_cols)
    
    # Calculate the rolling average of the last n data points for each group
    ma = groups.apply(lambda x: x.rolling(window=n).mean().shift(1))
    
    # Remove the NaN values resulting from the shift
    ma = ma.reset_index().dropna().set_index(index_cols)
    
    # Join the moving average calculation back into the original data
    ma.columns = [f'ma_{n}']
    df = df.join(ma, on=index_cols)
    
    return df




