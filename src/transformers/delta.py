import pandas as pd

def get_diff_from_previous(df, index_cols, column):
    # Sort the DataFrame by the index columns
    df = df.sort_values(by=index_cols)
    
    # Create a new DataFrame with the difference from the previous value for each index group
    prev = df.groupby(index_cols)[column].shift(1)
    diff = df[column] - prev
    
    # Join the difference from the previous value DataFrame back into the original data
    df = df.join(diff.rename(f'diff_from_prev_{column}'), on=index_cols)
    
    return df
