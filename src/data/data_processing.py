import os
import numpy as np
import pandas as pd
from typing import List


FILE = "../../data/coffee_exports.csv"

def main():

    data = load_data(FILE)
    data = re4mat(data)
    data = filter_out_countries(data)
    data = filter_out_years(data)

    return data

def load_data(filepath:str)->pd.DataFrame:

    return pd.read_csv(filepath)

def re4mat(data:pd.DataFrame, index:List[str]=["country"])->pd.DataFrame:

    assert(isinstance(index, list))
    assert(len(index) > 0)
    assert(all([isinstance(_, str) for _ in index]))
    
    return data.set_index(index).stack().reset_index().rename(columns={f"level_{len(index)}":"year", 0:"counts"})

def filter_out_countries(data:pd.DataFrame, leave_out:List[str]=["Total"])->pd.DataFrame:

    return __filter_list_from_df(data, "country", leave_out)

def filter_out_years(data:pd.DataFrame, leave_out:List[str]=["all_years"])->pd.DataFrame:

    return __filter_list_from_df(data, "year", leave_out)

def __filter_list_from_df(data:pd.DataFrame, col:str="", target_list:List[str]=[])->pd.DataFrame:

    return data[~data[col].isin(target_list)]

if __name__ == "__main__":

    data = main()
    print(f"Completed! Here goes a snippet of the data created:\n{data.head()}")