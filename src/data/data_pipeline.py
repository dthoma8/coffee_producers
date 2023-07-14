import os
import duckdb
import numpy as np
import pandas as pd
from typing import List
from dagster import asset, Field
from argparse import ArgumentParser


FLAT_FILE = "../../data/coffee_exports.csv"
STORE = "../../data/db/duck.db"
DATA_ENV = "db"
TABLE_NAME_IN = "coffee_exports"

OUTFILE_PATH = "../../data/intermediate/coffee_exports.csv"
TABLE_NAME_OUT = "coffee_exports_intermediate"

@asset
def load_data()->pd.DataFrame:
    
    if DATA_ENV == "local":
        return pd.read_csv(FILE)
    elif DATA_ENV == "db":
        conn = duckdb.connect(database = DB_FILE)
        # TODO: add a test to check for table existence
        res = pd.read_sql_query(f"select * from {TABLE_NAME_IN}", conn)
        print(res.head())
        conn.close()
        return res

@asset
def re4mat(load_data:pd.DataFrame)->pd.DataFrame:
    index:List[str]=["country"]

    assert(isinstance(index, list))
    assert(len(index) > 0)
    assert(all([isinstance(_, str) for _ in index]))
    assert(isinstance(load_data, pd.DataFrame))
    assert(not load_data.empty)

    
    return load_data.set_index(index).stack().reset_index().rename(columns={f"level_{len(index)}":"year", 0:"counts"})

@asset
def filter_out_countries(re4mat:pd.DataFrame)->pd.DataFrame:
    
    return re4mat[~np.in1d(re4mat.country, "Total")]

@asset
def filter_out_years(filter_out_countries:pd.DataFrame)->pd.DataFrame:
    leave_out:List[str]=["all_years"]
    return filter_out_countries[~np.in1d(filter_out_countries.year, "all_years")]
    
@asset
def write_out_flat_df(filter_out_years:pd.DataFrame):

    filter_out_years.to_csv(OUTFILE_PATH)

@asset
def write_out_db_df(filter_out_years:pd.DataFrame):

    conn = duckdb.connect(database = STORE)
    conn.register(TABLE_NAME_OUT, filter_out_years)
    conn.close()

# @pipeline(
#     config = {
#         "data_env":Field(str, description="Specify what data methods to leverage.")
#     }
# )
# def main():

#     data = load_data()
#     data = re4mat(data)
#     data = filter_out_countries(data)
#     data = filter_out_years(data)
#     __filter_list_from_df(data)


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("--data_env", dest="data_env", default="local", type=str)
    args = parser.parse_args()
    print(f"Completed! Here goes a snippet of the data created:\n{data.head()}")