import os
import duckdb
import pandas as pd


FILE = "../../data/raw/coffee_exports.csv"
TABLE_NAME = "coffee_exports"
STORE_PATH = "../../data/db/duck.db"
# consider an architecture that expects data to have subfolders where each subfolder is a table and the the subfolders contain as many files as possible


def init_conn(store_name:str=STORE_PATH):

    conn = duckdb.connect(store_name)
    return conn

def dump_data():

    conn = init_conn()
    conn.register(TABLE_NAME, pd.read_csv(FILE))
    conn.close()

if __name__ == "__main__":

    dump_data()