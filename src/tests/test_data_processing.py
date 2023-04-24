import os
from typing import List
import src.data.data_processing as dp


FILEPATH = ""

def test__load_data():
    
    assert isinstance(FILEPATH, str)
    assert os.path.exists(FILEPATH)
    assert isinstance(dp.load_data(FILEPATH), pd.DataFrame)
    assert not dp.load_data(FILEPATH).empty

def test__filter_out_countries(col:str="country", target_list:List[str]=["Total"]):

    data = dp.load_data(FILEPATH)

    assert col in {data.columns.tolist()}
    assert isinstance(col, str)
    assert isinstance(target_list, list)
    assert len(target_list)>0
    assert isinstance(target_list[0], str)

def test__filter_out_years(col:str="year", target_list:List[str]=["all_years"]):

    data = dp.load_data(FILEPATH)

    assert col in {data.columns.tolist()}
    assert isinstance(col, str)
    assert isinstance(target_list, list)
    assert len(target_list)>0
    assert isinstance(target_list[0], str)

def test_re4mat():

    data = dp.load_data(FILEPATH)
    index = ["country"]

    res = dp.re4mat(data, index=index)

    assert not set(index).difference(data.columns.tolist())
    assert len(res.columns) == (len(index) + 2)
    assert "year" in res.columns.tolist()
    assert "counts" in res.columns.tolist()