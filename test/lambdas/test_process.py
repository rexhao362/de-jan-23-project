from src.lambdas.process import (load_file_from_local, process, print_csv)
from pandas import DataFrame
import pytest
import numpy as np
full_json = 'test/json_files/process_test_1.json'
empty_json = 'test/json_files/process_test_2.json'
no_data_json = 'test/json_files/process_test_3.json'


def test_load_file_from_local_returns_dict():
    result = load_file_from_local(empty_json)
    assert type(result) == dict

def test_load_file_from_local_dict_contains_data():
    result = load_file_from_local(no_data_json)
    assert result['Headers'] == []
    assert result['Data'] == []
    result = load_file_from_local(full_json)
    assert result['Headers'] == ["Name", "Type", "Description"]
    assert result['Data'] == \
    [   
    ["foo1", "bar1","foo bar1"],
    ["foo2", "bar2", "foobar2"],
    ["foo3", "bar3", "foobar3"]
    ]
    
def test_process_with_valid_empty_data_from_local_returns_pd_dataframe():
    raw_data = load_file_from_local(no_data_json)
    dataframe = process(raw_data)
    assert dataframe.empty

def test_process_with_valid_data_from_local_returns_pd_dataframe():
    raw_data = load_file_from_local(full_json)
    dataframe = process(raw_data)
    assert isinstance(dataframe, DataFrame)
    
def test_process_with_empty_json_throws_error():
    raw_data = load_file_from_local(empty_json)
    with pytest.raises(KeyError):
        dataframe = process(raw_data)
        assert True
        
def test_process_returns_dataframe_with_correct_columns():
    raw_data = load_file_from_local(full_json)
    dataframe = process(raw_data)
    dataframe_cols = dataframe.columns.values
    assert np.equal(dataframe_cols, ['Name', 'Type', 'Description']).all()

def test_process_returns_dataframe_containing_input_data():
    raw_data = load_file_from_local(full_json)
    dataframe = process(raw_data)
    headers = ['Name', 'Type', 'Description']
    j = 0
    #could refactor this to use enumerate
    for header in headers:
        for i in range(len(raw_data['Data'])):
            assert dataframe[header][i] == raw_data['Data'][i][j]
        j += 1
    
def test_print_pd_dataframe_to_csv():
    raw_data = load_file_from_local(full_json)
    dataframe = process(raw_data)
    print_csv(dataframe, 'test/csv_files/temp_test_1.csv')
    # print(dataframe.head())
    assert True