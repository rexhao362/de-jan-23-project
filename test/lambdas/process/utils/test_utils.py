import sys
sys.path.append('./src/')
from process.putils import ( load_file_from_s3, load_file_from_local, process, print_csv)
from pandas import DataFrame
import pytest
import numpy as np
from moto import mock_s3
import boto3
import os
from pandas import read_parquet
full_json = 'test/lambdas/process/json_files/currency.json'
empty_json = 'test/lambdas/process/json_files/empty.json'
no_data_json = 'test/lambdas/process/json_files/no_data.json'
parquet = 'test/parquets/dim_currency_formatted.parquet'


def test_load_file_from_local_returns_dict():
    result = load_file_from_local(full_json)
    assert type(result) == dict
    
def test_load_file_from_local_dict_contains_data():
    result = load_file_from_local(no_data_json)["table"]
    assert result['headers'] == []
    assert result['data'] == []
    result = load_file_from_local(full_json)["table"]
    assert result['headers'] == ["currency_id", "currency_code", "created_at", "last_updated"]
    assert result['data'] == [
    [1, "GBP", "2022-11-03T14:20:49.962000", "2022-11-03T14:20:49.962000"],
    [2, "USD", "2022-11-03T14:20:49.962000", "2022-11-03T14:20:49.962000"],
    [3, "EUR", "2022-11-03T14:20:49.962000", "2022-11-03T14:20:49.962000"]
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
    with pytest.raises(Exception):
        dataframe = process(raw_data)
        assert True

def test_process_returns_dataframe_with_correct_columns():
    raw_data = load_file_from_local(full_json)
    dataframe = process(raw_data)
    dataframe_cols = dataframe.columns.values
    assert np.equal(dataframe_cols, ["currency_id", "currency_code", "created_at", "last_updated"]).all()

def test_process_returns_dataframe_containing_input_data():
    raw_data = load_file_from_local(full_json)
    dataframe = process(raw_data)
    headers = ["currency_id", "currency_code", "created_at", "last_updated"]
    j = 0
    for header in headers:
        for i in range(len(raw_data['table']['data'])):
            assert dataframe[header][i] == raw_data['table']['data'][i][j]
        j += 1

def test_print_pd_dataframe_to_csv():
    raw_data = load_file_from_local(full_json)
    dataframe = process(raw_data)
    print_csv(dataframe, 'test/lambdas/process/csv_files/temp_test_1.csv')
    # print(dataframe.head())
    assert True