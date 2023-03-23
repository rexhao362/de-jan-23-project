from src.lambdas.process import (load_file_from_local, process, build_dim_date)
from pandas import DataFrame
from pandas import testing as pdtesting
from pandas import to_datetime as to_datetime
from numpy import equal
import numpy as np

def test_returns_dataframe():
    df = build_dim_date('2020/01/01', '2022/01/01')
    assert isinstance(df, DataFrame)
    
def test_returned_dataframe_has_expected_index_and_columns():
    df = build_dim_date('2020/01/01', '2022/01/01')
    expected_cols = ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
    actual_cols = df.columns.values
    assert equal(expected_cols, actual_cols).all()
    
def test_returned_data_columns_have_expected_data_types():
    df = build_dim_date('2020/01/01', '2022/01/01')
    cols = ['year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
    expected_dtypes = ['int64', 'int64', 'int64', 'int64', 'O', 'O', 'int64']
    types = df.dtypes
    for i in range(len(cols)):
        assert types[cols[i]] == expected_dtypes[i]
    
def test_returned_data_is_correct():
    df = build_dim_date('2023/03/20', '2023/03/26')
    expected_first_row = [to_datetime('2023-03-20'), 2023, 3, 20, 0, 'Monday', 'March', 1]
    actual_first_row = df.iloc[0]
    for i in range(len(expected_first_row)):
        assert expected_first_row[i] == actual_first_row[i]