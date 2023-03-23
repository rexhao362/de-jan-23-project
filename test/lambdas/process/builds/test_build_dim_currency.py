from src.lambdas.process.utils import load_file_from_local, process
from src.lambdas.process.build import build_dim_currency
from pandas import DataFrame
from numpy import equal

currency_json_path = 'test/json_files/currency_test_1.json'

currency_data = load_file_from_local(currency_json_path)
currency_dataframe = process(currency_data)
dim_currency = build_dim_currency(currency_dataframe)

def test_returns_dataframe():
    assert isinstance(dim_currency, DataFrame)

def test_returned_dataframe_has_expected_columns():
    dim_currency_cols = dim_currency.columns.values
    assert equal(dim_currency_cols, ['currency_id', 'currency_code', 'currency_name']).all()

def test_returned_data_is_maintained_through_build():
    assert dim_currency['currency_id'][0] == 1
    assert dim_currency['currency_code'][0] == 'GBP'
    assert dim_currency['currency_name'][0] == 'Pounds'

def test_returned_data_is_of_correct_type():
    for entry in dim_currency['currency_id']:
        assert type(entry) == int
    for entry in dim_currency['currency_code']:
        assert type(entry) == str
    for entry in dim_currency['currency_name']:
        assert type(entry) == str