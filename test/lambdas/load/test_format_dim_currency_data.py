import pytest
import pandas
from src.lambdas.load.format_dim_currency_data import format_dim_currency_data

# happy path
test_path = "test/lambdas/load/input_files"

def _test_format_dim_currency_data(path, ref_output = None):
    data_frame = pandas.read_parquet(path)
    assert format_dim_currency_data(data_frame) == ref_output

def test_returns_empty_list_when_passed_table_with_no_data():
    path = f'{test_path}/empty_dim_currency.parquet'
    ref_output = []
    _test_format_dim_currency_data(path, ref_output )

def test_returns_one_element_when_passed_table_with_one_row():
    path = f'{test_path}/one_dim_currency.parquet'
    ref_output = [
        [1, "GBP", "Pounds"]
    ]
    _test_format_dim_currency_data(path, ref_output )

def test_returns_multiple_elements_when_passed_table_with_multiple_rows():
    path = f'{test_path}/dim_currency.parquet'
    ref_output = [
        [1, "GBP", "Pounds"],
        [2, "USD", "Dollars"],
        [3, "EUR", "Euros"]
    ]
    _test_format_dim_currency_data(path, ref_output )

# sad path

def test_raises_exception_on_errors():
    # does not have all necessary columns
    with pytest.raises(Exception) as e:
        path = f'{test_path}/dim_staff.parquet'
        _test_format_dim_currency_data(path)

    # this is part of the original read_parquet, no need to test
    # # file does not exist
    # with pytest.raises(Exception) as e:
    #     format_dim_currency_data("")

    # # file of 0 bytes
    # with pytest.raises(Exception) as e:
    #     format_dim_currency_data("test/lambdas/load/input_files/invalid.parquet")

    # # invalid format type
    # with pytest.raises(Exception) as e:
    #     format_dim_currency_data("./README.md")
    
    

    # inalid data types - MAYBE TODO?
    # with pytest.raises(Exception) as e:
    #     format_dim_currency_data("test/lambdas/load/input_files/dim_currency.parquet")