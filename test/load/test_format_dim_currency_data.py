import pytest
import pandas
from src.load.format_dim_currency_data import format_dim_currency_data

# happy path

def test_returns_empty_list_when_passed_table_with_no_data():
    input_file_name = "test/load/input_files/empty_dim_currency.parquet"
    data_frame = pandas.read_parquet(input_file_name)
    assert format_dim_currency_data(data_frame) == []

def test_returns_one_element_when_passed_table_with_one_row():
    input_file_name = "test/load/input_files/one_dim_currency.parquet"
    data_frame = pandas.read_parquet(input_file_name)
    ref_output = [
        [1, "GBP", "Pounds"]
    ]
    assert format_dim_currency_data(data_frame) == ref_output

def test_returns_multiple_elements_when_passed_table_with_multiple_rows():
    input_file_name = "test/load/input_files/dim_currency.parquet"
    data_frame = pandas.read_parquet(input_file_name)
    ref_output = [
        [1, "GBP", "Pounds"],
        [2, "USD", "Dollars"],
        [3, "EUR", "Euros"]
    ]
    assert format_dim_currency_data(data_frame) == ref_output

# sad path

def test_raises_exception_on_errors():
    # # file does not exist
    # with pytest.raises(Exception) as e:
    #     format_dim_currency_data("")

    # # file of 0 bytes
    # with pytest.raises(Exception) as e:
    #     format_dim_currency_data("test/load/input_files/invalid.parquet")

    # # invalid format type
    # with pytest.raises(Exception) as e:
    #     format_dim_currency_data("./README.md")
    
    # does not have all necessary columns
    with pytest.raises(Exception) as e:
        input_file_name = "test/load/input_files/dim_staff.parquet"
        data_frame = pandas.read_parquet(input_file_name)
        format_dim_currency_data(data_frame)

    # inalid data types - MAYBE TODO?
    # with pytest.raises(Exception) as e:
    #     format_dim_currency_data("test/load/input_files/dim_currency.parquet")