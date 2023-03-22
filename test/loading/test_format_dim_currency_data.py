from src.loading.format_dim_currency_data import format_dim_currency_data

def test_returns_empty_list_when_passed_table_with_no_data():
    input_file_name = "test/loading/input_files/empty_dim_currency.parquet"
    assert format_dim_currency_data(input_file_name) == []

def test_returns_one_element_when_passed_table_with_one_row():
    input_file_name = "test/loading/input_files/one_dim_currency.parquet"
    ref_output = [
        [1, "GBP", "Pounds"]
    ]
    assert format_dim_currency_data(input_file_name) == ref_output

def test_returns_multiple_elements_when_passed_table_with_multiple_rows():
    input_file_name = "test/loading/input_files/dim_currency.parquet"
    ref_output = [
        [1, "GBP", "Pounds"],
        [2, "USD", "Dollars"],
        [3, "EUR", "Euros"]
    ]
    assert format_dim_currency_data(input_file_name) == ref_output

# invalid format file
# different set of columns
# inalid data types