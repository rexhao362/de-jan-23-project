import pytest
from src.lambdas.load.testutils.test_populate_tables import _test_populate_independent_table

table_name = "dim_currency"

def test_does_not_change_db_when_passed_empty_data():
    # arrange
    data = []
    _test_populate_independent_table(table_name, data)

def test_loads_one_row_when_passed_one_currency_item():
    # arrange
    input_currency_data = [
        [1, "USD", "US dollar"]
    ]

    _test_populate_independent_table(table_name, input_currency_data)

def test_loads_multiple_rows_when_passed_multiple_currency_item():
    # arrange
    input_currency_data = [
        [1, "USD", "US dollar"],
        [5, "GBP", "pound"],
        [13, "JPY", "yen"]
    ]

    _test_populate_independent_table(table_name, input_currency_data)