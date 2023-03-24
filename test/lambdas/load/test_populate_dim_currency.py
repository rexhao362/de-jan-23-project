import pytest
from src.lambdas.load.utils.test_populate_tables import _test_populate_independent_table
from src.lambdas.load.populate_dim_currency import (validate_data, populate_dim_currency)

table_name = "dim_currency"

# test validator
def test_validate_raises_exception_when_passed_string():
    with pytest.raises(TypeError):
        data = "invalid data"
        validate_data(data)

def test_validate_raises_exception_when_dict():
    with pytest.raises(TypeError):
        data = {}
        validate_data(data)

def test_validate_raises_exception_when_passed_list_with_no_list_inside():
    with pytest.raises(TypeError):
        data = [ "a" ]
        validate_data(data)

def test_validate_raises_exception_when_passed_list_with_empty_list():
    with pytest.raises(ValueError):
        data = [ [] ]
        validate_data(data)

def test_validate_raises_exception_when_passed_invalid_types_in_nested_list():
    with pytest.raises(TypeError):
        data = [ ["a", "b", "c"] ]
        validate_data(data)

    with pytest.raises(TypeError):
        data = [ [1, 2, "c"] ]
        validate_data(data)

    with pytest.raises(TypeError):
        data = [ [1, "b", 3] ]
        validate_data(data)

# happy path

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