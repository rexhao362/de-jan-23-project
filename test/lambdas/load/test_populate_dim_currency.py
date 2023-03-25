import pytest
from src.lambdas.load.populate_dim_currency import validate_data
from src.lambdas.load.testutils.test_populate_tables import _test_populate_independent_table

table_name = "dim_currency"

# test validator
def test_validate_raises_exception_when_passed_non_list():
    with pytest.raises(TypeError):
        validate_data("invalid data")

    with pytest.raises(TypeError):
        validate_data( {} )

def test_validate_raises_exception_when_passed_list_with_no_list_inside():
    with pytest.raises(TypeError):
        validate_data( [ "a" ] )

def test_validate_raises_exception_when_passed_list_with_empty_list():
    with pytest.raises(ValueError):
        validate_data( [ [] ] )

def test_validate_raises_exception_when_passed_invalid_types_in_nested_list():
    with pytest.raises(TypeError):
        validate_data( [ ["a", "b", "c"] ] )

    with pytest.raises(TypeError):
        validate_data( [ [1, 2, "c"] ] )

    with pytest.raises(TypeError):
        validate_data( [ [1, "b", 3] ] )

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