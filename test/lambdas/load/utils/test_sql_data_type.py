import pytest
import pyarrow as pa
from src.lambdas.load.utils.sql_data_type import SQLDataType

test_table = pa.table({
    "currency_name": ["Pounds", "Dollars", "Euros"],
    "currency_id": [1, 2, 3],
    "currency_code": ["GBP", "USD", "EUR"]
})

# happy path

def test_constructs_int_type():
    assert SQLDataType("int").data_type_name == "INT"
    assert SQLDataType("INT").data_type_name == "INT"

def test_constructs_varchar_type():
    assert SQLDataType("varchar").data_type_name == "VARCHAR"
    assert SQLDataType("VARCHAR").data_type_name == "VARCHAR"

def test_constructs_time_type():
    assert SQLDataType("time").data_type_name == "TIME"
    assert SQLDataType("TIME").data_type_name == "TIME"

def test_constructs_date_type():
    assert SQLDataType("date").data_type_name == "DATE"
    assert SQLDataType("DATE").data_type_name == "DATE"

def test_matches_pyarrow_type():
    assert SQLDataType("int").matches_pyarrow_type( test_table["currency_id"].type )
    assert SQLDataType("varchar").matches_pyarrow_type( test_table["currency_name"].type )

# sad path

def test_raises_type_error_if_constructed_with_non_string_argument():
    re = r'type_name should be a string'
    with pytest.raises(TypeError, match=re) as exc_info:
        SQLDataType(1)

    print(f'Exception raised: {exc_info.value.args[0]}')

    with pytest.raises(TypeError, match=re) as exc_info:
        SQLDataType( [] )

    print(f'Exception raised: {exc_info.value.args[0]}')

def test_raises_value_error_if_constructed_with_unsupported_type_name():
    re = r'unsupported/invalid SQL data type'

    with pytest.raises(ValueError, match=re) as exc_info:
        SQLDataType("invalid_type_name")

    print(f'Exception raised: {exc_info.value.args[0]}')

    with pytest.raises(ValueError, match=re) as exc_info:
        SQLDataType("STRING")

    print(f'Exception raised: {exc_info.value.args[0]}')

# def test_has_name_method():
#     type_name = "INT"
#     sql_dt = SQLDataType(type_name)
#     assert sql_dt.name() == type_name