# to allow running tests without PYTHONPATH
import sys
sys.path.append('./src/')
sys.path.append('./src/load')

import pytest
import pyarrow as pa
from utils.data_table_source import \
    DataFromPyArrowTable, DataFromParquetFile
from utils.data_table import DataTable

@pytest.fixture
def test_path():
    return "./test/load/input_files"

@pytest.fixture
def test_table_name():
    return "dim_currency"

@pytest.fixture
def test_table_schema():
    return {
        "currency_id": "INT",
        "currency_code": "VARCHAR",
        "currency_name": "VARCHAR"
    }

@pytest.fixture
def test_table_invalid_schema():
    return {
        "currency_id": "INT",
        "currency_code": "INT",
        "currency_name": "VARCHAR"
    }

@pytest.fixture
def test_table():
    return pa.table({
        "currency_id": [1, 2, 3],
        "currency_code": ["GBP", "USD", "EUR"],
        "currency_name": ["Pounds", "Dollars", "Euros"],
    })

@pytest.fixture
def test_table_with_extras():
    return pa.table({
        "currency_name": ["Pounds", "Dollars", "Euros"],
        "currency_id": [1, 2, 3],
        "currency_code": ["GBP", "USD", "EUR"],
        "extra": [48, 48, 48]
    })

@pytest.fixture
def empty_test_data_table(test_table_name, test_table_schema):
    return DataTable(test_table_name, test_table_schema)

@pytest.fixture
def test_data_table_from_pyarrow(test_table_name, test_table_schema, test_table):
    dt = DataTable(test_table_name, test_table_schema)
    dt.from_pyarrow(test_table)
    return dt

@pytest.fixture
def test_table_sql_values():
    return \
"""('1', 'GBP', 'Pounds'),
('2', 'USD', 'Dollars'),
('3', 'EUR', 'Euros')"""
    return value

def test_constructs_empty_data_table_when_passed_name_and_schema(empty_test_data_table, test_table_name, test_table_schema):
    try:
        dt = empty_test_data_table
        assert dt.name == test_table_name, f'invalid value of "name" property ("{dt.name}")'
        assert dt.schema == test_table_schema, f'invalid value of "schema" property ({dt.schema})'
        assert dt.column_names == [column_name for column_name in test_table_schema], 'invalid value of "column_names" property'
        assert dt.source == None, 'property "source" should be None'
        assert dt.table == None, 'property "table"  should be None'
        assert not dt.has_data(), "method has_data() should return False"

    except Exception as exc:
        assert False, exc

def test_loads_data_from_pyarrow_table(empty_test_data_table, test_table):
    dt = empty_test_data_table.from_pyarrow(test_table)
    assert isinstance(dt.source, DataFromPyArrowTable), '"source" property should be of type DataFromPyArrowTable'
    assert dt.source.is_initialized(), '"source" property is not initialized'
    assert dt.table is not None, 'property "table" should be initialized'
    assert dt.has_data() is True, "method has_data() should return True"

# TODO: use mock
def test_loads_data_from_parquet_file(empty_test_data_table, test_path):
    dt = empty_test_data_table.from_parquet(test_path)
    source = dt.source
    assert isinstance(source, DataFromParquetFile), '"source" property should be of type DataFromParquetFile'
    assert source.is_initialized(), '"source" property is not initialized'
    assert source.path is not None, '"source" property cannot be None'
    assert dt.table is not None, 'property "table" cannot be None'
    assert dt.has_data() is True, "method has_data() should return True"

def test_table_columns_as_per_schema(empty_test_data_table, test_table_with_extras, test_table_schema):
    dt = empty_test_data_table.from_pyarrow(test_table_with_extras)
    # assert
    assert dt.table.num_columns == len(test_table_schema), f'table should have {len(test_table_schema)} columns'
    for schema_column_name, table_column_name in zip(test_table_schema, dt.table.column_names):
        assert schema_column_name == table_column_name, "columns are not in correct order"

def test_to_sql_values_method_returns_correct_string(test_data_table_from_pyarrow, test_table_sql_values):
    res = test_data_table_from_pyarrow.to_sql_values()
    assert res == test_table_sql_values


@pytest.mark.parametrize("db_schema", [None, "test"] )
def test_to_sql_request_method_returns_correct_value(test_data_table_from_pyarrow, db_schema):
    dt = test_data_table_from_pyarrow
    table_name = f'{db_schema}.{dt.name}' if db_schema else dt.name
    ref_sql_request = "INSERT INTO " + table_name + "\n" + \
        "(" + ", ".join(dt.column_names) + ")" + "\n" + \
        "VALUES\n" + \
        dt.to_sql_values()
    assert test_data_table_from_pyarrow.to_sql_request(db_schema) == ref_sql_request
    
# sad path

def test_raises_type_error_when_table_column_type_does_not_match_schema(test_table_name, test_table_invalid_schema, test_table):
    dt = DataTable(test_table_name, test_table_invalid_schema)
    re = r'column "\w+" should be of type .+$'
    with pytest.raises(TypeError, match=re) as exc_info:
        dt.from_pyarrow(test_table)

    #print(f'Exception raised: {exc_info.value.args[0]}')
    


    
