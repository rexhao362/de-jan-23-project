import pytest
import pyarrow as pa
from src.lambdas.load.utils.data_table_source import \
    DataFromPyArrowTable, DataFromParquetFile
from src.lambdas.load.utils.data_table import DataTable

dim_currency_table_name = "dim_currency"
dim_currency_schema = {
    "currency_id": "INT",
    "currency_code": "VARCHAR",
    "currency_name": "VARCHAR"
}

dim_currency_invalid_schema = {
    "currency_id": "INT",
    "currency_code": "INT",
    "currency_name": "VARCHAR"
}

test_table = pa.table({
    "currency_name": ["Pounds", "Dollars", "Euros"],
    "currency_id": [1, 2, 3],
    "currency_code": ["GBP", "USD", "EUR"],
    "suxx": [48, 48, 48]
})

test_table_sql_values = \
"""('1', 'GBP', 'Pounds'),
('2', 'USD', 'Dollars'),
('3', 'EUR', 'Euros')"""

test_path = "local/aws/s3/processed"

def _test_constructs_empty_data_table_when_passed_name_and_schema():
    try:
        dt = DataTable(dim_currency_table_name, dim_currency_schema)
        assert dt.name == dim_currency_table_name, f'invalid value of "name" property ("{dt.name}")'
        assert dt.schema == dim_currency_schema, f'invalid value of "schema" property ({dt.schema})'
        assert dt.column_names == [column_name for column_name in dim_currency_schema], 'invalid value of "column_names" property'
        assert dt.source == None, 'property "source" should be None'
        assert dt.table == None, 'property "table"  should be None'
        assert not dt.has_data(), "method has_data() should return False"

    except Exception as exc:
        assert False, exc

def _test_loads_data_from_pyarrow_table():
    dt = DataTable(dim_currency_table_name, dim_currency_schema)
    dt.from_pyarrow(test_table)
    assert isinstance(dt.source, DataFromPyArrowTable) and dt.source.is_initialized(), \
        'invalid value of "source" property'
    assert dt.table != None, 'property "table" should be initialized'
    assert dt.has_data(), "method has_data() should return True"

def _test_loads_data_from_parquet_file():
    dt = DataTable(dim_currency_table_name, dim_currency_schema)
    dt.from_parquet(test_path)
    source = dt.source

    assert isinstance(source, DataFromParquetFile) and source.is_initialized() and source.path != None, \
        'invalid value of "source" property'
    assert dt.table != None, 'property "table" should be initialized'
    assert dt.has_data(), "method has_data() should return True"

def _test_table_columns_as_per_schema():
    # arrange
    dt = DataTable(dim_currency_table_name, dim_currency_schema)
    # act
    dt.from_parquet(test_path)
    # assert
    assert dt.table.num_columns == len(dim_currency_schema), f'table should have {len(dim_currency_schema)} columns'
    for schema_column_name, table_column_name in zip(dim_currency_schema, dt.table.column_names):
        assert schema_column_name == table_column_name, "columns are not in correct order"

def test_to_sql_values_method_returns_correct_string():
    dt = DataTable(dim_currency_table_name, dim_currency_schema)
    dt.from_pyarrow(test_table)

    res = dt.to_sql_values()
    print(res)
    assert res == test_table_sql_values

@pytest.mark.xfail
def test_to_sql_request_method_returns_correct_value():
    assert False, "TODO"
    
# sad path

def _test_raises_type_error_when_table_column_type_does_not_match_schema():
    dt = DataTable(dim_currency_table_name, dim_currency_invalid_schema)
    re = r'column "\w+" should be of type .+$'
    with pytest.raises(TypeError, match=re) as exc_info:
        dt.from_pyarrow(test_table)

    print(f'Exception raised: {exc_info.value.args[0]}')
    


    
