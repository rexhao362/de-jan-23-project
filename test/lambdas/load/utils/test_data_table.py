import pytest
import pyarrow as pa
from src.lambdas.load.utils.data_table_source import \
    DataFromPyArrowTable, DataFromParquetFile
from src.lambdas.load.utils.data_table import DataTable

dim_currency_table_name = "dim_currency"
dim_currency_format = {
    "currency_id": "INT",
    "currency_code": "VARCHAR",
    "currency_name": "VARCHAR"
}

dim_currency_invalid_format = {
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

def _test_constructs_empty_data_table_when_passed_name_and_table_format():
    try:
        dt = DataTable(dim_currency_table_name, dim_currency_format)
        assert dt.name == dim_currency_table_name, f'invalid value of "name" property ("{dt.name}")'
        assert dt.format == dim_currency_format, f'invalid value of "format" property ({dt.format})'
        assert dt.column_names == [column_name for column_name in dim_currency_format], 'invalid value of "column_names" property'
        assert dt.source == None, 'property "source" should be None'
        assert dt.table == None, 'property "table"  should be None'
        assert not dt.has_data(), "method has_data() should return False"

    except Exception as exc:
        assert False, exc

def _test_loads_data_from_pyarrow_table():
    dt = DataTable(dim_currency_table_name, dim_currency_format)
    dt.from_pyarrow(test_table)
    assert isinstance(dt.source, DataFromPyArrowTable) and dt.source.is_initialized(), \
        'invalid value of "source" property'
    assert dt.table != None, 'property "table" should be initialized'
    assert dt.has_data(), "method has_data() should return True"

def _test_loads_data_from_parquet_file():
    dt = DataTable(dim_currency_table_name, dim_currency_format)
    dt.from_parquet(test_path)
    source = dt.source

    assert isinstance(source, DataFromParquetFile) and source.is_initialized() and source.path != None, \
        'invalid value of "source" property'
    assert dt.table != None, 'property "table" should be initialized'
    assert dt.has_data(), "method has_data() should return True"

def _test_table_columns_as_per_format():
    # arrange
    dt = DataTable(dim_currency_table_name, dim_currency_format)
    # act
    dt.from_parquet(test_path)
    # assert
    assert dt.table.num_columns == len(dim_currency_format), f'table should have {len(dim_currency_format)} columns'
    for format_column_name, table_column_name in zip(dim_currency_format, dt.table.column_names):
        assert format_column_name == table_column_name, "columns are not in correct order"

def test_to_sql_values_method_returns_correct_string():
    dt = DataTable(dim_currency_table_name, dim_currency_format)
    dt.from_pyarrow(test_table)

    res = dt.to_sql_values()
    print(res)
    assert res == test_table_sql_values

def test_prepare_sql_request_method_returns_correct_value():
    assert False, "TODO"
    
# sad path

def _test_raises_type_error_when_table_column_type_does_not_match_format():
    dt = DataTable(dim_currency_table_name, dim_currency_invalid_format)
    re = r'column "\w+" should be of type .+$'
    with pytest.raises(TypeError, match=re) as exc_info:
        dt.from_pyarrow(test_table)

    print(f'Exception raised: {exc_info.value.args[0]}')
    


    
