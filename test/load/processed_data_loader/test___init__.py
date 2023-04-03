# to allow running tests without PYTHONPATH
import sys
sys.path.append('./')
# to allow for flattened lambda file structure
sys.path.append('./src/load/')

import pytest
from src.load.processed_data_loader import _ProcessedDataLoader
from src.load.db_schema import mvp_database_schema as default_database_schema
import src.load.db_schema
from src.load.utils.data_table import DataTable

def test_creates_loader_with_default_db_schema():
    dl = _ProcessedDataLoader()
    assert len(dl.db_schema) == len(default_database_schema)
    for dt, default_dt in zip(dl.db_schema, default_database_schema):
        assert dt.name == default_dt.name
        assert dt.schema == default_dt.schema
        assert dt.column_names == default_dt.column_names
        assert dt.source == default_dt.source
        assert dt.table == default_dt.table

def test_creates_loader_with_custom_db_schema():
    test_data_table = DataTable("test_table", {"column": "int"} )
    custom_db_schema = [test_data_table]

    dl = _ProcessedDataLoader(custom_db_schema)
    schema = dl.db_schema
    assert schema == custom_db_schema
    assert len(schema) == 1
    assert schema[0] == test_data_table