import sys
sys.path.append('./src/')
from load.data_loader import DataLoader
from load.db_schema import db_schema
from load.lutils.data_table import DataTable

def test_creates_loader_with_default_db_schema():
    dl = DataLoader()
    assert dl.db_schema == db_schema

def test_creates_loader_with_custom_db_schema():
    test_data_table = DataTable("test_table", {"column": "int"} )
    custom_db_schema = [test_data_table]

    dl = DataLoader(custom_db_schema)
    schema = dl.db_schema
    assert schema == custom_db_schema
    assert len(schema) == 1
    assert schema[0] == test_data_table