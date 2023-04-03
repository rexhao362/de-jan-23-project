# to allow running tests without PYTHONPATH
import sys
sys.path.append('./')

from src.load.utils.data_table_source import \
    BaseTableDataSource, DataFromPyArrowTable, DataFromParquetFile

def test_creates_uninitialized_object_when_passed_no_arguments():
    source = BaseTableDataSource()
    assert source.type == None, 'property "type" should be None'
    assert source.path == None, 'property "path" should be None'
    assert not source.is_initialized(), "method is_initialized() should return False"

def test_creates_table_source():
    source = DataFromPyArrowTable()
    ref_value = "pyarrow_table"
    assert source.type == ref_value, f'value of property "type" is invalid ({source.type}), expected "{ref_value}"'
    assert source.is_initialized(), "method is_initialized() should return True"

def test_creates_parquet_source():
    source = DataFromParquetFile("test/path")
    ref_value = "parquet_file"
    assert source.type == ref_value, f'value of property "type" is invalid ({source.type}), expected "{ref_value}"'
    assert source.path != None, 'property "path" should be initialized'
    assert source.is_initialized(), "method is_initialized() should return True"