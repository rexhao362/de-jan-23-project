import pytest
from src.utils.db.make_schema_table_name import make_schema_table_name

def test_returns_correct_result_when_passed_valid_arguments():
    # arrange
    schema_name = "test_schema"
    table_name = "test_table"

    ref_output = f'{schema_name}.{table_name}'

    # act & assert
    assert make_schema_table_name(schema_name, table_name) == ref_output

def test_raises_exception_when_passed_empty_strings():
    with pytest.raises(ValueError):
        make_schema_table_name("", "table")

    with pytest.raises(ValueError):
        make_schema_table_name("schema", "")

    with pytest.raises(ValueError):
        make_schema_table_name("", "")

def test_raises_exception_when_passed_nonstring_arguments():
    with pytest.raises(TypeError):
        make_schema_table_name(1, 2)

    with pytest.raises(TypeError):
        make_schema_table_name(None, "")

    with pytest.raises(TypeError):
        make_schema_table_name( [], {"a": 5} )