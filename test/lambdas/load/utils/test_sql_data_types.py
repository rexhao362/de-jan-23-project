import pytest
import pyarrow as pa

from src.lambdas.load.utils.sql_data_types import \
    get_sql_data_type, \
    SQLDataTypeINT, \
    SQLDataTypeVARCHAR, \
    SQLDataTypeTIME, \
    SQLDataTypeDATE, \
    SQLDataTypeNUMERIC

test_table = pa.table({
    "currency_name": ["Pounds", "Dollars", "Euros"],
    "currency_id": [1, 2, 3],
    "currency_code": ["GBP", "USD", "EUR"],
    "creation_time": ["10:00:00", "11:10:00", "08:00:03"],                                                                                                                                                                                                                      
    "updated_on": ["01.12.1973", "20.08.2002", "05.01.2023"],
    "unit_price": [2.78, 5.12, 3.56]
})

def test_get_sql_data_type_returns_int_object():                                                                                                                                                                                                                                                                                                        
    dt = get_sql_data_type("INT")
    assert isinstance(dt, SQLDataTypeINT)
    assert isinstance( get_sql_data_type("integer"), SQLDataTypeINT)
    assert isinstance( get_sql_data_type("int4"), SQLDataTypeINT)
    assert dt.matches_pyarrow_type( test_table["currency_id"].type ), f'class {dt.__class__.__name__} does not pass matches_pyarrow_type() test'

def test_get_sql_data_type_returns_varchar_object():
    dt = get_sql_data_type("varchar")
    assert isinstance(dt, SQLDataTypeVARCHAR)
    assert dt.max_length == None
    assert dt.matches_pyarrow_type( test_table["currency_code"].type ), f'class {dt.__class__.__name__} does not pass matches_pyarrow_type() test'

    max_length = 10
    dt = get_sql_data_type( f"VARCHAR({max_length})" )
    assert isinstance(dt, SQLDataTypeVARCHAR)
    assert dt.max_length == max_length

    dt = get_sql_data_type("character varying")
    assert isinstance(dt, SQLDataTypeVARCHAR)
    assert dt.max_length == None

    max_length = 3
    dt = get_sql_data_type(f"character  varying ( {max_length} )")
    assert isinstance(dt, SQLDataTypeVARCHAR)
    assert dt.max_length == max_length

def test_get_sql_data_type_returns_time_object():
    # time
    dt = get_sql_data_type("time")
    assert isinstance(dt, SQLDataTypeTIME)
    assert not dt.precision
    assert dt.without_time_zone == True
    assert dt.matches_pyarrow_type( test_table["creation_time"].type ), f'class {dt.__class__.__name__} does not pass matches_pyarrow_type() test'

    # time (p)
    precision = 2
    dt = get_sql_data_type( f"time ({precision})" )
    assert isinstance(dt, SQLDataTypeTIME)
    assert dt.precision == precision
    assert dt.without_time_zone == True

    # time without time zone
    dt = get_sql_data_type( f"time without time zone" )
    assert isinstance(dt, SQLDataTypeTIME)
    assert not dt.precision
    assert dt.without_time_zone == True

    # time (p) without time zone
    precision = 5
    dt = get_sql_data_type( f"time ({precision}) without time zone" )
    assert isinstance(dt, SQLDataTypeTIME)
    assert dt.precision == precision
    assert dt.without_time_zone == True

    # time with time zone
    dt = get_sql_data_type("time with time zone" )
    assert isinstance(dt, SQLDataTypeTIME)
    assert not dt.precision
    assert dt.without_time_zone == False

    # time (p) with time zone
    precision = 3
    dt = get_sql_data_type( f"time ({precision}) with time zone" )
    assert isinstance(dt, SQLDataTypeTIME)
    assert dt.precision == precision
    assert dt.without_time_zone == False

def test_get_sql_data_type_returns_date_object():
    dt = get_sql_data_type("date")
    assert isinstance(dt, SQLDataTypeDATE)
    assert dt.matches_pyarrow_type( test_table["updated_on"].type ), f'class {dt.__class__.__name__} does not pass matches_pyarrow_type() test'

def test_get_sql_data_type_returns_numeric_object():
    # NUMERIC
    dt = get_sql_data_type("numeric")
    assert isinstance(dt, SQLDataTypeNUMERIC)
    assert dt.precision == None
    assert dt.scale == None
    # TODO
    #assert dt.matches_pyarrow_type( test_table["unit_price"].type ), f'class {dt.__class__.__name__} does not pass matches_pyarrow_type() test'

    # NUMERIC (p)
    precision = 10
    dt = get_sql_data_type( f"NUMERIC({precision})" )
    assert isinstance(dt, SQLDataTypeNUMERIC)
    assert dt.precision == precision
    assert dt.scale == None

    # NUMERIC (p, s)
    precision = 10
    scale = 2
    dt = get_sql_data_type( f"NUMERIC({precision}, {scale})" )
    assert isinstance(dt, SQLDataTypeNUMERIC)
    assert dt.precision == precision
    assert dt.scale == scale

    # repetition, refactor!
    # decimal
    dt = get_sql_data_type("DECIMAL")
    assert isinstance(dt, SQLDataTypeNUMERIC)
    assert dt.precision == None
    assert dt.scale == None

    # decimal (p)
    precision = 10
    dt = get_sql_data_type( f"decimal({precision})" )
    assert isinstance(dt, SQLDataTypeNUMERIC)
    assert dt.precision == precision
    assert dt.scale == None

    # decimal (p, s)
    precision = 10
    scale = 2
    dt = get_sql_data_type( f"decimal({precision}, {scale})" )
    assert isinstance(dt, SQLDataTypeNUMERIC)
    assert dt.precision == precision
    assert dt.scale == scale

# test_table = pa.table({
#     "currency_name": ["Pounds", "Dollars", "Euros"],
#     "currency_id": [1, 2, 3],
#     "currency_code": ["GBP", "USD", "EUR"]
# })

# # happy path

# def test_constructs_int_type():
#     assert SQLDataType("int").data_type_name == "INT"
#     assert SQLDataType("INT").data_type_name == "INT"

# def test_constructs_varchar_type():
#     assert SQLDataType("varchar").data_type_name == "VARCHAR"
#     assert SQLDataType("VARCHAR").data_type_name == "VARCHAR"

# def test_constructs_time_type():
#     assert SQLDataType("time").data_type_name == "TIME"
#     assert SQLDataType("TIME").data_type_name == "TIME"

# def test_constructs_date_type():
#     assert SQLDataType("date").data_type_name == "DATE"
#     assert SQLDataType("DATE").data_type_name == "DATE"

# def test_constructs_decimal_type():
#     assert SQLDataType("numeric").data_type_name == "NUMERIC"
#     assert SQLDataType("NUMERIC").data_type_name == "NUMERIC"

# def test_matches_pyarrow_type():
#     assert SQLDataType("int").matches_pyarrow_type( test_table["currency_id"].type )
#     assert SQLDataType("varchar").matches_pyarrow_type( test_table["currency_name"].type )

# # sad path

# def test_raises_type_error_if_constructed_with_non_string_argument():
#     re = r'type_name should be a string'
#     with pytest.raises(TypeError, match=re) as exc_info:
#         SQLDataType(1)

#     print(f'Exception raised: {exc_info.value.args[0]}')

#     with pytest.raises(TypeError, match=re) as exc_info:
#         SQLDataType( [] )

#     print(f'Exception raised: {exc_info.value.args[0]}')

# def test_raises_value_error_if_constructed_with_unsupported_type_name():
#     re = r'unsupported/invalid SQL data type'

#     with pytest.raises(ValueError, match=re) as exc_info:
#         SQLDataType("invalid_type_name")

#     print(f'Exception raised: {exc_info.value.args[0]}')

#     with pytest.raises(ValueError, match=re) as exc_info:
#         SQLDataType("STRING")

#     print(f'Exception raised: {exc_info.value.args[0]}')

# # def test_has_name_method():
# #     type_name = "INT"
# #     sql_dt = SQLDataType(type_name)
# #     assert sql_dt.name() == type_name