import re
import pyarrow.types as pytypes

class _SQLDataType:
    def __init__(self, type_match_function, not_null=False):
        self.type_match_function = type_match_function
        self.not_null = not_null

    def matches_pyarrow_type(self, pyarrow_type):
        res = False
        if self.not_null:
            res = self.type_match_function(pyarrow_type)
        else:
            res = self.type_match_function(pyarrow_type) or pytypes.is_null(pyarrow_type)

        return res

# int | integer | int4
class SQLDataTypeINT(_SQLDataType):
    def __init__(self, not_null):
        super().__init__(pytypes.is_integer, not_null)

# character varying [ (n) ] | varchar [ (n) ]
class SQLDataTypeVARCHAR(_SQLDataType):
    def __init__(self, not_null, max_length=None):
        super().__init__(pytypes.is_string, not_null)
        self.max_length = max_length

# time [ (p) ] [ without time zone ] | time [ (p) ] with time zone
#
# where p: 0 to 6
class SQLDataTypeTIME(_SQLDataType):
    def __init__(self, not_null, precision=None, without_time_zone=True):
        super().__init__(pytypes.is_time64, not_null)
        self.precision = precision
        self.without_time_zone = without_time_zone

# date
class SQLDataTypeDATE(_SQLDataType):
    def __init__(self, not_null):
        super().__init__(pytypes.is_date32, not_null)
        #super().__init__(pytypes.is_timestamp, not_null)

# NUMERIC [ (precision [, scale ] ) ] | DECIMAL [ (precision [, scale ] ) ]
class SQLDataTypeNUMERIC(_SQLDataType):
    def __init__(self, not_null, precision=None, scale=None):
        super().__init__(pytypes.is_floating, not_null)
        self.precision = precision
        self.scale = scale


def get_sql_data_type(data_type_name):
    arg_type = type(data_type_name)
    if arg_type is not str:
        msg = f'{__name__}: data_type_name should be a string, got {arg_type}'
        raise TypeError(msg)

    # INT
    pattern = r'^int(4|(eger))?\s*(not\s+null)?\s*$'
    m = re.match(pattern, data_type_name, re.IGNORECASE)
    if m:
        not_null = m.group(3) != None
        return SQLDataTypeINT(not_null)

    # VARCHAR
    pattern = r'^(character\s+)?var(char|ying)(\s*\(\s*(\d+)\s*\))?\s*(not\s+null)?\s*$'
    m = re.match(pattern, data_type_name, re.IGNORECASE)
    if m:
        max_length = m.group(4)
        if max_length:
            max_length = int(max_length)
        
        not_null = m.group(5) != None
        return SQLDataTypeVARCHAR(not_null, max_length)

    # TIME
    pattern = r'^time((\s*\(\s*(\d)\))?(\s+with(out)?\s+time\s+zone)?)?\s*(not\s+null)?\s*$'
    m = re.match(pattern, data_type_name, re.IGNORECASE)
    if m:
        precision = m.group(3)
        if precision:
            precision = int(precision)

        without_time_zone = not (m.group(4) and not m.group(5))
        not_null = m.group(6) != None
        return SQLDataTypeTIME(not_null, precision, without_time_zone)

    # DATE
    pattern = r'^date\s*(not\s+null)?\s*$'
    m = re.match(pattern, data_type_name, re.IGNORECASE)
    if m:
        not_null = m.group(1) != None
        return SQLDataTypeDATE(not_null)

    # NUMERIC
    pattern = r'^(numeric|decimal)\s*(\(\s*(\d+)\s*(,\s*(\d+))?\))?\s*(not\s+null)?\s*$'
    m = re.match(pattern, data_type_name, re.IGNORECASE)
    if m:
        precision = m.group(3)
        if precision:
            precision = int(precision)

        scale = m.group(5)
        if scale:
            scale = int(scale)

        not_null = m.group(6) != None
        return SQLDataTypeNUMERIC(precision, scale)

    msg = f'unsupported/invalid SQL data type "{data_type_name}"'
    raise ValueError(msg)