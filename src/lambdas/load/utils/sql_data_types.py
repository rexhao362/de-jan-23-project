import re
import pyarrow.types as pytypes

class _SQLDataType:
    def __init__(self, type_match_function):
        self.type_match_function = type_match_function

    def matches_pyarrow_type(self, pyarrow_type):
        return self.type_match_function(pyarrow_type)

# int | integer | int4
class SQLDataTypeINT(_SQLDataType):
    def __init__(self):
        super().__init__(pytypes.is_integer)

# character varying [ n) ] | varchar [ (n) ]
class SQLDataTypeVARCHAR(_SQLDataType):
    def __init__(self, max_length=None):
        super().__init__(pytypes.is_string)
        self.max_length = max_length

# time [ (p) ] [ without time zone ] | time [ (p) ] with time zone
#
# where p: 0 to 6
class SQLDataTypeTIME(_SQLDataType):
    def __init__(self, precision=None, without_time_zone=True):
        super().__init__(pytypes.is_string)
        self.precision = precision
        self.without_time_zone = without_time_zone

# date
class SQLDataTypeDATE(_SQLDataType):
    def __init__(self):
        super().__init__(pytypes.is_string)

# NUMERIC [ (precision [, scale ] ) ] | DECIMAL [ (precision [, scale ] ) ]
class SQLDataTypeNUMERIC(_SQLDataType):
    def __init__(self, precision=None, scale=None):
        super().__init__(pytypes.is_floating)
        self.precision = precision
        self.scale = scale


def get_sql_data_type(data_type_name):
    arg_type = type(data_type_name)
    if arg_type is not str:
        msg = f'{__name__}: data_type_name should be a string, got {arg_type}'
        raise TypeError(msg)

    pattern = r'^int(4|(eger))?$'
    if re.match(pattern, data_type_name, re.IGNORECASE):
        return SQLDataTypeINT()

    pattern = r'^(character\s+)?var(char|ying)(\s*\(\s*(\d+)\s*\))?'
    m = re.match(pattern, data_type_name, re.IGNORECASE)
    if m:
        max_length = m.group(4)
        if max_length:
            max_length = int(max_length)
        return SQLDataTypeVARCHAR(max_length)

    pattern = r'^time((\s*\(\s*(\d)\))?(\s+with(out)?\s+time\s+zone)?)?$'
    m = re.match(pattern, data_type_name, re.IGNORECASE)
    if m:
        precision = m.group(3)
        if precision:
            precision = int(precision)

        without_time_zone = not (m.group(4) and not m.group(5))
        return SQLDataTypeTIME(precision, without_time_zone)

    pattern = r'^date$'
    if re.match(pattern, data_type_name, re.IGNORECASE):
        return SQLDataTypeDATE()

    pattern = r'^(numeric|decimal)\s*(\(\s*(\d+)\s*(,\s*(\d+))?\))?$'
    m = re.match(pattern, data_type_name, re.IGNORECASE)
    if m:
        precision = m.group(3)
        if precision:
            precision = int(precision)

        scale = m.group(5)
        if scale:
            scale = int(scale)

        return SQLDataTypeNUMERIC(precision, scale)

    msg = f'unsupported/invalid SQL data type "{data_type_name}"'
    raise ValueError(msg)
    

# class SQLDataType:
#     # class variables
#     INT = "INT"
#     VARCHAR = "VARCHAR"
#     TIME = "TIME"
#     DATE = "DATE"
#     NUMERIC = "NUMERIC"

#     supported_data_types = {
#         INT: pytypes.is_integer,
#         VARCHAR: pytypes.is_string,
#         TIME: pytypes.is_string,
#         DATE: pytypes.is_string,
#         NUMERIC: pytypes.is_decimal,
#     }

#     def __init__(self, data_type_name):
#         class_name = self.__class__.__name__

#         if not type(data_type_name) is str:
#             msg = f"{class_name}({data_type_name}): data_type_name should be a string"
#             raise TypeError(msg)

#         uc_data_type_name = data_type_name.upper()
#         if not uc_data_type_name in self.supported_data_types:
#             raise ValueError(f'{class_name}("{uc_data_type_name}"): unsupported/invalid SQL data type')

#         self.data_type_name = uc_data_type_name

#     def matches_pyarrow_type(self, pyarrow_type):
#         return self.supported_data_types[self.data_type_name](pyarrow_type)

#     # def name(self):
#     #     return self.data_type_name