import pyarrow as pa

class SQLDataType:
    # class variables
    INT = "INT"
    VARCHAR = "VARCHAR"

    supported_data_types = {
        INT: pa.types.is_integer,
        VARCHAR: pa.types.is_string
    }

    def __init__(self, data_type_name):
        class_name = self.__class__.__name__

        if not type(data_type_name) is str:
            msg = f"{class_name}({data_type_name}): data_type_name should be a string"
            raise TypeError(msg)

        uc_data_type_name = data_type_name.upper()
        if not uc_data_type_name in self.supported_data_types:
            raise ValueError(f'{class_name}("{data_type_name}"): unsupported SQL data type')

        self.data_type_name = uc_data_type_name

    def matches_pyarrow_type(self, pyarrow_type):
        return self.supported_data_types[self.data_type_name](pyarrow_type)

    # def name(self):
    #     return self.data_type_name