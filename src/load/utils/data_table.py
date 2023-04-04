import pyarrow.parquet as pq
import src.gutils.path as path
from src.load.utils.data_table_source import \
    DataFromPyArrowTable, DataFromParquetFile
from src.load.utils.sql_data_types import get_sql_data_type

default_parquet_extension = "parquet"

class DataTable:
    """
    Wrapper class for pyarrow.Table that has only columns from schema
    It allows importing data from pyarrow.Table or parquet file
    and validates it using schema.
    Imported data can be exported as a SQL query (INSERT INTO table)
    """
    def __init__(self, name, schema, *, dont_import=False):
        self.name = name
        self.schema = schema # TODO: check for duplicate elements
        self.dont_import = dont_import
        self.column_names = [column_name for column_name in self.schema]
        self.source = None
        self.table = None

    def has_data(self):
        return self.source != None and \
            self.table != None and \
            self.table.num_rows

    def from_pyarrow(self, table):
        return self.__from_source(table, DataFromPyArrowTable() )

    def get_file_name(self, extension=default_parquet_extension):
        return f'{self.name}.{extension}'

    def get_file_path(self, bucket_path, override_file_name=None):
        file_name = override_file_name if override_file_name \
            else self.get_file_name()

        return path.join(bucket_path, file_name)

    def from_parquet(self, bucket_path, override_file_name=None):                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
        file_path = self.get_file_path(bucket_path)
        table = pq.read_table(file_path)
        return self.__from_source(table, DataFromParquetFile(file_path) )

    def __from_source(self, table, source):                                                                                                                                                                                                                                                                                                                                                                                                     
        self.table = table.select(self.column_names)
        self.__check_column_types()
        self.source = source
        return self

    @staticmethod
    def __format(element):
        """
        Returns a "NULL" string if was passed None to get the correct value in DB
        Escapes single quotes in values by adding a single quote (psql style) and encloses the result in single quotes
        """
        return "NULL" if element == None else "'" + str(element).replace("'", r"''") + "'"

    def to_sql_values(self):
        assert self.has_data()

        rows = self.table.to_pylist()
        rows_strs = []
        for row_number, row in enumerate(rows):
            row_str = "(" + ', '.join( [ f'{self.__format(element)}' for element in row.values() ] ) + ")"
            rows_strs.append(row_str)

        return ',\n'.join(rows_strs)

    def to_sql_request(self, db_schema=None):
        assert self.has_data()

        table_name = f'{db_schema}.{self.name}' if db_schema else self.name
        sql_request = "INSERT INTO " + table_name + "\n" + \
            "(" + ", ".join(self.column_names) + ")" + "\n" + \
            "VALUES\n" + \
            self.to_sql_values()

        return sql_request

    def __check_column_types(self):
        """
        Tests that actual table's columns types match the table schema
        """
        for column_name, sql_data_type_name in self.schema.items():
            column_type = self.table[column_name].type
            # print(f'\n{column_name} is {column_type}, class {column_type.__class__.__name__}')
            # print(dir(column_type))

            if not get_sql_data_type(sql_data_type_name).matches_pyarrow_type(column_type):
                msg = f'table "{self.name}": column "{column_name}" should be of type "{sql_data_type_name}", got "{column_type}"'
                self.__invalidate()
                raise TypeError(msg)

    def __invalidate(self):
        self.source = None
        self.table = None

    