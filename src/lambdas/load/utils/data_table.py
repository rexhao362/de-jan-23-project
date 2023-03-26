import os
import pyarrow.parquet as pq
from src.lambdas.load.utils.data_table_source import \
    DataFromPyArrowTable, DataFromParquetFile
from src.lambdas.load.utils.sql_data_type import get_sql_data_type

default_parquet_extension = ".parquet"

class DataTable:
    def __init__(self, name, table_format):
        self.name = name
        self.format = table_format
        self.column_names = [column_name for column_name in self.format]
        self.source = None
        self.table = None

    def has_data(self):
        return self.source != None and \
            self.table != None and \
            self.table.num_rows

    def from_pyarrow(self, table):
        self.__from_source(table, DataFromPyArrowTable() )

    def from_parquet(self, path, override_file_name=None):
        file_name = override_file_name if override_file_name \
            else f'{self.name}{default_parquet_extension}'

        full_path = os.path.join(path, file_name)
        table = pq.read_table(full_path)
        self.__from_source(table, DataFromParquetFile(full_path) )

    def __from_source(self, table, source):
        self.table = table.select(self.column_names)
        self.__check_column_types()
        self.source = source

    def to_sql_values(self):
        assert self.has_data()

        rows = self.table.to_pylist()
        rows_strs = []
        for row_number, row in enumerate(rows):
            row_str = "(" + ', '.join( [ f"'{element}'" for element in row.values() ] ) + ")"
            rows_strs.append(row_str)

        return ',\n'.join(rows_strs)

    def prepare_sql_request(self, schema=None):
        assert self.has_data()

        table_name = f'{schema}.{self.name}' if schema else self.name
        sql_request = "INSERT INTO " + table_name + "\n" + \
            "(" + ", ".join(self.column_names) + ")" + "\n" + \
            "VALUES\n" + \
            self.to_sql_values()

        return sql_request

    def __check_column_types(self):
        """
        Tests that actual table's columns types match the table format
        """
        for column_name, sql_data_type_name in self.format.items():
            column_type = self.table[column_name].type

            if not get_sql_data_type(sql_data_type_name).matches_pyarrow_type(column_type):
                msg = f'table "{self.name}": column "{column_name}" should be of type "{sql_data_type_name}", got "{column_type}"'
                self.__invalidate()
                raise TypeError(msg)

    def __invalidate(self):
        self.source = None
        self.table = None

    