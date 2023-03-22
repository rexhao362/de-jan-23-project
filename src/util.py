from pg8000.native import Connection
from src.loading.config import (user, passwd, test_db)
import src.loading.populate_functions as populate_functions

def _test_populate_independent_table(table_name, input_data):
    with Connection(user, password=passwd, database=test_db) as connection:
        num_columns = len(input_data[0])
        num_rows = len(input_data)
        
        # act
        connection.run(f'DELETE FROM {table_name}')
        populate_function = getattr(populate_functions, f'populate_{table_name}' )
        populate_function(connection, input_data, table_name)
        rows = connection.run(f'SELECT * FROM {table_name}')

        # assert
        assert len(rows) == num_rows
        for row_index in range(num_rows):
            row = rows[row_index]
            assert len(row) == num_columns

            for column_index in range(num_columns):
                assert row[column_index] == input_data[row_index][column_index]