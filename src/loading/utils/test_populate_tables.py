from pg8000.native import Connection
import src.loading.populate_functions as populate_functions
from os import environ

user = environ.get('WAREHOUSE_DB_USER')
password = environ.get('WAREHOUSE_DB_PASSWORD')
host = environ.get('WAREHOUSE_DB_HOST', 'localhost')
port = environ.get('WAREHOUSE_DB_PORT', 5432)
database = environ.get('WAREHOUSE_DB_DATABASE')
#schema = environ.get('WAREHOUSE_DB_DATABASE_SCHEMA')

def _test_populate_independent_table(schema, table_name, input_data):
    with Connection(user, password=password, host=host, port=port, database=database) as connection:
        num_columns = len(input_data[0])
        num_rows = len(input_data)
        full_table_name = f'{schema}.{table_name}'
        
        # act
        connection.run(f'DELETE FROM {full_table_name}')
        populate_function = getattr(populate_functions, f'populate_{table_name}' )
        populate_function(connection, input_data, schema, table_name)
        rows = connection.run(f'SELECT * FROM {full_table_name}')

        # assert
        assert len(rows) == num_rows
        for row_index in range(num_rows):
            row = rows[row_index]
            assert len(row) == num_columns

            for column_index in range(num_columns):
                assert row[column_index] == input_data[row_index][column_index]