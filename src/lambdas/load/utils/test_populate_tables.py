from pg8000.native import Connection
from src.environ.warehouse_db import env_warehouse_db as env
import src.lambdas.load.populate_functions as populate_functions
from src.utils.db.make_schema_table_name import make_schema_table_name

def _test_populate_independent_table(table_name, input_data):
    with Connection(env["user"], password=env["password"], host=env["host"], port=env["port"], database=env["database"]) as connection:
        num_columns = len(input_data[0])
        num_rows = len(input_data)
        full_table_name = make_schema_table_name( env["schema"], table_name)
        
        # act
        connection.run(f'DELETE FROM {full_table_name}')
        populate_function = getattr(populate_functions, f'populate_{table_name}' )
        populate_function(connection, input_data, env["schema"], table_name)
        rows = connection.run(f'SELECT * FROM {full_table_name}')

        # assert
        assert len(rows) == num_rows
        for row_index in range(num_rows):
            row = rows[row_index]
            assert len(row) == num_columns

            for column_index in range(num_columns):
                assert row[column_index] == input_data[row_index][column_index]