from pg8000.native import Connection

from environ.warehouse_db import warehouse_db_user as user
from environ.warehouse_db import warehouse_db_password as passwd
from environ.warehouse_db import warehouse_db_host as host
from environ.warehouse_db import warehouse_db_port as port
from environ.warehouse_db import warehouse_db_database as db
from environ.warehouse_db import warehouse_db_schema as db_schema

import load.populate_functions as populate_functions
from gutils.db.make_schema_table_name import make_schema_table_name

def _test_populate_independent_table(table_name, input_data):
    with Connection(user, password=passwd, host=host, port=port, database=db) as connection:
        num_rows = len(input_data)
        num_columns = len(input_data[0]) if num_rows else 0
        full_table_name = make_schema_table_name(db_schema, table_name)
        
        # act
        connection.run(f'DELETE FROM {full_table_name}')
        populate_function = getattr(populate_functions, f'populate_{table_name}' )
        populate_function(connection, input_data, db_schema, table_name)
        rows = connection.run(f'SELECT * FROM {full_table_name}')

        # assert
        assert len(rows) == num_rows
        for row_index in range(num_rows):
            row = rows[row_index]
            assert len(row) == num_columns

            for column_index in range(num_columns):
                assert row[column_index] == input_data[row_index][column_index]