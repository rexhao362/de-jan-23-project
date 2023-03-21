from pg8000.native import Connection
from src.loading.populate_dim_currency import populate_dim_currency
from src.loading.config import (user, passwd, test_db)

def test_loads_one_row_when_passed_one_currency_item():
    with Connection(user, password=passwd, database=test_db) as connection:
        # arrange
        input_currency_data = [
            [1, "USD", "US dollar"]
        ]
        
        # act
        connection.run('DELETE FROM dim_currency')
        populate_dim_currency(connection, input_currency_data)
        rows = connection.run('SELECT * FROM dim_currency')

        # assert
        assert len(rows) == 1
        row = rows[0]
        columns = len(row)
        assert columns == 3

        for index in range(columns):
            assert row[index] == input_currency_data[0][index]

def test_loads_multiple_rows_when_passed_multiple_currency_item():
    with Connection(user, password=passwd, database=test_db) as connection:
        # arrange
        input_currency_data = [
            [1, "USD", "US dollar"],
            [5, "GBP", "pound"],
            [13, "JPY", "yen"]
        ]
        
        num_columns = len(input_currency_data[0])
        num_rows = len(input_currency_data)
        
        # act
        connection.run('DELETE FROM dim_currency')
        populate_dim_currency(connection, input_currency_data)
        rows = connection.run('SELECT * FROM dim_currency')

        # assert
        assert len(rows) == num_rows
        for row_index in range(num_rows):
            row = rows[row_index]
            assert len(row) == num_columns

            for column_index in range(num_columns):
                assert row[column_index] == input_currency_data[row_index][column_index]
