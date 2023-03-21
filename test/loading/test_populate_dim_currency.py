from pg8000.native import Connection
from src.loading.populate_dim_currency import populate_dim_currency
from src.loading.config import (user, passwd, db)

def test_loads_one_row_when_passed_one_currency_item():
    with Connection(user, password=passwd, database=db) as connection:
        input_currency_data = [
            [1, "USD", "US dollar"]
        ]
        populate_dim_currency(connection, input_currency_data)
        rows = connection.run('SELECT * FROM dim_currency')
        assert len(rows) == 1
        row = rows[0]
        assert len(row) == 3
        assert row[0] == input_currency_data[0][0]
        assert row[1] == input_currency_data[0][1]
        assert row[2] == input_currency_data[0][2]

# def test_makes_no_chages_to_db_when_passed_empty_data_structure():
#     with Connection(user, password=passwd, database=db) as connection:
#         input_currency_data = []
#         populate_dim_currency(connection, input_currency_data)
#         rows = connection.run('SELECT * FROM dim_currency')
#         assert rows == []

