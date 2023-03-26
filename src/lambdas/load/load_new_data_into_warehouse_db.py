from pg8000.native import Connection
from src.environ.warehouse_db import env_warehouse_db as db_env
from src.lambdas.load.data_table import DataTable

data_tables = [
    DataTable(
        "dim_currency", {
            "currency_id": "INT",
            "currency_code": "VARCHAR",
            "currency_name": "VARCHAR"
        }
    )
]

def load_new_data_into_warehouse_db(data_path):
    new_tables_count = 0

    for data_table in data_tables:
        #print(f'Reading data for "{data_table.name}" table from {data_path}..')
        data_table.from_parquet(data_path)
        if data_table.has_data():
            new_tables_count += 1

    msg = "no new data to load"

    if new_tables_count:
        user = db_env["user"]
        passwd = db_env["password"]
        host = db_env["host"]
        port = db_env["port"]
        database = db_env["database"]
        schema = db_env["schema"]

        with Connection(user, password=passwd, host=host, port=port, database=database) as connection:
            for data_table in data_tables:
                if data_table.has_data():
                    request = data_table.prepare_sql_request(schema)
                    res = connection.run(request)

        msg = f'{new_tables_count} table(s) loaded into the warehouse db'

    #print(msg)

if __name__ == "__main__":
    test_path = "local/aws/s3/processed"

    try:
        load_new_data_into_warehouse_db(test_path)

    except Exception as exc:
        msg = f"\nError: {exc}"
        exit(msg)  # replace with log() to CloudWatch

    finally:
        pass
        # clean up the process files
        #cleanup()