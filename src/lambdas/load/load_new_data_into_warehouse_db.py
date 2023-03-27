from pg8000.native import Connection

from src.environ.warehouse_db import warehouse_db_user as user
from src.environ.warehouse_db import warehouse_db_password as passwd
from src.environ.warehouse_db import warehouse_db_host as host
from src.environ.warehouse_db import warehouse_db_port as port
from src.environ.warehouse_db import warehouse_db_database as db
from src.environ.warehouse_db import warehouse_db_schema as db_schema_name

from src.lambdas.load.db_schema import db_schema

def debug_print(msg):
    """
    Prints msg only if the module is run directly
    """
    if __name__ == "__main__":
        print(msg)

def load_new_data_into_warehouse_db(path):
    tables_ready_to_load = []

    for table in db_schema:
        if table.dont_import:
            debug_print( f'Don\'t import data to "{table.name}"' )
            continue
        debug_print( f'Reading data for "{table.name}" table from {path}..' )
        table.from_parquet(path)
        if table.has_data():
            debug_print( f'\tdata for "{table.name}" is ready to be loaded' )
            tables_ready_to_load.append(table)

    msg = "no new data to load"

    num_tables_to_load = len(tables_ready_to_load)
    if num_tables_to_load:
        with Connection(user, password=passwd, host=host, port=port, database=db) as connection:
            for table in tables_ready_to_load:
                assert table.has_data()
                request = table.to_sql_request(db_schema_name)
                debug_print("\nSQL request:")
                debug_print(request)
                connection.run(request)

        msg = f'{num_tables_to_load} table{"s" if num_tables_to_load > 1 else ""} loaded into "{db}" database (schema "{db_schema_name}")'

    debug_print(msg)

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