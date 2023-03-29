from os.path import join
import logging
from pg8000.native import Connection
from src.utils.secrets_manager import secrets_manager
from src.lambdas.load.db_schema import db_schema

# from src.environ.warehouse_db import warehouse_db_user as user
# from src.environ.warehouse_db import warehouse_db_password as passwd
# from src.environ.warehouse_db import warehouse_db_host as host
# from src.environ.warehouse_db import warehouse_db_port as port
# from src.environ.warehouse_db import warehouse_db_database as db
# from src.environ.warehouse_db import warehouse_db_schema as db_schema_name

default_host = 'localhost'
default_port = 5432

user = secrets_manager.get_secret_value('WAREHOUSE_DB_USER')
passwd = secrets_manager.get_secret_value('WAREHOUSE_DB_PASSWORD')
host = secrets_manager.get_secret_value('WAREHOUSE_DB_HOST', default_host)
port = secrets_manager.get_secret_int_value('WAREHOUSE_DB_PORT', default_port)
db = secrets_manager.get_secret_value('WAREHOUSE_DB_DATABASE')
db_schema_name = secrets_manager.get_secret_value('WAREHOUSE_DB_DATABASE_SCHEMA')

logger = logging.getLogger(__name__)

def load_new_data_into_warehouse_db(path):
    path = join(path, "processed")  # TODO: use global/config variable
    tables_ready_to_load = []

    for table in db_schema:
        if table.dont_import:
            logger.info( f'don\'t import data to "{table.name}"' )
            continue
        logger.info( f'reading data for "{table.name}" table from {path}..' )
        table.from_parquet(path)
        if table.has_data():
            logger.info( f'data for "{table.name}" is ready' )
            tables_ready_to_load.append(table)

    msg = "no new data to load"

    num_tables_to_load = len(tables_ready_to_load)
    if num_tables_to_load:
        with Connection(user, password=passwd, host=host, port=port, database=db) as connection:
            for table in tables_ready_to_load:
                assert table.has_data()
                request = table.to_sql_request(db_schema_name)
                logger.debug("run SQL query:")
                logger.debug(request)
                response = connection.run(request)
                # TODO: check response
                msg = f'table {table.name}: {table.table.num_rows} row(s) loaded to the warehouse DB'
                logger.info(msg)

        msg = f'{num_tables_to_load} table{"s" if num_tables_to_load > 1 else ""} loaded into "{db}" database (schema "{db_schema_name}")'

    logger.info(msg)