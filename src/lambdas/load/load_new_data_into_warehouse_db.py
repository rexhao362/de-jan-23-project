from os.path import join
import logging
from pg8000.native import Connection
from src.utils.secrets_manager import secrets_manager
from src.utils.environ import is_dev_environ
from src.lambdas.load.db_schema import db_schema

logger = logging.getLogger("DE_Q2_LOAD")
if is_dev_environ():
    logger.setLevel(logging.INFO)

def load_new_data_into_warehouse_db(path):
    path = join(path, "processed")  # TODO: use global/config variable
    tables_ready_to_load = []

    for table in db_schema:
        prefix = f'table {table.name}:'
        if table.dont_import:
            logger.info( f'{prefix} data import is disabled by its schema, ignoring' )
            continue
        logger.info( f'{prefix} importing data from {path} ..' )
        table.from_parquet(path)
        if table.has_data():
            logger.info( f'\t{table.table.num_rows} row(s) imported' )
            tables_ready_to_load.append(table)

    msg = "no new data to load"

    num_tables_to_load = len(tables_ready_to_load)
    if num_tables_to_load:
        warehouse_db_credentials = {
            "user": secrets_manager.get_secret_value('WAREHOUSE_DB_USER'),
            "password": secrets_manager.get_secret_value('WAREHOUSE_DB_PASSWORD'),
            "host": secrets_manager.get_secret_value('WAREHOUSE_DB_HOST'),
            "port": secrets_manager.get_secret_int_value('WAREHOUSE_DB_PORT', 5432),
            "database": secrets_manager.get_secret_value('WAREHOUSE_DB_DATABASE'),
            }
        database_schema = secrets_manager.get_secret_value('WAREHOUSE_DB_DATABASE_SCHEMA')

        with Connection(**warehouse_db_credentials) as connection:
            for table in tables_ready_to_load:
                prefix = f'table {database_schema}.{table.name}:'

                request = table.to_sql_request(database_schema)
                logger.debug("run SQL query:")
                logger.debug(request)
                logger.info( f'{prefix} inserting values ..' )
                connection.run(request)
                msg = f'\t{table.table.num_rows} row(s) inserted'
                logger.info(msg)

        db = warehouse_db_credentials["database"]
        msg = f'database {db} (schema {database_schema}): new data loaded into {num_tables_to_load} table{"s" if num_tables_to_load > 1 else ""}'

    logger.info(msg)