from os import remove
import logging
from pg8000.native import Connection
from src.utils.secrets_manager import secrets_manager
from src.utils.environ import is_production_environ
from src.load.db_schema import mvp_database_schema as default_database_schema

logger = logging.getLogger("DE_Q2_LOAD")
if not is_production_environ():
    logger.setLevel(logging.INFO)

class _ProcessedDataLoader:
    def __init__(self, db_schema=default_database_schema):
        logger.debug( f'_ProcessedDataLoader.__init__()' )
        self.db_config = secrets_manager.get_secret_warehouse_db_config()
        self.db_schema = db_schema
        self._processed_bucket_path = None

    def set_processed_bucket_path(self, processed_bucket_path=None):
        """
        Stores the last path to processed data passed to run()
        """
        logger.debug( f'set_processed_bucket_path({processed_bucket_path})' )
        self._processed_bucket_path = processed_bucket_path

    def get_processed_bucket_path():
        """
        Stores the last path to processed data passed to run()
        """
        logger.debug( f'get_processed_bucket_path()={self._processed_bucket_path}' )
        return self._processed_bucket_path

    def run(self, processed_bucket_path):
        """
        Reads data from .parquet file(s) according to the db_schema and
        loads it into the database
        """
        logger.debug( f'run({processed_bucket_path})' )
        self.set_processed_bucket_path(processed_bucket_path)
        tables_with_new_data = self.__from_parquet(processed_bucket_path)
        num_tables_with_new_data = len(tables_with_new_data)
        msg = f'no new data to load'

        if num_tables_with_new_data:
            self.__to_db(tables_with_new_data)
            msg = f'database {self.db_config["credentials"]["database"]} (schema {self.db_config["schema"]}): new data loaded into {num_tables_with_new_data} table{"s" if num_tables_with_new_data > 1 else ""}'

        logger.info(msg)

    def __from_parquet(self, processed_bucket_path):
        logger.debug( f'__from_parquet({processed_bucket_path})' )
        tables_ready_to_load = []

        for table in self.db_schema:
            prefix = f'table {table.name}:'
            if table.dont_import:
                logger.info( f'{prefix} data import is disabled by its schema, ignoring' )
                continue
            logger.info( f'{prefix} importing data from {table.get_file_path(processed_bucket_path)} ..' )
            table.from_parquet(processed_bucket_path)
            if table.has_data():
                logger.info( f'\t{table.table.num_rows} row(s) imported' )
                tables_ready_to_load.append(table)

        return tables_ready_to_load

    def __to_db(self, tables):
        logger.debug( f'__to_db()' )
        with Connection( **self.db_config["credentials"] ) as connection:
            for table in tables:
                prefix = f'table { self.db_config["schema"] }.{ table.name }:'
                request = table.to_sql_request( self.db_config["schema"] )
                logger.debug("run SQL query:")
                logger.debug(request)
                logger.info( f'{prefix} inserting values ..' )
                connection.run(request)
                msg = f'\t{table.table.num_rows} row(s) inserted'
                logger.info(msg)

    def cleanup(self):
        logger.debug( f'cleanup()' )
        self.delete_processed_files()

    def delete_processed_files(self):
        logger.debug( f'delete_processed_files()' )
        if is_production_environ():
            processed_bucket_path = self.get_processed_bucket_path()
            # delete all files in this bucket
            raise NotImplementedError
        
        # delete all files in 
        for table in self.db_schema:
            if table.has_data():
                file_path = table.source.path
                logger.info( f'removing { table.source.path } ..')
                # don't want to break if something goes wrong with rm
                try:
                    remove(file_path)
                except BaseException as exc:
                    msg = ''.join( traceback.format_tb(exc.__traceback__, traceback_depth_limit) ) + str(exc)
                    logger.warning(msg)

        self.set_processed_bucket_path()

processed_data_loader = _ProcessedDataLoader()

def lambda_handler(event, context):
    import logging
    from src.utils.environ import is_production_environ
    import src.utils.path as path
    from src.load.processed_data_loader import processed_data_loader

    logger = logging.getLogger('DE_Q2_LOAD')
    logger.setLevel(logging.INFO)
    if not is_production_environ():
        # this reroutes all logging to console in dev environment, useful for debugging
        import sys
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    logger.info('started')

    s3_data_path = "s3://data" if is_production_environ() else "./local/aws/s3"
    processed_bucket_name = "processed"
    processed_bucket_path = path.join(s3_data_path, processed_bucket_name)

    try:
        processed_data_loader.run(processed_bucket_path)

    except BaseException as exc:
        import traceback
        logger.critical( f'{exc.__class__.__name__} exception raised' )

        traceback_depth_limit = 1 if is_production_environ() else None # to get more info in dev env
        msg = ''.join( traceback.format_tb(exc.__traceback__, traceback_depth_limit) ) + str(exc)
        logger.critical(msg)
        exit(1)

    finally:
        processed_data_loader.cleanup()
        logger.info('completed')