import logging
# these 2 should be moved to main
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# debug only, remove these 2 lines from production code
import sys
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


from pg8000.native import Connection
from src.lambdas.load.db_schema import db_schema as default_db_schema

# import credentials
from src.environ.warehouse_db import warehouse_db_user as user
from src.environ.warehouse_db import warehouse_db_password as passwd
from src.environ.warehouse_db import warehouse_db_host as host
from src.environ.warehouse_db import warehouse_db_port as port
from src.environ.warehouse_db import warehouse_db_database as db
from src.environ.warehouse_db import warehouse_db_schema as db_schema_name



class DataLoader:
    def __init__(self, db_schema=default_db_schema):
        self.db_schema = db_schema

    def run(self, data_path):
        """
        Reads data from .parquet file(s) according to the db_schema and
        loads it into the database
        """
        tables_ready_to_load = self.__from_parquet(data_path)
        num_tables_to_load = len(tables_ready_to_load)
        msg = f'no new data to load into "{db}" database'

        if num_tables_to_load:
            self.__to_db(tables_ready_to_load)
            msg = f'{num_tables_to_load} table{"s" if num_tables_to_load > 1 else ""} loaded into "{db}" database (schema "{db_schema_name}")'

        logger.info(msg)

    def __from_parquet(self, data_path):
        tables_ready_to_load = []

        for table in self.db_schema:
            if table.dont_import:
                logger.info( f'don\'t import data to "{table.name}"' )
                continue
            logger.info( f'reading data for "{table.name}" table from {data_path}..' )
            table.from_parquet(data_path)
            if table.has_data():
                logger.info( f'data for "{table.name}" is ready' )
                tables_ready_to_load.append(table)

        return tables_ready_to_load

    def __to_db(self, tables):
        print("__to_db")
        with Connection(user, password=passwd, host=host, port=port, database=db) as connection:
            for table in tables:
                assert table.has_data()
                request = table.to_sql_request(db_schema_name)
                connection.run(request)


if __name__ == "__main__":
    test_path = "local/aws/s3/processed"

    try:
        msg = DataLoader(test_path).run()
        logger.info(msg)

    except Exception as exc:
        logger.critical(exc)
        exit(1)

    finally:
        pass
        # cleanup(test_path)