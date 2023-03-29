from os import path
import logging
from src.utils.environ import is_production_environ
from src.lambdas.ingestion.ingestion import data_ingestion
from src.lambdas.process.process import main_local
from src.lambdas.load.load_new_data_into_warehouse_db import load_new_data_into_warehouse_db

logger = logging.getLogger('DE_Q2_MAIN') # change it in your lambda!
logger.setLevel(logging.ERROR)

if not is_production_environ():
    # this reroutes all logging to console in dev environment, useful for debugging
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

local_bucket_path = "local/aws/s3"

try:
    data_ingestion(local_bucket_path)
    main_local(local_bucket_path)
    load_new_data_into_warehouse_db(local_bucket_path)

except BaseException as exc:
    import traceback
    logger.critical( f'{exc.__class__.__name__} exception raised' )

    traceback_depth_limit = 1 if is_production_environ() else None # to get more info in dev env
    msg = ''.join( traceback.format_tb(exc.__traceback__, traceback_depth_limit) ) + str(exc)
    logger.critical(msg)
    exit(1)

finally:
    pass
    # clean up the process files
    # cleanup()