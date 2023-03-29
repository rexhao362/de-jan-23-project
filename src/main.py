from os import path
import logging
from src.lambdas.ingestion.ingestion import data_ingestion
from src.lambdas.process.process import main_local
from src.lambdas.load.load_new_data_into_warehouse_db import load_new_data_into_warehouse_db

logger = logging.getLogger('main')
logger.setLevel(logging.INFO)

# debug only, remove in production
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
    # TODO: this one maybe for production
    #msg = ''.join( traceback.format_tb(exc.__traceback__, 1) ) + str(exc)
    msg = ''.join( traceback.format_tb(exc.__traceback__) ) + str(exc)
    logger.critical(msg)
    exit(1)

finally:
    pass
    # clean up the process files
    # cleanup()