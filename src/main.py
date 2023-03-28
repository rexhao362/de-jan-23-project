from os import path
import logging
from src.lambdas.ingestion.ingestion import data_ingestion
from src.lambdas.process.process import main_local

logger = logging.getLogger('main')
logger.setLevel(logging.INFO)

# debug only, remove in production
import sys
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

local_bucket_path = "local/aws/s3"

try:
    data_ingestion(local_bucket_path)
    main_local(local_bucket_path)
    load_new_data_into_warehouse_db(local_bucket_path)

except Exception as exc:
    print("Exception:", exc)
    logger.error(exc)
    exit(1)

finally:
    pass
    # clean up the process files
    #cleanup()