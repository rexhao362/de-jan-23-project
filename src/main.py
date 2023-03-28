import logging
from src.lambdas.ingestion.ingestion import data_ingestion

logger = logging.getLogger('main')
logger.setLevel(logging.INFO)

try:
    data_ingestion()
    process()
    load_new_data_into_warehouse_db(test_path)

except Exception as exc:
    logger.error(exc)
    exit(1)

finally:
    pass
    # clean up the process files
    #cleanup()