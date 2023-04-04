import logging
from src.utils.environ import is_production_environ
import src.utils.path as path
from src.ingestion.ingestion import data_ingestion
from src.process.process import main_local
from src.load.processed_data_loader import processed_data_loader

logger = logging.getLogger('DE_Q2_MAIN') # change it in your lambda!
logger.setLevel(logging.INFO)
if not is_production_environ():
    # this reroutes all logging to console in dev environment, useful for debugging
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

logger.info('started')

s3_data_path = "s3://data" if is_production_environ() else "./local/aws/s3"
ingestion_bucket_name = "ingestion"
processed_bucket_name = "processed"
processed_bucket_path = path.join(s3_data_path, processed_bucket_name)

try:
    data_ingestion(s3_data_path)
    main_local(path=s3_data_path)
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