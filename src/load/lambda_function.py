import logging
import pandas as pd
import awswrangler as wr

logger = logging.getLogger('qq')
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def load_processed_data(a, b):
    # bucket = "s3://de-01-2023-q2-prj-processed-20230403095204602800000006/test"
    # path = f'{bucket}/dim_currency.parquet'
    # df = wr.s3.read_parquet(path)

    # logger.info( df.head() )
    logger.info("Hello   there!")

if __name__ == "__main__":
    load_processed_data({}, {})