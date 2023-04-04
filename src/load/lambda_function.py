import logging
import pandas as pd
import awswrangler as wr

logger = logging.getLogger('qq')
import sys
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def lambda_handler(a, b):
    bucket = "s3://de-01-2023-q2-prj-processed-20230403095204602800000006/test"
    path = f'{bucket}/dim_currency.parquet'
    df = wr.s3.read_parquet(path)

    logger.info( df.head() )

if __name__ == "__main__":
    lambda_handler({}, {})