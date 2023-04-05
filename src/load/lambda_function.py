import logging
import boto3
import pandas as pd
import awswrangler as wr

logger = logging.getLogger('qq')
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def load_processed_data(a, b):

    bucket = "de-01-2023-q2-prj-processed-20230403095204602800000006"
    path = f's3://{bucket}/test/dim_currency.parquet'

    s3 = boto3.client('s3')
    list_buckets = s3.list_buckets()
    bucket_prefix = 'de-01-2023-q2-prj-processed'
    bucket_name = ''
    for bucket in list_buckets['Buckets']:
        logger.info(bucket)
        if bucket['Name'].startswith(bucket_prefix):
            logger.info( f'FOUND: - {bucket["Name"]}' )
            bucket_name = bucket['Name']



    df = wr.s3.read_parquet(path)
    logger.info( df.head() )
    # logger.info("Hello   there!")

if __name__ == "__main__":
    load_processed_data({}, {})