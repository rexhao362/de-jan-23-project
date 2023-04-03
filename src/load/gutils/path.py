import logging
from os import path
from gutils.environ import is_production_environ

logger = logging.getLogger("DE_Q2_LOAD")
logger.setLevel(logging.INFO)

def join(data_path, file_name):
    if is_production_environ():
        return f'{data_path}/{file_name}'
    else:
        from os import path
        return path.join(data_path, file_name)

def get_bucket_path(bucket_label, dev_local_root_s3_path=None):
    """
    Returns bucket path for both production and dev environments

    bucket_label: a substring in production mode and exact folder name in dev
    default_local_path: a string representing the root of simulated S3 in dev mode

    Raises
        Exception if bucket not found (production) or no root path supplied
    """
    if is_production_environ():
        import boto3
        try:
            s3 = boto3.client('s3')            
            buckets = s3.list_buckets()

            for bucket in buckets['Buckets']:
                bucket_name = bucket['Name']
                if bucket_name.find(bucket_label) != -1:
                    return f's3://{ bucket_name }'

            raise Exception( f'{bucket_label} not found' )

        except BaseException as exc:
            msg = str(exc)
            logging.error(msg)
            raise Exception(msg)
    else:
        if dev_local_root_s3_path is None:
            raise Exception( f'dev_local_root_s3_path cannot be None in dev environment' )
        return path.join(dev_local_root_s3_path, bucket_label)