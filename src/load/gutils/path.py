import logging
from gutils.environ import is_production_environ

logger = logging.getLogger("DE_Q2_LOAD")
logger.setLevel(logging.INFO)

def join(data_path, file_name):
    if is_production_environ():
        return f'{data_path}/{file_name}'
    else:
        from os import path
        return path.join(data_path, file_name)

def get_bucket_path(default_local_path, bucket_label):
    if is_production_environ():
        import boto3
        try:
            s3 = boto3.client('s3')
            buckets = s3.list_buckets()
            matched_bucket_name = ''
            for bucket in buckets['Buckets']:
                bucket_name = bucket['Name']
                if bucket_name.find(bucket_label) != -1:
                    matched_bucket_name = bucket_name
                    break

            raise Exception("")

        except Exception as e:
            logging.error(e, f'{bucket_label} not found')
    else:
        return path.join(default_local_path, bucket_label)