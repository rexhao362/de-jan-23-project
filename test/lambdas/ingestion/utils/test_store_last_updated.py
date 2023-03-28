import json
from src.lambdas.ingestion.utils.utils import get_table_names
from src.lambdas.ingestion.utils.utils import retrieve_last_updated
from src.lambdas.ingestion.utils.utils import store_last_updated
from src.lambdas.ingestion.utils.utils import get_ingested_bucket_name
from moto import mock_s3
import boto3
from datetime import datetime
import pytest
import os.path
import os


# Mocking AWS credentials
@pytest.fixture(scope='module')
def aws_credentials():

    '''Mocked AWS credentials for moto.'''

    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope='module')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3')

@pytest.fixture(scope='function')
def bucket(s3):
    s3.create_bucket(
        Bucket='s3-de-ingestion-query-queens-test-bucket'
    )


def test_store_last_updated_stores_last_updated(bucket, s3):
    test_path = "./local/aws/s3/ingestion"
    # with open(f'{test_path}/date/last_updated.json', 'w') as f:
    #     test_date = {"last_updated": "2000-11-03T14:20:49.962000"}
    #     f.write(json.dumps(test_date))
    # with open(f'{test_path}/date/last_updated.json', 'rb') as f:
    #     s3.put_object(Body=f, Bucket=get_ingested_bucket_name(),
    #                   Key='date/date_1.json')
        
    dt = datetime(2012, 1, 14, 12, 00, 1, 000000)
    store_last_updated(dt, test_path)
    response = s3.list_objects_v2(Bucket=get_ingested_bucket_name(), Prefix='date/')
    list_of_files = [item['Key'] for item in response['Contents']]
    assert ['date/date_1.json'] == list_of_files
    result = retrieve_last_updated()
    assert result > dt


def test_store_last_updated_copies_previous_update(bucket, s3):
    test_path = "./local/aws/s3/ingestion"
    with open(f'{test_path}/date/last_updated.json', 'w') as f:
        test_date = {"last_updated": "2000-11-03T14:20:49.962000"}
        f.write(json.dumps(test_date))
    with open(f'{test_path}/date/last_updated.json', 'rb') as f:
        s3.put_object(Body=f, Bucket=get_ingested_bucket_name(),
                      Key='date/date_1.json')
        
    dt = datetime(2012, 1, 14, 12, 00, 1, 000000)
    store_last_updated(dt, test_path)
    response = s3.list_objects_v2(Bucket=get_ingested_bucket_name(), Prefix='date/')
    list_of_files = [item['Key'] for item in response['Contents']]
    assert ['date/date_1.json','date/date_2.json'] == list_of_files
    result = retrieve_last_updated()
    assert result > dt

    res = s3.get_object(
            Bucket=get_ingested_bucket_name(), Key='date/date_2.json')
    json_res = json.loads(res['Body'].read())
    timestamp = json_res['last_updated']
    assert timestamp == "2000-11-03T14:20:49.962000"