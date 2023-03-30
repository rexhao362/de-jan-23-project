import json
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
def s3_s(aws_credentials):
    with mock_s3():
        yield boto3.client('s3')


@pytest.fixture(scope='function')
def bucket(s3_s):
    s3_s.create_bucket(
        Bucket='s3-de-ingestion-query-queens-test-bucket'
    )


def test_store_last_updated_stores_last_updated(bucket, s3_s):
    dt = datetime(2012, 1, 14, 12, 00, 1, 000000)
    store_last_updated(dt)
    response = s3_s.list_objects_v2(
        Bucket=get_ingested_bucket_name(),
        Prefix='date/'
    )
    list_of_files = [item['Key'] for item in response['Contents']]
    assert ['date/last_updated.json'] == list_of_files
    result = retrieve_last_updated()
    assert result > dt


def test_store_last_updated_copies_previous_update(bucket, s3_s):
    test_date = {"last_updated": "2000-11-03T14:20:49.962000"}
    s3_s.put_object(
        Body=json.dumps(test_date),
        Bucket=get_ingested_bucket_name(),
        Key='date/last_updated.json'
    )

    dt = datetime(2012, 1, 14, 12, 00, 1, 000000)
    store_last_updated(dt)
    response = s3_s.list_objects_v2(
        Bucket=get_ingested_bucket_name(),
        Prefix='date/'
        )
    list_of_files = [item['Key'] for item in response['Contents']]
    assert ['date/date_2.json', 'date/last_updated.json'] == list_of_files
    result = retrieve_last_updated()
    assert result > dt

    res = s3_s.get_object(
        Bucket=get_ingested_bucket_name(),
        Key='date/date_2.json'
        )
    json_res = json.loads(res['Body'].read())
    timestamp = json_res['last_updated']
    assert timestamp == "2000-11-03T14:20:49.962000"

def test_returns_string_format_of_timestamp(bucket, s3_s):
    dt = datetime(2012, 1, 14, 12, 00, 1, 000000)
    store_last_updated(dt)
    ts = retrieve_last_updated().strftime('%Y-%m-%dT%H:%M:%S.%f')
    assert store_last_updated(dt) == f'{ts[:10]}/{ts[11:19]}'
