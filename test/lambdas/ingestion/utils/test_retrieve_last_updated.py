from ingestion.utils import get_ingested_bucket_name
from ingestion.dates import retrieve_last_updated
import pytest
from moto import mock_s3
import boto3
from datetime import datetime
import os.path
import os
import json


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


def test_retrieve_last_updated_returns_default_date_if_last_updated_file_not_in_bucket(bucket, s3):
    response = s3.list_objects_v2(Bucket=get_ingested_bucket_name(), Prefix='date/')
    assert 'Contents' not in response
    result = retrieve_last_updated()
    assert result == None


def test_retrieve_last_updated_function_retrieves_last_updated(bucket, s3):
    test_date = {"last_updated": "2000-11-03T14:20:49.962000"}

    s3.put_object(
        Body=json.dumps(test_date),
        Bucket=get_ingested_bucket_name(),
        Key='date/date.json'
        )
    assert retrieve_last_updated() == datetime(2000, 11, 3, 14, 20, 49, 962000)

