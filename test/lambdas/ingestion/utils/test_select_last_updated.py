import os
import pytest
from src.ingestion.dates import retrieve_last_updated, select_last_updated, store_last_updated
from moto import mock_s3
import boto3


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

@pytest.fixture(scope='module')
def bucket(s3):
    s3.create_bucket(
        Bucket='s3-de-ingestion-query-queens-test-bucket'
    )



def test_returns_date_string_format_of_current_time(bucket, s3):
    result = select_last_updated(None)
    store_last_updated(result, '')
    timestamp = retrieve_last_updated().strftime('%Y-%m-%dT%H:%M:%S.%f')
    assert result == timestamp

