from src.ingestion.utils.utils import get_ingested_bucket_name
import pytest
from moto import mock_s3
import boto3
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


def test_get_ingested_bucket_name_function_gets_correct_bucket_name(bucket, s3):
    s3.create_bucket(
        Bucket='bucket-test-2'
    )
    s3.create_bucket(
        Bucket='bucket-test-3'
    )
    s3.create_bucket(
        Bucket='bucket-test-4'
    )
    assert get_ingested_bucket_name() == 's3-de-ingestion-query-queens-test-bucket'
