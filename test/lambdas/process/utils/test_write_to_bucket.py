import pytest
from moto import mock_s3
from moto.core import patch_client
import os
import boto3
# from src.lambdas.process.utils import write_to_bucket
import pandas as pd


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'

@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-1')

@pytest.fixture
def bucket(s3):
    s3.create_bucket(
        Bucket='test_bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

    # write_to_bucket('test_bucket', dataframe, 'TEST_1', '6666-66-66/66-66-66')
    # write_to_bucket('test_bucket', dataframe, 'TEST_FOO', '6666-66-66/66-66-66')

# def test_uploads_files(bucket, s3):
#     patch_client(s3)
#     result = s3.list_objects_v2(Bucket='test_bucket')
#     for item in result['Contents']:
#         print(item)

# def test_returns_dictionary():
#     dataframe = pd.read_parquet('test/lambdas/process/parquets/dim_currency.parquet')
#     result = write_to_bucket('test_bucket', dataframe, 'TEST_1', '6666-66-66/66-66-66')
#     assert type(result) == dict

