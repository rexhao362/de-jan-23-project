from src.lambdas.process.utils import ( load_file_from_s3, write_to_bucket)
import pytest
from moto import mock_s3
import boto3
import os
from pandas import read_parquet
from moto.core import patch_client


parquet = 'test/lambdas/process/parquets/dim_currency.parquet'

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
        yield boto3.client('s3', region_name='us-east-1')

@pytest.fixture
def bucket(s3):
    s3.create_bucket(
        Bucket='test_bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )


# @pytest.mark.skip
#TODO
# def test_load_file_from_s3_returns_dict(s3):
#     bucket_name = 'test_bucket'
#     key = '2020-02-19/24-45-45/test_1'
#     write_to_bucket(bucket_name, read_parquet(parquet), key)
#     result = s3.list_objects_v2(Bucket='test_bucket')
#     for item in result['Contents']:
#         print(item)
#     # response = load_file_from_s3(bucket_name, f"{key}.parquet")
#     # print(response)
#     assert True


# def test_uploads_files(bucket, s3):
#     result = s3.list_objects_v2(Bucket='test_bucket')
#     for item in result['Contents']:
#         print(item)