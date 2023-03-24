import boto3
from moto import mock_s3
from moto.core import patch_client
from pandas import read_parquet
import pytest
import os

dataframe1 = read_parquet('test/lambdas/process/parquets/dim_currency.parquet')
dataframe2 = read_parquet('test/lambdas/process/parquets/dim_currency_formatted.parquet')
bucket_name = "test_bucket"


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture(scope="module")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")

def test_create_bucket(s3):
    s3.create_bucket(Bucket="test_bucket")
    result = s3.list_buckets()
    assert len(result["Buckets"]) == 1
    assert result["Buckets"][0]["Name"] == bucket_name

def test_bucket_exists_through_tests(s3):
    result = s3.list_buckets()
    assert len(result["Buckets"]) == 1
    assert result["Buckets"][0]["Name"] == bucket_name

def test_write_to_s3_returns_dict(s3):
    from src.lambdas.process.utils import (write_to_bucket)
    obj_key_1 = 'test/test_1'
    upload = write_to_bucket(bucket_name, dataframe1, obj_key_1)
    assert type(upload) == dict 

def test_returns_status_code_200_with_successful_write(s3):
    pass
def test_returns_status_code_404_with_unsuccessful_write(s3):
    pass
def test_key_is_maintained_in_bucket(s3):
    pass
def test_file_is_still_valid_once_written_to_bucket(s3):
    pass

    # download = load_file_from_s3(bucket_name,f"{obj_key}.parquet")
    # print(download)

    # result = s3.list_objects_v2(Bucket='test_bucket')
    # def test_uploads_files(bucket, s3):
#     result = s3.list_objects_v2(Bucket='test_bucket')
#     for item in result['Contents']:
#         print(item)
    


