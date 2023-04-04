import sys
sys.path.append('./src/')
import boto3
from moto import mock_s3
from pandas import read_parquet
import pytest
import os
import boto3

dataframe1 = read_parquet('test/lambdas/process/parquets/dim_currency.parquet')
dataframe2 = read_parquet('test/lambdas/process/parquets/dim_currency_formatted.parquet')
bucket_name = "test_bucket_1"


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
    s3.create_bucket(Bucket=bucket_name)
    result = s3.list_buckets()
    assert len(result["Buckets"]) == 1
    assert result["Buckets"][0]["Name"] == bucket_name

def test_bucket_is_empty(s3):
    result = s3.list_objects_v2(Bucket=bucket_name)
    assert 'Contents' not in result

def test_returns_404_status_on_unsuccessful_get(s3):
    from process.utils import load_file_from_s3
    res = load_file_from_s3(bucket_name, 'test_1.json')
    assert res['status'] == 404

def test_returns_200_status_on_successful_get(s3):
    from process.utils import load_file_from_s3
    s3.upload_file('test/lambdas/process/json_files/currency.json', bucket_name, 'test_1.json')
    res = load_file_from_s3(bucket_name, 'test_1.json')
    assert res['status'] == 200

def test_returns_unmutated_file(s3):
    from process.utils import load_file_from_s3, load_file_from_local
    s3.upload_file('test/lambdas/process/json_files/currency.json', bucket_name, 'test_1.json')
    original = load_file_from_local('test/lambdas/process/json_files/currency.json')
    downloaded = load_file_from_s3(bucket_name, 'test_1.json')
    assert downloaded == original


