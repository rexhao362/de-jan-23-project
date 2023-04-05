import sys
sys.path.append('./src/')
import boto3
from moto import mock_s3
from pandas import read_parquet
import pytest
import os
import boto3

#TODO - Could do with more tests, or move this into test_utils.py and keep it short?

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

def test_create_bucket_with_last_updated(s3):
    s3.create_bucket(Bucket=bucket_name)
    result = s3.list_buckets()
    assert len(result["Buckets"]) == 1
    assert result["Buckets"][0]["Name"] == bucket_name
    res = s3.upload_file('./test/lambdas/process/json_files/last_updated.json', bucket_name, 'date/last_updated.json')
    objects = s3.list_objects_v2(Bucket=bucket_name)
    assert 'Contents' in objects

def test_returns_date_and_time_with_successful_get(s3):
    from process.putils import get_last_updated
    date, time = get_last_updated(bucket_name)
    assert date == '2020-11-03'
    assert time == '14:20:49'
    
