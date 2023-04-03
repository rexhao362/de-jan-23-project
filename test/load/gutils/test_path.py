# to allow running flattened lambdas locally
import sys
sys.path.append('./')

from os import path
from unittest.mock import patch
import pytest
import boto3
from moto import mock_s3
from src.load.gutils.environ import set_dev_environ
from src.load.gutils.path import (join, get_bucket_path)

@pytest.fixture
def mock_dev_environ():
    with patch.dict("os.environ", {}, clear=True) as e:
        set_dev_environ()
        yield e

aws_credentials = {
    'AWS_ACCESS_KEY_ID': "test",
    'AWS_SECRET_ACCESS_KEY': "test",
    'AWS_SECURITY_TOKEN': "test",
    'AWS_SESSION_TOKEN': "test",
    'AWS_DEFAULT_REGION': "us-east-1"
}

@pytest.fixture
def mock_production_environ():
    with patch.dict("os.environ", aws_credentials, clear=True) as e:
        yield e

# join
def test_join_returns_correct_value_in_dev(mock_dev_environ):
    data_path = "./local/aws/s3"
    file_name = "dim_currency.parquet"
    assert join(data_path, file_name) == path.join(data_path, file_name)

def test_join_returns_correct_value_in_production(mock_production_environ):
    data_path = "s3://test-bucket"
    file_name = "dim_currency.parquet"
    assert join(data_path, file_name) == f'{data_path}/{file_name}'

# get_bucket_path
def test_get_bucket_path_in_dev(mock_dev_environ):
    default_local_path = "./local/aws/s3"
    bucket_label = "processed"
    assert get_bucket_path(default_local_path, bucket_label) == path.join(default_local_path, bucket_label)

@pytest.fixture
def mock_aws_s3_client():
    return boto3.client('s3')

@mock_s3
def test_get_bucket_path_in_dev(mock_production_environ):
    processed_bucket_name = "de-q2-processed-bucket-1234"
    mock_aws_s3_client = boto3.client('s3')
    mock_aws_s3_client.create_bucket(Bucket="random-bucket")
    mock_aws_s3_client.create_bucket(Bucket=processed_bucket_name)
    mock_aws_s3_client.create_bucket(Bucket="3rd-bucket")

    bucket_label = "processed"
    assert get_bucket_path(None, bucket_label) == f's3://{processed_bucket_name}'



