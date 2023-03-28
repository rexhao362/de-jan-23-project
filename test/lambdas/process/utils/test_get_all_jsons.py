import boto3
from moto import mock_s3
from pandas import read_parquet
import pytest
import os
import boto3

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

def test_upload_all_last_updated_jsons(s3):
    from src.lambdas.process.utils import get_last_updated
    date, time = get_last_updated(bucket_name)
    s3.upload_file('test/lambdas/process/json_files/address_test_2.json', bucket_name, f'{date}/{time}/address.json')
    s3.upload_file('test/lambdas/process/json_files/counterparty_test_2.json', bucket_name, f'{date}/{time}/counterparty.json')
    s3.upload_file('test/lambdas/process/json_files/currency_test_2.json', bucket_name, f'{date}/{time}/currency.json')
    s3.upload_file('test/lambdas/process/json_files/department_test_1.json', bucket_name, f'{date}/{time}/department.json')
    s3.upload_file('test/lambdas/process/json_files/design_test_2.json', bucket_name, f'{date}/{time}/design.json')
    s3.upload_file('test/lambdas/process/json_files/sales_order_test_1.json', bucket_name, f'{date}/{time}/sales_order.json')
    s3.upload_file('test/lambdas/process/json_files/staff_test_1.json', bucket_name, f'{date}/{time}/staff.json')
    objects = s3.list_objects_v2(Bucket=bucket_name)
    assert len(objects['Contents']) == 8

def test_get_all_jsons_returns_dict(s3):
    from src.lambdas.process.utils import get_all_jsons, get_last_updated
    date, time = get_last_updated(bucket_name)
    jsons = get_all_jsons(bucket_name, date, time)
    assert type(jsons) == dict

#TODO - TDD really helps with building scripts when you don't have a partner, miss you George <3
def test_iteration_with_jsons(s3):
    from src.lambdas.process.utils import get_all_jsons, get_last_updated, process
    date, time = get_last_updated(bucket_name)
    jsons = get_all_jsons(bucket_name, date, time)
