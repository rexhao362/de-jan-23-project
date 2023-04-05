import sys
sys.path.append('./src/')
import boto3
from moto import mock_s3
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

# @pytest.fixture(scope='module')
# def bucket(s3):
#     with mock_s3():
#         s3.create_bucket(Bucket=bucket_name)

def test_returns_dict(s3):
    from process.putils import bucket_cleanup
    # s3.create_bucket(Bucket=bucket_name)
    # s3.upload_file('test/lambdas/process/json_files/address.json', bucket_name, 'address.json')
    # s3.upload_file('test/lambdas/process/json_files/counterparty.json', bucket_name, 'counterparty.json')
    # s3.upload_file('test/lambdas/process/json_files/currency.json', bucket_name, 'currency.json')
    # s3.upload_file('test/lambdas/process/json_files/department.json', bucket_name, 'department.json')
    # s3.upload_file('test/lambdas/process/json_files/design.json', bucket_name, 'design.json')
    # s3.upload_file('test/lambdas/process/json_files/sales_order.json', bucket_name, 'sales_order.json')
    # s3.upload_file('test/lambdas/process/json_files/staff.json', bucket_name, 'staff.json')
    # res = s3.list_objects_v2(Bucket=bucket_name)
    result = bucket_cleanup(bucket_name)
    assert type(result) == dict

def test_returns_200_status_code_when_successful(s3):
    from process.putils import bucket_cleanup
    s3.create_bucket(Bucket=bucket_name)
    s3.upload_file('test/lambdas/process/json_files/address.json', bucket_name, 'address.json')
    s3.upload_file('test/lambdas/process/json_files/counterparty.json', bucket_name, 'counterparty.json')
    result = bucket_cleanup(bucket_name)
    assert result['status'] == 200

def test_returns_404_status_code_when_unsuccessful(s3):
    from process.putils import bucket_cleanup
    s3.create_bucket(Bucket=bucket_name)
    s3.upload_file('test/lambdas/process/json_files/address.json', bucket_name, 'address.json')
    s3.upload_file('test/lambdas/process/json_files/counterparty.json', bucket_name, 'counterparty.json')
    result = bucket_cleanup(bucket_name + '_')
    assert result['status'] == 404
    
def test_bucket_is_empty_after_function_execution(s3):
    from process.putils import bucket_cleanup
    s3.create_bucket(Bucket=bucket_name)
    s3.upload_file('test/lambdas/process/json_files/address.json', bucket_name, 'address.json')
    s3.upload_file('test/lambdas/process/json_files/counterparty.json', bucket_name, 'counterparty.json')
    list_res = s3.list_objects_v2(Bucket=bucket_name)
    list_length = len(list_res['Contents'])
    assert 'Contents' in list_res
    assert list_length == 2
    bucket_cleanup(bucket_name)
    list_res = s3.list_objects_v2(Bucket=bucket_name)
    assert 'Contents' not in list_res
