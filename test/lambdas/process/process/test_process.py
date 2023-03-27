import boto3
from moto import mock_s3
from pandas import read_parquet
import pytest
import os
import boto3
from src.lambdas.process.process import main_s3

processing_bucket_name = "query_queens_processing_bucket"
ingestion_bucket_name = "query_queens_ingestion_bucket"


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

def test_process_main_s3(s3):
    prefix = "2020-11-03/14:20:49/"
    s3.create_bucket(Bucket=processing_bucket_name)
    s3.create_bucket(Bucket=ingestion_bucket_name)
    s3.upload_file('./test/lambdas/process/json_files/last_updated.json', ingestion_bucket_name, 'date/last_updated.json')
    s3.upload_file('./test/lambdas/process/json_files/address_test_2.json', ingestion_bucket_name, f'{prefix}address.json')
    s3.upload_file('./test/lambdas/process/json_files/counterparty_test_2.json', ingestion_bucket_name, f'{prefix}counterparty.json')
    s3.upload_file('./test/lambdas/process/json_files/currency_test_2.json', ingestion_bucket_name, f'{prefix}currency.json')
    s3.upload_file('./test/lambdas/process/json_files/department_test_1.json', ingestion_bucket_name, f'{prefix}department.json')
    s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', ingestion_bucket_name, f'{prefix}design.json')
    s3.upload_file('./test/lambdas/process/json_files/staff_test_1.json', ingestion_bucket_name, f'{prefix}staff.json')
    s3.upload_file('./test/lambdas/process/json_files/sales_order_test_1.json', ingestion_bucket_name, f'{prefix}sales_order.json')
    s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', ingestion_bucket_name, f'{prefix}purchase_order.json')
    s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', ingestion_bucket_name, f'{prefix}payment.json')
    s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', ingestion_bucket_name, f'{prefix}payment_type.json')
    s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', ingestion_bucket_name, f'{prefix}transaction.json')
    objects = s3.list_objects_v2(Bucket=ingestion_bucket_name)
    assert 'Contents' in objects
    main_s3()
    #print(s3.list_objects_v2(Bucket=processing_bucket_name))
