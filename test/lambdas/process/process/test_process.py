import boto3
from moto import mock_s3
from pandas import read_parquet
import pytest
import os
import boto3
import io
import pandas as pd
from src.lambdas.process.process import main_s3



PROCESSING_BUCKET_NAME = "query_queens_processing_bucket"
INGESTION_BUCKET_NAME = "query_queens_ingestion_bucket"
PREFIX = "2020-11-03/14:20:49/"






class Helpers:
    @staticmethod
    def mock_ingestion(s3):
        s3.create_bucket(Bucket=PROCESSING_BUCKET_NAME)
        s3.create_bucket(Bucket=INGESTION_BUCKET_NAME)
        s3.upload_file('./test/lambdas/process/json_files/last_updated.json', INGESTION_BUCKET_NAME, 'date/last_updated.json')
        s3.upload_file('./test/lambdas/process/json_files/address_test_2.json', INGESTION_BUCKET_NAME, f'{PREFIX}address.json')
        s3.upload_file('./test/lambdas/process/json_files/counterparty_test_2.json', INGESTION_BUCKET_NAME, f'{PREFIX}counterparty.json')
        s3.upload_file('./test/lambdas/process/json_files/currency_test_2.json', INGESTION_BUCKET_NAME, f'{PREFIX}currency.json')
        s3.upload_file('./test/lambdas/process/json_files/department_test_1.json', INGESTION_BUCKET_NAME, f'{PREFIX}department.json')
        s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', INGESTION_BUCKET_NAME, f'{PREFIX}design.json')
        s3.upload_file('./test/lambdas/process/json_files/staff_test_1.json', INGESTION_BUCKET_NAME, f'{PREFIX}staff.json')
        s3.upload_file('./test/lambdas/process/json_files/sales_order_test_1.json', INGESTION_BUCKET_NAME, f'{PREFIX}sales_order.json')
        s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', INGESTION_BUCKET_NAME, f'{PREFIX}purchase_order.json')
        s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', INGESTION_BUCKET_NAME, f'{PREFIX}payment.json')
        s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', INGESTION_BUCKET_NAME, f'{PREFIX}payment_type.json')
        s3.upload_file('./test/lambdas/process/json_files/design_test_2.json', INGESTION_BUCKET_NAME, f'{PREFIX}transaction.json')
        return
        
@pytest.fixture
def helpers():
    return Helpers








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

def test_process_main_s3(s3, helpers):
    helpers.mock_ingestion(s3)
    objects = s3.list_objects_v2(Bucket=INGESTION_BUCKET_NAME)
    assert 'Contents' in objects
   
    
    # Test if all keys are available
    
    expected_keys = ['2020-11-03/14:20:49/address.json', '2020-11-03/14:20:49/counterparty.json', '2020-11-03/14:20:49/currency.json', '2020-11-03/14:20:49/department.json', '2020-11-03/14:20:49/design.json', '2020-11-03/14:20:49/payment.json', '2020-11-03/14:20:49/payment_type.json', '2020-11-03/14:20:49/purchase_order.json', '2020-11-03/14:20:49/sales_order.json', '2020-11-03/14:20:49/staff.json', '2020-11-03/14:20:49/transaction.json', 'date/last_updated.json']
    actual_keys = [d['Key'] for d in objects['Contents']]
    assert len(expected_keys) == len(actual_keys)
    for key in expected_keys:
        assert key in actual_keys
    

    
def test_write_to_bucket(s3, helpers):
    helpers.mock_ingestion(s3)
    main_s3()
    processing_objects = s3.list_objects_v2(Bucket=PROCESSING_BUCKET_NAME)
    expected_keys = ['2020-11-03/14:20:49/counter_party.parquet', '2020-11-03/14:20:49/currency.parquet', '2020-11-03/14:20:49/date.parquet', '2020-11-03/14:20:49/design.parquet', '2020-11-03/14:20:49/location.parquet', '2020-11-03/14:20:49/sales_order.parquet', '2020-11-03/14:20:49/staff.parquet']
    actual_keys = [d['Key'] for d in processing_objects['Contents']]
    assert len(expected_keys) == len(actual_keys)
    for key in expected_keys:
        assert key in actual_keys
    

def test_main_s3_outputs_correct_parquet_files(s3, helpers):
    output_tables = ['counter_party', 'currency', 'date', 'design', 'location', 'sales_order', 'staff']
    expected_keys = [os.path.join(PREFIX, table + '.parquet') for table in output_tables]
    helpers.mock_ingestion(s3)
    main_s3()
    processed_contents = s3.list_objects_v2(Bucket=PROCESSING_BUCKET_NAME)['Contents']
    file_names = []
    for s3_object in processed_contents:
        s3_filepath = s3_object['Key']
        obj = s3.get_object(Bucket=PROCESSING_BUCKET_NAME, Key=s3_filepath)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        file_names.append(s3_filepath)

    assert len(expected_keys) == len(file_names)
    for key in expected_keys:
        assert key in file_names
        
      
   
