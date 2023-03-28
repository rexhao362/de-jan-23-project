import boto3
from moto import mock_s3
import pytest
import os
import boto3
import io
from pandas import read_parquet
from src.lambdas.process.process import (main_local, main_s3)

PROCESSING_BUCKET_NAME = "query-queens-processing-bucket"
INGESTION_BUCKET_NAME = "query-queens-ingestion-bucket"
PREFIX = "2020-11-03/14:20:49/"


class Helpers:
    @staticmethod
    def mock_ingestion(s3):
        s3.create_bucket(Bucket=PROCESSING_BUCKET_NAME)
        s3.create_bucket(Bucket=INGESTION_BUCKET_NAME)
        s3.upload_file('./test/lambdas/process/json_files/last_updated.json',
                       INGESTION_BUCKET_NAME, 'date/last_updated.json')
        s3.upload_file('./test/lambdas/process/json_files/address.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}address.json')
        s3.upload_file('./test/lambdas/process/json_files/counterparty.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}counterparty.json')
        s3.upload_file('./test/lambdas/process/json_files/currency.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}currency.json')
        s3.upload_file('./test/lambdas/process/json_files/department.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}department.json')
        s3.upload_file('./test/lambdas/process/json_files/design.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}design.json')
        s3.upload_file('./test/lambdas/process/json_files/staff.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}staff.json')
        s3.upload_file('./test/lambdas/process/json_files/sales_order.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}sales_order.json')
        s3.upload_file('./test/lambdas/process/json_files/design.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}purchase_order.json')
        s3.upload_file('./test/lambdas/process/json_files/design.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}payment.json')
        s3.upload_file('./test/lambdas/process/json_files/design.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}payment_type.json')
        s3.upload_file('./test/lambdas/process/json_files/design.json',
                       INGESTION_BUCKET_NAME, f'{PREFIX}transaction.json')
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


def test_ingestion_bucket_is_initialised(s3, helpers):
    helpers.mock_ingestion(s3)
    objects = s3.list_objects_v2(Bucket=INGESTION_BUCKET_NAME)
    assert 'Contents' in objects


def test_main_s3_write_to_bucket(s3, helpers):
    helpers.mock_ingestion(s3)
    main_s3()
    processing_objects = s3.list_objects_v2(Bucket=PROCESSING_BUCKET_NAME)
    expected_keys = ['2020-11-03/14:20:49/counterparty.parquet', '2020-11-03/14:20:49/currency.parquet', '2020-11-03/14:20:49/date.parquet',
                     '2020-11-03/14:20:49/design.parquet', '2020-11-03/14:20:49/location.parquet', '2020-11-03/14:20:49/sales_order.parquet', '2020-11-03/14:20:49/staff.parquet']
    actual_keys = [d['Key'] for d in processing_objects['Contents']]
    assert len(expected_keys) == len(actual_keys)
    for key in expected_keys:
        assert key in actual_keys


def test_main_s3_outputs_correct_parquet_files(s3, helpers):
    expected_column_names = {'counterparty': ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number'], 'currency': ['currency_id', 'currency_code', 'currency_name'], 'date': ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter'], 'design': [
        'design_id', 'design_name', 'file_location', 'file_name'], 'location': ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone'], 'sales_order': ['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']}
    table_column_dict = {}
    helpers.mock_ingestion(s3)
    main_s3()
    processed_contents = s3.list_objects_v2(
        Bucket=PROCESSING_BUCKET_NAME)['Contents']

    for s3_object in processed_contents:
        s3_filepath = s3_object['Key']
        obj = s3.get_object(Bucket=PROCESSING_BUCKET_NAME, Key=s3_filepath)
        df = read_parquet(io.BytesIO(obj['Body'].read()))
        table_name = os.path.splitext(os.path.basename(s3_filepath))[0]
        table_column_dict[table_name] = df.columns.tolist()

    for table_name, column_names in table_column_dict.items():
        assert sorted(column_names) == sorted(
            expected_column_names[table_name])


def test_local_write_to_bucket(s3, helpers):
    main_local()
    # list objects in the processing bucket
    actual_keys = os.listdir(os.path.join(PROCESSING_BUCKET_NAME, PREFIX))
    expected_keys = ['counterparty.parquet', 'currency.parquet', 'date.parquet',
                     'design.parquet', 'location.parquet', 'sales_order.parquet', 'staff.parquet']
    assert len(expected_keys) == len(actual_keys)
    for key in expected_keys:
        assert key in actual_keys


def test_local_outputs_correct_parquet_files(s3, helpers):
    expected_column_names = {'counterparty': ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country', 'counterparty_legal_phone_number'], 'currency': ['currency_id', 'currency_code', 'currency_name'], 'date': ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter'], 'design': [
        'design_id', 'design_name', 'file_location', 'file_name'], 'location': ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone'], 'sales_order': ['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']}
    table_column_dict = {}
    helpers.mock_ingestion(s3)
    main_local()
    filepaths = os.listdir(os.path.join('./',PROCESSING_BUCKET_NAME, PREFIX))

    for filepath in filepaths:
        filepath = os.path.join(PROCESSING_BUCKET_NAME, PREFIX, filepath)
        df = read_parquet(filepath)
        table_name = os.path.splitext(os.path.basename(filepath))[0]
        table_column_dict[table_name] = df.columns.tolist()

    for table_name, column_names in table_column_dict.items():
        assert sorted(column_names) == sorted(
            expected_column_names[table_name])
