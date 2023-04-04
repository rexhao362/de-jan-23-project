from datetime import datetime
from ingestion.ingestion import data_ingestion
from ingestion.dates import select_last_updated
from moto import mock_s3
import boto3
import pytest
import os.path
import os
from freezegun import freeze_time


# Mocking AWS credentials
@pytest.fixture(scope='module')
def aws_credentials():

    '''Mocked AWS credentials for moto.'''

    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope='module')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3')

@pytest.fixture(scope='module')
def bucket(s3):
    s3.create_bucket(
        Bucket='s3-de-ingestion-query-queens-test-bucket'
    )

@freeze_time('20-10-2021 14:10:01')
def test_upload_to_s3_function_uploads_files_to_specified_bucket(bucket, s3):
    table_names = ['counterparty', 'currency', 'department', 'design', 'payment', 'transaction', 'staff', 'sales_order', 'address', 'purchase_order', 'payment_type']
    dt = datetime(2012, 1, 14, 12, 00, 1, 000000)
    date_time = select_last_updated(dt)[0]
    data_ingestion()
    response = s3.list_objects_v2(Bucket='s3-de-ingestion-query-queens-test-bucket', Prefix=date_time)
    list_of_files = [item['Key'] for item in response['Contents']]
    for table_name in table_names:
        assert f'2021-10-20/14:10:01/{table_name}.json' in list_of_files
