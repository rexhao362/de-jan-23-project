from src.lambdas.ingestion.utils.utils import get_table_names
from src.lambdas.ingestion.utils.utils import upload_to_s3
from moto import mock_s3
import boto3
from datetime import datetime
import pytest
from freezegun import freeze_time
import os.path
import os


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

@pytest.fixture(scope='function')
def bucket(s3):
    s3.create_bucket(
        Bucket='s3-de-ingestion-query-queens-test-bucket'
    )


@ freeze_time("2012-01-14 12:00:01")
def test_upload_to_s3_function_uploads_files_to_specified_bucket(bucket, s3):
    table_names = ['counterparty', 'currency', 'department', 'design', 'payment', 'transaction', 'staff', 'sales_order', 'address', 'purchase_order', 'payment_type']
    upload_to_s3("./local/aws/s3/ingestion")
    response = s3.list_objects_v2(Bucket='s3-de-ingestion-query-queens-test-bucket')
    list_of_files = [item['Key'] for item in response['Contents']]
    for table in table_names:
        assert f'14-01-2012/12:00:01/{table}.json' in list_of_files
