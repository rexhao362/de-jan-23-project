from decimal import Decimal
from src.ingestion.ingestion import data_ingestion
from src.ingestion.utils.utils import get_table_names
from src.ingestion.utils.utils import get_ingested_bucket_name
from src.ingestion.utils.utils import get_table_data
import os.path
import os
import json
from datetime import datetime
import boto3
from moto import mock_s3
import pytest
from freezegun import freeze_time

@pytest.fixture(scope='package')
def unset():
    del os.environ['DE_Q2_DEV']


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
def test_data_ingestion(bucket, s3, unset):
    test_path = './local/aws/s3/ingested'
    data_ingestion(test_path)
    bucket_name = get_ingested_bucket_name()

    for table_name in get_table_names():
        key = f'2021-10-20/14:10:01/{table_name}.json'
        response = s3.get_object(
            Bucket=bucket_name,
            Key=key
        )
        json_res = json.loads(response['Body'].read())
        test_data = get_table_data(table_name, None)
        assert json_res['table_name'] == table_name
        assert json_res['headers'] == test_data[0]
        for row in test_data[1:]:
            for i in range(len(row)):
                if isinstance(row[i], datetime):
                    row[i] = row[i].strftime('%Y-%m-%dT%H:%M:%S.%f')
                elif isinstance(row[i], Decimal):
                    row[i] = float(row[i])
        assert json_res['data'] == test_data[1:]
