from decimal import Decimal
from src.lambdas.ingestion.ingestion import data_ingestion
from src.lambdas.ingestion.utils.utils import get_table_names
from src.lambdas.ingestion.utils.utils import upload_to_s3
from src.lambdas.ingestion.utils.utils import retrieve_last_updated
from src.lambdas.ingestion.utils.utils import get_ingested_bucket_name
from src.lambdas.ingestion.utils.utils import store_last_updated
from src.lambdas.ingestion.utils.utils import get_table_data
import os.path
import os
import json
import re
from datetime import datetime
import boto3
from moto import mock_s3
import pytest
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

@pytest.fixture(scope='function')
def bucket(s3):
    s3.create_bucket(
        Bucket='s3-de-ingestion-query-queens-test-bucket'
    )


@freeze_time("2012-01-14 12:00:01")
def test_data_ingestion(bucket, s3):
    data_ingestion("./local/aws/s3/ingestion")
    for table_name in get_table_names():
        filepath = f'./local/aws/s3/ingestion/table_data/{table_name}.json'
        assert os.path.isfile(filepath)
        with open(filepath, 'r') as f:
            json_data = json.loads(f.read())
            assert json_data['table_name'] == table_name
            assert json_data['headers'] == get_table_data(table_name, datetime(2022, 10, 5, 16, 30, 42, 962000))[0]
            test_data = get_table_data(table_name, datetime(2022, 10, 5, 16, 30, 42, 962000))[1:]
            for row in test_data:
                for i in range(len(row)):
                    if isinstance(row[i], datetime):
                            row[i] = row[i].strftime('%Y-%m-%dT%H:%M:%S.%f')
                    elif isinstance(row[i], Decimal):
                        row[i] = float(row[i])
            assert json_data['data'] == test_data
        response = s3.get_object(Bucket=get_ingested_bucket_name(), Key=f'14-01-2012/12:00:01/{table_name}.json')
        json_response = json.loads(response['Body'].read())
        assert json_data == json_response

