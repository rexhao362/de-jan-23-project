import boto3
from moto import mock_s3
from pandas import read_parquet
import pytest
import os
import io
import boto3
import pyarrow.parquet as pq
from numpy import equal

dataframe1 = read_parquet('test/lambdas/process/parquets/dim_currency.parquet')
dataframe2 = read_parquet('test/lambdas/process/parquets/dim_currency_formatted.parquet')
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

def test_create_bucket(s3):
    s3.create_bucket(Bucket=bucket_name)
    result = s3.list_buckets()
    assert len(result["Buckets"]) == 1
    assert result["Buckets"][0]["Name"] == bucket_name

# def test_bucket_exists_through_tests(s3):
#     result = s3.list_buckets()
#     assert len(result["Buckets"]) == 1
#     assert result["Buckets"][0]["Name"] == bucket_name

def test_write_to_s3_returns_dict(s3):
    from src.lambdas.process.utils import (write_to_bucket)
    obj_key_1 = 'test/test_1'
    upload = write_to_bucket(bucket_name, dataframe1, obj_key_1)
    assert type(upload) == dict 

def test_returns_status_code_200_with_successful_write(s3):
    from src.lambdas.process.utils import (write_to_bucket)
    obj_key_1 = 'test/test_1'
    upload = write_to_bucket(bucket_name, dataframe1, obj_key_1)
    objects = s3.list_objects_v2(Bucket=bucket_name)
    assert obj_key_1 in objects['Contents'][0]['Key']
    assert upload['status'] == 200
    
def test_returns_status_code_404_with_unsuccessful_write(s3):
    from src.lambdas.process.utils import (write_to_bucket)
    obj_key_1 = 'test/test_1'
    upload = write_to_bucket(bucket_name + "_", dataframe1, obj_key_1)
    assert upload['status'] == 404
    assert upload['response'] == None


def test_key_is_maintained_in_bucket(s3):
    from src.lambdas.process.utils import (write_to_bucket)
    obj_key_1 = 'test/test_1'
    obj_key_2 = 'test/test_2'
    write_to_bucket(bucket_name, dataframe1, obj_key_1)
    write_to_bucket(bucket_name, dataframe1, obj_key_2)
    objects = s3.list_objects_v2(Bucket=bucket_name)
    assert obj_key_1 in objects['Contents'][0]['Key']
    assert obj_key_2 in objects['Contents'][1]['Key']

def test_file_is_still_valid_once_written_to_bucket(s3):
    buffer = io.BytesIO()
    resource = boto3.resource('s3')
    s3_object = resource.Object('test_bucket_1', 'test/test_1')
    s3_object.download_fileobj(buffer)
    table = pq.read_table(buffer)
    df = table.to_pandas()
    assert equal(df.columns.values,['currency_id', 'currency_code', 'currency_name']).all()
    assert equal(df['currency_id'], dataframe1['currency_id']).all()
    assert equal(df['currency_code'], dataframe1['currency_code']).all()
    assert equal(df['currency_name'], dataframe1['currency_name']).all()

