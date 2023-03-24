from decimal import Decimal
from src.app_inge import data_ingestion
from src.utils.utils_inge import get_table_data
from src.utils.utils_inge import get_table_names
from src.utils.utils_inge import upload_to_s3
from src.utils.utils_inge import retrieve_last_updated
from src.utils.utils_inge import get_ingested_bucket_name
from src.utils.utils_inge import store_last_updated
import os.path
import os
import json
import re
from datetime import datetime
import boto3
from moto import mock_s3
import pytest
from freezegun import freeze_time

# test get_table_names


def test_get_table_names_queries_database_for_table_names():
    result = get_table_names()
    print(result)
    table_names = ['address',
                      'counterparty',
                      'currency',
                      'department',
                      'design',
                      'payment_type',
                      'payment',
                      'purchase_order',
                      'sales_order',
                      'staff',
                      'transaction']
    for table_name in table_names:
        assert table_name in result
    assert len(result) == 11


# test get_headers
@pytest.mark.parametrize('address_column_names', ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'])
def test_get_headers_extracts_column_names_for_address(address_column_names):
    result = get_table_data('address', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert address_column_names in result[0]

@pytest.mark.parametrize('counterparty_column_names', ["counterparty_id", "counterparty_legal_name", "legal_address_id", "commercial_contact", "delivery_contact", "created_at", "last_updated"])
def test_get_headers_extracts_column_names_for_counterparty(counterparty_column_names):
    result = get_table_data('counterparty', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert counterparty_column_names in result[0]

@pytest.mark.parametrize('currency_column_names', ["currency_id", "currency_code", "created_at", "last_updated"])
def test_get_headers_extracts_column_names_for_currency(currency_column_names):
    result = get_table_data('currency', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert currency_column_names in result[0]

@pytest.mark.parametrize('department_column_names', ["department_id", "department_name", "location", "manager", "created_at", "last_updated"])
def test_get_headers_extracts_column_names_for_department(department_column_names):
    result = get_table_data('department', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert department_column_names in result[0]

@pytest.mark.parametrize('design_column_names', ["design_id", "created_at", "design_name", "file_location", "file_name", "last_updated"])
def test_get_headers_extracts_column_names_for_design(design_column_names):
    result = get_table_data('design', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert design_column_names in result[0]

@pytest.mark.parametrize('payment_type_column_names', ["payment_type_id", "payment_type_name", "created_at", "last_updated"])
def test_get_headers_extracts_column_names_for_payment_type(payment_type_column_names):
    result = get_table_data('payment_type', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert payment_type_column_names in result[0]

@pytest.mark.parametrize('payment_column_names', ["payment_id", "created_at", "last_updated", "transaction_id", "counterparty_id", "payment_amount", "currency_id", "payment_type_id", "paid", "payment_date", "company_ac_number", "counterparty_ac_number"])
def test_get_headers_extracts_column_names_for_payment(payment_column_names):
    result = get_table_data('payment', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert payment_column_names in result[0]

@pytest.mark.parametrize('purchase_order_column_names', ["purchase_order_id", "created_at", "last_updated", "staff_id", "counterparty_id", "item_code", "item_quantity", "item_unit_price", "currency_id", "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_id"])
def test_get_headers_extracts_column_names_for_purchase_order(purchase_order_column_names):
    result = get_table_data('purchase_order', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert purchase_order_column_names in result[0]


@pytest.mark.parametrize('sales_order_column_names', ["sales_order_id", "created_at", "last_updated", "design_id", "staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_id"])
def test_get_headers_extracts_column_names_for_sales_order(sales_order_column_names):
    result = get_table_data('sales_order', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert sales_order_column_names in result[0]

@pytest.mark.parametrize('staff_column_names', ["staff_id", "first_name", "last_name", "department_id", "email_address", "created_at", "last_updated"])
def test_get_headers_extracts_column_names_for_staff(staff_column_names):
    result = get_table_data('staff', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert staff_column_names in result[0]

@pytest.mark.parametrize('transaction_column_names', ["transaction_id", "transaction_type", "sales_order_id", "purchase_order_id", "created_at", "last_updated"])
def test_get_headers_extracts_column_names_for_transaction(transaction_column_names):
    result = get_table_data('transaction', datetime(2022, 10, 5, 16, 30, 42, 962000))
    assert transaction_column_names in result[0]


# test get_table_data
def test_get_table_data_extracts_list_table_data_address():
    result = get_table_data('address', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == str or result[i][2] == None
        assert type(result[i][3]) == str or result[i][3] == None
        assert type(result[i][4]) == str
        assert type(result[i][5]) == str
        assert type(result[i][6]) == str
        assert type(result[i][7]) == str and re.match(
            '[0-9]{4} [0-9]{6}', result[i][7]) != None
        assert type(result[i][8]) == datetime
        assert type(result[i][9]) == datetime


def test_get_table_data_extracts_list_table_data_counterparty():
    result = get_table_data('counterparty', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == int
        assert type(result[i][3]) == str or result[i][3] == None
        assert type(result[i][4]) == str or result[i][4] == None
        assert type(result[i][5]) == datetime
        assert type(result[i][6]) == datetime


def test_get_table_data_extracts_list_table_data_currency():
    result = get_table_data('currency', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str and len(
            result[i][1]) == 3 and result[i][1].isupper()
        assert type(result[i][2]) == datetime
        assert type(result[i][3]) == datetime


def test_get_table_data_extracts_list_table_data_department():
    result = get_table_data('department', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == str or result[i][2] == None
        assert type(result[i][3]) == str or result[i][3] == None
        assert type(result[i][4]) == datetime
        assert type(result[i][5]) == datetime


def test_get_table_data_extracts_list_table_data_design():
    result = get_table_data('design', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == datetime
        assert type(result[i][2]) == str
        assert type(result[i][3]) == str
        assert type(result[i][4]) == str
        assert type(result[i][5]) == datetime


def test_get_table_data_extracts_list_table_data_payment_type():
    result = get_table_data('payment_type', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str and result[i][1] in ['SALES_RECEIPT', 'SALES_REFUND', 'PURCHASE_PAYMENT', 'PURCHASE_REFUND']
        assert type(result[i][2]) == datetime
        assert type(result[i][3]) == datetime


def test_get_table_data_extracts_list_table_data_payment():
    result = get_table_data('payment', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == datetime
        assert type(result[i][2]) == datetime
        assert type(result[i][3]) == int
        assert type(result[i][4]) == int
        assert type(result[i][5]) == Decimal and result[i][5] >= 1 and result[i][5] <= 1000000
        assert type(result[i][6]) == int
        assert type(result[i][7]) == int
        assert type(result[i][8]) == bool
        assert type(result[i][9]) == str and re.match('^\d{4}-((0\d)|(1[012]))-(([012]\d)|3[01])', result[i][9]) != None
        assert type(result[i][10]) == int
        assert type(result[i][11]) == int


def test_get_table_data_extracts_list_table_data_purchase_order():
    result = get_table_data('purchase_order', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == datetime
        assert type(result[i][2]) == datetime
        assert type(result[i][3]) == int
        assert type(result[i][4]) == int
        assert type(result[i][5]) == str
        assert type(result[i][6]) == int
        assert type(result[i][7]) == Decimal
        assert type(result[i][8]) == int
        assert type(result[i][9]) == str
        assert type(result[i][10]) == str
        assert type(result[i][11]) == int


def test_get_table_data_extracts_list_table_data_sales_order():
    result = get_table_data('sales_order', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == datetime
        assert type(result[i][2]) == datetime
        assert type(result[i][3]) == int
        assert type(result[i][4]) == int
        assert type(result[i][5]) == int
        assert type(result[i][6]) == int
        assert type(result[i][7]) == Decimal
        assert type(result[i][8]) == int
        assert type(result[i][9]) == str
        assert type(result[i][10]) == str
        assert type(result[i][11]) == int


def test_get_table_data_extracts_list_table_data_staff():
    result = get_table_data('staff', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == str
        assert type(result[i][3]) == int
        assert type(result[i][4]) == str
        assert type(result[i][5]) == datetime
        assert type(result[i][6]) == datetime


def test_get_table_data_extracts_list_table_data_transaction():
    result = get_table_data('transaction', datetime(2022, 10, 5, 16, 30, 42, 962000))
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == int or result[i][2] == None
        assert type(result[i][3]) == int or result[i][3] == None
        assert type(result[i][4]) == datetime
        assert type(result[i][5]) == datetime

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


# test data ingestion
# need to make this test work for data key
@freeze_time("2012-01-14 12:00:01")
def test_data_ingestion(bucket, s3):
    data_ingestion()
    list_files = s3.list_objects_v2(Bucket=get_ingested_bucket_name())
    for table in get_table_names():
        filepath = f'./ingestion_function/data/{table}.json'
        assert os.path.isfile(filepath)
        with open(filepath, 'r') as f:
            json_data = json.loads(f.read())
            assert json_data['table_name'] == table
            assert json_data['headers'] == get_table_data(table, datetime(2022, 10, 5, 16, 30, 42, 962000))[0]
            # for row in json_data['data']:
            #     for i in range(len(row)):
            #         if re.match('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}', row[i]) != None:
            #             row[i] = row[i].strptime('%Y-%m-%dT%H:%M:%S.%f')
            #         elif isinstance(row[i], float):
            #             row[i] = Decimal(row[i])
            # assert json_data['data'] == get_table_data(table, datetime(2022, 10, 5, 16, 30, 42, 962000))[1:]
        response = s3.get_object(Bucket=get_ingested_bucket_name(), Key=f'14-01-2012/12:00:01/{table}.json')
        json_response = json.loads(response['Body'].read())
        assert json_data == json_response

# need to change this test later for mocking
def test_get_table_data_function_returns_rows_newer_than_last_updated_date():
    table_data = get_table_data('address', datetime(2022, 12, 5, 10, 30, 41, 962000))
    assert len(table_data) == 1


def test_get_ingested_bucket_name_function_gets_correct_bucket_name(bucket, s3):
    s3.create_bucket(
        Bucket='bucket-test-2'
    )
    s3.create_bucket(
        Bucket='bucket-test-3'
    )
    s3.create_bucket(
        Bucket='bucket-test-4'
    )
    assert get_ingested_bucket_name() == 's3-de-ingestion-query-queens-test-bucket'

@ freeze_time("2012-01-14 12:00:01")
def test_upload_to_s3_function_uploads_files_to_specified_bucket(bucket, s3):
    table_names = ['counterparty', 'currency', 'department', 'design', 'payment', 'transaction', 'staff', 'sales_order', 'address', 'purchase_order', 'payment_type']
    upload_to_s3()
    response = s3.list_objects_v2(Bucket='s3-de-ingestion-query-queens-test-bucket')
    list_of_files = [item['Key'] for item in response['Contents']]
    for table in table_names:
        assert f'14-01-2012/12:00:01/{table}.json' in list_of_files


def test_retrieve_last_updated_function_retrieves_last_updated(bucket, s3):
    with open('./test/last_updated.json', 'w') as f:
        test_date = {"last_updated": "2000-11-03T14:20:49.962000"}
        f.write(json.dumps(test_date))
    with open('./test/last_updated.json', 'rb') as f:
        s3.put_object(Body=f, Bucket=get_ingested_bucket_name(),
                      Key='date/last_updated.json')
    assert retrieve_last_updated() == datetime(2000, 11, 3, 14, 20, 49, 962000)


def test_retrieve_last_updated_returns_default_date_if_last_updated_file_not_in_bucket(s3):
    response = s3.list_objects_v2(Bucket=get_ingested_bucket_name())
    list_files = [item['Key'] for item in response['Contents']]
    for file in list_files:
        s3.delete_objects(Bucket=get_ingested_bucket_name(), Delete={'Objects': [{'Key':file}]})
    result = retrieve_last_updated()
    assert result == datetime(2022, 10, 5, 16, 30, 42, 962000)


def test_store_last_updated_stores_last_updated(bucket, s3):
    store_last_updated(datetime(2012, 1, 14, 12, 00, 1, 000000))
    response = s3.list_objects_v2(Bucket=get_ingested_bucket_name())
    list_of_files = [item['Key'] for item in response['Contents']]
    assert 'date/last_updated.json' in list_of_files
    result = retrieve_last_updated()
    assert result == datetime(2022, 11, 3, 14, 20, 52, 186000)


