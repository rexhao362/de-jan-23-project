from decimal import Decimal
from ingestion_function.ingestion import data_ingestion
from ingestion_function.ingestion import get_table_data
from ingestion_function.ingestion import get_table_names
from ingestion_function.ingestion import upload_to_s3
from ingestion_function.ingestion import retrieve_last_updated
from ingestion_function.ingestion import get_ingested_bucket_name
import os.path
import os
import json
import re
from datetime import datetime
import boto3
from moto import mock_s3
import pytest

# test get_table_names


# def test_get_table_names_queries_database_for_table_names():
#     result = get_table_names()
#     assert 'address' in result
#     assert 'counterparty' in result
#     assert 'currency' in result
#     assert 'department' in result
#     assert 'design' in result
#     assert 'payment_type' in result
#     assert 'payment' in result
#     assert 'purchase_order' in result
#     assert 'sales_order' in result
#     assert 'staff' in result
#     assert 'transaction' in result
#     assert len(result) == 11


# # test get_headers
# def test_get_headers_extracts_column_names_for_address():
#     result = get_table_data('address')
#     assert 'address_id' in result[0]
#     assert 'address_line_1' in result[0]
#     assert 'address_line_2' in result[0]
#     assert 'district' in result[0]
#     assert 'city' in result[0]
#     assert 'postal_code' in result[0]
#     assert 'country' in result[0]
#     assert 'phone' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]


# def test_get_headers_extracts_column_names_for_counterparty():
#     result = get_table_data('counterparty')
#     assert 'counterparty_id' in result[0]
#     assert 'counterparty_legal_name' in result[0]
#     assert 'legal_address_id' in result[0]
#     assert 'commercial_contact' in result[0]
#     assert 'delivery_contact' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]


# def test_get_headers_extracts_column_names_for_currency():
#     result = get_table_data('currency')
#     assert 'currency_id' in result[0]
#     assert 'currency_code' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]


# def test_get_headers_extracts_column_names_for_department():
#     result = get_table_data('department')
#     assert 'department_id' in result[0]
#     assert 'department_name' in result[0]
#     assert 'location' in result[0]
#     assert 'manager' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]


# def test_get_headers_extracts_column_names_for_design():
#     result = get_table_data('design')
#     assert 'design_id' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]
#     assert 'design_name' in result[0]
#     assert 'file_location' in result[0]
#     assert 'file_name' in result[0]


# def test_get_headers_extracts_column_names_for_payment_type():
#     result = get_table_data('payment_type')
#     assert 'payment_type_id' in result[0]
#     assert 'payment_type_name' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]


# def test_get_headers_extracts_column_names_for_payment():
#     result = get_table_data('payment')
#     assert 'payment_id' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]
#     assert "transaction_id" in result[0]
#     assert "counterparty_id" in result[0]
#     assert "payment_amount" in result[0]
#     assert "currency_id" in result[0]
#     assert "payment_type_id" in result[0]
#     assert "paid" in result[0]
#     assert "payment_date" in result[0]
#     assert "company_ac_number" in result[0]
#     assert "counterparty_ac_number" in result[0]


# def test_get_headers_extracts_column_names_for_purchase_order():
#     result = get_table_data('purchase_order')
#     assert 'purchase_order_id' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]
#     assert 'staff_id' in result[0]
#     assert 'counterparty_id' in result[0]
#     assert 'item_code' in result[0]
#     assert 'item_quantity' in result[0]
#     assert 'item_unit_price' in result[0]
#     assert 'currency_id' in result[0]
#     assert 'agreed_delivery_date' in result[0]
#     assert 'agreed_payment_date' in result[0]
#     assert 'agreed_delivery_location_id' in result[0]


# def test_get_headers_extracts_column_names_for_sales_order():
#     result = get_table_data('sales_order')
#     assert 'sales_order_id' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]
#     assert 'design_id' in result[0]
#     assert 'staff_id' in result[0]
#     assert 'counterparty_id' in result[0]
#     assert 'units_sold' in result[0]
#     assert 'unit_price' in result[0]
#     assert 'currency_id' in result[0]
#     assert 'agreed_delivery_date' in result[0]
#     assert 'agreed_payment_date' in result[0]
#     assert 'agreed_delivery_location_id' in result[0]


# def test_get_headers_extracts_column_names_for_staff():
#     result = get_table_data('staff')
#     assert 'staff_id' in result[0]
#     assert 'first_name' in result[0]
#     assert 'last_name' in result[0]
#     assert 'department_id' in result[0]
#     assert 'email_address' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]


# def test_get_headers_extracts_column_names_for_transaction():
#     result = get_table_data('transaction')
#     assert 'transaction_id' in result[0]
#     assert 'transaction_type' in result[0]
#     assert 'sales_order_id' in result[0]
#     assert 'purchase_order_id' in result[0]
#     assert 'created_at' in result[0]
#     assert 'last_updated' in result[0]


# # test get_table_data
# def test_get_table_data_extracts_list_table_data_address():
#     result = get_table_data('address')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == str
#         assert type(result[i][2]) == str or result[i][2] == None
#         assert type(result[i][3]) == str or result[i][3] == None
#         assert type(result[i][4]) == str
#         assert type(result[i][5]) == str
#         assert type(result[i][6]) == str
#         assert type(result[i][7]) == str and re.match(
#             '[0-9]{4} [0-9]{6}', result[i][7]) != None
#         assert type(result[i][8]) == datetime
#         assert type(result[i][9]) == datetime


# def test_get_table_data_extracts_list_table_data_counterparty():
#     result = get_table_data('counterparty')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == str
#         assert type(result[i][2]) == int
#         assert type(result[i][3]) == str or result[i][3] == None
#         assert type(result[i][4]) == str or result[i][4] == None
#         assert type(result[i][5]) == datetime
#         assert type(result[i][6]) == datetime


# def test_get_table_data_extracts_list_table_data_currency():
#     result = get_table_data('currency')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == str and len(
#             result[i][1]) == 3 and result[i][1].isupper()
#         assert type(result[i][2]) == datetime
#         assert type(result[i][3]) == datetime


# def test_get_table_data_extracts_list_table_data_department():
#     result = get_table_data('department')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == str
#         assert type(result[i][2]) == str
#         assert type(result[i][3]) == str
#         assert type(result[i][4]) == datetime
#         assert type(result[i][5]) == datetime


# def test_get_table_data_extracts_list_table_data_design():
#     result = get_table_data('design')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == datetime
#         assert type(result[i][2]) == str
#         assert type(result[i][3]) == str
#         assert type(result[i][4]) == str
#         assert type(result[i][5]) == datetime


# def test_get_table_data_extracts_list_table_data_payment_type():
#     result = get_table_data('payment_type')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == str
#         assert type(result[i][2]) == datetime
#         assert type(result[i][3]) == datetime


# def test_get_table_data_extracts_list_table_data_payment():
#     result = get_table_data('payment')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == datetime
#         assert type(result[i][2]) == datetime
#         assert type(result[i][3]) == int
#         assert type(result[i][4]) == int
#         assert type(result[i][5]) == Decimal
#         assert type(result[i][6]) == int
#         assert type(result[i][7]) == int
#         assert type(result[i][8]) == bool
#         assert type(result[i][9]) == str
#         assert type(result[i][10]) == int
#         assert type(result[i][11]) == int


# def test_get_table_data_extracts_list_table_data_purchase_order():
#     result = get_table_data('purchase_order')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == datetime
#         assert type(result[i][2]) == datetime
#         assert type(result[i][3]) == int
#         assert type(result[i][4]) == int
#         assert type(result[i][5]) == str
#         assert type(result[i][6]) == int
#         assert type(result[i][7]) == Decimal
#         assert type(result[i][8]) == int
#         assert type(result[i][9]) == str
#         assert type(result[i][10]) == str
#         assert type(result[i][11]) == int


# def test_get_table_data_extracts_list_table_data_sales_order():
#     result = get_table_data('sales_order')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == datetime
#         assert type(result[i][2]) == datetime
#         assert type(result[i][3]) == int
#         assert type(result[i][4]) == int
#         assert type(result[i][5]) == int
#         assert type(result[i][6]) == int
#         assert type(result[i][7]) == Decimal
#         assert type(result[i][8]) == int
#         assert type(result[i][9]) == str
#         assert type(result[i][10]) == str
#         assert type(result[i][11]) == int


# def test_get_table_data_extracts_list_table_data_staff():
#     result = get_table_data('staff')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == str
#         assert type(result[i][2]) == str
#         assert type(result[i][3]) == int
#         assert type(result[i][4]) == str
#         assert type(result[i][5]) == datetime
#         assert type(result[i][6]) == datetime


# def test_get_table_data_extracts_list_table_data_transaction():
#     result = get_table_data('transaction')
#     for i in range(1, len(result)):
#         assert type(result[i][0]) == int
#         assert type(result[i][1]) == str
#         assert type(result[i][2]) == int or result[i][2] == None
#         assert type(result[i][3]) == int or result[i][3] == None
#         assert type(result[i][4]) == datetime
#         assert type(result[i][5]) == datetime

# # test data ingestion


# def test_data_ingestion_address():
#     data_ingestion()
#     filepath = './ingestion_function/data/address.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'address'
#         assert json_data['headers'] == get_table_data('address')[0]


# def test_data_ingestion_counterparty():
#     data_ingestion()
#     filepath = './ingestion_function/data/counterparty.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'counterparty'
#         assert json_data['headers'] == get_table_data('counterparty')[0]


# def test_data_ingestion_currency():
#     data_ingestion()
#     filepath = './ingestion_function/data/currency.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'currency'
#         assert json_data['headers'] == get_table_data('currency')[0]


# def test_data_ingestion_department():
#     data_ingestion()
#     filepath = './ingestion_function/data/department.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'department'
#         assert json_data['headers'] == get_table_data('department')[0]


# def test_data_ingestion_design():
#     data_ingestion()
#     filepath = './ingestion_function/data/design.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'design'
#         assert json_data['headers'] == get_table_data('design')[0]


# def test_data_ingestion_payment_type():
#     data_ingestion()
#     filepath = './ingestion_function/data/payment_type.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'payment_type'
#         assert json_data['headers'] == get_table_data('payment_type')[0]


# def test_data_ingestion_payment():
#     data_ingestion()
#     filepath = './ingestion_function/data/payment.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'payment'
#         assert json_data['headers'] == get_table_data('payment')[0]


# def test_data_ingestion_purchase_order():
#     data_ingestion()
#     filepath = './ingestion_function/data/purchase_order.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'purchase_order'
#         assert json_data['headers'] == get_table_data('purchase_order')[0]


# def test_data_ingestion_sales_order():
#     data_ingestion()
#     filepath = './ingestion_function/data/sales_order.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'sales_order'
#         assert json_data['headers'] == get_table_data('sales_order')[0]


# def test_data_ingestion_staff():
#     data_ingestion()
#     filepath = './ingestion_function/data/staff.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'staff'
#         assert json_data['headers'] == get_table_data('staff')[0]


# def test_data_ingestion_transaction():
#     data_ingestion()
#     filepath = './ingestion_function/data/transaction.json'
#     assert os.path.isfile(filepath)
#     with open(filepath, 'r') as f:
#         json_data = json.loads(f.read())
#         assert json_data['table_name'] == 'transaction'
#         assert json_data['headers'] == get_table_data('transaction')[0]


# Mocking AWS credentials
@pytest.fixture
def aws_credentials():

    '''Mocked AWS credentials for moto.'''

    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3')

@pytest.fixture
def bucket(s3):
    s3.create_bucket(
        Bucket='s3-de-ingestion-query-queens-test-bucket'
    )

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

def test_upload_to_s3_function_uploads_files_to_specified_bucket(bucket, s3):
    table_names = ['counterparty', 'currency', 'department', 'design', 'payment', 'transaction', 'staff', 'sales_order', 'address', 'purchase_order', 'payment_type']
    for table in table_names:
        with open(f'./ingestion_function/data/{table}.json', 'w') as f:
            f.write('')
    upload_to_s3()
    response = s3.list_objects_v2(Bucket='s3-de-ingestion-query-queens-test-bucket')
    list_of_files = [item['Key'] for item in response['Contents']]
    print(list_of_files)
    for table in table_names:
        assert f'ingested_data/{table}.json' in list_of_files


def test_retrieve_last_updated_function_retrieves_last_updated(bucket, s3):
    with open('./last_updated.json', 'rb') as f:
        s3.put_object(Body=f, Bucket=get_ingested_bucket_name(),
                      Key='date/last_updated.json')
    assert retrieve_last_updated() == datetime(2000, 11, 3, 14, 20, 49, 962000)


def test_retrieve_last_updated_returns_default_date_if_last_updated_file_not_in_bucket(bucket, s3):
    result = retrieve_last_updated()
    assert result == datetime(2022, 10, 5, 16, 30, 42, 962000)


# def test_store_last_updated_stores_last_updated(bucket, s3):
#     data_ingestion()
#     response = s3.get_object(
#         Bucket=get_ingested_bucket_name(), Key='non_existent_file.json')
#     print(response)
#     assert False




    











#test upload files to s3 bucket
# def test_upload_to_s3_function_uploads_files_to_correct_bucket():
#     s3 = boto3.client('s3')
#     upload_to_s3()
#     list_objects = s3.list_objects_v2(Bucket='s3-de-ingestion-query-queens-4781192')
#     pass
#     # TODO MOTO


#test store_last_updated
# @mock_s3
# def test_retrieve_last_updated_function_returns_correct_value():
#     s3 = boto3.client('s3')
#     bucket = 's3-de-ingestion-query-queens'
#     s3.create_bucket(Bucket=bucket)
#     # upload mock file to mock bucket?
#     result = retrieve_last_updated()
#     print(result)
#     assert False




