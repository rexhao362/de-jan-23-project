from ingestion_function.function import data_ingestion, get_table_data, get_table_names
import os.path
import json
from datetime import datetime

def test_get_table_names_queries_database_for_table_names():
    result = get_table_names()
    assert 'address' in result
    assert 'counterparty' in result
    assert 'currency' in result
    assert 'department' in result
    assert 'design' in result
    assert 'payment_type' in result
    assert 'payment' in result
    assert 'purchase_order' in result
    assert 'sales_order' in result
    assert 'staff' in result
    assert 'transaction' in result
    assert len(result) == 11


def test_get_headers_extracts_column_names_for_current_table():
    result = get_table_data('payment')
    assert 'payment_id' in result[0]
    assert 'created_at' in result[0]
    assert 'last_updated' in result[0]
    assert "transaction_id" in result[0]
    assert "counterparty_id" in result[0]
    assert "payment_amount" in result[0]
    assert "currency_id" in result[0]
    assert "payment_type_id" in result[0]
    assert "paid" in result[0]
    assert "payment_date" in result[0]
    assert "company_ac_number" in result[0]
    assert "counterparty_ac_number" in result[0]


def test_get_table_data_extracts_list_table_data():
    result = get_table_data('department')
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == str
        assert type(result[i][3]) == str
        assert type(result[i][4]) == datetime
        assert type(result[i][5]) == datetime


def test_data_ingestion_():
    data_ingestion()
    filepath = './ingestion_function/data/currency.json'
    assert os.path.isfile(filepath)
    with open(filepath, 'r') as f:
        json_data = json.loads(f.read())
        assert json_data['table_name'] == 'currency'
        assert json_data['headers'] == get_table_data('currency')[0]
