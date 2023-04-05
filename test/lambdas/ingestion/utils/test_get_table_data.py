import sys
sys.path.append('./src/')
from ingestion.utils import get_table_data
from datetime import datetime
import re
from decimal import Decimal

def test_get_table_data_extracts_list_table_data_address():
    result = get_table_data('address', None)
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
    result = get_table_data('counterparty', None)
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == int
        assert type(result[i][3]) == str or result[i][3] == None
        assert type(result[i][4]) == str or result[i][4] == None
        assert type(result[i][5]) == datetime
        assert type(result[i][6]) == datetime


def test_get_table_data_extracts_list_table_data_currency():
    result = get_table_data('currency', None)
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str and len(
            result[i][1]) == 3 and result[i][1].isupper()
        assert type(result[i][2]) == datetime
        assert type(result[i][3]) == datetime


def test_get_table_data_extracts_list_table_data_department():
    result = get_table_data('department', None)
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == str or result[i][2] == None
        assert type(result[i][3]) == str or result[i][3] == None
        assert type(result[i][4]) == datetime
        assert type(result[i][5]) == datetime


def test_get_table_data_extracts_list_table_data_design():
    result = get_table_data('design', None)
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == datetime
        assert type(result[i][2]) == str
        assert type(result[i][3]) == str
        assert type(result[i][4]) == str
        assert type(result[i][5]) == datetime


def test_get_table_data_extracts_list_table_data_payment_type():
    result = get_table_data('payment_type', None)
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str and result[i][1] in ['SALES_RECEIPT', 'SALES_REFUND', 'PURCHASE_PAYMENT', 'PURCHASE_REFUND']
        assert type(result[i][2]) == datetime
        assert type(result[i][3]) == datetime


def test_get_table_data_extracts_list_table_data_payment():
    result = get_table_data('payment', None)
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
        assert type(result[i][9]) == str and re.match('\d{4}-((0\d)|(1[012]))-(([012]\d)|3[01])', result[i][9]) != None
        assert type(result[i][10]) == int
        assert type(result[i][11]) == int


def test_get_table_data_extracts_list_table_data_purchase_order():
    result = get_table_data('purchase_order', None)
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
    result = get_table_data('sales_order', None)
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
    result = get_table_data('staff', None)
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == str
        assert type(result[i][3]) == int
        assert type(result[i][4]) == str
        assert type(result[i][5]) == datetime
        assert type(result[i][6]) == datetime


def test_get_table_data_extracts_list_table_data_transaction():
    result = get_table_data('transaction', None)
    for i in range(1, len(result)):
        assert type(result[i][0]) == int
        assert type(result[i][1]) == str
        assert type(result[i][2]) == int or result[i][2] == None
        assert type(result[i][3]) == int or result[i][3] == None
        assert type(result[i][4]) == datetime
        assert type(result[i][5]) == datetime


# need to change this test later for mocking
def test_get_table_data_function_returns_rows_newer_than_last_updated_date():
    dt = datetime(2023, 1, 1, 10, 30, 41, 962000)
    table_data = get_table_data('purchase_order', dt)
    for row in table_data[1:]:
        for i in range(len(row)):
            if isinstance(row[i], datetime):
                row[i] > dt
