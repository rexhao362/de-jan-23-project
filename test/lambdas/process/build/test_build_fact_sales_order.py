from src.lambdas.utils import load_file_from_local, process
from src.lambdas.build import build_fact_sales_order
from pandas import DataFrame
from numpy import equal, append
import re

sales_order_path = 'test/lambdas/process/json_files/sales_order_test_1.json'
sales_order_data = load_file_from_local(sales_order_path)
sales_order_dataframe = process(sales_order_data)

fact_sales_order = build_fact_sales_order(sales_order_dataframe)

def test_returns_dataframe():
    assert isinstance(fact_sales_order, DataFrame)

def test_returned_dataframe_has_expected_columns():
    cols = list(fact_sales_order.columns.values)

    expected = ['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']
    assert equal(cols, expected).all()

def test_returns_date_entries_in_YYYY_MM_SS_format():
    date_cols = ['created_date', 'last_updated_date', 'agreed_payment_date', 'agreed_delivery_date']
    for cols in date_cols:
        for date in fact_sales_order[cols]:
            match = bool(re.match(r'\d{4}-\d{2}-\d{2}', str(date)))
            assert match == True

def test_returns_time_entries_in_HH_MM_SS_format():
    time_cols = ['created_time', 'last_updated_time']
    for cols in time_cols:
        for time in fact_sales_order[cols]:
            match = bool(re.match(r'\d{2}:\d{2}:\d{2}\.\d{6}', str(time)))
            assert match == True

