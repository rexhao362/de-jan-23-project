from src.ingestion.utils.utils import get_table_data
from datetime import datetime
import pytest


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