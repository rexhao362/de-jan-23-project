from src.lambdas.load.utils.data_table import DataTable

db_schema = [
    DataTable("dim_staff", {
        "staff_id": "INT",
        "first_name": "VARCHAR",
        "last_name": "VARCHAR",
        "department_name": "VARCHAR",
        "location": "VARCHAR",
        "email_address": "VARCHAR" }, dont_import = False
    ),
    DataTable("dim_location", {
        "location_id": "INT",
        "address_line_1": "VARCHAR",
        "address_line_2": "VARCHAR",
        "district": "VARCHAR",
        "city": "VARCHAR",
        "postal_code": "VARCHAR",
        "country": "VARCHAR",
        "phone": "VARCHAR" }, dont_import = True
    ),
    DataTable("dim_currency", {
        "currency_id": "INT",
        "currency_code": "VARCHAR",
        "currency_name": "VARCHAR" }, dont_import = False
    ),
    DataTable("dim_design", {
        "design_id": "INT",
        "design_name": "VARCHAR",
        "file_location": "VARCHAR",
        "file_name": "VARCHAR" }, dont_import = False
    ),
    DataTable("dim_date", {
        "date_id": "DATE",
        "year": "INT",
        "month": "INT",
        "day": "INT",
        "day_of_week": "INT",
        "day_name": "VARCHAR",
        "month_name": "VARCHAR",
        "quarter": "INT" }, dont_import = True
    ),
    DataTable("dim_counterparty", {
        "counterparty_id": "INT",
        "counterparty_legal_name": "VARCHAR",
        "counterparty_legal_address_line_1": "VARCHAR",
        "counterparty_legal_address_line_2": "VARCHAR",
        "counterparty_legal_district": "VARCHAR",
        "counterparty_legal_city": "VARCHAR",
        "counterparty_legal_postal_code": "VARCHAR",
        "counterparty_legal_country": "VARCHAR",
        "counterparty_legal_phone_number": "VARCHAR" }, dont_import = False
    ),
    DataTable("fact_sales_order", {
        "sales_order_id": "INT",
        "created_date": "DATE",
        "created_time": "TIME",
        "last_updated_date": "DATE",
        "last_updated_time": "TIME",
        "sales_staff_id": "INT",
        "counterparty_id": "INT",
        "units_sold": "INT",
        "unit_price": "NUMERIC(10, 2)",
        "currency_id": "INT",
        "design_id": "INT",
        "agreed_payment_date": "DATE",
        "agreed_delivery_date": "DATE",
        "agreed_delivery_location_id": "INT" }, dont_import = True
    )
]