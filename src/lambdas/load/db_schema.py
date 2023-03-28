from src.lambdas.load.utils.data_table import DataTable

db_schema = [
    DataTable("dim_staff", {
        "staff_id": "INT",
        "first_name": "VARCHAR",
        "last_name": "VARCHAR",
        "department_name": "VARCHAR",
        "location": "VARCHAR",
        "email_address": "VARCHAR"}
        ),
    DataTable("dim_location", {
        "location_id": "INT",
        "address_line_1": "VARCHAR",
        "address_line_2": "VARCHAR",
        "district": "VARCHAR",
        "city": "VARCHAR",
        "postal_code": "VARCHAR",
        "country": "VARCHAR",
        "phone": "VARCHAR"}, dont_import = True
        ),
    DataTable("dim_currency", {
        "currency_id": "INT",
        "currency_code": "VARCHAR",
        "currency_name": "VARCHAR"}
        ),
    DataTable("dim_design", {
        "design_id": "INT",
        "design_name": "VARCHAR",
        "file_location": "VARCHAR",
        "file_name": "VARCHAR"}
        )
]