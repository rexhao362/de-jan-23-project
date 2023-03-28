from src.lambdas.load.utils.data_table import DataTable

db_schema = [
    DataTable("dim_staff", {
        "staff_id": "int",
        "first_name": "varchar",
        "last_name": "varchar",
        "department_name": "varchar",
        "location": "varchar",
        "email_address": "varchar"}
    ),
    DataTable("dim_location", {
        "location_id": "int",
        "address_line_1": "varchar",
        "address_line_2": "varchar",
        "district": "varchar",
        "city": "varchar",
        "postal_code": "varchar",
        "country": "varchar",
        "phone": "varchar"}, dont_import = True
    ),
    DataTable("dim_currency", {
        "currency_id": "INT",
        "currency_code": "VARCHAR",
        "currency_name": "VARCHAR"}
    ),
]