from src.lambdas.load.utils.data_table import DataTable

db_schema = [
    DataTable("dim_currency", {
        "currency_id": "INT",
        "currency_code": "VARCHAR",
        "currency_name": "VARCHAR"}, dont_import=False
    ),
    DataTable("dim_staff", {
        "staff_id": "int",
        "first_name": "varchar",
        "last_name": "varchar",
        "department_name": "varchar",
        "location": "varchar",
        "email_address": "varchar"}, dont_import=False
    )
]