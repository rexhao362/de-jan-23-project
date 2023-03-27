from os import environ

default_host = 'localhost'
default_port = 5432

warehouse_db_user = environ.get('WAREHOUSE_DB_USER')
warehouse_db_password = environ.get('WAREHOUSE_DB_PASSWORD')
warehouse_db_host = environ.get('WAREHOUSE_DB_HOST', default_host)
warehouse_db_port = environ.get('WAREHOUSE_DB_PORT', default_port)
warehouse_db_database = environ.get('WAREHOUSE_DB_DATABASE')
warehouse_db_schema = environ.get('WAREHOUSE_DB_DATABASE_SCHEMA')
