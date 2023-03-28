from os import environ

default_host = 'localhost'
default_port = 5432

totesys_db_user = environ.get('TOTESYS_DB_USER')
totesys_db_password = environ.get('TOTESYS_DB_PASSWORD')
totesys_db_host = environ.get('TOTESYS_DB_HOST', default_host)
totesys_db_port = environ.get('TOTESYS_DB_PORT', default_port)
totesys_db_database = environ.get('TOTESYS_DB_DATABASE')
totesys_db_schema = environ.get('TOTESYS_DB_DATABASE_SCHEMA')
