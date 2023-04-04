from sys import exit
from os import environ

import pyarrow.parquet as pq
import pandas
from pg8000.native import Connection

from load.format_dim_currency_data import format_dim_currency_data
from load.populate_dim_currency import populate_dim_currency

# setup credentials fron env variables
# based on https://github.com/tlocke/pg8000#use-environment-variables-as-connection-defaults
user = environ.get('WAREHOUSE_DB_USER')
password = environ.get('WAREHOUSE_DB_PASSWORD')
host = environ.get('WAREHOUSE_DB_HOST', 'localhost')
port = environ.get('WAREHOUSE_DB_PORT', 5432)
database = environ.get('WAREHOUSE_DB_DATABASE')
schema = environ.get('WAREHOUSE_DB_DATABASE_SCHEMA')

#input_file_name = ""
#input_file_name = "test/lambdas/load/input_files/invalid.parquet"
#input_file_name = "./README.md"

input_file_name = "test/lambdas/load/input_files/dim_currency.parquet"
#input_file_name = "s3://parquet-101/dim_currency.parquet"
input_data = []

try:
    table = pq.read_table(input_file_name)
    data_frame = table.to_pandas()
    input_data = format_dim_currency_data(data_frame)

    
except Exception as e:
    exit( str(e) )

with Connection(user, password=password, host=host, port=port, database=database) as connection:
    populate_dim_currency(connection, input_data, schema)