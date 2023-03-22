import os
import json
from decimal import Decimal
from ingestion_function.connection import con
from datetime import datetime
import boto3

# Create data dir
if not os.path.exists('./ingestion-function/data'):
    os.makedirs('./ingestion-function/data')


# Queries database for table names
def get_table_names():
    table_names = con.run(
        'SELECT table_name, table_schema FROM information_schema.tables')
    return [item[0] for item in table_names if item[1] == 'public']


# Extracts column names for current table
def get_headers():
    return [c['name'] for c in con.columns]


# Extracts list of table data
def get_table_data(table_name):
    table_data = con.run(f'SELECT * FROM {table_name}')
    table_data.insert(0, get_headers())
    return table_data


# Writes dictionary of table to file
def data_ingestion():
    for table_name in get_table_names():
        table_data = get_table_data(table_name)
        for row in table_data:
            for i in range(len(row)):
                if isinstance(row[i], datetime):
                    row[i] = row[i].strftime('%Y-%m-%dT%H:%M:%S.%f')
                elif isinstance(row[i], Decimal):
                    row[i] = float(row[i])

        dict = {
            'table_name': table_name,
            'headers': table_data[0],
            'data': table_data[1:]
        }

        with open(f'./ingestion_function/data/{table_name}.json', 'w') as f:
            f.write(json.dumps(dict))


def upload_to_s3():
    # terraform will create the bucket
    # list buckets
    # choose bucket with right prefix
    # upload files
    s3 = boto3.client('s3')
    list_buckets = s3.list_buckets()
    bucket_prefix = 's3-de-ingestion-query-queens'
    bucket_name = ''
    for bucket in list_buckets['Buckets']:
        if bucket['Name'].startswith(bucket_prefix):
            bucket_name = bucket['Name']
    for file_name in os.listdir('./ingestion_function/data'):
        with open(f'./ingestion_function/data/{file_name}', 'rb') as f:
            s3.put_object(Body=f, Bucket=bucket_name, Key=f'data/{file_name}')


def store_last_updated():
    pass
