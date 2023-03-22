import os
import json
from decimal import Decimal
from ingestion_function.connection import con
from datetime import datetime
import boto3

# Create data dir
if not os.path.exists('./ingestion_function/data'):
    os.makedirs('./ingestion_function/data')


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


def get_ingested_bucket_name():
    s3 = boto3.client('s3')
    list_buckets = s3.list_buckets()
    bucket_prefix = 's3-de-ingestion-query-queens'
    bucket_name = ''
    for bucket in list_buckets['Buckets']:
        if bucket['Name'].startswith(bucket_prefix):
            bucket_name = bucket['Name']
    return bucket_name


def upload_to_s3():
    # terraform will create the bucket
    # list buckets
    #  choose bucket with right prefix
    # upload files
    s3 = boto3.client('s3')
    for file_name in os.listdir('./ingestion_function/data'):
        with open(f'./ingestion_function/data/{file_name}', 'rb') as f:
            s3.put_object(Body=f, Bucket=get_ingested_bucket_name(),
                          Key=f'ingested_data/{file_name}')


def retrieve_last_updated():
    s3 = boto3.client('s3')
    response = s3.get_object(
        Bucket=get_ingested_bucket_name(), Key='ingested_data/last_updated.json')
    json_res = json.loads(response['Body'].read())
    last_updated = json_res['last_updated']
    return datetime.strptime(last_updated, '%Y-%m-%dT%H:%M:%S.%f')


def store_last_updated():
    date_to_store = retrieve_last_updated()
    for table in get_table_names():
        last_updated = con.run(
            f'SELECT last_updated FROM {table} GROUP BY last_updated ORDER BY last_updated LIMIT 1')[0][0]
        if last_updated >= date_to_store:
            date_to_store = last_updated

    with open('./ingestion_function/data/last_updated.json', 'w') as f:
        date_string = date_to_store.strftime('%Y-%m-%dT%H:%M:%S.%f')
        date_object = {'last_updated': date_string}
        f.write(json.dumps(date_object))

    with open('./ingestion_function/data/last_updated.json', 'rb') as f:
        s3 = boto3.client('s3')
        s3.put_object(Body=f, Bucket=get_ingested_bucket_name(),
                      Key='ingested_data/last_updated.json')


def lambda_handler():
    data_ingestion()
    upload_to_s3()
    store_last_updated()

