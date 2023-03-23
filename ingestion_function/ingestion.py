import os
import json
from decimal import Decimal
from ingestion_function.connection import con
from datetime import datetime
import boto3

# Create data directory
if not os.path.exists('./ingestion_function/data'):
    os.makedirs('./ingestion_function/data')


def get_table_names():
    """
    Queries database for table names

    Args:
        No args

    Returns:
        List of table names

    Raises:
        KeyError: Raises an exception.
    """
    table_names = con.run(
        'SELECT table_name FROM information_schema.tables WHERE table_schema = :schema', schema='public')
    return [item[0] for item in table_names]


def get_headers():
    """
    Extracts column names for current table

    Args:
        No args

    Returns:
        List of column names

    Raises:
        KeyError: Raises an exception.
    """
    return [c['name'] for c in con.columns]


def get_table_data(table_name, timestamp):
    """
    Extracts list of table data

    Args:
        param1: table name
        param2: the last update timestamp retrieved using the retrieve_last_updated() function

    Returns:
        list of nested lists with the first row contains the column names and each subsequent row is a row of data from the table

    Raises:
        Error: Raises an exception.
    """
    table_data = con.run(f'SELECT * FROM {table_name} WHERE last_updated > TO_TIMESTAMP(:topic, :topic_two)',
                         topic=timestamp.strftime('%d-%m-%Y %H:%M:%S'), topic_two="dd-mm-yyyy hh24:mi:ss")
    table_data.insert(0, get_headers())
    return table_data


def data_ingestion(timestamp):
    """
    Uses the get_table_names() and the get_table_data() functions to retrieve data for each table. Formats datetime objects into string and Decimal objects into float. Turns each table into a dictionary and saves them to json files.

    Args:
        param1: the last update timestamp retrieved using the retrieve_last_updated() function to pass to the get_table_data()

    Returns:
        no return

    Raises:
        Error: Raises an exception.
    """
    for table_name in get_table_names():
        table_data = get_table_data(table_name, timestamp)
        for row in table_data:
            for i in range(len(row)):
                if isinstance(row[i], datetime):
                    row[i] = row[i].strftime('%Y-%m-%dT%H:%M:%S.%f')
                elif isinstance(row[i], Decimal):
                    row[i] = float(row[i])
        data = []
        if len(table_data) > 1:
            data = table_data[1:]

        dict = {
            'table_name': table_name,
            'headers': table_data[0],
            'data': data
        }

        with open(f'./ingestion_function/data/{table_name}.json', 'w') as f:
            f.write(json.dumps(dict))


def get_ingested_bucket_name():
    """
    Retrieves ingested bucket name 

    Args:
        no args

    Returns:
        The name of the igested bucket name

    Raises:
        Error: Raises an exception.
    """
    s3 = boto3.client('s3')
    list_buckets = s3.list_buckets()
    bucket_prefix = 's3-de-ingestion-query-queens'
    bucket_name = ''
    for bucket in list_buckets['Buckets']:
        if bucket['Name'].startswith(bucket_prefix):
            bucket_name = bucket['Name']
    return bucket_name


def upload_to_s3():
    """
    Uploads files made by data_ingestion() function to the necessary s3 bucket with the current date and times as the file path

    Args:
        No args

    Returns:
        No returns

    Raises:
        Error: Raises an exception.
    """
    s3 = boto3.client('s3')
    for file_name in os.listdir('./ingestion_function/data'):
        with open(f'./ingestion_function/data/{file_name}', 'rb') as f:
            dt_now = datetime.now()
            current_day = dt_now.strftime('%d-%m-%Y')
            current_time = dt_now.strftime('%H:%M:%S')
            s3.put_object(Body=f, Bucket=get_ingested_bucket_name(),
                          Key=f'{current_day}/{current_time}/{file_name}')


def retrieve_last_updated():
    """
    Retrieves the last updated date from the s3 bucket. If the file does not exist then uses a default date.

    Args:
        No args

    Returns:
        Returns the last updated date

    Raises:
        Error: Raises an exception.
    """
    s3 = boto3.client('s3')
    list = s3.list_objects_v2(Bucket=get_ingested_bucket_name())
    if 'Contents' not in list:
        return datetime(2022, 10, 5, 16, 30, 42, 962000)
    else:
        response = s3.get_object(
            Bucket=get_ingested_bucket_name(), Key='date/last_updated.json')
        json_res = json.loads(response['Body'].read())
        last_updated = json_res['last_updated']
        return datetime.strptime(last_updated, '%Y-%m-%dT%H:%M:%S.%f')


def store_last_updated(timestamp):
    """
    Finds the last updated date. Writes it to a file and then sends it to the s3 bucket 

    Args:
        param1: the last update timestamp retrieved using the retrieve_last_updated() function to pass to the get_table_data()

    Returns:
        No returns

    Raises:
        Error: Raises an exception.
    """
    date_to_store = timestamp
    for table in get_table_names():
        most_recent = con.run(
            f'SELECT last_updated FROM {table} GROUP BY last_updated ORDER BY last_updated LIMIT 1')[0][0]
        if most_recent >= date_to_store:
            date_to_store = most_recent

    with open('./ingestion_function/data/last_updated.json', 'w') as f:
        date_string = date_to_store.strftime('%Y-%m-%dT%H:%M:%S.%f')
        date_object = {'last_updated': date_string}
        f.write(json.dumps(date_object))

    #  uploads files to S3 bucket
    with open('./ingestion_function/data/last_updated.json', 'rb') as f:
        s3 = boto3.client('s3')
        s3.put_object(Body=f, Bucket=get_ingested_bucket_name(),
                      Key='date/last_updated.json')


def lambda_handler():
    timestamp = retrieve_last_updated()
    data_ingestion(timestamp)
    upload_to_s3()
    store_last_updated(timestamp)
