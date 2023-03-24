
from datetime import datetime
import boto3
import pg8000.native
import os
import json



# DB connection
con = pg8000.native.Connection(
    user=TOTESYS_DB_USER,
    host=TOTESYS_DB_HOST,
    database=TOTESYS_DB_DATABASE,
    port=TOTESYS_DB_PORT,
    password=TOTESYS_DB_PASSWORD
)

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
        'SELECT table_name FROM information_schema.tables WHERE table_schema = :schema', schema=TOTESYS_DB_DATABASE_SCHEMA)
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
    if table_name in ['address',
                      'counterparty',
                      'currency',
                      'department',
                      'design',
                      'payment_type',
                      'payment',
                      'purchase_order',
                      'sales_order',
                      'staff',
                      'transaction']:
        table_data = con.run(f'SELECT * FROM {table_name} WHERE last_updated > TO_TIMESTAMP(:update_ts, :date_format)',
                             update_ts=timestamp.strftime('%d-%m-%Y %H:%M:%S'), date_format="dd-mm-yyyy hh24:mi:ss")
        table_data.insert(0, get_headers())
        return table_data


def get_ingested_bucket_name():
    """
    Retrieves ingested bucket name 

    Args:
        no args

    Returns:
        The name of the ingested bucket name

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
    dt_now = datetime.now()
    current_day = dt_now.strftime('%d-%m-%Y')
    current_time = dt_now.strftime('%H:%M:%S')
    for file_name in os.listdir('./src/lambdas/ingestion/data/table_data'):
        with open(f'./src/lambdas/ingestion/data/table_data/{file_name}', 'rb') as f:
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
        timestamp = json_res['last_updated']
        return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    

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
        if table in ['address',
                      'counterparty',
                      'currency',
                      'department',
                      'design',
                      'payment_type',
                      'payment',
                      'purchase_order',
                      'sales_order',
                      'staff',
                      'transaction']:
            most_recent = con.run(
                f'SELECT last_updated FROM {table} GROUP BY last_updated ORDER BY last_updated LIMIT 1')[0][0]
            if most_recent >= date_to_store:
                date_to_store = most_recent

    # writes files to local folder
    with open('./src/lambdas/ingestion/data/date/last_updated.json', 'w') as f:
        date_string = date_to_store.strftime('%Y-%m-%dT%H:%M:%S.%f')
        date_object = {'last_updated': date_string}
        f.write(json.dumps(date_object))

    #  uploads files to S3 bucket
    with open('./src/lambdas/ingestion/data/date/last_updated.json', 'rb') as f:
        s3 = boto3.client('s3')
        s3.put_object(Body=f, Bucket=get_ingested_bucket_name(),
                      Key='date/last_updated.json')