import sys
import os
import boto3
import json
import logging
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

if os.path.exists('./src/'):
    sys.path.append('./src/lambdas/ingestion')
from utils.connection import schema
from utils.connection import con

if os.path.exists('./src/'):
    sys.path.append('./src')
from utils.environ import is_dev_environ
from utils.environ import is_production_environ


def get_table_names():
    """
    Queries database for table names

    Args:
        No args

    Returns:
        List of table names

    Raises:
        Error: Raises an exception.
    """
    try:
        table_names = con.run(
            """SELECT table_name FROM information_schema.tables
            WHERE table_schema = :schema""",
            schema=schema
        )
        return [item[0] for item in table_names]

    except Exception as e:
        logging.error(e, 'Table names were not extracted')


def get_headers():
    """
    Extracts column names for current table

    Args:
        No args

    Returns:
        List of column names for the
        last queried table

    Raises:
        Error: Raises an exception.
    """
    try:
        return [c['name'] for c in con.columns]

    except Exception as e:
        logging.error(e, 'Column names were not extracted')


def get_table_data(table_name, timestamp):
    """
    Extracts list of table data

    Args:
        param1: table name
        param2: the last update timestamp retrieved using the
        retrieve_last_updated() function

    Returns:
        list of nested lists with the first row contains
        the column names and each subsequent row is a
        row of data from the table

    Raises:
        Error: Raises an exception.
    """

    try:
        if table_name in [
            'address',
            'counterparty',
            'currency',
            'department',
            'design',
            'payment_type',
            'payment',
            'purchase_order',
            'sales_order',
            'staff',
            'transaction'
        ]:
            if timestamp is None:
                table_data = con.run(f'SELECT * FROM {table_name}')
            else:
                table_data = con.run(f"""
                SELECT * FROM {table_name}
                WHERE last_updated >
                TO_TIMESTAMP(:update_ts, :date_format)
                """,
                                     update_ts=(
                                         timestamp + timedelta(seconds=1))
                                     .strftime(
                                         '%d-%m-%Y %H:%M:%S.%f'),
                                     date_format="dd-mm-yyyy hh24:mi:ss"
                                     )

            # if len(table_data) > 0:
            #     logging.info(f'{table_name} has {len(table_data)} rows')
            table_data.insert(0, get_headers())
            return table_data

    except Exception as e:
        logging.error(e, f'Table {table_name} was not extracted')


def get_ingested_bucket_name():
    """
    Returns name of ingested S3 bucket

    Args:
        no args

    Returns:
        The name of the ingested bucket name

    Raises:
        Error: Raises an exception.
    """
    try:
        s3 = boto3.client('s3')
        list_buckets = s3.list_buckets()
        bucket_prefix = 's3-de-ingestion-query-queens'
        bucket_name = ''
        for bucket in list_buckets['Buckets']:
            if bucket['Name'].startswith(bucket_prefix):
                bucket_name = bucket['Name']
        return bucket_name

    except Exception as e:
        logging.error(e, 'Bucket name was not extracted')


def upload_to_s3(table_dict, date_key, filepath):
    """
    Uploads files made during data_ingestion() function to the
    necessary s3 bucket with the current date
    and times as the file path.

    Can also write the file when in local setting

    Args:
        param1:table in dictionary form including table name,
        table headers and table data
        param2: date key string for naming file prefixes


    Returns:
        No returns

    Raises:
        Error: Raises an exception.
    """
    try:
        key = f'{date_key}/{table_dict["table_name"]}.json'
        table_json = json.dumps(table_dict)

        if is_production_environ():
            s3 = boto3.client('s3')
            s3.put_object(
                Body=table_json,
                Bucket=get_ingested_bucket_name(),
                Key=key
            )

        if is_dev_environ():
            with open(f'{filepath}/{key}', 'w') as f:
                f.write(table_json)

    except Exception as e:
        logging.error(e, 'JSON files were not uploaded')


def make_table_dict(table_name, table_data):
    """
    Turns table data into dictionary form

    Args:
        param1: table name
        param2: table data received from get_table_data function

    Returns:
        dictionary form of table data
        including table headers and data

    Raises:
        Error: Raises an exception.
    """
    try:
        for row in table_data:
            for i in range(len(row)):
                if isinstance(row[i], datetime):
                    row[i] = row[i].strftime('%Y-%m-%dT%H:%M:%S.%f')
                elif isinstance(row[i], Decimal):
                    row[i] = float(row[i])

        data = table_data[1:] if len(table_data) > 1 else []

        return {
            'table_name': table_name,
            'headers': table_data[0],
            'data': data
        }

    except Exception as e:
        logging.error(e, 'Table was not made in dict form')
