import os
import boto3
import json
import logging
from datetime import datetime
from src.lambdas.ingestion.utils.utils import get_ingested_bucket_name
from src.lambdas.ingestion.utils.utils import get_table_names
from src.lambdas.ingestion.utils.environ import con
from src.lambdas.ingestion.utils.environ import is_production_environ
from src.utils.environ import is_dev_environ


def retrieve_last_updated():
    """
    Retrieves the last updated date from the s3 bucket.
    If the file does not exist then uses a default date.

    Args:
        No args

    Returns:
        Returns the last updated date
        as a datetime object

    Raises:
        Error: Raises an exception.
    """
    try:
        if is_production_environ():
            s3 = boto3.client('s3')
            bucket_name = get_ingested_bucket_name()
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix='date/date'
            )
            if 'Contents' not in response:
                return None
            else:
                res = s3.get_object(
                    Bucket=bucket_name,
                    Key='date/date.json'
                )
            json_res = json.loads(res['Body'].read())
            timestamp = json_res['last_updated']
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')

        else:
            path = './local/aws/s3/ingested/date/date.json'
            if not os.path.exists(path):
                return None
            else:
                with open(path, 'rb') as f:
                    timestamp = json.loads(f.read())['last_updated']
                    return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')

    except Exception as e:
        logging.error(e, 'Timestamp was not retrieved')


def select_last_updated(timestamp):
    """
    Finds the last updated date.
    by querying the database

    Args:
        param1: the last update timestamp retrieved using
        the retrieve_last_updated() function to pass
        to the get_table_data()

    Returns:
        String for use in file path

    Raises:
        Error: Raises an exception.
    """

    try:
        for table_name in get_table_names():
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
                update = con.run(
                    f"""SELECT last_updated FROM {table_name}
                    GROUP BY last_updated ORDER BY
                    last_updated DESC LIMIT 1""")[0][0]
                if timestamp is None or update > timestamp:
                    timestamp = update

        return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')

    except Exception as e:
        logging.error(e, 'Timestamp was not selected')


def create_date_string():
    """
    Uses the current date and time
    to be used for other functions

    Args: No parameters

    Returns: a string format of the current date
    and time

    Raises:
        Error: Raises an exception.
    """
    try:
        return (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S.%f')

    except Exception as e:
        logging.error(e, 'Date string was not created')


def create_date_key(date_string):
    """
    Uses the current date and time
    to create a file naming key for
    table data json files

    Args:
        param1: a date string formed from the
        current date and time

    Returns: a string format of the date key
    for file name prefixes

    Raises:
        Error: Raises an exception.
    """
    try:
        return f'{date_string[:10]}/{date_string[11:19]}'

    except Exception as e:
        logging.error(e, 'Date key file name was not created')


def store_last_updated(date_string, date_key, path=None):
    """
    Stores the last updated date time made by select_last_updated
    function, and the date string made by the create_date_string
    function in s3 buckets or locally

    Args:
        param1: a date string formed from the
        querying the database
        param2: a date string from the current
        date and time

    Returns: no returns

    Raises:
        Error: Raises an exception.
    """
    try:
        if is_production_environ():
            s3 = boto3.client('s3')
            bucket_name = get_ingested_bucket_name()
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix='date/'
            )
            if 'Contents' in response:
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource=f'{bucket_name}/date/date.json',
                    Key='date/temp_date.json'
                )

        # writes files to local folder
        last_update = {'last_updated': date_string}
        update_json = json.dumps(last_update)
        date_object = {'last_updated': date_key}
        date_json = json.dumps(date_object)

        if is_dev_environ():
            os.makedirs(f'{path}/date', exist_ok=True)
            with open(f'{path}/date/date.json', 'w') as f:
                f.write(update_json)
            with open(f'{path}/date/last_updated.json', 'w') as f:
                f.write(date_json)

        # uploads files to S3 bucket
        if is_production_environ():
            s3.put_object(
                Body=update_json,
                Bucket=bucket_name,
                Key='date/date.json'
            )
            s3.put_object(
                Body=update_json,
                Bucket=bucket_name,
                Key='date/last_updated.json'
            )

    except Exception as e:
        logging.error(e, 'Timestamp was not stored')
